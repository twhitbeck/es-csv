import { Readable } from "node:stream";
import { createServer } from "node:http";

const authorizationHeader = Buffer.from(
  `${process.env.ES_USERNAME}:${process.env.ES_PASSWORD}`,
).toString("base64");

function esFetch(path, init = {}) {
  return fetch(process.env.ES_CLUSTER_URL + path, {
    ...init,
    headers: {
      ...init.headers,
      Authorization: `Basic ${authorizationHeader}`,
    },
  }).then((response) => {
    if (!response.ok) {
      throw new Error(
        `Bad response: ${response.status} ${response.statusText}`,
      );
    }

    return response;
  });
}

const json = (response) => response.json();

const server = createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/csv",
    "Content-Disposition": 'attachment; filename="orders.csv"',
  });

  const pageSize = 500;

  const esStream = new Readable({
    construct(callback) {
      esFetch(`/orders/_pit?${new URLSearchParams({ keep_alive: "1m" })}`, {
        method: "POST",
      })
        .then(json)
        .then(({ id }) => {
          this.pitId = id;

          this.push("order_number,status,priority,stord_accepted\n");

          callback();
        })
        .catch((error) => {
          callback(error);
        });
    },

    read(size) {
      console.time("fetch page");

      esFetch("/_search", {
        headers: {
          "Content-Type": "application/json",
        },
        method: "POST",
        body: JSON.stringify({
          pit: {
            id: this.pitId,
            keep_alive: "1m",
          },
          size: pageSize,
          _source: false,
          fields: ["order_number", "status", "priority", "inserted_at"],
          query: {
            bool: {
              must: [
                {
                  term: {
                    type: {
                      value: "sales",
                    },
                  },
                },
                {
                  term: {
                    network_id: {
                      value: process.env.NETWORK_ID,
                    },
                  },
                },
              ],
            },
          },
          sort: [
            {
              inserted_at: {
                order: "desc",
              },
            },
            {
              "order_number.raw": {
                order: "asc",
              },
            },
          ],
          ...(this.searchAfter && { search_after: this.searchAfter }),
        }),
      })
        .then(json)
        .then(({ pit_id: updatedPitId, hits: { hits } }) => {
          console.timeEnd("fetch page");

          if (this.destroyed) {
            console.log("destroyed, ignoring results");
            return;
          }

          this.pitId = updatedPitId;
          this.searchAfter = hits.at(-1)?.sort;

          for (const hit of hits) {
            this.push(
              [
                hit.fields.order_number[0],
                hit.fields.status[0],
                hit.fields.priority[0],
                hit.fields.inserted_at[0],
              ]
                .map((value) => {
                  if (typeof value !== "string") {
                    return value;
                  }

                  if (/[,"]/.test(value)) {
                    return `"${value.replace(/"/g, '""')}"`;
                  }

                  return value;
                })
                .join(",") + "\n",
            );
          }

          if (hits.length < pageSize) {
            this.push(null);
          }
        })
        .catch((error) => {
          this.destroy(error);
        });
    },

    destroy(error, callback) {
      if (this.pitId) {
        esFetch("/_pit", {
          method: "DELETE",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            id: this.pitId,
          }),
        })
          .then(() => {
            callback(error);
          })
          .catch((deleteError) => {
            callback(deleteError);
          });
      } else {
        callback(error);
      }
    },
  });

  esStream.pipe(res);

  res.on("close", () => {
    esStream.destroy();
  });
});

server.listen(3000);
