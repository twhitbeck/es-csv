import { Readable, Transform } from "node:stream";

const pageSize = 500;
const limit = 10_000;

Readable.from(
  (async function* () {
    let scrollId,
      page = 1,
      totalHits = 0,
      done = false;

    while (!done) {
      page++;
      console.time("fetching page");

      const {
        _scroll_id,
        hits: { total, hits },
      } = await (scrollId
        ? esFetch("/_search/scroll", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              scroll: "1m",
              scroll_id: scrollId,
            }),
          }).then((response) => response.json())
        : esFetch(
            `/orders/_search?${new URLSearchParams({
              scroll: "1m",
            })}`,
            {
              method: "POST",
              headers: {
                "Content-Type": "application/json",
              },
              body: JSON.stringify({
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
              }),
            },
          ).then((response) => response.json()));

      console.timeEnd("fetching page");

      scrollId = _scroll_id;

      totalHits += hits.length;

      if (hits.length < pageSize) {
        done = true;
      }

      if (totalHits >= limit) {
        done = true;
      }

      for (const hit of hits) {
        yield hit;
      }
    }
  })(),
)
  .pipe(
    new Transform({
      writableObjectMode: true,
      transform(hit, encoding, callback) {
        callback(
          null,
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
      },
    }),
  )
  .on("data", () => {});

const authorizationHeader = Buffer.from(
  `${process.env.ES_USERNAME}:${process.env.ES_PASSWORD}`,
).toString("base64");

function esFetch(path: `/${string}`, init: RequestInit = {}) {
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
