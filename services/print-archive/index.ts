import { BaseIndexer } from "./modules/BaseIndexer";
import { arGql, GQLUrls } from "ar-gql";
import { getClickhouseClient } from "./modules/clickhouse/utils";
import type { GQLEdgeInterface } from "ar-gql/dist/faces";
import { backgroundDBProcess } from "./modules/BackgroundDBProcess";
import { sleep } from "bun";
import { batchInsertIntoClickhouseTable } from "./modules/clickhouse/batch-insert";
const argql = arGql({ endpointUrl: GQLUrls.goldsky });

function prepareClickHouseInsertRow(edge: GQLEdgeInterface) {
  const node = edge.node;
  const tagsArray = node.tags
    .filter((tag) => {
      const whitelistedTypes = ["string", "number", "boolean"];

      if (!whitelistedTypes.includes(typeof tag.value)) {
        return false;
      }

      if (typeof tag.value === "string") {
        try {
          JSON.parse(tag.value);

          return false;
        } catch (e) {
          return true;
        }
      }
      return true;
    })
    .map((tag) => `('${tag.name}', '${tag.value}')`)
    .join(",");

  return [
    node.id,
    node.owner.address,
    node.block.height,
    node.block.timestamp,
    `[${tagsArray}]`,
  ];
}

async function ensurePrintArchiveFromArconnectTableExists(tableName: string) {
  const client = await getClickhouseClient();
  const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id String CODEC(ZSTD),
        owner_address String CODEC(ZSTD),
        block_height UInt64 CODEC(Delta, ZSTD),
        block_timestamp DateTime CODEC(Delta, ZSTD),
        tags Array(Tuple(String, String)) CODEC(ZSTD)
      ) ENGINE = ReplacingMergeTree()
      PARTITION BY toYYYYMM(block_timestamp)
      PRIMARY KEY (id)
      ORDER BY (id, owner_address, block_timestamp);
    `;

  await client.exec({
    query: createTableQuery,
  });
}

async function ensureCheckpointTableExists(tableName: string) {
  const client = await getClickhouseClient();
  const checkpointTableExists = await client.query({
    query: `
          SELECT count(*) as count
          FROM system.tables
          WHERE database = currentDatabase()
          AND name = 'ardis_checkpoints'
        `,
  });

  const jsonRes = (await checkpointTableExists.json()) as any;
  const count = jsonRes[0]?.count || 0;

  if (parseInt(count) === 0) {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ardis_checkpoints (
        ardis_name String,
        cursor String,
        timestamp DateTime DEFAULT now()
      ) ENGINE = ReplacingMergeTree()
      ORDER BY (ardis_name)
    `;

    await client.exec({ query: createTableQuery });
  }
}

const QUERY = `
query($cursor: String) {
  transactions(
    # your query parameters
    tags: [
      { name: "App-Name", values: ["ArConnect"] },
      { name: "Type", values: ["Print-Archive"] }
    ]  
    # standard template below
    after: $cursor
    first: 100
    sort: HEIGHT_ASC
  ) {
    pageInfo {
      hasNextPage
    }
    edges {
      cursor
      node {
        id
        owner { address }
        tags { name value }
        block { height, timestamp }
      }
    }
  }
}
`;

const columns = [
  "id",
  "owner_address",
  "block_height",
  "block_timestamp",
  "tags",
];

class Indexer extends BaseIndexer {
  tableName: string = "ardis_print_archive_from_arconnect";
  constructor(name: string) {
    super(name);
  }

  async traverseAndStore() {
    await ensurePrintArchiveFromArconnectTableExists(this.tableName);
    await ensureCheckpointTableExists("ardis_checkpoints");

    const cursorInDBCheckpoint = (await this.getCheckpoint()) as {
      cursor: string;
    };

    let lastStoredCursor = cursorInDBCheckpoint.cursor;
    console.log("lastStoredCursor restored: ", lastStoredCursor);
    while (true) {
      let hasNextPage = true;

      while (hasNextPage) {
        const res = await argql.run(QUERY, { cursor: lastStoredCursor });
        const { edges, pageInfo } = res.data.transactions;
        console.log("pageInfo: ", pageInfo);
        if (edges && edges.length) {
          console.log(`Catching up to ${edges.length} transactions...`);

          for (const edge of edges) {
            const row = prepareClickHouseInsertRow(edge);
            await batchInsertIntoClickhouseTable(
              this.tableName,
              [row],
              columns
            );

            if (edge.cursor) {
              console.log("Saving checkpoint cursor: ", edge.cursor);
              await batchInsertIntoClickhouseTable(
                "ardis_checkpoints",
                [[this.name, edge.cursor, Math.floor(Date.now() / 1000)]],
                ["ardis_name", "cursor", "timestamp"]
              );
            }
          }
        } else {
          console.log("No more transactions found.");
        }
        console.log("Sleeping for 2 second...");
        await sleep(2000);
        const lastCursor = (await this.getCheckpoint()) as { cursor: string };
        lastStoredCursor = lastCursor.cursor;
        console.log("lastStoredCursor: ", lastStoredCursor);
        hasNextPage = pageInfo.hasNextPage;
      }

      console.log("No more transactions to fetch. Sleeping for 12 seconds...");
      await sleep(12000);
    }
  }
}

const indexer = new Indexer("print_archive_from_arconnect");

indexer.start();
