import { BaseIndexer } from "./modules/BaseIndexer";
import { getClickhouseClient } from "./modules/clickhouse/utils";
import { sleep } from "bun";
import { batchInsertIntoClickhouseTable } from "./modules/clickhouse/batch-insert";
import { getWalletBalance } from "./modules/getWalletBalance";

async function ensureArBalanceByAddressArconnectTableExists(tableName: string) {
  const client = await getClickhouseClient();
  const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        owner_address String CODEC(ZSTD),
        balance_ar Float64 CODEC(Delta, ZSTD),
        balance_winston UInt64 CODEC(Delta, ZSTD),
        timestamp DateTime DEFAULT now()
      ) ENGINE = ReplacingMergeTree()
      PRIMARY KEY (owner_address)
      ORDER BY (owner_address, timestamp);
    `;

  await client.exec({
    query: createTableQuery,
  });
}

async function checkArFromArconnectTableExists() {
  const client = await getClickhouseClient();
  const arFromArconnectTableExists = await client.query({
    query: `
          SELECT DISTINCT owner_address
          FROM ardis_ar_from_arconnect
        `,
  });

  const jsonRes = (await arFromArconnectTableExists.json()) as any;
  const count = jsonRes?.rows || 0;
  console.log({ count });
  return parseInt(count) > 0;
}

const columns = ["owner_address", "balance_ar", "balance_winston", "timestamp"];

class Indexer extends BaseIndexer {
  tableName: string = "ardis_ar_balance_by_address_arconnect";
  constructor(name: string) {
    super(name);
  }

  async traverseAndStore() {
    await ensureArBalanceByAddressArconnectTableExists(this.tableName);
    const arFromArconnectTableExists = await checkArFromArconnectTableExists();
    if (!arFromArconnectTableExists) {
      console.log("ArFromArconnectTableExists is not found");
      return;
    }

    const client = await getClickhouseClient();
    const res = await client.query({
      query: `
        SELECT DISTINCT owner_address FROM ardis_ar_from_arconnect ORDER BY owner_address;
      `,
    });

    const jsonRes = (await res.json()) as any;

    const count = jsonRes.rows;
    let completed = 0;
    const data = jsonRes.data as { owner_address: string }[];

    console.log("addresses count to catch up: ", count);

    for (const row of data) {
      try {
        const currentBalances = await getWalletBalance(row.owner_address);

        const rowToInsert = [
          row.owner_address,
          currentBalances.arBalance,
          currentBalances.winBalance,
          Math.floor(Date.now() / 1000),
        ];

        await batchInsertIntoClickhouseTable(
          this.tableName,
          [rowToInsert],
          columns
        );

        console.log(
          `AR Balance of wallet ${row.owner_address}: ${currentBalances.arBalance}`
        );
        completed++;
        console.log(`Completed: ${completed}/${count}`);

        // Add random sleep between 1-3 seconds
        const sleepDuration = Math.floor(Math.random() * 2000) + 1000; // Random number between 1000-3000 ms
        console.log(`Sleeping for ${sleepDuration} ms...`);
        await sleep(sleepDuration);
      } catch (error) {
        console.error(
          `Error fetching balance of wallet ${row.owner_address}:`,
          error
        );
      }
    }

    console.log("Done");
  }
}

const indexer = new Indexer("ar_balance_by_address_arconnect");

indexer.start();
