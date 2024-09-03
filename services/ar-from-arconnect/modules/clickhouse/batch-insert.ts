import { sleep } from "bun";
import { getClickhouseClient } from "./utils";

interface Buffer {
  [tableName: string]: any[][];
}

let buffer: Buffer = {};
function isValidArrayString(str: string): boolean {
  return str.trim().startsWith("[") && str.trim().endsWith("]");
}
async function batchInsertIntoClickhouseTable(
  table: string,
  rows: any[][],
  columns: string[] = []
) {
  try {
    const formattedRows = rows
      .map(
        (row) =>
          `(${row
            .map((value) => {
              if (typeof value === "string" && isValidArrayString(value)) {
                return value;
              } else {
                return JSON.stringify(value, null, 0)
                  .replace(/^"/, "'")
                  .replace(/"$/, "'");
              }
            })
            .join(",")})`
      )
      .join(", ");

    const sql = `INSERT INTO ${table} (*) SETTINGS async_insert=1, wait_for_async_insert=1  VALUES ${formattedRows}`;
    const client = await getClickhouseClient();
    console.log("Inserting into Clickhouse: ", sql);

    await client.exec({ query: sql });
    console.log("Inserted into Clickhouse");
  } catch (e) {
    if (rows.length > 1) {
      const mid = Math.floor(rows.length / 2);
      console.log(
        `Error inserting into ${table}: ${e}. Retrying with smaller batches...`
      );

      await batchInsertIntoClickhouseTable(table, rows.slice(0, mid), columns);
      await batchInsertIntoClickhouseTable(table, rows.slice(mid), columns);
    } else {
      console.log(`Error inserting single row into ${table}: ${e}`);

      const formattedRows = rows
        .map(
          (row) => `(${row.map((value) => JSON.stringify(value)).join(",")})`
        )
        .join(", ");
      const sql = `INSERT INTO ${table} (*) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES ${formattedRows}`;

      console.log("Error inserting single row into Clickhouse: ", sql);
      throw e;
    }
  }
}

async function bufferInsert(tableName: string, row: any[]) {
  if (!buffer[tableName]) {
    buffer[tableName] = [];
  }

  buffer[tableName].push(row);

  // Throttle if buffer is getting too large
  while (buffer[tableName] && buffer[tableName].length > 1_000_000) {
    await sleep(1000);
  }
}

async function flushBuffer(
  executor: any,
  startedCb: () => void,
  doneCb: (taskCount: number, rowCount: number) => void
) {
  while (true) {
    startedCb();
    let tasks: [string, any[][]][] = [];

    tasks = Object.entries(buffer);
    buffer = {};

    const promises = tasks.map(([tableName, rows]) =>
      executor.submit(() => batchInsertIntoClickhouseTable(tableName, rows))
    );

    await Promise.all(promises);

    doneCb(
      tasks.length,
      tasks.reduce((sum, [, rows]) => sum + rows.length, 0)
    );
    await sleep(1000);
  }
}

export { bufferInsert, flushBuffer, batchInsertIntoClickhouseTable };
