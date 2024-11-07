import { sleep } from "bun";
import { backgroundDBProcess } from "./BackgroundDBProcess";
import { getClickhouseClient } from "./clickhouse/utils";

export class BaseIndexer {
  checkpointCursor: string = "";
  lastBufferFlushCallCursor: string = "";
  name: string | null = null;

  constructor(name: string) {
    this.name = name;

    this.bufferFlushStarted = this.bufferFlushStarted.bind(this);
    this.bufferFlushCompleted = this.bufferFlushCompleted.bind(this);
  }

  async start() {
    console.log(`Starting ${this.name} indexer`);
    console.log("sleeping for 10s");
    await sleep(10000);
   
    await getClickhouseClient();

    backgroundDBProcess.start(
      this.bufferFlushStarted,
      this.bufferFlushCompleted
    );
    this.traverseAndStore();
  }

  async traverseAndStore() {
    console.log(`Traversing and storing for ${this.name} indexer`);
  }

  async getCheckpoint() {
    const client = await getClickhouseClient();

    const tableExists = await client.query({
      query: `
        SELECT count(*) as count
        FROM system.tables
        WHERE database = currentDatabase()
        AND name = 'ardis_checkpoints'
      `,
      format: "JSONEachRow",
    });

    const jsonRes = (await tableExists.json()) as any;
    const count = jsonRes[0]?.count || 0;
    if (parseInt(count) === 0) {
      return {
        cursor: "",
      };
    }

    const query = `
      SELECT cursor
      FROM ardis_checkpoints
      WHERE ardis_name = '${this.name}'
      ORDER BY timestamp DESC
      LIMIT 1
    `;

    const result = await client.query({ query, format: "JSONEachRow" });
    const rows = await result.json();
 
    if (rows && rows.length > 0) {
      return rows[0];
    } else {
      return {
        cursor: "",
      };
    }
  }

  async bufferFlushStarted() {
    this.lastBufferFlushCallCursor = this.checkpointCursor;
  }

  async bufferFlushCompleted(tables: number, rows: number) {
    if (this.lastBufferFlushCallCursor === "") {
      return;
    }

    const client = await getClickhouseClient();
    console.log(
      `Cursor ${this.lastBufferFlushCallCursor}: Flushed ${rows} rows across ${tables} tables to Clickhouse`
    );

    // Create checkpoint table if it doesn't exist
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
      await client.query({ query: createTableQuery });
    }

    // Update checkpoint
    await backgroundDBProcess.bufferInsert(
      "ardis_checkpoints",
      [this.name, this.lastBufferFlushCallCursor, Math.floor(Date.now() / 1000)],
      ["ardis_name", "cursor", "timestamp"]
    );
  }
}
