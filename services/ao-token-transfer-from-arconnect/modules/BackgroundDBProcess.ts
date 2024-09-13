import { sleep } from "bun";
import { batchInsertIntoClickhouseTable } from "./clickhouse/batch-insert";

interface Buffer {
  [tableName: string]: {
    rows: any[][];
    columns: string[];
  };
}

class BackgroundDBProcess {
  private buffer: Buffer = {};
  private isRunning: boolean = false;
  private preFlushCallback: () => Promise<void> = async () => {};
  private postFlushCallback: (tables: number, rows: number) => Promise<void> = async (tables, rows) => {};

  constructor(private flushIntervalMs: number = 10000) {}

  start(preFlushCallback: () => Promise<void>, postFlushCallback: (tables: number, rows: number) => Promise<void>) {
    this.preFlushCallback = preFlushCallback;
    this.postFlushCallback = postFlushCallback;

    if (this.isRunning) return;
    this.isRunning = true;
    this.run();
  }

  stop() {
    this.isRunning = false;
  }

  async bufferInsert(tableName: string, row: any[], columns: string[]) {
    if (!this.buffer[tableName]) {
      this.buffer[tableName] = {
        rows: [],
        columns: [],
      };
    }
    this.buffer[tableName].rows.push(row);
    this.buffer[tableName].columns = columns;

    // Throttle if buffer is getting too large
    while (
      this.buffer[tableName] &&
      this.buffer[tableName].rows.length > 1000000
    ) {
      await sleep(5000);
    }
  }

  private async run() {
    while (this.isRunning) {
      await sleep(this.flushIntervalMs);
    
      await this.flushBuffer();
    }
  }

  private async flushBuffer() {
    await this.preFlushCallback();

    const tempBuffer = { ...this.buffer };
    this.buffer = {};

    const entries = Object.entries(tempBuffer);
    for (const [tableName, { rows, columns }] of entries) {
      await batchInsertIntoClickhouseTable(tableName, rows, columns);
    }

    await this.postFlushCallback(entries.length, entries.reduce((acc, [_, { rows }]) => acc + rows.length, 0));
  }
}
// Singleton instance
const backgroundDBProcess = new BackgroundDBProcess();

export { backgroundDBProcess };
