import {
  ResultSet,
  type InsertParams,
  createClient,
  type InsertResult,
  type PingResult,
} from "@clickhouse/client";

export type ClickhouseConfig = {
  url: string;
  database: string;
};

const clickhouseConfig: ClickhouseConfig = {
  url: process.env.CLICKHOUSE_HOST as string,
  database: process.env.CLICKHOUSE_DATABASE as string,
};

export const getClient = () => {
  const client = createClient(clickhouseConfig);

  return client;
};
