import { getClient } from "./getClient";
const RESERVED_KEYWORDS = new Set([
  "INDEX",
  "ENGINE",
  "TABLE",
  "DATABASE",
  "ORDER",
  "BY",
  "PRIMARY",
  "KEY",
  "UNIQUE",
  "PARTITION",
  "TTL",
  "SETTINGS",
  "FORMAT",
  "ALIAS",
  "TTL",
  "SAMPLE",
  "AS",
  "WHERE",
  "HAVING",
  "IN",
  "LIMIT",
  "UNION",
  "ALL",
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "WITH",
  "ALTER",
  "DROP",
  "RENAME",
  "OPTIMIZE",
]);

function escapeColumnName(columnName: string): string {
  return RESERVED_KEYWORDS.has(columnName.toUpperCase())
    ? `\`${columnName}\``
    : columnName;
}

const tableExistsCache = new Map<string, boolean>();

async function tableExists(tableName: string): Promise<boolean> {
  if (tableExistsCache.has(tableName)) {
    return tableExistsCache.get(tableName)!;
  }

  const query = `SHOW TABLES LIKE '${tableName}'`;

  const client = await getClickhouseClient();
  const result = await client.query({ query });
  const rows = (await result.json()).rows;
  const exists = rows ? rows > 0 : false;
  tableExistsCache.set(tableName, exists);
  
  return exists;
}

let clickhouseClient: ReturnType<typeof getClient> | null = null;

async function getClickhouseClient(
  retries = 10,
  delay = 1000
): Promise<ReturnType<typeof getClient>> {
  if (clickhouseClient) return clickhouseClient;

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      clickhouseClient = getClient();

      await clickhouseClient.ping();
      return clickhouseClient;
    } catch (e) {
      console.error(`Error connecting to Clickhouse: ${e}`);
      
      if (attempt < retries - 1) {
        console.log(`Retrying in ${delay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        throw e;
      }
    }
  }

  throw new Error("Failed to connect to Clickhouse after multiple attempts");
}


export { escapeColumnName, tableExists, getClickhouseClient };
