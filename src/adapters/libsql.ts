import { type Client } from "@libsql/client";
import { createCollection } from "../collection";

export function createLibSQLAdapter(client: Client) {
  return {
    async collection<TDATA = any, TID = number>(table: string, options?: any) {
      return await createCollection<TDATA, TID>(
        table,
        {
          execute: async (sql: string, args?: any[]) => {
            return await client.execute({ sql, args });
          },
          select: async function* (sql: string, args?: any[]) {
            const res = await client.execute({ sql, args });
            const rows = res.rows ?? [];
            for (const row of rows) yield row;
          },
          get: async (sql: string, args?: any[]) => {
            const res = await client.execute({ sql, args });
            return (res.rows && res.rows[0]) || undefined;
          },
          lastInsertRowid: async (result: any) => {
            return result.lastInsertRowid;
          },
        },
        options
      );
    },
  };
}
