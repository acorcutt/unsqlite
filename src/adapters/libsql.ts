import { createCollection } from "../collection";

export const collection = async <TDATA = any, TID = number>(client: any, table: string, options?: any) =>
  await createCollection<TDATA, TID>(
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
