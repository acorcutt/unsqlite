import { Database } from "bun:sqlite";
import { createCollection } from "../collection";

export class BunAdapter {
  static async collection<TDATA = any, TID = number>(db: Database, table: string, options?: any) {
    return await createCollection<TDATA, TID>(
      table,
      {
        execute: async (sql: string, args?: any[]) => {
          const stmt = db.prepare(sql);
          return args ? stmt.run(...args) : stmt.run();
        },
        select: async function* (sql: string, args?: any[]) {
          const stmt = db.query(sql);
          const iter = stmt.iterate(...(args ?? []));
          for (const row of iter) {
            yield row;
          }
        },
        get: async (sql: string, args?: any[]) => {
          const stmt = db.query(sql);
          const rows = args ? stmt.all(...args) : stmt.all();
          return rows[0] || undefined;
        },
        lastInsertRowid: async (result: any) => {
          return result.lastInsertRowid;
        },
      },
      options
    );
  }
}
