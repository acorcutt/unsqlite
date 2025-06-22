import type { Database } from "better-sqlite3";

export function createBetterSqlite3Adapter(db: Database) {
  return {
    execute: async (sql: string, args?: any[]) => {
      if (args && args.length) {
        return db.prepare(sql).run(...args);
      } else {
        return db.prepare(sql).run();
      }
    },
    select: async function* (sql: string, args?: any[]) {
      const stmt = db.prepare(sql);
      const rows = args && args.length ? stmt.all(...args) : stmt.all();
      for (const row of rows) yield row;
    },
    get: async (sql: string, args?: any[]) => {
      const stmt = db.prepare(sql);
      return args && args.length ? stmt.get(...args) : stmt.get();
    },
    lastInsertRowid: (result: any) => result.lastInsertRowid,
  };
}
