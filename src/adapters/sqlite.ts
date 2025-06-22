import { Database } from "sqlite";

export function createSqliteAdapter(db: Database) {
  return {
    execute: async (sql: string, args?: any[]) => {
      return db.run(sql, ...(args || []));
    },
    select: async function* (sql: string, args?: any[]) {
      const rows = await db.all(sql, ...(args || []));
      for (const row of rows) yield row;
    },
    get: (sql: string, args?: any[]) => {
      return db.get(sql, ...(args || []));
    },
    lastInsertRowid: (result: any) => result.lastID,
  };
}
