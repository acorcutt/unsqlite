import { Database } from "sqlite3";

export function createSqlite3Adapter(db: Database) {
  return {
    execute: async (sql: string, args?: any[]) => {
      return new Promise((resolve, reject) => {
        db.run(sql, ...(args || []), function (err: Error | null) {
          if (err) reject(err);
          else resolve(this);
        });
      });
    },
    select: async function* (sql: string, args?: any[]) {
      const rows: any[] = await new Promise((resolve, reject) => {
        db.all(sql, ...(args || []), (err: Error | null, rows: any[]) => {
          if (err) reject(err);
          else resolve(rows);
        });
      });
      for (const row of rows) yield row;
    },
    get: (sql: string, args?: any[]) => {
      return new Promise((resolve, reject) => {
        db.get(sql, ...(args || []), (err: Error | null, row: any) => {
          if (err) reject(err);
          else resolve(row);
        });
      });
    },
    lastInsertRowid: (result: any) => result.lastID,
  };
}
