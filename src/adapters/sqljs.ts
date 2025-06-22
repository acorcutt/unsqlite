import type { Database } from "sql.js";

// Usage: const SQL = await initSqlJs(); const db = new SQL.Database();
// Pass the db instance to this adapter.

export function createSqljsAdapter(db: Database) {
  return {
    execute: async (sql: string, args?: any[]) => {
      db.run(sql, args || []);
      // sql.js does not return a result object for run
      return undefined;
    },
    select: async function* (sql: string, args?: any[]) {
      const stmt = db.prepare(sql);
      try {
        if (args && args.length) stmt.bind(args);
        while (stmt.step()) {
          yield stmt.getAsObject();
        }
      } finally {
        stmt.free();
      }
    },
    get: async (sql: string, args?: any[]) => {
      const stmt = db.prepare(sql);
      try {
        if (args && args.length) stmt.bind(args);
        if (stmt.step()) {
          return stmt.getAsObject();
        } else {
          return undefined;
        }
      } finally {
        stmt.free();
      }
    },
    lastInsertRowid: (_result: any) => {
      const stmt = db.prepare("SELECT last_insert_rowid() AS id");
      try {
        stmt.step();
        const row = stmt.getAsObject();
        return row.id;
      } finally {
        stmt.free();
      }
    },
  };
}
