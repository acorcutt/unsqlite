import { expect, test } from "bun:test";
import { createBunAdapter, createCollection, createLibSQLAdapter, createSqlite3Adapter, createSqliteAdapter, createSqljsAdapter } from "../src";

// Bun adapter
// Use in-memory DB
// Note: bun:sqlite uses ':memory:' for in-memory
// https://bun.sh/docs/api/sqlite

test("bun adapter basic", async () => {
  const { Database } = await import("bun:sqlite");
  const db = new Database(":memory:");
  const adapter = createBunAdapter(db);
  const col = await adapter.collection<any>("bun_test");
  const id = await col.insert({ foo: 1 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(1);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  db.close();
});

// LibSQL adapter
// Use in-memory DB
// https://docs.turso.tech/reference/sqlite/uri

test("libsql adapter basic", async () => {
  const { createClient } = await import("@libsql/client");
  const client = createClient({ url: ":memory:" });
  const adapter = createLibSQLAdapter(client);
  const col = await adapter.collection<any>("libsql_test");
  const id = await col.insert({ foo: 2 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(2);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  await client.close();
});

// better-sqlite3 adapter
// Disabled: Bun ABI incompatibility (see https://github.com/oven-sh/bun/issues/issue-tracker)
/*
test("better-sqlite3 adapter basic", async () => {
  const BetterSqlite3 = (await import("better-sqlite3")).default;
  const db = new BetterSqlite3(":memory:");
  const adapter = createBetterSqlite3Adapter(db);
  const col = await createCollection<any>("better_sqlite3_test", adapter);
  const id = await col.insert({ foo: 3 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(3);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  db.close();
});
*/

// sqlite3 adapter
// Use in-memory DB

test("sqlite3 adapter basic", async () => {
  const sqlite3 = (await import("sqlite3")).verbose();
  const db = new sqlite3.Database(":memory:");
  const adapter = createSqlite3Adapter(db);
  const col = await createCollection<any>("sqlite3_test", adapter);
  const id = await col.insert({ foo: 4 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(4);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  db.close();
});

// sqlite (sqlite package) adapter
// Use in-memory DB

test("sqlite adapter basic", async () => {
  const { open } = await import("sqlite");
  const sqlite3 = await import("sqlite3");
  const db = await open({ filename: ":memory:", driver: sqlite3.Database });
  const adapter = createSqliteAdapter(db);
  // Use a non-reserved table name (avoid 'sqlite_' and 'test')
  const col = await createCollection<any>("adapter_sqlite_example", adapter);
  const id = await col.insert({ foo: 5 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(5);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  await db.close();
});

// sql.js adapter
// Use in-memory DB (default)

test("sqljs adapter basic", async () => {
  const initSqlJs = (await import("sql.js")).default;
  const SQL = await initSqlJs();
  const db = new SQL.Database();
  const adapter = createSqljsAdapter(db);
  const col = await createCollection<any>("sqljs_test", adapter);
  const id = await col.insert({ foo: 6 });
  const got = await col.get(id);
  expect((got as any).foo).toBe(6);
  const all = await col.find().all();
  expect(all.length).toBeGreaterThan(0);
  db.close();
});
