import { Database } from "bun:sqlite";
import { beforeAll, describe, expect, it } from "bun:test";
import { createBunAdapter } from "../src/adapters/bun";
import { $, add, cast, fn } from "../src/operators";

let db: Database;
let adapter: ReturnType<typeof createBunAdapter>;
let col: Awaited<ReturnType<ReturnType<typeof createBunAdapter>["collection"]>>;

beforeAll(async () => {
  db = new Database(":memory:");
  adapter = createBunAdapter(db);
  col = await adapter.collection("test_index");
});

describe("Collection.index()", () => {
  it("uses the index in query plan when available", async () => {
    // Insert some data
    for (let i = 1; i <= 10; ++i) {
      await col.insert({ foo: i, bar: { baz: i }, email: `user${i}@x.com`, age: 20 + i, score: i });
    }
    // Create an index on foo
    await col.index("idx_foo", $("foo"));
    // Run a query and get the explain plan
    const query = col.find({ $gt: [$("foo"), 5] });
    const plan = await query.explain();
    //console.log("Query Plan:", plan);
    // Check that the plan mentions using an index (not a full table scan)
    // Accept either 'USING INDEX' or 'USING COVERING INDEX' or similar
    const planStr = typeof plan === "string" ? plan : JSON.stringify(plan);
    expect(planStr.toLowerCase()).toMatch(/index/);
    expect(planStr.toLowerCase()).not.toMatch(/scan users|scan test_index/);
  });
  it("creates a simple index on a field", async () => {
    await col.index("idx_foo_simple", $("foo"));
    const indexes = db.query(`PRAGMA index_list(test_index)`).all();
    let found = false;
    for (const idx of indexes as { name: string }[]) {
      if (idx.name.includes("foo")) found = true;
    }
    expect(found).toBe(true);
  });

  it("creates an index on a nested JSON field", async () => {
    await col.index("idx_bar_baz", $("bar.baz"));
    const indexes = db.query(`PRAGMA index_list(test_index)`).all();
    let found = false;
    for (const idx of indexes as { name: string }[]) {
      if (idx.name.includes("bar_baz")) found = true;
    }
    expect(found).toBe(true);
  });

  it("creates an index using a function expression", async () => {
    await col.index("idx_lower_email", fn("lower", $("email")));
    const indexes = db.query(`PRAGMA index_list(test_index)`).all();
    let found = false;
    for (const idx of indexes as { name: string }[]) {
      if (idx.name.includes("lower")) found = true;
    }
    expect(found).toBe(true);
  });

  it("creates an index using a type cast expression", async () => {
    await col.index("idx_cast_age", cast($("age"), "INTEGER"));
    const indexes = db.query(`PRAGMA index_list(test_index)`).all();
    let found = false;
    for (const idx of indexes as { name: string }[]) {
      if (idx.name === "idx_cast_age") found = true;
    }
    expect(found).toBe(true);
  });

  it("creates an index using an arithmetic expression", async () => {
    await col.index("idx_score_plus_10", add($("score"), 10));
    const indexes = db.query(`PRAGMA index_list(test_index)`).all();
    let found = false;
    for (const idx of indexes as { name: string }[]) {
      if (idx.name.includes("score")) found = true;
    }
    expect(found).toBe(true);
  });

  it("throws on invalid field", async () => {
    await expect(col.index("idx_invalid", 123 as any)).rejects.toThrow();
  });
});
