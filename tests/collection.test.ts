import { createClient } from "@libsql/client";
import { Database } from "bun:sqlite";
import { beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createBunAdapter } from "../src/adapters/bun";
import { createLibSQLAdapter } from "../src/adapters/libsql";
import type { Collection } from "../src/collection";

import { $, and, eq, gt, or } from "../src/operators";

interface UserType {
  name: string;
  value: number;
}

describe("Collection", () => {
  it("can iterate results with .iterate()", async () => {
    const db = new Database(":memory:");
    const col = await createBunAdapter(db).collection<{ n: number }>("iter_users");
    for (let i = 1; i <= 5; ++i) await col.insert({ n: i });
    const iter = col
      .find(gt($("n"), 2))
      .order($("n"), "desc")
      .iterate();
    const results: number[] = [];
    for await (const user of iter) {
      results.push(user.n);
    }
    expect(results).toEqual([5, 4, 3]);
  });

  it("supports query chaining and operators", async () => {
    const db = new Database(":memory:");
    const col = await createBunAdapter(db).collection<{ n: number }>("chain_users");
    for (let i = 1; i <= 10; ++i) await col.insert({ n: i });
    // Query: n > 3 and n < 7
    const query = col.find(and(gt($("n"), 3), { $lt: [$("n"), 7] })).order($("n"), "asc");
    const all = await query.all();
    expect(all.map((u) => u.n)).toEqual([4, 5, 6]);
    // Query: n == 2 or n == 9
    const query2 = col.find(or(eq($("n"), 2), eq($("n"), 9)));
    const all2 = await query2.all();
    expect(all2.map((u) => u.n).sort()).toEqual([2, 9]);
  });
  let col: Collection<number, UserType>;
  let db: Database;
  const user1 = { name: "Alice", value: 1 };
  const user2 = { name: "Bob", value: 2 };
  const id1 = 1;
  const id2 = 2;

  beforeEach(async () => {
    db = new Database(":memory:");
    col = await createBunAdapter(db).collection<UserType>("users");
  });
  it("set and get single item", async () => {
    await col.set(id1, user1);
    const result = await col.get(id1);
    expect(result).toEqual(user1);
  });

  it("insert returns new id and get works", async () => {
    const newId = await col.insert(user2);
    expect(typeof newId === "string" || typeof newId === "number").toBe(true);
    const result = await col.get(newId);
    expect(result).toEqual(user2);
  });

  it("get multiple items", async () => {
    await col.set(id1, user1);
    await col.set(id2, user2);
    const results = await col.get([id1, id2, 9999]); // 9999 does not exist
    expect(results).toEqual([user1, user2, undefined]);
  });
});

// LibSQLAdapter example usage
describe("LibSQL Collection", () => {
  let client: any;
  beforeAll(() => {
    client = createClient({ url: ":memory:" });
  });
  it("set and get single item", async () => {
    const testId = 42;
    const testData: UserType = { name: "Test", value: 99 };
    const libsqlCol = await createLibSQLAdapter(client).collection<UserType>("test_table");
    await libsqlCol.set(testId, testData);
    const libsqlOut = await libsqlCol.get(testId);
    expect(libsqlOut).toEqual(testData);
  });
});

// String primary key with random id generator
describe("Collection with string primary key and random id generator", () => {
  function randomId() {
    return Math.random().toString(36).slice(2, 10);
  }
  let col: Collection<string, UserType>;
  let db: Database;
  const user = { name: "Charlie", value: 3 };
  let generatedId: string;

  beforeEach(async () => {
    db = new Database(":memory:");
    col = await createBunAdapter(db).collection<UserType, string>("users_string", {
      idColumn: "user_id",
      idType: "TEXT PRIMARY KEY",
      idGenerate: randomId,
      dataColumn: "user_data",
      dataFormat: "JSON",
    });
  });

  it("insert uses random string id and get works", async () => {
    generatedId = await col.insert(user);
    expect(typeof generatedId).toBe("string");
    const result = await col.get(generatedId);
    expect(result).toEqual(user);
  });

  it("set and get with explicit string id", async () => {
    const explicitId = "custom-id-123";
    await col.set(explicitId, user);
    const result = await col.get(explicitId);
    expect(result).toEqual(user);
  });
});

// Test JSONB data format
describe("Collection with JSONB data format", () => {
  let col: Collection<number, UserType>;
  let db: Database;
  const user = { name: "JsonBee", value: 7 };
  const id = 100;

  beforeEach(async () => {
    db = new Database(":memory:");
    col = await createBunAdapter(db).collection<UserType>("users_jsonb", {
      dataFormat: "JSONB",
    });
  });

  it("set and get works with JSONB", async () => {
    await col.set(id, user);
    const result = await col.get(id);
    expect(result).toEqual(user);
  });
});
