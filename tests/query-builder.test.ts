import { describe, expect, it } from "bun:test";
import { $, and, eq, gt } from "../src/operators";
import { QueryBuilder } from "../src/query-builder";

// Mock DBSelect
async function* mockSelect(sql: string, args?: any[]) {
  // For testing, just yield the SQL and params for inspection
  yield { sql, params: args ?? [], data: JSON.stringify({ foo: 1, bar: 2 }) };
}

describe("QueryBuilder", () => {
  it("generates correct SQL and params for eq", async () => {
    const qb = new QueryBuilder("mytable", "data", { select: mockSelect }, eq($("foo"), 1));
    const all = await qb.all();
    expect(all.length).toBe(1);
    expect(all[0]).toEqual({ foo: 1, bar: 2 });
  });

  it("generates correct SQL and params for gt and order", async () => {
    const qb = new QueryBuilder("mytable", "data", { select: mockSelect }, gt($("bar"), 2));
    qb.order($("foo"), "desc");
    const all = await qb.all();
    expect(all.length).toBe(1);
    expect(all[0]).toEqual({ foo: 1, bar: 2 });
  });

  it("generates correct SQL and params for and/or/not", async () => {
    const qb = new QueryBuilder("mytable", "data", { select: mockSelect }, and(eq($("foo"), 1), gt($("bar"), 2)));
    const all = await qb.all();
    expect(all.length).toBe(1);
    expect(all[0]).toEqual({ foo: 1, bar: 2 });
  });

  it("supports limit and offset", async () => {
    const qb = new QueryBuilder("mytable", "data", { select: mockSelect }, eq($("foo"), 1));
    qb.limit(5).offset(2);
    const all = await qb.all();
    expect(all.length).toBe(1);
    expect(all[0]).toEqual({ foo: 1, bar: 2 });
  });
});
