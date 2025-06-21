import { describe, expect, it } from "bun:test";
import { $, and, eq, gt, not, or } from "../src/operators";
import { queryCompiler } from "../src/query-compiler";

describe("queryCompiler", () => {
  it("compiles eq with field selector and value", () => {
    const q = eq($("foo"), 1);
    const { sql, params } = queryCompiler(q);
    expect(sql).toBe("json_extract(data, '$.foo') = ?");
    expect(params).toEqual([1]);
  });

  it("compiles gt with value and field selector", () => {
    const q = gt(2, $("bar"));
    const { sql, params } = queryCompiler(q);
    expect(sql).toBe("? > json_extract(data, '$.bar')");
    expect(params).toEqual([2]);
  });

  it("compiles and/or/not", () => {
    const q = and(eq($("foo"), 1), gt($("bar"), 2));
    const { sql, params } = queryCompiler(q);
    expect(sql).toBe("(json_extract(data, '$.foo') = ?) AND (json_extract(data, '$.bar') > ?)");
    expect(params).toEqual([1, 2]);

    const q2 = or(eq($("foo"), 1), gt($("bar"), 2));
    const { sql: sql2, params: params2 } = queryCompiler(q2);
    expect(sql2).toBe("(json_extract(data, '$.foo') = ?) OR (json_extract(data, '$.bar') > ?)");
    expect(params2).toEqual([1, 2]);

    const q3 = not(eq($("foo"), 1));
    const { sql: sql3, params: params3 } = queryCompiler(q3);
    expect(sql3).toBe("NOT (json_extract(data, '$.foo') = ?)");
    expect(params3).toEqual([1]);
  });

  it("compiles nested logic", () => {
    const q = and(or(eq($("foo"), 1), gt($("bar"), 2)), not(eq($("baz"), 3)));
    const { sql, params } = queryCompiler(q);
    expect(sql).toBe("((json_extract(data, '$.foo') = ?) OR (json_extract(data, '$.bar') > ?)) AND (NOT (json_extract(data, '$.baz') = ?))");
    expect(params).toEqual([1, 2, 3]);
  });

  it("compiles plain object queries (not just operators)", () => {
    // Simple $eq
    const q = { $eq: [{ $: "foo" }, 1] };
    const { sql, params } = queryCompiler(q);
    expect(sql).toBe("json_extract(data, '$.foo') = ?");
    expect(params).toEqual([1]);

    // $gt with value on left
    const q2 = { $gt: [2, { $: "bar" }] };
    const { sql: sql2, params: params2 } = queryCompiler(q2);
    expect(sql2).toBe("? > json_extract(data, '$.bar')");
    expect(params2).toEqual([2]);

    // $and with plain objects
    const q3 = { $and: [{ $eq: [{ $: "foo" }, 1] }, { $gt: [{ $: "bar" }, 2] }] };
    const { sql: sql3, params: params3 } = queryCompiler(q3);
    expect(sql3).toBe("(json_extract(data, '$.foo') = ?) AND (json_extract(data, '$.bar') > ?)");
    expect(params3).toEqual([1, 2]);

    // $not with plain object
    const q4 = { $not: [{ $eq: [{ $: "foo" }, 1] }] };
    const { sql: sql4, params: params4 } = queryCompiler(q4);
    expect(sql4).toBe("NOT (json_extract(data, '$.foo') = ?)");
    expect(params4).toEqual([1]);
  });
});
