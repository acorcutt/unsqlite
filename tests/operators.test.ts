import { describe, expect, it } from "bun:test";
import { $, and, eq, gt, gte, lt, lte, ne, not, or } from "../src/operators";

describe("operators", () => {
  it("$ field selector returns correct object", () => {
    expect($("foo")).toEqual({ $: "foo" });
    expect($("bar.baz")).toEqual({ $: "bar.baz" });
  });

  it("eq operator returns correct object for primitives", () => {
    expect(eq("foo", 1)).toEqual({ $eq: ["foo", 1] });
    expect(eq(2, 3)).toEqual({ $eq: [2, 3] });
    expect(eq("a", "b")).toEqual({ $eq: ["a", "b"] });
  });

  it("eq operator works with $ field selector", () => {
    expect(eq($("foo"), 1)).toEqual({ $eq: [{ $: "foo" }, 1] });
    expect(eq($("bar.baz"), "val")).toEqual({ $eq: [{ $: "bar.baz" }, "val"] });
  });

  it("eq operator works with $ field selector on right", () => {
    expect(eq(1, $("foo"))).toEqual({ $eq: [1, { $: "foo" }] });
  });

  it("ne operator returns correct object", () => {
    expect(ne("foo", 1)).toEqual({ $ne: ["foo", 1] });
    expect(ne($("foo"), 2)).toEqual({ $ne: [{ $: "foo" }, 2] });
  });

  it("gt/gte/lt/lte operators return correct objects", () => {
    expect(gt("foo", 1)).toEqual({ $gt: ["foo", 1] });
    expect(gt($("foo"), 2)).toEqual({ $gt: [{ $: "foo" }, 2] });
    expect(gte("foo", 1)).toEqual({ $gte: ["foo", 1] });
    expect(gte($("foo"), 2)).toEqual({ $gte: [{ $: "foo" }, 2] });
    expect(lt("foo", 1)).toEqual({ $lt: ["foo", 1] });
    expect(lt($("foo"), 2)).toEqual({ $lt: [{ $: "foo" }, 2] });
    expect(lte("foo", 1)).toEqual({ $lte: ["foo", 1] });
    expect(lte($("foo"), 2)).toEqual({ $lte: [{ $: "foo" }, 2] });
  });

  it("and/or/not operators return correct objects", () => {
    expect(and(eq("foo", 1), gt("bar", 2))).toEqual({ $and: [{ $eq: ["foo", 1] }, { $gt: ["bar", 2] }] });
    expect(or(eq("foo", 1), lt("bar", 2))).toEqual({ $or: [{ $eq: ["foo", 1] }, { $lt: ["bar", 2] }] });
    expect(not(eq("foo", 1))).toEqual({ $not: [{ $eq: ["foo", 1] }] });
  });
});
