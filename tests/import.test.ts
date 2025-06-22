import { describe, expect, it } from "bun:test";

// Import from the dist build
import * as unsqlite from "../dist/index.js";

describe("dist build import", () => {
  it("should import createBunAdapter and create a collection", async () => {
    expect(typeof unsqlite.createBunAdapter).toBe("function");
    // Optionally, test that a collection can be created (smoke test)
    const { Database } = await import("bun:sqlite");
    const db = new Database(":memory:");
    const users = await unsqlite.createBunAdapter(db).collection("users");
    expect(users).toBeTruthy();
  });
});
