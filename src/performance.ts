// Performance comparison: libsql table vs NoSQL collection
import { createClient } from "@libsql/client";
import { Database } from "bun:sqlite";
import { createBunAdapter } from "./adapters/bun";
import { createLibSQLAdapter } from "./adapters/libsql";

// --- Configurable options ---
const ROWS = 10_000;
const DATA_FORMAT: "JSON" | "JSONB" = "JSONB"; // Change to "JSON" or "JSONB" as needed
const QUERY_TYPE: "eq" | "gt" = "gt"; // Change to 'gt' to test greater-than queries
const QUERY_LIMIT = 100; // Limit for all queries

const randomString = () => Math.random().toString(36).slice(2, 10);
const randomJson = () => ({ foo: Math.random(), bar: randomString() });

const makeData = (n: number) =>
  Array.from({ length: n }, (_, i) => ({
    num: Math.floor(Math.random() * 1000),
    str: randomString(),
    json: randomJson(),
    indexed: Math.floor(Math.random() * 1000),
  }));

async function main() {
  // Helper to extract rows scanned from SQLite EXPLAIN QUERY PLAN output (Bun)
  function extractRowsScannedFromPlan(plan: any): string {
    // Bun: plan is an array of objects with 'detail' property
    if (Array.isArray(plan)) {
      const details = plan.map((row) => row.detail || JSON.stringify(row)).join(" ");
      // Try to extract 'scan' or 'search' with N rows
      const match = details.match(/scan (?:table|index) [^ ]+ (?:as [^ ]+ )?(?:using [^ ]+ )?(?:.*?)(?:~(\d+))?/i);
      if (match && match[1]) return match[1];
      // Sometimes 'scan table ...'
      const fallback = details.match(/scan table [^ ]+/i);
      if (fallback) return "?";
    }
    // fallback
    return "?";
  }

  // Helper to extract rows scanned from libsql/unsqlite .explain() output
  function extractRowsScannedFromLibsqlPlan(plan: any): string {
    if (typeof plan === "string") {
      // Try to find 'scan' or 'search' with N rows
      const match = plan.match(/scan (?:table|index) [^ ]+ (?:as [^ ]+ )?(?:using [^ ]+ )?(?:.*?)(?:~(\d+))?/i);
      if (match && match[1]) return match[1];
      if (/scan table/i.test(plan)) return "?";
    } else if (Array.isArray(plan)) {
      // Similar to Bun
      return extractRowsScannedFromPlan(plan);
    } else if (plan && typeof plan === "object") {
      // Try to find a 'detail' property
      const details = JSON.stringify(plan);
      const match = details.match(/scan (?:table|index) [^ ]+ (?:as [^ ]+ )?(?:using [^ ]+ )?(?:.*?)(?:~(\d+))?/i);
      if (match && match[1]) return match[1];
    }
    return "?";
  }
  // --- Setup shared data and timers ---
  const data = makeData(ROWS);
  let t0: number, t1: number;

  const mid = Math.floor(data.length / 2);
  // --- Bun NoSQL Collection (file-based) ---
  const bunNosqlDbFile = "perf_bun_nosql.db";
  const bunDb = new Database(bunNosqlDbFile);
  const bunNosql = await createBunAdapter(bunDb).collection<{ num: number; str: string; json: any; indexed: number }>("perf_bun_nosql");
  await bunNosql.index("idx_indexed", { $: "indexed" });
  t0 = performance.now();
  for (const row of data) await bunNosql.insert(row);
  t1 = performance.now();
  const bunNosqlInsertMs = (t1 - t0).toFixed(1);
  const bunIdxOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  const bunIndexedQuery = bunNosql.find({ [bunIdxOp]: [{ $: "indexed" }, data[mid].indexed] }).limit(QUERY_LIMIT);
  t0 = performance.now();
  const bunQIdx = await bunIndexedQuery.all();
  t1 = performance.now();
  const bunNosqlIdxMs = (t1 - t0).toFixed(1);
  const bunNosqlIdxCount = bunQIdx.length;
  const bunNosqlPlan = await bunIndexedQuery.explain();
  const bunNosqlPlanRaw = await bunIndexedQuery.explain(true);
  const bunNosqlIndexUsed = typeof bunNosqlPlan === "string" ? /index/i.test(bunNosqlPlan) : JSON.stringify(bunNosqlPlan).toLowerCase().includes("index");
  // num
  const bunNumOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const bunQ1 = await bunNosql
    .find({ [bunNumOp]: [{ $: "num" }, data[mid].num] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const bunNosqlNumMs = (t1 - t0).toFixed(1);
  const bunNosqlNumCount = bunQ1.length;
  // str
  const bunStrOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const bunQ2 = await bunNosql
    .find({ [bunStrOp]: [{ $: "str" }, data[mid].str] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const bunNosqlStrMs = (t1 - t0).toFixed(1);
  const bunNosqlStrCount = bunQ2.length;
  // json.foo
  const bunJsonOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const bunQ3 = await bunNosql
    .find({ [bunJsonOp]: [{ $: "json.foo" }, data[mid].json.foo] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const bunNosqlJsonMs = (t1 - t0).toFixed(1);
  const bunNosqlJsonCount = bunQ3.length;

  // --- Bun SQL Table (file-based) ---
  const bunSqlDbFile = "perf_bun_sql.db";
  const bunSqlDb = new Database(bunSqlDbFile);
  bunSqlDb.query(`DROP TABLE IF EXISTS perf_bun_sql`).run();
  bunSqlDb.query(`CREATE TABLE perf_bun_sql (id INTEGER PRIMARY KEY, num INTEGER, str TEXT, json ${DATA_FORMAT}, indexed INTEGER)`).run();
  bunSqlDb.query(`CREATE INDEX idx_indexed ON perf_bun_sql(indexed)`).run();
  t0 = performance.now();
  const bunInsertStmt = bunSqlDb.prepare(`INSERT INTO perf_bun_sql (num, str, json, indexed) VALUES (?, ?, ?, ?)`);
  for (const row of data) {
    bunInsertStmt.run(row.num, row.str, JSON.stringify(row.json), row.indexed);
  }
  t1 = performance.now();
  const bunSqlInsertMs = (t1 - t0).toFixed(1);
  // num
  t0 = performance.now();
  let bunRes = bunSqlDb.query(`SELECT * FROM perf_bun_sql WHERE num ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`).all(data[mid].num, QUERY_LIMIT);
  t1 = performance.now();
  const bunSqlNumMs = (t1 - t0).toFixed(1);
  const bunSqlNumCount = bunRes.length;
  // str
  t0 = performance.now();
  bunRes = bunSqlDb.query(`SELECT * FROM perf_bun_sql WHERE str ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`).all(data[mid].str, QUERY_LIMIT);
  t1 = performance.now();
  const bunSqlStrMs = (t1 - t0).toFixed(1);
  const bunSqlStrCount = bunRes.length;
  // json.foo
  t0 = performance.now();
  bunRes = bunSqlDb
    .query(`SELECT * FROM perf_bun_sql WHERE json_extract(json, '$.foo') ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`)
    .all(data[mid].json.foo, QUERY_LIMIT);
  t1 = performance.now();
  const bunSqlJsonMs = (t1 - t0).toFixed(1);
  const bunSqlJsonCount = bunRes.length;
  // indexed
  t0 = performance.now();
  bunRes = bunSqlDb.query(`SELECT * FROM perf_bun_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`).all(data[mid].indexed, QUERY_LIMIT);
  t1 = performance.now();
  const bunSqlIdxMs = (t1 - t0).toFixed(1);
  const bunSqlIdxCount = bunRes.length;
  const bunSqlPlan = bunSqlDb
    .query(`EXPLAIN QUERY PLAN SELECT * FROM perf_bun_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`)
    .all(data[mid].indexed, QUERY_LIMIT);
  const bunSqlPlanRaw = bunSqlDb
    .query(`EXPLAIN SELECT * FROM perf_bun_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`)
    .all(data[mid].indexed, QUERY_LIMIT);
  const bunSqlIndexUsed = JSON.stringify(bunSqlPlan).toLowerCase().includes("index");

  // --- NoSQL Collection (libsql, separate DB) ---
  const nosqlDbFile = "perf_nosql.db";
  const nosqlClient = createClient({ url: `file:${nosqlDbFile}` });
  await nosqlClient.execute({ sql: `DROP TABLE IF EXISTS perf_nosql` });
  const nosql = await createLibSQLAdapter(nosqlClient).collection<{ num: number; str: string; json: any; indexed: number }>("perf_nosql", {
    dataFormat: DATA_FORMAT,
  });

  // Create index on 'indexed' field
  await nosql.index("idx_indexed", { $: "indexed" });

  t0 = performance.now();
  for (const row of data) await nosql.insert(row);
  t1 = performance.now();
  const nosqlInsertMs = (t1 - t0).toFixed(1);

  // indexed
  const nosqlIdxOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  const indexedQuery = nosql.find({ [nosqlIdxOp]: [{ $: "indexed" }, data[mid].indexed] }).limit(QUERY_LIMIT);
  t0 = performance.now();
  const qIdx = await indexedQuery.all();
  t1 = performance.now();
  const nosqlIdxMs = (t1 - t0).toFixed(1);
  const nosqlIdxCount = qIdx.length;
  // Explain query plan for indexed field
  const nosqlPlan = await indexedQuery.explain();
  const nosqlPlanRaw = await indexedQuery.explain(true);
  const nosqlIndexUsed = typeof nosqlPlan === "string" ? /index/i.test(nosqlPlan) : JSON.stringify(nosqlPlan).toLowerCase().includes("index");

  // num
  const nosqlNumOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const q1 = await nosql
    .find({ [nosqlNumOp]: [{ $: "num" }, data[mid].num] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const nosqlNumMs = (t1 - t0).toFixed(1);
  const nosqlNumCount = q1.length;

  // str
  const nosqlStrOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const q2 = await nosql
    .find({ [nosqlStrOp]: [{ $: "str" }, data[mid].str] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const nosqlStrMs = (t1 - t0).toFixed(1);
  const nosqlStrCount = q2.length;

  // json.foo
  const nosqlJsonOp = QUERY_TYPE === "eq" ? "$eq" : "$gt";
  t0 = performance.now();
  const q3 = await nosql
    .find({ [nosqlJsonOp]: [{ $: "json.foo" }, data[mid].json.foo] })
    .limit(QUERY_LIMIT)
    .all();
  t1 = performance.now();
  const nosqlJsonMs = (t1 - t0).toFixed(1);
  const nosqlJsonCount = q3.length;

  try {
    await nosqlClient.close?.();
  } catch {}

  // --- Standard SQL Table (libsql, separate DB) ---
  const sqlDbFile = "perf_sql.db";
  const sqlClient = createClient({ url: `file:${sqlDbFile}` });
  await sqlClient.execute({ sql: `DROP TABLE IF EXISTS perf_sql` });
  // Use DATA_FORMAT for the json column
  await sqlClient.execute({
    sql: `CREATE TABLE perf_sql (id INTEGER PRIMARY KEY, num INTEGER, str TEXT, json ${DATA_FORMAT}, indexed INTEGER)`,
  });
  // Create index on indexed field
  await sqlClient.execute({ sql: `CREATE INDEX idx_indexed ON perf_sql(indexed)` });

  t0 = performance.now();
  for (const row of data) {
    await sqlClient.execute({
      sql: `INSERT INTO perf_sql (num, str, json, indexed) VALUES (?, ?, ?, ?)`,
      args: [row.num, row.str, JSON.stringify(row.json), row.indexed],
    });
  }
  t1 = performance.now();
  const sqlInsertMs = (t1 - t0).toFixed(1);

  // num
  t0 = performance.now();
  let res = await sqlClient.execute({
    sql: `SELECT * FROM perf_sql WHERE num ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`,
    args: [data[mid].num, QUERY_LIMIT],
  });
  t1 = performance.now();
  const sqlNumMs = (t1 - t0).toFixed(1);
  const sqlNumCount = res.rows.length;

  // str
  t0 = performance.now();
  res = await sqlClient.execute({ sql: `SELECT * FROM perf_sql WHERE str ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`, args: [data[mid].str, QUERY_LIMIT] });
  t1 = performance.now();
  const sqlStrMs = (t1 - t0).toFixed(1);
  const sqlStrCount = res.rows.length;

  // json.foo
  t0 = performance.now();
  res = await sqlClient.execute({
    sql: `SELECT * FROM perf_sql WHERE json_extract(json, '$.foo') ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`,
    args: [data[mid].json.foo, QUERY_LIMIT],
  });
  t1 = performance.now();
  const sqlJsonMs = (t1 - t0).toFixed(1);
  const sqlJsonCount = res.rows.length;

  // indexed
  t0 = performance.now();
  res = await sqlClient.execute({
    sql: `SELECT * FROM perf_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ? LIMIT ?`,
    args: [data[mid].indexed, QUERY_LIMIT],
  });
  t1 = performance.now();
  const sqlIdxMs = (t1 - t0).toFixed(1);
  const sqlIdxCount = res.rows.length;

  // Explain query plan for indexed field
  const explain = await sqlClient.execute({
    sql: `EXPLAIN QUERY PLAN SELECT * FROM perf_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ?`,
    args: [data[mid].indexed],
  });
  const sqlPlan = explain.rows;
  // Run plain EXPLAIN for libsql SQL table
  const explainRaw = await sqlClient.execute({
    sql: `EXPLAIN SELECT * FROM perf_sql WHERE indexed ${QUERY_TYPE === "eq" ? "=" : ">"} ?`,
    args: [data[mid].indexed],
  });
  const sqlPlanRaw = explainRaw.rows;
  const sqlIndexUsed = JSON.stringify(sqlPlan).toLowerCase().includes("index");

  // --- Cleanup ---
  try {
    await sqlClient.close?.();
  } catch {}
  // Clean up all database files
  for (const file of [nosqlDbFile, sqlDbFile, bunNosqlDbFile, bunSqlDbFile]) {
    try {
      Bun.spawnSync(["rm", "-f", file]);
      console.log(`[Cleanup] ${file} removed`);
    } catch (e) {
      // Ignore if file does not exist or cannot be removed
    }
  }

  // --- Output performance info at the very end ---

  // Improved padding using padEnd
  function pad(str: string, len: number) {
    return String(str).padEnd(len, " ");
  }
  const col1 = 22,
    col2 = 20,
    col3 = 20,
    col4 = 20,
    col5 = 20;
  // Helper to estimate rows scanned from EXPLAIN bytecode
  function estimateRowsScannedFromExplainRaw(raw: any): string {
    // raw is an array of rows, each with opcode, p1, p2, p3, p4, p5, comment
    if (!Array.isArray(raw)) return "?";
    // Look for opcodes that scan tables: e.g. 'Next', 'Rewind', 'Column', 'SeekGE', 'SeekLE', 'SeekRowid', 'IdxGE', 'IdxLT', etc.
    // We'll count the number of 'Next' or 'Prev' opcodes as a proxy for rows scanned
    let nextCount = 0;
    for (const row of raw) {
      if (row.opcode === "Next" || row.opcode === "Prev") nextCount++;
    }
    return nextCount > 0 ? String(nextCount) : "?";
  }

  // Extract rows scanned for each backend using both methods
  const rowsScanned = {
    nosql: {
      plan: extractRowsScannedFromLibsqlPlan(nosqlPlan),
      raw: estimateRowsScannedFromExplainRaw(nosqlPlanRaw),
    },
    sql: {
      plan: extractRowsScannedFromLibsqlPlan(sqlPlan),
      raw: estimateRowsScannedFromExplainRaw(sqlPlanRaw),
    },
    bunNosql: {
      plan: extractRowsScannedFromPlan(bunNosqlPlan),
      raw: estimateRowsScannedFromExplainRaw(bunNosqlPlanRaw),
    },
    bunSql: {
      plan: extractRowsScannedFromPlan(bunSqlPlan),
      raw: estimateRowsScannedFromExplainRaw(bunSqlPlanRaw),
    },
  };

  // console.log("\n[NoSQL] Query plan for indexed (libsql):", nosqlPlan);
  // console.log("[NoSQL] Raw EXPLAIN (libsql):", nosqlPlanRaw);
  // console.log("[SQL]   Query plan for indexed (libsql):", sqlPlan);
  // console.log("[SQL]   Raw EXPLAIN (libsql):", sqlPlanRaw);
  // console.log("[NoSQL] Query plan for indexed (bun):", bunNosqlPlan);
  // console.log("[NoSQL] Raw EXPLAIN (bun):", bunNosqlPlanRaw);
  // console.log("[SQL]   Query plan for indexed (bun):", bunSqlPlan);
  // console.log("[SQL]   Raw EXPLAIN (bun):", bunSqlPlanRaw);

  console.log("\n=== Performance Comparison ===");
  console.log(pad("Operation", col1) + pad("NoSQL-libsql", col2) + pad("SQL-libsql", col3) + pad("NoSQL-bun", col4) + pad("SQL-bun", col5));
  console.log("-".repeat(col1 + col2 + col3 + col4 + col5));
  console.log(pad("Insert (ms)", col1) + pad(nosqlInsertMs, col2) + pad(sqlInsertMs, col3) + pad(bunNosqlInsertMs, col4) + pad(bunSqlInsertMs, col5));
  console.log(
    pad("Query by num (ms)", col1) +
      pad(nosqlNumMs + ` (${nosqlNumCount})`, col2) +
      pad(sqlNumMs + ` (${sqlNumCount})`, col3) +
      pad(bunNosqlNumMs + ` (${bunNosqlNumCount})`, col4) +
      pad(bunSqlNumMs + ` (${bunSqlNumCount})`, col5)
  );
  console.log(
    pad("Query by str (ms)", col1) +
      pad(nosqlStrMs + ` (${nosqlStrCount})`, col2) +
      pad(sqlStrMs + ` (${sqlStrCount})`, col3) +
      pad(bunNosqlStrMs + ` (${bunNosqlStrCount})`, col4) +
      pad(bunSqlStrMs + ` (${bunSqlStrCount})`, col5)
  );
  console.log(
    pad("Query by json.foo (ms)", col1) +
      pad(nosqlJsonMs + ` (${nosqlJsonCount})`, col2) +
      pad(sqlJsonMs + ` (${sqlJsonCount})`, col3) +
      pad(bunNosqlJsonMs + ` (${bunNosqlJsonCount})`, col4) +
      pad(bunSqlJsonMs + ` (${bunSqlJsonCount})`, col5)
  );
  console.log(
    pad("Query by indexed (ms)", col1) +
      pad(nosqlIdxMs + ` (${nosqlIdxCount})`, col2) +
      pad(sqlIdxMs + ` (${sqlIdxCount})`, col3) +
      pad(bunNosqlIdxMs + ` (${bunNosqlIdxCount})`, col4) +
      pad(bunSqlIdxMs + ` (${bunSqlIdxCount})`, col5)
  );
  console.log(
    pad("Index used?", col1) +
      pad(nosqlIndexUsed ? "YES" : "NO", col2) +
      pad(sqlIndexUsed ? "YES" : "NO", col3) +
      pad(bunNosqlIndexUsed ? "YES" : "NO", col4) +
      pad(bunSqlIndexUsed ? "YES" : "NO", col5)
  );

  console.log(
    pad("Rows scanned (plan)", col1) +
      pad(rowsScanned.nosql.plan, col2) +
      pad(rowsScanned.sql.plan, col3) +
      pad(rowsScanned.bunNosql.plan, col4) +
      pad(rowsScanned.bunSql.plan, col5)
  );
  console.log(
    pad("Rows scanned (raw)", col1) +
      pad(rowsScanned.nosql.raw, col2) +
      pad(rowsScanned.sql.raw, col3) +
      pad(rowsScanned.bunNosql.raw, col4) +
      pad(rowsScanned.bunSql.raw, col5)
  );

  try {
    await sqlClient.close?.();
  } catch {}
  // Clean up both database files
  for (const file of [nosqlDbFile, sqlDbFile]) {
    try {
      Bun.spawnSync(["rm", "-f", file]);
      console.log(`[Cleanup] ${file} removed`);
    } catch (e) {
      // Ignore if file does not exist or cannot be removed
    }
  }
}

main();
