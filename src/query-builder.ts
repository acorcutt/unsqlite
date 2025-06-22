// Query object type for the query compiler/operator DSL
export type QueryPrimitive = string | number | boolean | null;
export type QueryValue = QueryPrimitive | QueryObject | QueryValue[];
export type QueryObject = { [key: string]: QueryValue };
// --- QueryBuilder for Collection ---
import type { DBSelect } from "./collection";
import { queryCompiler } from "./query-compiler";

export class QueryBuilder<TDATA> {
  private jsonExtract: string;
  /**
   * Helper to build SQL and params for SELECT queries.
   * Accepts options for explain, count, and whether to include limit/offset.
   */
  private _buildSQL(options?: { explain?: boolean; debugExplain?: boolean; count?: boolean; includeLimitOffset?: boolean }): { sql: string; params: any[] } {
    const explainType = options?.explain ? (options?.debugExplain ? "EXPLAIN" : "EXPLAIN QUERY PLAN") : "";
    let selectClause = options?.count ? `SELECT COUNT(*) as count` : `SELECT ${this.dataCol}`;
    let sql = `${explainType ? explainType + " " : ""}${selectClause} FROM ${this.table}`;
    let params: any[] = [];
    if (this.query) {
      const where = queryCompiler(this.query, this.dataCol, this.jsonExtract);
      if (where.sql) {
        sql += ` WHERE ${where.sql}`;
        params = where.params;
      }
    }
    if (this._order.length && !options?.count) {
      sql +=
        " ORDER BY " +
        this._order
          .map(([f, d]) => {
            if (typeof f === "object" && f !== null && "$" in f) {
              return `${this.jsonExtract}(${this.dataCol}, '$.${f.$}') ${d.toUpperCase()}`;
            } else {
              return `${f} ${d.toUpperCase()}`;
            }
          })
          .join(", ");
    }
    if (options?.includeLimitOffset !== false && !options?.count) {
      if (this._limit !== undefined) {
        sql += ` LIMIT ${this._limit}`;
      }
      if (this._offset !== undefined) {
        sql += ` OFFSET ${this._offset}`;
      }
    }
    return { sql, params };
  }
  private table: string;
  private dataCol: string;
  private db: { select: DBSelect };
  private query: QueryObject | undefined;
  private _order: [string | { $: string }, "asc" | "desc"][] = [];
  private _limit?: number;
  private _offset?: number;

  constructor(table: string, dataCol: string, db: { select: DBSelect }, query?: QueryObject, jsonExtract: string = "json_extract") {
    this.table = table;
    this.dataCol = dataCol;
    this.db = db;
    this.query = query;
    this.jsonExtract = jsonExtract;
  }

  order(field: string | { $: string }, dir: "asc" | "desc" = "asc") {
    this._order.push([field, dir]);
    return this;
  }

  limit(n: number) {
    this._limit = n;
    return this;
  }

  offset(n: number) {
    this._offset = n;
    return this;
  }

  async all(): Promise<TDATA[]> {
    const { sql, params } = this._buildSQL();
    const result: TDATA[] = [];
    for await (const row of this.db.select(sql, params)) {
      const val = row[this.dataCol];
      if (val === undefined || val === null) continue;
      if (typeof val === "string") result.push(JSON.parse(val));
      else result.push(val);
    }
    return result;
  }

  async first(): Promise<TDATA | undefined> {
    const { sql, params } = this._buildSQL({ includeLimitOffset: true });
    // Always limit 1 for first()
    const sqlWithLimit = sql.includes("LIMIT") ? sql : sql + " LIMIT 1";
    for await (const row of this.db.select(sqlWithLimit, params)) {
      const val = row[this.dataCol];
      if (val === undefined || val === null) continue;
      if (typeof val === "string") return JSON.parse(val);
      return val;
    }
    return undefined;
  }

  async count(): Promise<number> {
    const { sql, params } = this._buildSQL({ count: true, includeLimitOffset: false });
    for await (const row of this.db.select(sql, params)) {
      return row.count ?? 0;
    }
    return 0;
  }
  iterate(): AsyncIterable<TDATA> {
    const { sql, params } = this._buildSQL();
    const self = this;
    async function* gen() {
      for await (const row of self.db.select(sql, params)) {
        const val = row[self.dataCol];
        if (val === undefined || val === null) continue;
        if (typeof val === "string") yield JSON.parse(val);
        else yield val;
      }
    }
    return gen();
  }

  async explain(debug?: boolean): Promise<any> {
    const { sql, params } = this._buildSQL({ explain: true, debugExplain: debug });
    const result: any[] = [];
    for await (const row of this.db.select(sql, params)) {
      result.push(row);
    }
    return result;
  }
  toString(): string {
    const { sql, params } = this._buildSQL();
    return sql + " " + JSON.stringify(params);
  }
}
