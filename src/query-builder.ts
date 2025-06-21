// Query object type for the query compiler/operator DSL
export type QueryPrimitive = string | number | boolean | null;
export type QueryValue = QueryPrimitive | QueryObject | QueryValue[];
export type QueryObject = { [key: string]: QueryValue };
// --- QueryBuilder for Collection ---
import type { DBSelect } from "./collection";
import { queryCompiler } from "./query-compiler";

export class QueryBuilder<TDATA> {
  private table: string;
  private dataCol: string;
  private db: { select: DBSelect };
  private query: QueryObject | undefined;
  private _order: [string | { $: string }, "asc" | "desc"][] = [];
  private _limit?: number;
  private _offset?: number;

  constructor(table: string, dataCol: string, db: { select: DBSelect }, query?: QueryObject) {
    this.table = table;
    this.dataCol = dataCol;
    this.db = db;
    this.query = query;
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
    // Build SQL
    let sql = `SELECT ${this.dataCol} FROM ${this.table}`;
    let params: any[] = [];
    if (this.query) {
      const where = queryCompiler(this.query, this.dataCol);
      if (where.sql) {
        sql += ` WHERE ${where.sql}`;
        params = where.params;
      }
    }
    if (this._order.length) {
      sql +=
        " ORDER BY " +
        this._order
          .map(([f, d]) => {
            if (typeof f === "object" && f !== null && "$" in f) {
              // JsonPath: use json_extract
              return `json_extract(${this.dataCol}, '$.${f.$}') ${d.toUpperCase()}`;
            } else {
              return `${f} ${d.toUpperCase()}`;
            }
          })
          .join(", ");
    }
    if (this._limit !== undefined) {
      sql += ` LIMIT ${this._limit}`;
    }
    if (this._offset !== undefined) {
      sql += ` OFFSET ${this._offset}`;
    }
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
    // Like all(), but LIMIT 1 and return first result or undefined
    let sql = `SELECT ${this.dataCol} FROM ${this.table}`;
    let params: any[] = [];
    if (this.query) {
      const where = queryCompiler(this.query, this.dataCol);
      if (where.sql) {
        sql += ` WHERE ${where.sql}`;
        params = where.params;
      }
    }
    if (this._order.length) {
      sql +=
        " ORDER BY " +
        this._order
          .map(([f, d]) => {
            if (typeof f === "object" && f !== null && "$" in f) {
              return `json_extract(${this.dataCol}, '$.${f.$}') ${d.toUpperCase()}`;
            } else {
              return `${f} ${d.toUpperCase()}`;
            }
          })
          .join(", ");
    }
    sql += ` LIMIT 1`;
    for await (const row of this.db.select(sql, params)) {
      const val = row[this.dataCol];
      if (val === undefined || val === null) continue;
      if (typeof val === "string") return JSON.parse(val);
      return val;
    }
    return undefined;
  }

  async count(): Promise<number> {
    // Build count SQL
    let sql = `SELECT COUNT(*) as count FROM ${this.table}`;
    let params: any[] = [];
    if (this.query) {
      const where = queryCompiler(this.query, this.dataCol);
      if (where.sql) {
        sql += ` WHERE ${where.sql}`;
        params = where.params;
      }
    }
    for await (const row of this.db.select(sql, params)) {
      return row.count ?? 0;
    }
    return 0;
  }
  iterate(): AsyncIterable<TDATA> {
    // Build SQL
    let sql = `SELECT ${this.dataCol} FROM ${this.table}`;
    let params: any[] = [];
    if (this.query) {
      const where = queryCompiler(this.query, this.dataCol);
      if (where.sql) {
        sql += ` WHERE ${where.sql}`;
        params = where.params;
      }
    }
    if (this._order.length) {
      sql +=
        " ORDER BY " +
        this._order
          .map(([f, d]) => {
            if (typeof f === "object" && f !== null && "$" in f) {
              // JsonPath: use json_extract
              return `json_extract(${this.dataCol}, '$.${f.$}') ${d.toUpperCase()}`;
            } else {
              return `${f} ${d.toUpperCase()}`;
            }
          })
          .join(", ");
    }
    if (this._limit !== undefined) {
      sql += ` LIMIT ${this._limit}`;
    }
    if (this._offset !== undefined) {
      sql += ` OFFSET ${this._offset}`;
    }
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
}
