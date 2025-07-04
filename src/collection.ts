// --- QueryBuilder for Collection ---
import { QueryBuilder, type QueryObject } from "./query-builder";

// --- Collection Class Implementation ---
type CollectionOptions<TID = any, TDATA = any> = {
  idColumn?: string;
  idType?: string; // Full SQL type string, e.g. "INTEGER PRIMARY KEY", "TEXT UNIQUE", etc.
  idGenerate?: (data: TDATA) => TID;
  dataColumn?: string;
  dataFormat?: "JSON" | "JSONB"; // Default to "JSON" for better compatibility & index support
};

export class Collection<TID, TDATA> {
  private table: string;
  private idCol: { column: string; type: string; generate?: (data: TDATA) => TID };
  private dataCol: { column: string; type: "JSON" | "JSONB" };
  private db: { execute: DBExecute; select: DBSelect; get: DBGet; lastInsertRowid: DBLastInsertRowid<TID> };

  constructor(
    table: string,
    db: { execute: DBExecute; select: DBSelect; get: DBGet; lastInsertRowid: DBLastInsertRowid<TID> },
    options: CollectionOptions<TID, TDATA> = {}
  ) {
    this.table = table;
    // Use user-supplied idType or default to "INTEGER PRIMARY KEY"
    let generate = options.idGenerate;
    let idType = options.idType || "INTEGER PRIMARY KEY";
    this.idCol = {
      column: options.idColumn || "id",
      type: idType,
      generate,
    };
    // For JSON, use TEXT (or JSON as alias); for JSONB, use BLOB
    const format = (options.dataFormat || "JSON").toUpperCase() as "JSON" | "JSONB";
    this.dataCol = {
      column: options.dataColumn || "data",
      type: format,
    };
    this.db = db;
  }

  async get(id: TID): Promise<TDATA | undefined>;
  async get(ids: TID[]): Promise<(TDATA | undefined)[]>;
  async get(idOrIds: TID | TID[]): Promise<TDATA | undefined | (TDATA | undefined)[]> {
    if (Array.isArray(idOrIds)) {
      if (idOrIds.length === 0) return [];
      const placeholders = idOrIds.map(() => "?").join(", ");
      let sql: string;
      if (this.dataCol.type === "JSONB") {
        sql = `SELECT ${this.idCol.column}, json(${this.dataCol.column}) as data FROM ${this.table} WHERE ${this.idCol.column} IN (${placeholders})`;
      } else {
        sql = `SELECT ${this.idCol.column}, ${this.dataCol.column} as data FROM ${this.table} WHERE ${this.idCol.column} IN (${placeholders})`;
      }
      const rowMap = new Map<any, any>();
      const iter = this.db.select(sql, idOrIds);
      for await (const row of iter) {
        let val = row["data"];
        if (val !== undefined && val !== null && typeof val === "string") val = JSON.parse(val);
        rowMap.set(row[this.idCol.column], val);
      }
      return idOrIds.map((id) => (rowMap.has(id) ? rowMap.get(id) : undefined));
    } else {
      let sql: string;
      if (this.dataCol.type === "JSONB") {
        sql = `SELECT json(${this.dataCol.column}) as data FROM ${this.table} WHERE ${this.idCol.column} = ?`;
      } else {
        sql = `SELECT ${this.dataCol.column} as data FROM ${this.table} WHERE ${this.idCol.column} = ?`;
      }
      const row = await this.db.get(sql, [idOrIds]);
      if (!row) return undefined;
      const val = row["data"];
      if (val === undefined || val === null) return undefined;
      if (typeof val === "string") return JSON.parse(val);
      return val;
    }
  }

  async set(id: TID, data: TDATA): Promise<void> {
    let sql: string;
    let value: any;
    if (this.dataCol.type === "JSONB") {
      sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, jsonb(?))`;
      value = JSON.stringify(data);
    } else {
      sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, json(?))`;
      value = JSON.stringify(data);
    }
    await this.db.execute(sql, [id, value]);
  }

  async insert(data: TDATA): Promise<TID> {
    let sql: string;
    let value: any;
    if (this.dataCol.type === "JSONB") {
      value = JSON.stringify(data);
      if (this.idCol.generate) {
        const generatedId = this.idCol.generate(data);
        sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, jsonb(?))`;
        await this.db.execute(sql, [generatedId, value]);
        return generatedId;
      } else {
        sql = `INSERT INTO ${this.table} (${this.dataCol.column}) VALUES (jsonb(?))`;
        const result = await this.db.execute(sql, [value]);
        return await this.db.lastInsertRowid(result);
      }
    } else {
      value = JSON.stringify(data);
      if (this.idCol.generate) {
        const generatedId = this.idCol.generate(data);
        sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, json(?))`;
        await this.db.execute(sql, [generatedId, value]);
        return generatedId;
      } else {
        sql = `INSERT INTO ${this.table} (${this.dataCol.column}) VALUES (json(?))`;
        const result = await this.db.execute(sql, [value]);
        return await this.db.lastInsertRowid(result);
      }
    }
  }

  find(query?: QueryObject) {
    // Use json_extract for JSON, jsonb_extract for JSONB
    const jsonExtract = this.dataCol.type === "JSONB" ? "jsonb_extract" : "json_extract";
    return new QueryBuilder<TDATA>(this.table, this.dataCol.column, { select: this.db.select }, query, jsonExtract);
  }

  async index(name: string, expr: any, options: { unique?: boolean; type?: string; order?: "ASC" | "DESC" } = {}): Promise<void> {
    // Accepts any valid index expression (field path, fn, cast, arithmetic, etc.)
    // For backward compatibility, allow string or { $: string } as a field selector
    let indexExpr = expr;
    if (typeof expr === "string") {
      indexExpr = { $: expr };
    }
    // Validate input: must be string, { $: string }, or a supported operator object
    function isValidIndexExpr(e: any): boolean {
      if (typeof e === "string") return true;
      if (e && typeof e === "object") {
        if (typeof e.$ === "string") return true;
        if (e.$fn && Array.isArray(e.$fn) && typeof e.$fn[0] === "string") return true;
        if (e.$cast && Array.isArray(e.$cast) && e.$cast.length === 2) return true;
        if (e.$add && Array.isArray(e.$add) && e.$add.length === 2) return true;
        if (e.$sub && Array.isArray(e.$sub) && e.$sub.length === 2) return true;
        if (e.$mul && Array.isArray(e.$mul) && e.$mul.length === 2) return true;
        if (e.$div && Array.isArray(e.$div) && e.$div.length === 2) return true;
      }
      return false;
    }
    if (!isValidIndexExpr(expr)) {
      throw new Error("Invalid index expression: must be a string, field path, function, cast, or arithmetic expression");
    }
    // Use the index-compiler to generate the SQL expression
    const { compileIndexExpression } = await import("./index-compiler.js");
    const jsonExtract = this.dataCol.type === "JSONB" ? "jsonb_extract" : "json_extract";
    let compiled: string = compileIndexExpression(indexExpr, jsonExtract, this.dataCol.column);
    const unique = options.unique ? "UNIQUE" : "";
    const type = options.type ? `USING ${options.type}` : "";
    const order = options.order ? ` ${options.order}` : "";
    const sql = `CREATE ${unique} INDEX IF NOT EXISTS ${name} ON ${this.table} (${compiled}${order}) ${type}`;
    await this.db.execute(sql);
  }
}

// ID Column interface
export interface IdColumn<TID = any, TDATA = any> {
  column: string; // e.g. "id"
  type: string; // e.g. "INTEGER PRIMARY KEY", "TEXT UNIQUE", etc.
  generate?: (data: TDATA) => TID; // Optional function to generate ID from data
}

// --- Base Collection Builder ---
export type DBExecute = (sql: string, args?: any[]) => Promise<any>;
export type DBSelect = (sql: string, args?: any[]) => AsyncIterable<any>;
export type DBGet = (sql: string, args?: any[]) => Promise<any>;
export type DBLastInsertRowid<Id> = (result: any) => Promise<Id> | Id;

// --- Collection Factory ---
export async function createCollection<TDATA = any, TID = number>(
  table: string,
  db: { execute: DBExecute; select: DBSelect; get: DBGet; lastInsertRowid: DBLastInsertRowid<TID> },
  options: CollectionOptions<TID, TDATA> = {}
): Promise<Collection<TID, TDATA>> {
  // Create table if not exists
  let generate = options.idGenerate;
  let idType = options.idType || "INTEGER PRIMARY KEY";
  const idCol = {
    column: options.idColumn || "id",
    type: idType,
    generate,
  };
  // For JSON, use TEXT (or JSON as alias); for JSONB, use BLOB
  const format = (options.dataFormat || "JSON").toUpperCase() as "JSON" | "JSONB";
  const dataCol = {
    column: options.dataColumn || "data",
    type: format,
  };
  let dataColType: string;
  if (format === "JSONB") {
    dataColType = "BLOB";
  } else {
    dataColType = "JSON"; // or TEXT, but JSON is an alias for TEXT
  }
  const createSQL = `CREATE TABLE IF NOT EXISTS ${table} (
    ${idCol.column} ${idCol.type},
    ${dataCol.column} ${dataColType}
  )`;
  await db.execute(createSQL);
  // Check table schema for id and data columns
  const pragmaSQL = `PRAGMA table_info(${table})`;
  const columns: Array<{ name: string; type: string; pk: number }> = [];
  for await (const row of db.select(pragmaSQL)) {
    columns.push({ name: row.name, type: row.type, pk: row.pk });
  }
  const idColDef = columns.find((c) => c.name === idCol.column);
  const dataColDef = columns.find((c) => c.name === dataCol.column);
  // Compare only base type for id column, and check pk if PRIMARY KEY is in idType
  function baseType(type: string) {
    return type.split(" ")[0].toUpperCase();
  }
  const expectedIdBaseType = baseType(idCol.type);
  const actualIdBaseType = idColDef ? baseType(idColDef.type) : undefined;
  if (!idColDef || actualIdBaseType !== expectedIdBaseType) {
    throw new Error(
      `Table '${table}' id column '${idCol.column}' type mismatch: expected base type '${expectedIdBaseType}', found '${idColDef ? idColDef.type : "none"}'`
    );
  }
  if (idCol.type.toUpperCase().includes("PRIMARY KEY") && idColDef.pk !== 1) {
    throw new Error(`Table '${table}' id column '${idCol.column}' is not PRIMARY KEY as expected.`);
  }
  // Accept BLOB for JSONB, JSON (or TEXT) for JSON
  if (!dataColDef) {
    throw new Error(`Table '${table}' data column '${dataCol.column}' type mismatch: expected '${dataCol.type}', found 'none'`);
  }
  if (dataCol.type === "JSONB") {
    if (dataColDef.type.toUpperCase() !== "BLOB") {
      throw new Error(`Table '${table}' data column '${dataCol.column}' type mismatch: expected 'BLOB', found '${dataColDef.type}'`);
    }
  } else {
    if (dataColDef.type.toUpperCase() !== "JSON" && dataColDef.type.toUpperCase() !== "TEXT") {
      throw new Error(`Table '${table}' data column '${dataCol.column}' type mismatch: expected 'JSON' or 'TEXT', found '${dataColDef.type}'`);
    }
  }
  return new Collection<TID, TDATA>(table, db, options);
}
