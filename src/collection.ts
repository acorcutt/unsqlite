// --- QueryBuilder for Collection ---
import { QueryBuilder, type QueryObject } from "./query-builder";

// --- Collection Class Implementation ---
type IdType = "INTEGER" | "STRING";
type CollectionOptions<TID = number | string, TDATA = any> = {
  idColumn?: string;
  idType?: IdType;
  idGenerate?: (data: TDATA) => TID;
  dataColumn?: string;
  dataFormat?: "JSON" | "JSONB";
};

class Collection<TID, TDATA> {
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
    // Determine SQL type and generator based on idType
    const idType: IdType = options.idType || "INTEGER";
    let sqlType: string;
    let generate = options.idGenerate;
    if (idType === "INTEGER") {
      sqlType = "INTEGER PRIMARY KEY";
    } else if (idType === "STRING") {
      sqlType = "TEXT PRIMARY KEY";
      if (!generate) {
        // Default to uuid generator for string ids
        generate = () => crypto.randomUUID() as any;
      }
    } else {
      throw new Error(`Unsupported idType: ${idType}`);
    }
    this.idCol = {
      column: options.idColumn || "id",
      type: sqlType,
      generate,
    };
    this.dataCol = {
      column: options.dataColumn || "data",
      type: (options.dataFormat || "JSON").toUpperCase() as "JSON" | "JSONB",
    };
    this.db = db;
  }

  async get(id: TID): Promise<TDATA | undefined>;
  async get(ids: TID[]): Promise<(TDATA | undefined)[]>;
  async get(idOrIds: TID | TID[]): Promise<TDATA | undefined | (TDATA | undefined)[]> {
    if (Array.isArray(idOrIds)) {
      if (idOrIds.length === 0) return [];
      const placeholders = idOrIds.map(() => "?").join(", ");
      const sql = `SELECT ${this.idCol.column}, ${this.dataCol.column} FROM ${this.table} WHERE ${this.idCol.column} IN (${placeholders})`;
      const rowMap = new Map<any, any>();
      const iter = this.db.select(sql, idOrIds);
      for await (const row of iter) {
        let val = row[this.dataCol.column];
        if (val !== undefined && val !== null && typeof val === "string") val = JSON.parse(val);
        rowMap.set(row[this.idCol.column], val);
      }
      return idOrIds.map((id) => (rowMap.has(id) ? rowMap.get(id) : undefined));
    } else {
      const sql = `SELECT ${this.dataCol.column} FROM ${this.table} WHERE ${this.idCol.column} = ?`;
      const row = await this.db.get(sql, [idOrIds]);
      if (!row) return undefined;
      const val = row[this.dataCol.column];
      if (val === undefined || val === null) return undefined;
      if (typeof val === "string") return JSON.parse(val);
      return val;
    }
  }

  async set(id: TID, data: TDATA): Promise<void> {
    const sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, ?)`;
    const value = JSON.stringify(data);
    await this.db.execute(sql, [id, value]);
  }

  async insert(data: TDATA): Promise<TID> {
    if (this.idCol.generate) {
      const generatedId = this.idCol.generate(data);
      const sql = `INSERT OR REPLACE INTO ${this.table} (${this.idCol.column}, ${this.dataCol.column}) VALUES (?, ?)`;
      const value = JSON.stringify(data);
      await this.db.execute(sql, [generatedId, value]);
      return generatedId;
    } else {
      const sql = `INSERT INTO ${this.table} (${this.dataCol.column}) VALUES (?)`;
      const value = JSON.stringify(data);
      const result = await this.db.execute(sql, [value]);
      return await this.db.lastInsertRowid(result);
    }
  }

  find(query?: QueryObject) {
    return new QueryBuilder<TDATA>(this.table, this.dataCol.column, { select: this.db.select }, query);
  }
}

// ID Column interface
export interface IdColumn<TID = any, TDATA = any> {
  column: string; // e.g. "id"
  type: string; // e.g. "INTEGER PRIMARY KEY"
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
  // Determine SQL type and generator based on idType
  const idType: IdType = options.idType || "INTEGER";
  let sqlType: string;
  let generate = options.idGenerate;
  if (idType === "INTEGER") {
    sqlType = "INTEGER PRIMARY KEY";
    // If no generator, auto-increment is handled by SQLite
  } else if (idType === "STRING") {
    sqlType = "TEXT PRIMARY KEY";
    if (!generate) {
      // Default to uuid generator for string ids
      generate = () => crypto.randomUUID() as any;
    }
  } else {
    throw new Error(`Unsupported idType: ${idType}`);
  }
  const idCol = {
    column: options.idColumn || "id",
    type: sqlType,
    generate,
  };
  const dataCol = {
    column: options.dataColumn || "data",
    type: (options.dataFormat || "JSON").toUpperCase() as "JSON" | "JSONB",
  };
  const createSQL = `CREATE TABLE IF NOT EXISTS ${table} (
    ${idCol.column} ${idCol.type},
    ${dataCol.column} ${dataCol.type}
  )`;
  await db.execute(createSQL);
  return new Collection<TID, TDATA>(table, db, options);
}
