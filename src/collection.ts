// --- QueryBuilder for Collection ---
import { QueryBuilder, type QueryObject } from "./query-builder";

// --- Collection Class Implementation ---
type CollectionOptions<TID = any, TDATA = any> = {
  idColumn?: string;
  idType?: string; // Full SQL type string, e.g. "INTEGER PRIMARY KEY", "TEXT UNIQUE", etc.
  idGenerate?: (data: TDATA) => TID;
  dataColumn?: string;
  dataFormat?: "JSON" | "JSONB";
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
  const dataCol = {
    column: options.dataColumn || "data",
    type: (options.dataFormat || "JSON").toUpperCase() as "JSON" | "JSONB",
  };
  const createSQL = `CREATE TABLE IF NOT EXISTS ${table} (
    ${idCol.column} ${idCol.type},
    ${dataCol.column} ${dataCol.type}
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
  if (!dataColDef || dataColDef.type.toUpperCase() !== dataCol.type.toUpperCase()) {
    throw new Error(
      `Table '${table}' data column '${dataCol.column}' type mismatch: expected '${dataCol.type}', found '${dataColDef ? dataColDef.type : "none"}'`
    );
  }
  return new Collection<TID, TDATA>(table, db, options);
}
