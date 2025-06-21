// Compile a query object (using new operator format) to SQL and params
export function queryCompiler(query: any, jsonCol = "data", jsonExtract = "json_extract"): { sql: string; params: any[] } {
  function expr(val: any): string {
    // If it's a field selector, use json_extract
    if (val && typeof val === "object" && "$" in val) {
      return `${jsonExtract}(${jsonCol}, '$.${val.$}')`;
    }
    // Otherwise, it's a literal (will be a parameter)
    return "?";
  }

  function walk(q: any): { sql: string; params: any[] } {
    if (!q || typeof q !== "object") throw new Error("Invalid query");
    const keys = Object.keys(q);
    if (keys.length !== 1) throw new Error("Query object must have exactly one operator");
    const op = keys[0];
    if (!op) throw new Error("Query operator is missing");
    const val = (q as Record<string, any>)[op];

    switch (op) {
      case "$eq":
      case "$ne":
      case "$gt":
      case "$gte":
      case "$lt":
      case "$lte": {
        if (!Array.isArray(val) || val.length !== 2) throw new Error(`${op} expects [field, value]`);
        const [a, b] = val;
        // If a or b is a field selector, use expr(a) and expr(b)
        // If not, use ? and push to params
        let sql = "";
        let params: any[] = [];
        const opMap: Record<string, string> = {
          $eq: "=",
          $ne: "!=",
          $gt: ">",
          $gte: ">=",
          $lt: "<",
          $lte: "<=",
        };
        const opSql = opMap[op];
        const left = expr(a);
        const right = expr(b);
        sql = `${left} ${opSql} ${right}`;
        if (left === "?") params.push(a);
        if (right === "?") params.push(b);
        return { sql, params };
      }
      case "$and": {
        if (!Array.isArray(val)) throw new Error("$and expects array");
        const parts = val.map(walk);
        return {
          sql: parts.map((p) => `(${p.sql})`).join(" AND "),
          params: parts.flatMap((p) => p.params),
        };
      }
      case "$or": {
        if (!Array.isArray(val)) throw new Error("$or expects array");
        const parts = val.map(walk);
        return {
          sql: parts.map((p) => `(${p.sql})`).join(" OR "),
          params: parts.flatMap((p) => p.params),
        };
      }
      case "$not": {
        if (!Array.isArray(val) || val.length !== 1) throw new Error("$not expects single-element array");
        const inner = walk(val[0]);
        return { sql: `NOT (${inner.sql})`, params: inner.params };
      }
      default:
        throw new Error(`Unknown operator: ${op}`);
    }
  }

  return walk(query);
}
