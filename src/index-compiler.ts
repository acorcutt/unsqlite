// Index Compiler: Converts a field path or supported operator expression into a valid SQLite index expression string.

// Supported input:
// - FieldPath: { $: 'field.nested' }
// - Function call: { $fn: [fnName, ...args] }
// - Type cast: { $cast: [expr, type] }
// - Arithmetic: { $add: [a, b] }, { $sub: [a, b] }, { $mul: [a, b] }, { $div: [a, b] }
//
// Example:
//   compileIndexExpression({ $: 'foo.bar' })
//   => "json_extract(data, '$.foo.bar')"
//   compileIndexExpression({ $fn: ['lower', { $: 'foo' }] })
//   => "lower(json_extract(data, '$.foo'))"
//   compileIndexExpression({ $cast: [{ $: 'foo' }, 'TEXT'] })
//   => "CAST(json_extract(data, '$.foo') AS TEXT)"

type FieldPath = { $: string };
type FnCall = { $fn: [string, ...any[]] };
type Cast = { $cast: [any, string] };
type Arithmetic = { $add: [any, any] } | { $sub: [any, any] } | { $mul: [any, any] } | { $div: [any, any] };

type IndexExpr = FieldPath | FnCall | Cast | Arithmetic | any;

function compileFieldPath(field: FieldPath, jsonExtract: string, jsonCol: string): string {
  return `${jsonExtract}(${jsonCol}, '$.${field.$}')`;
}

function sqlLiteral(val: any): string {
  if (typeof val === "string") return `'${val.replace(/'/g, "''")}'`;
  if (val === null) return "NULL";
  if (typeof val === "boolean") return val ? "1" : "0";
  return String(val);
}

export function compileIndexExpression(expr: IndexExpr, jsonExtract: string = "json_extract", jsonCol: string = "data"): string {
  // FieldPath
  if (expr && typeof expr === "object" && "$" in expr) {
    return compileFieldPath(expr as FieldPath, jsonExtract, jsonCol);
  }
  // Function call
  if (expr && typeof expr === "object" && "$fn" in expr) {
    const [fn, ...args] = expr.$fn;
    return `${fn}(${args.map((a) => compileIndexExpression(a, jsonExtract, jsonCol)).join(", ")})`;
  }
  // Type cast
  if (expr && typeof expr === "object" && "$cast" in expr) {
    const [arg, type] = expr.$cast;
    return `CAST(${compileIndexExpression(arg, jsonExtract, jsonCol)} AS ${type})`;
  }
  // Arithmetic
  if (expr && typeof expr === "object") {
    if ("$add" in expr) {
      const [a, b] = expr.$add;
      return `(${compileIndexExpression(a, jsonExtract, jsonCol)} + ${compileIndexExpression(b, jsonExtract, jsonCol)})`;
    }
    if ("$sub" in expr) {
      const [a, b] = expr.$sub;
      return `(${compileIndexExpression(a, jsonExtract, jsonCol)} - ${compileIndexExpression(b, jsonExtract, jsonCol)})`;
    }
    if ("$mul" in expr) {
      const [a, b] = expr.$mul;
      return `(${compileIndexExpression(a, jsonExtract, jsonCol)} * ${compileIndexExpression(b, jsonExtract, jsonCol)})`;
    }
    if ("$div" in expr) {
      const [a, b] = expr.$div;
      return `(${compileIndexExpression(a, jsonExtract, jsonCol)} / ${compileIndexExpression(b, jsonExtract, jsonCol)})`;
    }
  }
  // Literal value
  return sqlLiteral(expr);
}
