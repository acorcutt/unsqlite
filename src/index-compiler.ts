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

function compileFieldPath(field: FieldPath): string {
  return `json_extract(data, '$.${field.$}')`;
}

function sqlLiteral(val: any): string {
  if (typeof val === "string") return `'${val.replace(/'/g, "''")}'`;
  if (val === null) return "NULL";
  if (typeof val === "boolean") return val ? "1" : "0";
  return String(val);
}

export function compileIndexExpression(expr: IndexExpr): string {
  // FieldPath
  if (expr && typeof expr === "object" && "$" in expr) {
    return compileFieldPath(expr as FieldPath);
  }
  // Function call
  if (expr && typeof expr === "object" && "$fn" in expr) {
    const [fn, ...args] = expr.$fn;
    return `${fn}(${args.map(compileIndexExpression).join(", ")})`;
  }
  // Type cast
  if (expr && typeof expr === "object" && "$cast" in expr) {
    const [arg, type] = expr.$cast;
    return `CAST(${compileIndexExpression(arg)} AS ${type})`;
  }
  // Arithmetic
  if (expr && typeof expr === "object") {
    if ("$add" in expr) {
      const [a, b] = expr.$add;
      return `(${compileIndexExpression(a)} + ${compileIndexExpression(b)})`;
    }
    if ("$sub" in expr) {
      const [a, b] = expr.$sub;
      return `(${compileIndexExpression(a)} - ${compileIndexExpression(b)})`;
    }
    if ("$mul" in expr) {
      const [a, b] = expr.$mul;
      return `(${compileIndexExpression(a)} * ${compileIndexExpression(b)})`;
    }
    if ("$div" in expr) {
      const [a, b] = expr.$div;
      return `(${compileIndexExpression(a)} / ${compileIndexExpression(b)})`;
    }
  }
  // Literal value
  return sqlLiteral(expr);
}
