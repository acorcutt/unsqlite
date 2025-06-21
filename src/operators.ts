// --- Minimal Query Builder Primitives ---

// Field path selector
export type FieldPath = { $: string };

export function $(path: string): FieldPath {
  return { $: path };
}

// eq operator: eq(a, b) => { $eq: [a, b] }
export function eq(a: any, b: any) {
  return { $eq: [a, b] };
}

// ne operator: ne(a, b) => { $ne: [a, b] }
export function ne(a: any, b: any) {
  return { $ne: [a, b] };
}

// gt operator: gt(a, b) => { $gt: [a, b] }
export function gt(a: any, b: any) {
  return { $gt: [a, b] };
}

// gte operator: gte(a, b) => { $gte: [a, b] }
export function gte(a: any, b: any) {
  return { $gte: [a, b] };
}

// lt operator: lt(a, b) => { $lt: [a, b] }
export function lt(a: any, b: any) {
  return { $lt: [a, b] };
}

// lte operator: lte(a, b) => { $lte: [a, b] }
export function lte(a: any, b: any) {
  return { $lte: [a, b] };
}

// and operator: and(...args) => { $and: [ ...args ] }
export function and(...args: any[]) {
  return { $and: args };
}

// or operator: or(...args) => { $or: [ ...args ] }
export function or(...args: any[]) {
  return { $or: args };
}

// not operator: not(arg) => { $not: [ arg ] }
export function not(arg: any) {
  return { $not: [arg] };
}
