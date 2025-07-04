# unsqlite

Turn your SQLite database into a NoSQL database!

## Usage

```typescript
import { Database } from "bun:sqlite";
import { createBunAdapter } from "unsqlite";

// Define your data type (optional)
interface UserType {
  name: string;
  value: number;
}

// Setup your db
const db = new Database(":memory:");

// Create a collection with the table name "users"
const users = await createBunAdapter(db).collection<UserType>("users");

// Insert data
const id1 = await users.insert({ name: "Alice", value: 1 });
const id2 = await users.insert({ name: "Bob", value: 2 });
await users.insert({ name: "Charlie", value: 3 });

// Update an item
await users.set(id1, { name: "Alice", value: 10 });

// Get item(s)
const results = await users.get([id1, id2]);
console.log(results);
```

## Query Example

```typescript
import { createClient } from "@libsql/client";
import { createLibSQLAdapter } from "unsqlite";
import { operators } from "unsqlite";
const { $, gt } = operators;

// Define your data type (optional)
interface UserType {
  name: string;
  value: number;
}

const db = createClient({ url: ":memory:" });
const users = await createLibSQLAdapter(db).collection<UserType>("users");
// Add an index on the "value" field
await users.createIndex("value_index", $("value"));

await users.insert({ name: "Alice", value: 1 });
await users.insert({ name: "Bob", value: 2 });
await users.insert({ name: "Charlie", value: 3 });
await users.insert({ name: "Dave", value: 4 });
await users.insert({ name: "Eve", value: 5 });

// Find users with value greater than 1, ordered by value descending
const query = users.find(gt($("value"), 1)).order($("value"), "desc");
for await (const user of query.iterate()) {
  console.log(user);
}

const count = await query.count();
const explain = await query.explain();
console.log(`Total users with value > 1: ${count}`, explain);
```
