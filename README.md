# unsqlite

Turn your SQLite database into a NoSQL database!

## Usage

```typescript
import { Database } from "bun:sqlite";
import { collection } from "unsqlite/adapters/bun";

// Define your data type (optional)
interface UserType {
  name: string;
  value: number;
}

// Setup your db
const db = new Database(":memory:");

// Create a collection with the table name "users"
const users = await collection<UserType>(db, "users");

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
import { $, gt } from "unsqlite/operators";

// Find users with value greater than 1, ordered by value descending
const query = users.find(gt($("value"), 1)).order($("value"), "desc");
for await (const user of query.iterate()) {
  console.log(user);
}

const count = await query.count();
console.log(`Total users with value > 1: ${count}`);
```
