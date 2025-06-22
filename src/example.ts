// Import your db
import { Database } from "bun:sqlite";

// Import the Bun adapter factory
import { createBunAdapter } from "./adapters/bun";
import { $, gt } from "./operators";

// Define your data type (optional)
interface UserType {
  name: string;
  value: number;
}

// Setup your db
const db = new Database(":memory:");

// Create a collection with the table name "users"
const users = await createBunAdapter(db).collection<UserType>("users");

// Just insert data
const id1 = await users.insert({ name: "Alice", value: 1 });
const id2 = await users.insert({ name: "Bob", value: 2 });
await users.insert({ name: "Charlie", value: 3 });
await users.insert({ name: "Dave", value: 4 });
await users.insert({ name: "Eve", value: 5 });
await users.insert({ name: "Frank", value: 6 });
await users.insert({ name: "Grace", value: 7 });
await users.insert({ name: "Heidi", value: 8 });

// Update an item
await users.set(id1, { name: "Alice", value: 10 });

// Get item(s)
const results = await users.get([id1, id2]);

console.log(results);

// Get multiple items with a query where value is greater than 5
const query = users.find(gt($("value"), 5)).order($("value"), "desc");

console.log("Query: ", await query.all(), "Count: ", await query.count());

// Iterate over the query results
for await (const user of query.iterate()) {
  console.log("Iterating User: ", user);
}

//Explain query
console.log("Explain: ", await query.explain());
