// build.ts - Bun build script for unsqlite
const adapters = [
  "./src/index.ts",
  "./src/adapters/bun.ts",
  "./src/adapters/libsql.ts",
  //Add more adapters here as needed
];

const status = await Bun.build({
  entrypoints: adapters,
  outdir: "./dist",
  target: "node",
});

console.log("Build complete.");
if (status.outputs) {
  for (const output of status.outputs) {
    const size = output.size.toLocaleString();
    console.log(`${output.path}  ${size} bytes`);
  }
}
