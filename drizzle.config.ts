import { defineConfig } from "drizzle-kit";

export default defineConfig({
  out: "./src/drizzle",
  schema: "./src/do/repo/db/schema.ts",
  dialect: "sqlite",
  driver: "durable-sqlite",
});
