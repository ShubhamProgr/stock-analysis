import { Pool } from "pg";
import dotenv from "dotenv";
import path from "path";

// Explicitly load the root .env file to replicate Docker's injection
dotenv.config({ path: path.join(process.cwd(), '../.env') });

declare global {
  var _pgPool: Pool | undefined;
}

function createPool() {
  const connectionString = process.env.DATABASE_URL;
  
  if (!connectionString) {
    throw new Error("DATABASE_URL is not set – check root .env file");
  }
  
  return new Pool({
    connectionString,
    ssl: { rejectUnauthorized: false },
    max: 5,
  });
}

// Reuse the pool across hot reloads in dev so we don't exhaust the Supabase pooler
export const pool = global._pgPool ?? createPool();

if (process.env.NODE_ENV !== "production") {
  global._pgPool = pool;
}

export async function query<T = any>(sql: string, params?: any[]) {
  const result = await pool.query(sql, params);
  return result.rows as T[];
}