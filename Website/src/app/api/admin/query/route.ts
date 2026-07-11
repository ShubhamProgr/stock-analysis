import { NextResponse } from "next/server";
import { pool } from "@/lib/db";

const BLOCKED_KEYWORDS =
  /\b(insert|update|delete|drop|alter|truncate|grant|revoke|create|copy|call|do|vacuum|reindex|refresh)\b/i;

function sanitize(rawQuery: string): { ok: true; sql: string } | { ok: false; error: string } {
  const trimmed = rawQuery.trim().replace(/;+\s*$/, "");

  if (!trimmed) return { ok: false, error: "Query is empty." };
  if (trimmed.includes(";")) {
    return { ok: false, error: "Only a single statement is allowed (no semicolons)." };
  }
  if (!/^select\s/i.test(trimmed)) {
    return { ok: false, error: "Only SELECT statements are permitted from this console." };
  }
  if (BLOCKED_KEYWORDS.test(trimmed)) {
    return { ok: false, error: "Query contains a disallowed keyword." };
  }

  const hasLimit = /\blimit\s+\d+/i.test(trimmed);
  const sql = hasLimit ? trimmed : `${trimmed} limit 200`;
  return { ok: true, sql };
}

export async function POST(req: Request) {
  const { sql: rawQuery } = await req.json().catch(() => ({ sql: "" }));
  if (typeof rawQuery !== "string") {
    return NextResponse.json({ error: "Missing query." }, { status: 400 });
  }

  const sanitized = sanitize(rawQuery);
  if (!sanitized.ok) {
    return NextResponse.json({ error: sanitized.error }, { status: 400 });
  }

  const client = await pool.connect();
  try {
    await client.query("set statement_timeout = 5000");
    await client.query("set transaction read only");
    const result = await client.query(sanitized.sql);
    return NextResponse.json({
      columns: result.fields.map((f) => f.name),
      rows: result.rows
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Query failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  } finally {
    client.release();
  }
}
