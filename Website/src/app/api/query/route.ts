import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";

export async function POST(req: NextRequest) {
  try {
    const { query } = await req.json();

    if (!query || typeof query !== "string") {
      return NextResponse.json({ error: "No query provided" }, { status: 400 });
    }

    // Basic safety check: only allow SELECT queries
    const upperQuery = query.trim().toUpperCase();
    if (
      upperQuery.includes("INSERT") ||
      upperQuery.includes("UPDATE") ||
      upperQuery.includes("DELETE") ||
      upperQuery.includes("DROP") ||
      upperQuery.includes("ALTER") ||
      upperQuery.includes("TRUNCATE")
    ) {
      return NextResponse.json(
        { error: "Only SELECT queries are allowed for safety." },
        { status: 403 }
      );
    }

    const result = await pool.query(query);

    return NextResponse.json({
      rows: result.rows,
      rowCount: result.rowCount,
      fields: result.fields.map((f) => f.name),
    });
  } catch (error: any) {
    console.error("Query Error:", error);
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}
