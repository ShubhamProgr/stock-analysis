import { NextResponse } from "next/server";
import { getScreenerData } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  try {
    const rows = await getScreenerData();
    return NextResponse.json({ rows });
  } catch (err) {
    console.error("GET /api/screener failed", err);
    return NextResponse.json({ error: "Failed to load screener data" }, { status: 500 });
  }
}
