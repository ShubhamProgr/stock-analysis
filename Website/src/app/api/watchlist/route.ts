import { NextResponse } from "next/server";
import { getWatchlist } from "@/lib/queries";

export async function GET() {
  try {
    const rows = await getWatchlist();
    return NextResponse.json({ rows });
  } catch (err) {
    console.error("GET /api/watchlist failed", err);
    return NextResponse.json({ error: "Failed to load watchlist" }, { status: 500 });
  }
}
