import { NextResponse } from "next/server";
import { getMarketOverview } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  try {
    const rows = await getMarketOverview();
    return NextResponse.json({ rows });
  } catch (err) {
    console.error("GET /api/market failed", err);
    return NextResponse.json({ error: "Failed to load market overview" }, { status: 500 });
  }
}
