import { NextRequest, NextResponse } from "next/server";
import { getOHLCData } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(req: NextRequest, { params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;
  const rangeParam = req.nextUrl.searchParams.get("range");
  const rangeDays = rangeParam ? parseInt(rangeParam, 10) : 126;

  try {
    const data = await getOHLCData(decodeURIComponent(ticker), rangeDays);
    return NextResponse.json({ data });
  } catch (err) {
    console.error(`GET /api/ohlc/${ticker} failed`, err);
    return NextResponse.json({ error: "Failed to load OHLC data" }, { status: 500 });
  }
}
