import { NextRequest, NextResponse } from "next/server";
import { getTickerBundle } from "@/lib/queries";

export async function GET(req: NextRequest, { params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;
  const rangeParam = req.nextUrl.searchParams.get("range");
  const rangeDays = rangeParam ? parseInt(rangeParam, 10) : 126;

  try {
    const bundle = await getTickerBundle(decodeURIComponent(ticker), rangeDays);
    if (!bundle) {
      return NextResponse.json({ error: "Ticker not found" }, { status: 404 });
    }
    return NextResponse.json(bundle);
  } catch (err) {
    console.error(`GET /api/ticker/${ticker} failed`, err);
    return NextResponse.json({ error: "Failed to load ticker data" }, { status: 500 });
  }
}
