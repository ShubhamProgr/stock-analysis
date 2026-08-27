import { NextRequest, NextResponse } from "next/server";
import { getComparisonBundle } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(req: NextRequest) {
  const tickersParam = req.nextUrl.searchParams.get("tickers");
  const rangeParam = req.nextUrl.searchParams.get("range");

  if (!tickersParam) {
    return NextResponse.json({ error: "tickers parameter required" }, { status: 400 });
  }

  const tickers = tickersParam.split(",").map((t) => t.trim()).filter(Boolean).slice(0, 3);
  const rangeDays = rangeParam ? parseInt(rangeParam, 10) : 126;

  try {
    const bundle = await getComparisonBundle(tickers, rangeDays);
    return NextResponse.json(bundle);
  } catch (err) {
    console.error("GET /api/compare failed", err);
    return NextResponse.json({ error: "Failed to load comparison data" }, { status: 500 });
  }
}
