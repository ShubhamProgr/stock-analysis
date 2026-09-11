import { NextRequest, NextResponse } from "next/server";
import { getPeerComparison } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(req: NextRequest, { params }: { params: Promise<{ ticker: string }> }) {
  const { ticker } = await params;

  try {
    const peers = await getPeerComparison(decodeURIComponent(ticker));
    return NextResponse.json({ peers });
  } catch (err) {
    console.error(`GET /api/peers/${ticker} failed`, err);
    return NextResponse.json({ error: "Failed to load peer data" }, { status: 500 });
  }
}
