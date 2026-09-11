import { NextResponse } from "next/server";
import { getSentimentCalendar } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(
  request: Request,
  { params }: { params: Promise<{ ticker: string }> }
) {
  try {
    const resolvedParams = await params;
    const t = resolvedParams.ticker;
    if (!t) return NextResponse.json({ error: "No ticker provided" }, { status: 400 });

    const { searchParams } = new URL(request.url);
    const days = parseInt(searchParams.get("days") ?? "365", 10);

    const data = await getSentimentCalendar(t, days);
    return NextResponse.json(data);
  } catch (err) {
    console.error("GET /api/sentiment/calendar/[ticker] failed", err);
    return NextResponse.json({ error: "Failed to load sentiment calendar" }, { status: 500 });
  }
}

