import { NextRequest, NextResponse } from "next/server";
import { getPredictions } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(req: NextRequest) {
  const date = req.nextUrl.searchParams.get("date") ?? undefined;

  try {
    const predictions = await getPredictions(date);
    return NextResponse.json({ predictions: predictions ?? [], date: date ?? null });
  } catch (err) {
    console.error("GET /api/predictions failed", err);
    return NextResponse.json({ error: "Failed to load predictions" }, { status: 500 });
  }
}