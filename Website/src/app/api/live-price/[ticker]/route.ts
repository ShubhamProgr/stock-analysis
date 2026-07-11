import { NextResponse } from "next/server";
import { fetchLivePrice } from "@/lib/pipeline";

export async function GET(_req: Request, { params }: { params: { ticker: string } }) {
  const data = await fetchLivePrice(params.ticker);
  if (!data) {
    return NextResponse.json({ error: "Live price unavailable." }, { status: 502 });
  }
  return NextResponse.json(data);
}
