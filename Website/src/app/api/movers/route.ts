import { NextResponse } from "next/server";
import { getTopMovers } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  try {
    const data = await getTopMovers();
    return NextResponse.json(data);
  } catch (err) {
    console.error("GET /api/movers failed", err);
    return NextResponse.json({ error: "Failed to load top movers" }, { status: 500 });
  }
}
