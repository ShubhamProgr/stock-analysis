import { NextResponse } from "next/server";
import { getAccuracyData } from "@/lib/queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET() {
  try {
    const data = await getAccuracyData();
    return NextResponse.json(data);
  } catch (err) {
    console.error("GET /api/accuracy failed", err);
    return NextResponse.json({ error: "Failed to load accuracy data" }, { status: 500 });
  }
}
