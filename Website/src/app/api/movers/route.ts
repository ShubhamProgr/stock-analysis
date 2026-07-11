import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const revalidate = 300;

type Row = {
  ticker: string;
  company: string | null;
  predicted_closing_price: number;
  actual_closing_price: number;
};

export async function GET() {
  try {
    const rows = await query<Row>(
      `select ticker, company, predicted_closing_price, actual_closing_price
       from prediction_vs_actual
       where date = (select max(date) from prediction_vs_actual)
         and actual_closing_price is not null
         and actual_closing_price <> 0`
    );

    const movers = rows.map((r) => ({
      ticker: r.ticker,
      company: r.company ?? r.ticker,
      predicted: r.predicted_closing_price,
      actual: r.actual_closing_price,
      changePct: ((r.predicted_closing_price - r.actual_closing_price) / r.actual_closing_price) * 100
    }));

    return NextResponse.json(movers);
  } catch (error) {
    console.error("GET /api/movers failed", error);
    return NextResponse.json({ error: "Prediction comparison data is unavailable." }, { status: 500 });
  }
}
