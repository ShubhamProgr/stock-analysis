import { NextResponse } from "next/server";
import { query } from "@/lib/db";

export const revalidate = 300; // 5 min

type Row = {
  ticker: string;
  company: string;
  predicted_closing_price: number;
  prediction_date: string;
};

export async function GET() {
  try {
    const rows = await query<Row>(
      `select "Ticker" as ticker, "Company" as company, "Predicted_Closing_Price" as predicted_closing_price, "Prediction_Date" as prediction_date
       from final_analysis
       where "Prediction_Date" = (select max("Prediction_Date") from final_analysis)
       order by "Predicted_Closing_Price" desc
       limit 12`
    );

    const predictions = rows.map((r) => ({
      ticker: r.ticker,
      company: r.company,
      price: r.predicted_closing_price,
      predictionDate: r.prediction_date
    }));

    return NextResponse.json({ predictions });
  } catch (error) {
    console.error("GET /api/predictions failed", error);
    return NextResponse.json({ predictions: [], error: "Unable to load predictions." }, { status: 500 });
  }
}
