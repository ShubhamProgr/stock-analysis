import { getWatchlist, getTickerBundle, getPredictionDates, getPredictions } from "@/lib/queries";
import Dashboard from "@/components/Dashboard";
import Link from "next/link";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const DEFAULT_TICKER = "RELIANCE.NS";
const DEFAULT_RANGE_DAYS = 126;

export default async function DashboardPage() {
  const [predictionDates, predictions] = await Promise.all([getPredictionDates(), getPredictions()]);
  const initialPredictionDate = predictionDates[0] ?? predictions?.[0]?.Prediction_Date ?? null;
  const initialTicker = predictions?.[0]?.Ticker ?? DEFAULT_TICKER;

  const [watchlist, bundle] = await Promise.all([
    getWatchlist(),
    getTickerBundle(initialTicker, DEFAULT_RANGE_DAYS),
  ]);

  if (!bundle) {
    return (
      <main style={{ padding: 40, fontFamily: "'Inter', sans-serif", color: "#e7ecf5", background: "#0b0e14", minHeight: "100vh" }}>
        <h2 style={{ fontSize: 24, marginBottom: 12 }}>Unable to Load Stock Data</h2>
        <p style={{ color: "#8b96ad", marginBottom: 20 }}>
          Could not load {initialTicker} from the database. Check DATABASE_URL and ensure the stock_data table is accessible.
        </p>
        <Link 
          href="/" 
          style={{ 
            display: "inline-flex", 
            alignItems: "center", 
            gap: 6, 
            background: "linear-gradient(135deg, #3b5ba5, #7da0de)", 
            color: "#fff", 
            padding: "10px 20px", 
            borderRadius: 8, 
            textDecoration: "none", 
            fontWeight: 600,
            fontSize: 14 
          }}
        >
          &larr; Return to Landing Page
        </Link>
      </main>
    );
  }

  return (
    <Dashboard 
      initialWatchlist={watchlist} 
      initialBundle={bundle} 
      initialRangeDays={DEFAULT_RANGE_DAYS} 
      initialPredictionDate={initialPredictionDate}
      predictionDates={predictionDates}
      initialPredictions={predictions || []}
    />
  );
}
