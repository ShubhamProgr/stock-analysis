import { getWatchlist, getTickerBundle, getPredictions } from "@/lib/queries";
import Dashboard from "@/components/Dashboard";

const DEFAULT_TICKER = "RELIANCE.NS";
const DEFAULT_RANGE_DAYS = 126;

export default async function Home() {
  // Fetch your original data PLUS the new predictions
  const [watchlist, bundle, predictions] = await Promise.all([
    getWatchlist(),
    getTickerBundle(DEFAULT_TICKER, DEFAULT_RANGE_DAYS),
    getPredictions(), 
  ]);

  if (!bundle) {
    return (
      <main style={{ padding: 40 }}>
        <p>Could not load {DEFAULT_TICKER} from the database. Check DATABASE_URL and the stock_data table.</p>
      </main>
    );
  }

  // Pass the predictions down into the Dashboard component
  return (
    <Dashboard 
      initialWatchlist={watchlist} 
      initialBundle={bundle} 
      initialRangeDays={DEFAULT_RANGE_DAYS} 
      predictions={predictions || []} 
    />
  );
}