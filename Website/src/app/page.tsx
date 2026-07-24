import { getWatchlist, getTickerBundle, getPredictions } from "@/lib/queries";
import Dashboard from "@/components/Dashboard";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const DEFAULT_TICKER = "RELIANCE.NS";
const DEFAULT_RANGE_DAYS = 126;

export default async function Home() {
  const predictions = await getPredictions();
  const initialTicker = predictions?.[0]?.Ticker ?? DEFAULT_TICKER;

  const [watchlist, bundle] = await Promise.all([
    getWatchlist(),
    getTickerBundle(initialTicker, DEFAULT_RANGE_DAYS),
  ]);

  if (!bundle) {
    return (
      <main style={{ padding: 40 }}>
        <p>Could not load {initialTicker} from the database. Check DATABASE_URL and the stock_data table.</p>
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