import { getWatchlist, getTickerBundle } from "@/lib/queries";
import Dashboard from "@/components/Dashboard";

const DEFAULT_TICKER = "RELIANCE.NS";
const DEFAULT_RANGE_DAYS = 126;

export default async function Home() {
  const [watchlist, bundle] = await Promise.all([
    getWatchlist(),
    getTickerBundle(DEFAULT_TICKER, DEFAULT_RANGE_DAYS),
  ]);

  if (!bundle) {
    return (
      <main style={{ padding: 40 }}>
        <p>Could not load {DEFAULT_TICKER} from the database. Check DATABASE_URL and the stock_data table.</p>
      </main>
    );
  }

  return <Dashboard initialWatchlist={watchlist} initialBundle={bundle} initialRangeDays={DEFAULT_RANGE_DAYS} />;
}
