import { notFound } from "next/navigation";
import Link from "next/link";
import { query, queryOne } from "@/lib/db";
import { companyNameToTicker } from "@/lib/tickers";
import { CandlestickChart, type Candle } from "@/components/CandlestickChart";
import { StatCard } from "@/components/StatCard";
import { formatINR, formatPercent, formatDate } from "@/lib/format";

export const revalidate = 300;

type CompanyInfo = {
  ticker: string;
  long_name: string | null;
  sector: string | null;
  industry: string | null;
  market_cap: number | null;
  profit_margins: number | null;
  week52_change: number | null;
  trailing_pe: number | null;
};

type PriceRow = { date: string; open: number; high: number; low: number; close: number };
type PredictionRow = { predicted_closing_price: number; last_close: number; prediction_date: string; sentiment: string | null };

async function getCompanyInfo(ticker: string) {
  return queryOne<CompanyInfo>(
    `select ticker, long_name, sector, industry, market_cap, profit_margins, week52_change, trailing_pe
     from company_info where ticker = $1`,
    [ticker]
  );
}

async function getPriceHistory(ticker: string) {
  return query<PriceRow>(
    `select date, open, high, low, close
     from stock_data
     where ticker = $1
     order by date desc
     limit 180`,
    [ticker]
  );
}

async function getLatestPrediction(ticker: string) {
  return queryOne<PredictionRow>(
    `select predicted_closing_price, last_close, prediction_date, sentiment
     from final_analysis
     where ticker = $1
     order by prediction_date desc
     limit 1`,
    [ticker]
  );
}

export default async function CompanyPage({ params }: { params: { ticker: string } }) {
  const companyName = decodeURIComponent(params.ticker);
  const ticker = companyNameToTicker(companyName);

  if (!ticker) {
    return (
      <div className="mx-auto max-w-3xl px-6 py-16 text-center">
        <span className="text-xs uppercase tracking-wide text-signal-amber">Search result</span>
        <h1 className="mt-3 font-display text-2xl font-bold">
          We couldn&apos;t find &ldquo;{companyName}&rdquo; in the tracked company list.
        </h1>
        <p className="mt-3 text-text-muted">
          Try a mapped company keyword from the dashboard search, e.g. reliance, tcs, infosys, hdfc bank.
        </p>
        <Link href="/" className="mt-6 inline-block rounded-lg bg-signal-amber px-4 py-2 font-semibold text-ink-950">
          Back to dashboard
        </Link>
      </div>
    );
  }

  const [info, priceRows, prediction] = await Promise.all([
    getCompanyInfo(ticker),
    getPriceHistory(ticker),
    getLatestPrediction(ticker)
  ]);

  if (!info && priceRows.length === 0) {
    notFound();
  }

  const candles: Candle[] = [...priceRows]
    .reverse()
    .map((r) => ({ time: r.date, open: r.open, high: r.high, low: r.low, close: r.close }));

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <span className="text-xs uppercase tracking-wide text-signal-amber">Company intelligence</span>
          <h1 className="mt-2 font-display text-3xl font-bold">{info?.long_name ?? ticker}</h1>
          <p className="mt-1 text-text-muted">{ticker}</p>
        </div>
        <Link href="/" className="rounded-lg border border-ink-600 px-4 py-2 text-sm hover:border-signal-amber">
          Back to dashboard
        </Link>
      </div>

      <div className="mt-6 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
        <StatCard
          label="Market cap"
          value={info?.market_cap ? `${formatINR(info.market_cap / 1e7, { compact: true })} Cr` : "N/A"}
        />
        <StatCard label="Sector" value={info?.sector ?? "N/A"} sub={info?.industry ?? undefined} />
        <StatCard
          label="Profit margin"
          value={formatPercent(info?.profit_margins ?? null)}
          tone={info?.profit_margins != null ? (info.profit_margins > 0 ? "up" : "down") : "neutral"}
        />
        <StatCard
          label="52-week change"
          value={formatPercent(info?.week52_change ?? null)}
          tone={info?.week52_change != null ? (info.week52_change > 0 ? "up" : "down") : "neutral"}
        />
        <StatCard
          label="Latest prediction"
          value={prediction ? formatINR(prediction.predicted_closing_price) : "N/A"}
          sub={prediction ? formatDate(prediction.prediction_date) : undefined}
        />
      </div>

      <div className="mt-8 grid gap-6 lg:grid-cols-[2fr_1fr]">
        <article className="panel p-6">
          <h2 className="font-display text-lg font-semibold">Price history</h2>
          <div className="mt-4">
            <CandlestickChart data={candles} />
          </div>
        </article>

        <article className="panel p-6">
          <h2 className="font-display text-lg font-semibold">Modeled vs actual</h2>
          <p className="mt-1 text-sm text-text-muted">Latest RandomForest forecast vs the prior actual close.</p>
          <div className="mt-4 space-y-3">
            <div className="flex items-center justify-between rounded-lg bg-ink-700/40 px-3 py-2">
              <span className="text-sm text-text-muted">Predicted</span>
              <strong className="font-mono">{prediction ? formatINR(prediction.predicted_closing_price) : "N/A"}</strong>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-ink-700/40 px-3 py-2">
              <span className="text-sm text-text-muted">Last actual close</span>
              <strong className="font-mono">{prediction ? formatINR(prediction.last_close) : "N/A"}</strong>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-ink-700/40 px-3 py-2">
              <span className="text-sm text-text-muted">News sentiment</span>
              <strong>{prediction?.sentiment ?? "N/A"}</strong>
            </div>
          </div>
        </article>
      </div>
    </div>
  );
}
