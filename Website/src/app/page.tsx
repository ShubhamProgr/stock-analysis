import Link from "next/link";
import { query, queryOne } from "@/lib/db";
import { TickerRibbon } from "@/components/TickerRibbon";
import { HeroAreaChart } from "@/components/HeroAreaChart";
import { MoversList } from "@/components/MoversList";
import { PipelineRunner } from "@/components/PipelineRunner";
import { CompanySearch } from "@/components/CompanySearch";
import { StatCard } from "@/components/StatCard";
import { formatINR, formatDate } from "@/lib/format";
import { ALL_TICKERS, TICKER_COMPANY_MAP } from "@/lib/tickers";

export const revalidate = 300;

type PredictionRow = {
  ticker: string;
  company: string;
  predicted_closing_price: number;
  prediction_date: string;
};

async function getPredictions() {
  return query<PredictionRow>(
    `select ticker, company, predicted_closing_price, prediction_date
     from final_analysis
     where prediction_date = (select max(prediction_date) from final_analysis)
     order by predicted_closing_price desc
     limit 8`
  ).catch(() => [] as PredictionRow[]);
}

async function getLatestRunDate() {
  return queryOne<{ prediction_date: string }>(
    `select max(prediction_date) as prediction_date from final_analysis`
  ).catch(() => null);
}

export default async function DashboardPage() {
  const [predictions, latestRun] = await Promise.all([getPredictions(), getLatestRunDate()]);
  const companies = Object.values(TICKER_COMPANY_MAP).map((c) => c.toLowerCase());
  const heroPrediction = predictions[0];

  return (
    <>
      <TickerRibbon />

      <div className="mx-auto max-w-7xl px-6 py-8">
        <section className="grid gap-6 lg:grid-cols-[2fr_1fr]">
          <article className="panel p-6">
            <div className="flex items-start justify-between gap-4">
              <div>
                <span className="text-xs uppercase tracking-wide text-signal-amber">Forecast overview</span>
                <h1 className="mt-2 font-display text-3xl font-bold">
                  {heroPrediction ? heroPrediction.ticker : "No model run yet"}
                </h1>
                <p className="mt-1 font-mono text-2xl">
                  {heroPrediction ? formatINR(heroPrediction.predicted_closing_price) : "--"}
                </p>
              </div>
              <div className="text-right text-xs text-text-muted">
                <p>{latestRun?.prediction_date ? formatDate(latestRun.prediction_date) : "Run the ML pipeline"}</p>
                <p>predicted close</p>
              </div>
            </div>

            <div className="mt-6">
              <HeroAreaChart values={predictions.map((p) => Number(p.predicted_closing_price))} />
            </div>

            <div className="mt-6 grid grid-cols-2 gap-3 sm:grid-cols-4">
              {predictions.slice(0, 4).map((p) => (
                <StatCard key={p.ticker} label={p.ticker} value={formatINR(p.predicted_closing_price)} sub={p.company} />
              ))}
              {predictions.length === 0 && (
                <StatCard label="Model status" value="No predictions" sub="Run Final_Analysis from /admin" />
              )}
            </div>
          </article>

          <aside className="flex flex-col gap-6">
            <article className="panel p-6">
              <h2 className="font-display text-lg font-semibold">Find a company</h2>
              <p className="mt-1 text-sm text-text-muted">
                Search any of the {ALL_TICKERS.length}+ tracked NSE tickers for price history, fundamentals, and its latest forecast.
              </p>
              <div className="mt-4">
                <CompanySearch companies={companies} />
              </div>
            </article>

            <article className="panel p-6">
              <h2 className="font-display text-lg font-semibold">Pipeline</h2>
              <p className="mt-1 text-sm text-text-muted">
                Data refreshes run on Render. Trigger a manual run from the admin console.
              </p>
              <Link
                href="/admin"
                className="mt-4 inline-block rounded-lg bg-signal-amber px-4 py-2 text-sm font-semibold text-ink-950 hover:brightness-110"
              >
                Open admin console
              </Link>
            </article>
          </aside>
        </section>

        <section className="mt-8 grid gap-6 lg:grid-cols-2">
          <article className="panel p-6">
            <div className="mb-2 flex items-center justify-between">
              <h2 className="font-display text-lg font-semibold">Top gainers</h2>
              <span className="text-xs text-text-faint">model vs actual</span>
            </div>
            <MoversList direction="up" />
          </article>
          <article className="panel p-6">
            <div className="mb-2 flex items-center justify-between">
              <h2 className="font-display text-lg font-semibold">Top losers</h2>
              <span className="text-xs text-text-faint">model vs actual</span>
            </div>
            <MoversList direction="down" />
          </article>
        </section>
      </div>
    </>
  );
}
