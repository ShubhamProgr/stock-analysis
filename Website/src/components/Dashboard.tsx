"use client";

import { useState } from "react";
// 1. Added PredictionData to the imports
import type { TickerBundle, WatchlistRow, PredictionData } from "@/lib/types"; 
import Sidebar from "./Sidebar";
import TickerTape from "./TickerTape";
import StatTiles from "./StatTiles";
import PriceSentimentChart from "./PriceSentimentChart";
import StrategyTable from "./StrategyTable";
import CompanySentimentCard from "./CompanySentimentCard";
import NewsFeed from "./NewsFeed";
import { dayLabel } from "@/lib/format";

const RANGES = [
  { label: "1M", days: 22 },
  { label: "3M", days: 64 },
  { label: "6M", days: 126 },
  { label: "1Y", days: 252 },
  { label: "5Y", days: 1260 },
];

type Props = {
  initialWatchlist: WatchlistRow[];
  initialBundle: TickerBundle;
  initialRangeDays: number;
  // 2. Added predictions to the expected properties
  predictions: PredictionData[]; 
};

// 3. Added predictions to the function parameters
export default function Dashboard({ initialWatchlist, initialBundle, initialRangeDays, predictions }: Props) {
  const [watchlist] = useState(initialWatchlist);
  const [bundle, setBundle] = useState(initialBundle);
  const [rangeDays, setRangeDays] = useState(initialRangeDays);
  const [activeNav, setActiveNav] = useState("section-overview");
  const [loading, setLoading] = useState(false);
  const currentPrediction = predictions.find((prediction) => prediction.Ticker === bundle.ticker) ?? null;
  const predictionLabel = currentPrediction ? dayLabel(currentPrediction.Prediction_Date) : null;

  async function loadTicker(ticker: string, days: number) {
    setLoading(true);
    try {
      const res = await fetch(`/api/ticker/${encodeURIComponent(ticker)}?range=${days}`);
      if (!res.ok) throw new Error("failed to load ticker");
      const data: TickerBundle = await res.json();
      setBundle(data);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }

  function handleSelectTicker(ticker: string) {
    if (ticker === bundle.ticker) return;
    loadTicker(ticker, rangeDays);
  }

  function handleSelectRange(days: number) {
    setRangeDays(days);
    loadTicker(bundle.ticker, days);
  }

  function handleSelectNav(id: string) {
    setActiveNav(id);
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <div className="app">
      <Sidebar
        watchlist={watchlist}
        currentTicker={bundle.ticker}
        onSelectTicker={handleSelectTicker}
        activeNav={activeNav}
        onSelectNav={handleSelectNav}
      />

      <div>
        {/* 4. Updated TickerTape to use your new predictions data */}
        <TickerTape predictions={predictions} />

        <main className={loading ? "loadingOverlay" : ""}>
          <div className="pageHead">
            <div className="tickerTitle">
              <span className="sym mono">{bundle.ticker.replace(".NS", "")}</span>
              <span className="name">{bundle.name}</span>
              <span className="badgeLive">Live · Supabase</span>
            </div>
            <div className="rangeToggle">
              {RANGES.map((r) => (
                <button
                  key={r.label}
                  className={rangeDays === r.days ? "active" : ""}
                  onClick={() => handleSelectRange(r.days)}
                >
                  {r.label}
                </button>
              ))}
            </div>
          </div>

          <StatTiles bundle={bundle} predictionLabel={predictionLabel} />

          <section className="contentGrid">
            <div className="card">
              <div className="cardHead">
                <div className="cardTitle">Price &amp; sentiment trend</div>
              </div>
              <div className="cardBody">
                <PriceSentimentChart series={bundle.series} sentimentSeries={bundle.sentimentSeries} />
              </div>
              <div className="legendRow">
                <span>
                  <span className="dot" style={{ background: "var(--accent)" }}></span>Close price
                </span>
                <span>
                  <span className="dot" style={{ background: "var(--good)" }}></span>Sentiment &gt; 55
                </span>
                <span>
                  <span className="dot" style={{ background: "var(--critical)" }}></span>Sentiment &lt; 45
                </span>
              </div>

              <StrategyTable strategies={bundle.strategies} />
            </div>

            <div>
              <CompanySentimentCard sentiment={bundle.companySentiment} />
              <NewsFeed news={bundle.news} />
            </div>
          </section>
        </main>
      </div>
    </div>
  );
}