"use client";

import { useState } from "react";
import type { TickerBundle, WatchlistRow, PredictionData } from "@/lib/types"; 
import Sidebar from "./Sidebar";
import TickerTape from "./TickerTape";
import StatTiles from "./StatTiles";
import PriceSentimentChart from "./PriceSentimentChart";
import StrategyTable from "./StrategyTable";
import CompanySentimentCard from "./CompanySentimentCard";
import NewsFeed from "./NewsFeed";
import ModelInsights from "./ModelInsights";
import QueryRunner from "./QueryRunner";
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
  initialPredictionDate: string | null;
  predictionDates: string[];
  initialPredictions: PredictionData[];
};

export default function Dashboard({
  initialWatchlist,
  initialBundle,
  initialRangeDays,
  initialPredictionDate,
  predictionDates,
  initialPredictions,
}: Props) {
  const [watchlist] = useState(initialWatchlist);
  const [bundle, setBundle] = useState(initialBundle);
  const [rangeDays, setRangeDays] = useState(initialRangeDays);
  const [activeNav, setActiveNav] = useState("section-overview");
  const [loading, setLoading] = useState(false);
  const [analysisRows, setAnalysisRows] = useState(initialPredictions);
  const [selectedPredictionDate, setSelectedPredictionDate] = useState(initialPredictionDate);
  const [dateMenuOpen, setDateMenuOpen] = useState(false);

  const currentPrediction = analysisRows.find((prediction) => prediction.Ticker === bundle.ticker) ?? null;
  const predictionLabel = selectedPredictionDate ? dayLabel(selectedPredictionDate) : null;

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

  async function handleSelectPredictionDate(date: string) {
    if (date === selectedPredictionDate) {
      setDateMenuOpen(false);
      return;
    }

    setLoading(true);
    try {
      const res = await fetch(`/api/predictions?date=${encodeURIComponent(date)}`);
      if (!res.ok) throw new Error("failed to load predictions");
      const data: { predictions: PredictionData[] } = await res.json();
      setAnalysisRows(data.predictions);
      setSelectedPredictionDate(date);
      setDateMenuOpen(false);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
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
        <TickerTape predictions={analysisRows} />

        <main className={loading ? "loadingOverlay" : ""}>
          <div className="pageHead">
            <div className="tickerTitle">
              <span className="sym mono">{bundle.ticker.replace(".NS", "")}</span>
              <span className="name">{bundle.name}</span>
              <span className="badgeLive">Live · Supabase</span>
            </div>
            <div className="predictionControls">
              <div className="predictionPicker">
                <button
                  className="predictionPickerButton"
                  onClick={() => setDateMenuOpen((open) => !open)}
                  disabled={predictionDates.length === 0}
                >
                  Adjust Prediction Date
                  {predictionLabel && <span className="predictionPickerValue">{predictionLabel}</span>}
                </button>
                {dateMenuOpen && predictionDates.length > 0 && (
                  <div className="predictionMenu">
                    {predictionDates.map((date) => {
                      const isActive = date === selectedPredictionDate;
                      return (
                        <button
                          key={date}
                          className={isActive ? "active" : ""}
                          onClick={() => handleSelectPredictionDate(date)}
                        >
                          <span>{new Date(date).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" })}</span>
                          <span className="mono">{isActive ? "Selected" : ""}</span>
                        </button>
                      );
                    })}
                  </div>
                )}
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
          </div>

          <StatTiles bundle={bundle} predictionLabel={predictionLabel} prediction={currentPrediction} />

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
              <ModelInsights bundle={bundle} />
            </div>
          </section>

          <section style={{ marginTop: 24 }}>
            <QueryRunner />
          </section>
        </main>
      </div>
    </div>
  );
}