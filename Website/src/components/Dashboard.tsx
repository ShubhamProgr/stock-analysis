"use client";

import { useState, useRef, useEffect, useMemo } from "react";
import type { TickerBundle, WatchlistRow, PredictionData, DashboardView } from "@/lib/types"; 
import Sidebar from "./Sidebar";
import TickerTape from "./TickerTape";
import StatTiles from "./StatTiles";
import PriceSentimentChart from "./PriceSentimentChart";
import StrategyTable from "./StrategyTable";
import CompanySentimentCard from "./CompanySentimentCard";
import NewsFeed from "./NewsFeed";
import ModelInsights from "./ModelInsights";
import PredictionVsActualChart from "./PredictionVsActualChart";
import TechnicalOverlays from "./TechnicalOverlays";
import MarketView from "./MarketView";
import ScreenerView from "./ScreenerView";
import AccuracyView from "./AccuracyView";
import CompareView from "./CompareView";
import StrategyView from "./StrategyView";
import { dayLabel, fmtMoney, fmtPct } from "@/lib/format";

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
  const [tickerMenuOpen, setTickerMenuOpen] = useState(false);
  const [tickerSearch, setTickerSearch] = useState("");
  const [activeView, setActiveView] = useState<DashboardView>("stock");
  const [chartMode, setChartMode] = useState<"line" | "candlestick">("line");
  const tickerMenuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (tickerMenuRef.current && !tickerMenuRef.current.contains(e.target as Node)) {
        setTickerMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filteredWatchlist = useMemo(() => {
    if (!tickerSearch.trim()) return watchlist;
    const q = tickerSearch.toLowerCase().trim();
    return watchlist.filter(
      (w) => w.ticker.toLowerCase().includes(q) || w.name.toLowerCase().includes(q)
    );
  }, [watchlist, tickerSearch]);

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
    // If coming from another view, switch to stock view
    if (activeView !== "stock") {
      setActiveView("stock");
    }
    if (ticker === bundle.ticker && activeView === "stock") return;
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

  function handleSelectView(view: DashboardView) {
    setActiveView(view);
  }

  return (
    <div className="app">
      <Sidebar
        watchlist={watchlist}
        currentTicker={bundle.ticker}
        onSelectTicker={handleSelectTicker}
        activeNav={activeNav}
        onSelectNav={handleSelectNav}
        activeView={activeView}
        onSelectView={handleSelectView}
      />

      <div className="appContent">
        <TickerTape predictions={analysisRows} />

        {/* ==================== Stock View ==================== */}
        {activeView === "stock" && (
          <main className={loading ? "loadingOverlay" : ""}>
            <div className="pageHead">
              <div className="tickerTitle">
                <div className="tickerPicker" ref={tickerMenuRef}>
                  <button
                    className={`tickerPickerBtn ${tickerMenuOpen ? "open" : ""}`}
                    onClick={() => setTickerMenuOpen((open) => !open)}
                    title="Click to switch ticker"
                  >
                    <span className="sym mono">{bundle.ticker.replace(".NS", "")}</span>
                    <svg
                      className={`tickerPickerChevron ${tickerMenuOpen ? "open" : ""}`}
                      width="16"
                      height="16"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    >
                      <polyline points="6 9 12 15 18 9" />
                    </svg>
                  </button>

                  {tickerMenuOpen && (
                    <div className="tickerMenu">
                      <div className="tickerMenuSearchWrap">
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="var(--muted)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <circle cx="11" cy="11" r="8" />
                          <line x1="21" y1="21" x2="16.65" y2="16.65" />
                        </svg>
                        <input
                          type="text"
                          className="tickerMenuSearch"
                          placeholder="Search ticker or name…"
                          value={tickerSearch}
                          onChange={(e) => setTickerSearch(e.target.value)}
                          autoFocus
                        />
                        {tickerSearch && (
                          <button className="tickerMenuClear" onClick={() => setTickerSearch("")}>×</button>
                        )}
                      </div>
                      <div className="tickerMenuList">
                        {filteredWatchlist.map((row) => {
                          const isActive = row.ticker === bundle.ticker;
                          return (
                            <button
                              key={row.ticker}
                              className={`tickerMenuItem ${isActive ? "active" : ""}`}
                              onClick={() => {
                                handleSelectTicker(row.ticker);
                                setTickerMenuOpen(false);
                                setTickerSearch("");
                              }}
                            >
                              <div className="tickerMenuItemLeft">
                                <span className="tickerMenuItemSym mono">{row.ticker.replace(".NS", "")}</span>
                                <span className="tickerMenuItemName">{row.name}</span>
                              </div>
                              <div className="tickerMenuItemRight">
                                <span className="tickerMenuItemPrice mono">{fmtMoney(row.price)}</span>
                                <span className={`chip ${row.changePct >= 0 ? "up" : "down"}`}>
                                  {fmtPct(row.changePct)}
                                </span>
                              </div>
                            </button>
                          );
                        })}
                        {filteredWatchlist.length === 0 && (
                          <div className="tickerMenuEmpty">No tickers found</div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
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
                  <div className="cardTitle">
                    {chartMode === "line" ? "Price & sentiment trend" : "Candlestick chart"}
                  </div>
                  <div className="chartModeToggle">
                    <button
                      className={chartMode === "line" ? "active" : ""}
                      onClick={() => setChartMode("line")}
                    >
                      Line
                    </button>
                    <button
                      className={chartMode === "candlestick" ? "active" : ""}
                      onClick={() => setChartMode("candlestick")}
                    >
                      Candlestick
                    </button>
                  </div>
                </div>
                <div className="cardBody">
                  {chartMode === "line" ? (
                    <>
                      <PriceSentimentChart series={bundle.series} sentimentSeries={bundle.sentimentSeries} />
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
                    </>
                  ) : (
                    <TechnicalOverlays ticker={bundle.ticker} rangeDays={rangeDays} />
                  )}
                </div>

                <StrategyTable strategies={bundle.strategies} />
              </div>

              <div>
                <CompanySentimentCard sentiment={bundle.companySentiment} />
                <NewsFeed news={bundle.news} />
              </div>
            </section>

            <ModelInsights bundle={bundle} />

            <PredictionVsActualChart
              predictionHistory={bundle.predictionHistory}
              ticker={bundle.ticker}
            />
          </main>
        )}

        {/* ==================== Market View ==================== */}
        {activeView === "market" && (
          <main>
            <MarketView onSelectTicker={handleSelectTicker} />
          </main>
        )}

        {/* ==================== Screener View ==================== */}
        {activeView === "screener" && (
          <main>
            <ScreenerView onSelectTicker={handleSelectTicker} />
          </main>
        )}

        {/* ==================== Accuracy View ==================== */}
        {activeView === "accuracy" && (
          <main>
            <AccuracyView onSelectTicker={handleSelectTicker} />
          </main>
        )}

        {/* ==================== Compare View ==================== */}
        {activeView === "compare" && (
          <main>
            <CompareView watchlist={watchlist} onSelectTicker={handleSelectTicker} />
          </main>
        )}

        {/* ==================== Strategy View ==================== */}
        {activeView === "strategy" && (
          <main className="strategyMain">
            <StrategyView />
          </main>
        )}
      </div>
    </div>
  );
}