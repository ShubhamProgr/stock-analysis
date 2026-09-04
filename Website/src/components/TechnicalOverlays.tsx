"use client";

import { useState, useEffect } from "react";
import type { OHLCPoint } from "@/lib/types";
import CandlestickChart from "./CandlestickChart";

type Props = {
  ticker: string;
  rangeDays: number;
};

export default function TechnicalOverlays({ ticker, rangeDays }: Props) {
  const [data, setData] = useState<OHLCPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [showSMA20, setShowSMA20] = useState(true);
  const [showSMA50, setShowSMA50] = useState(true);
  const [showBollinger, setShowBollinger] = useState(false);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const res = await fetch(`/api/ohlc/${encodeURIComponent(ticker)}?range=${rangeDays}`);
        const json = await res.json();
        setData(json.data ?? []);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [ticker, rangeDays]);

  if (loading) {
    return (
      <div style={{ padding: 20, textAlign: "center", opacity: 0.6 }}>
        Loading chart data…
      </div>
    );
  }

  return (
    <div>
      {/* Toggle buttons */}
      <div className="techToggles">
        <button className={`techToggle ${showSMA20 ? "active" : ""}`} onClick={() => setShowSMA20(!showSMA20)}>
          SMA 20
        </button>
        <button className={`techToggle ${showSMA50 ? "active" : ""}`} onClick={() => setShowSMA50(!showSMA50)}>
          SMA 50
        </button>
        <button className={`techToggle ${showBollinger ? "active" : ""}`} onClick={() => setShowBollinger(!showBollinger)}>
          Bollinger
        </button>
      </div>

      {/* Candlestick chart */}
      <CandlestickChart
        data={data}
        showSMA20={showSMA20}
        showSMA50={showSMA50}
        showBollinger={showBollinger}
      />

      <div className="legendRow" style={{ marginTop: 8 }}>
        {showSMA20 && <span><span className="dot" style={{ background: "var(--accent)" }}></span>SMA 20</span>}
        {showSMA50 && <span><span className="dot" style={{ background: "var(--caution)" }}></span>SMA 50</span>}
        {showBollinger && <span><span className="dot" style={{ background: "var(--caution)", opacity: 0.5 }}></span>Bollinger Bands</span>}
        <span><span className="dot" style={{ background: "var(--good)" }}></span>Bullish candle</span>
        <span><span className="dot" style={{ background: "var(--critical)" }}></span>Bearish candle</span>
      </div>
    </div>
  );
}
