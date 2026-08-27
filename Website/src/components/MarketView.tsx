"use client";

import { useState, useEffect } from "react";
import type { MarketOverviewRow } from "@/lib/types";
import { fmtPct, fmtCompact } from "@/lib/format";
import SectorHeatmap from "./SectorHeatmap";

type Props = {
  onSelectTicker: (ticker: string) => void;
};

export default function MarketView({ onSelectTicker }: Props) {
  const [rows, setRows] = useState<MarketOverviewRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/market");
        const data = await res.json();
        setRows(data.rows ?? []);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  if (loading) {
    return (
      <div className="viewLoading">
        <div className="viewLoadingSpinner" />
        <p>Loading market overview…</p>
      </div>
    );
  }

  // Compute summary stats
  const totalMarketCap = rows.reduce((s, r) => s + r.marketCap, 0);
  const avgReturn = rows.length > 0 ? rows.reduce((s, r) => s + r.predictedReturn, 0) / rows.length : 0;
  const buyCount = rows.filter((r) => r.signal === "BUY").length;
  const sellCount = rows.filter((r) => r.signal === "SELL").length;
  const holdCount = rows.filter((r) => r.signal === "HOLD").length;

  // Top gainers / losers
  const sorted = [...rows].sort((a, b) => b.predictedReturn - a.predictedReturn);
  const topGainers = sorted.slice(0, 5);
  const topLosers = sorted.slice(-5).reverse();

  // Sector aggregation
  const sectorMap = new Map<string, { returns: number[]; sentiments: string[]; count: number }>();
  for (const r of rows) {
    const existing = sectorMap.get(r.sector) ?? { returns: [], sentiments: [], count: 0 };
    existing.returns.push(r.predictedReturn);
    existing.sentiments.push(r.sentiment);
    existing.count++;
    sectorMap.set(r.sector, existing);
  }
  const sectorSummary = [...sectorMap.entries()]
    .map(([sector, data]) => ({
      sector,
      avgReturn: data.returns.reduce((a, b) => a + b, 0) / data.count,
      positiveRatio: data.sentiments.filter((s) => s === "POSITIVE").length / data.count,
      count: data.count,
    }))
    .sort((a, b) => b.avgReturn - a.avgReturn);

  function handleNavigateToStock(ticker: string) {
    onSelectTicker(ticker);
  }

  return (
    <div className="marketViewWrap">
      <div className="viewHeader">
        <h1 className="viewTitle">Market Overview</h1>
        <span className="viewSubtitle">{rows.length} stocks tracked · Latest predictions</span>
      </div>

      {/* Summary tiles */}
      <section className="statRow">
        <div className="tile">
          <div className="eyebrow">Total Market Cap</div>
          <div className="value mono">{fmtCompact(totalMarketCap)}</div>
          <div className="sub">{rows.length} companies</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Avg Predicted Return</div>
          <div className="value mono">
            <span className={avgReturn >= 0 ? "pvGood" : "pvCritical"}>{fmtPct(avgReturn)}</span>
          </div>
          <div className="sub">across all stocks</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Signal Distribution</div>
          <div className="value" style={{ display: "flex", gap: 6, marginTop: 4 }}>
            <span className="chip buy">{buyCount} BUY</span>
            <span className="chip hold">{holdCount} HOLD</span>
            <span className="chip sell">{sellCount} SELL</span>
          </div>
          <div className="sub">composite signals</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Sectors</div>
          <div className="value mono">{sectorSummary.length}</div>
          <div className="sub">Top: {sectorSummary[0]?.sector ?? "—"}</div>
        </div>
      </section>

      {/* Sector Heatmap */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="cardHead">
          <div className="cardTitle">Sector Heatmap — Predicted Returns</div>
        </div>
        <div className="cardBody">
          <SectorHeatmap rows={rows} onSelectTicker={handleNavigateToStock} />
        </div>
        <div className="legendRow">
          <span><span className="dot" style={{ background: "var(--critical)" }}></span>Strong Sell (&lt;-2%)</span>
          <span><span className="dot" style={{ background: "rgba(232,99,95,0.4)" }}></span>Weak Sell</span>
          <span><span className="dot" style={{ background: "rgba(53,193,94,0.25)" }}></span>Weak Buy</span>
          <span><span className="dot" style={{ background: "var(--good)" }}></span>Strong Buy (&gt;+2%)</span>
        </div>
      </div>

      {/* Top Gainers / Losers + Sector Performance */}
      <div className="marketGridBottom">
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Top Predicted Gainers</div>
          </div>
          <div className="cardBody" style={{ padding: 0 }}>
            <table className="marketTable">
              <thead>
                <tr>
                  <th>Stock</th>
                  <th>Sector</th>
                  <th>Predicted</th>
                  <th>Sentiment</th>
                </tr>
              </thead>
              <tbody>
                {topGainers.map((r) => (
                  <tr key={r.ticker} onClick={() => handleNavigateToStock(r.ticker)} style={{ cursor: "pointer" }}>
                    <td>
                      <span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span>
                      <span className="marketTableName">{r.name}</span>
                    </td>
                    <td className="marketTableSector">{r.sector}</td>
                    <td><span className="chip up">{fmtPct(r.predictedReturn)}</span></td>
                    <td><span className={`chip ${r.sentiment.toLowerCase()}`}>{r.sentiment}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Top Predicted Losers</div>
          </div>
          <div className="cardBody" style={{ padding: 0 }}>
            <table className="marketTable">
              <thead>
                <tr>
                  <th>Stock</th>
                  <th>Sector</th>
                  <th>Predicted</th>
                  <th>Sentiment</th>
                </tr>
              </thead>
              <tbody>
                {topLosers.map((r) => (
                  <tr key={r.ticker} onClick={() => handleNavigateToStock(r.ticker)} style={{ cursor: "pointer" }}>
                    <td>
                      <span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span>
                      <span className="marketTableName">{r.name}</span>
                    </td>
                    <td className="marketTableSector">{r.sector}</td>
                    <td><span className="chip down">{fmtPct(r.predictedReturn)}</span></td>
                    <td><span className={`chip ${r.sentiment.toLowerCase()}`}>{r.sentiment}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card" style={{ gridColumn: "1 / -1" }}>
          <div className="cardHead">
            <div className="cardTitle">Sector Performance</div>
          </div>
          <div className="cardBody">
            <div className="sectorBars">
              {sectorSummary.map((s) => (
                <div key={s.sector} className="sectorBar">
                  <div className="sectorBarLabel">{s.sector} <span className="muted">({s.count})</span></div>
                  <div className="sectorBarTrack">
                    <div
                      className="sectorBarFill"
                      style={{
                        width: `${Math.min(100, Math.max(2, (Math.abs(s.avgReturn) / 3) * 100))}%`,
                        background: s.avgReturn >= 0 ? "var(--good)" : "var(--critical)",
                        opacity: 0.7 + Math.min(0.3, Math.abs(s.avgReturn) / 5),
                      }}
                    />
                  </div>
                  <span className={`sectorBarValue ${s.avgReturn >= 0 ? "pvGood" : "pvCritical"}`}>
                    {fmtPct(s.avgReturn)}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
