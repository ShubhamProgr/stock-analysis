"use client";

import { useState, useEffect, useMemo } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Bar,
  ComposedChart,
  Cell,
} from "recharts";
import type { AccuracyData, AccuracyRow } from "@/lib/types";
import { fmtMoney, fmtPct } from "@/lib/format";

type Props = {
  onSelectTicker: (ticker: string) => void;
};

type SortKey = "ticker" | "mape" | "directionAccuracy" | "totalPredictions" | "avgAbsError";
type SortDir = "asc" | "desc";

export default function AccuracyView({ onSelectTicker }: Props) {
  const [data, setData] = useState<AccuracyData | null>(null);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState<SortKey>("mape");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/accuracy");
        const json = await res.json();
        setData(json);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const sortedTickers = useMemo(() => {
    if (!data) return [];
    return [...data.perTicker].sort((a, b) => {
      let av = 0, bv = 0;
      switch (sortKey) {
        case "ticker": return sortDir === "asc" ? a.ticker.localeCompare(b.ticker) : b.ticker.localeCompare(a.ticker);
        case "mape": av = a.mape; bv = b.mape; break;
        case "directionAccuracy": av = a.directionAccuracy; bv = b.directionAccuracy; break;
        case "totalPredictions": av = a.totalPredictions; bv = b.totalPredictions; break;
        case "avgAbsError": av = a.avgAbsError; bv = b.avgAbsError; break;
      }
      return sortDir === "asc" ? av - bv : bv - av;
    });
  }, [data, sortKey, sortDir]);

  // Rolling MAPE (7-day window)
  const rollingMAPE = useMemo(() => {
    if (!data || data.timeSeries.length === 0) return [];
    const sorted = [...data.timeSeries].sort((a, b) => a.date.localeCompare(b.date));
    const window = 7;
    const result: { date: string; mape7d: number; mape30d: number | null }[] = [];
    for (let i = 0; i < sorted.length; i++) {
      const start7 = Math.max(0, i - window + 1);
      const slice7 = sorted.slice(start7, i + 1);
      const mape7d = slice7.reduce((s, v) => s + v.mape, 0) / slice7.length;

      const start30 = Math.max(0, i - 29);
      const slice30 = sorted.slice(start30, i + 1);
      const mape30d = slice30.length >= 10 ? slice30.reduce((s, v) => s + v.mape, 0) / slice30.length : null;

      result.push({
        date: new Date(sorted[i].date).toLocaleDateString("en-IN", { day: "numeric", month: "short" }),
        mape7d: Math.round(mape7d * 100) / 100,
        mape30d: mape30d !== null ? Math.round(mape30d * 100) / 100 : null,
      });
    }
    // Reduce to one entry per date (average if multiple tickers on same date)
    const dateMap = new Map<string, { mape7d: number[]; mape30d: (number | null)[] }>();
    for (const entry of result) {
      const existing = dateMap.get(entry.date) ?? { mape7d: [], mape30d: [] };
      existing.mape7d.push(entry.mape7d);
      existing.mape30d.push(entry.mape30d);
      dateMap.set(entry.date, existing);
    }
    return [...dateMap.entries()].map(([date, vals]) => ({
      date,
      mape7d: vals.mape7d.reduce((a, b) => a + b, 0) / vals.mape7d.length,
      mape30d: vals.mape30d.filter((v) => v !== null).length > 0
        ? vals.mape30d.filter((v): v is number => v !== null).reduce((a, b) => a + b, 0) / vals.mape30d.filter((v) => v !== null).length
        : null,
    }));
  }, [data]);

  // Error distribution histogram
  const errorDistribution = useMemo(() => {
    if (!data || data.timeSeries.length === 0) return [];
    const buckets = new Map<string, { count: number; label: string }>();
    const ranges = [
      { min: -Infinity, max: -5, label: "<-5%" },
      { min: -5, max: -3, label: "-5 to -3%" },
      { min: -3, max: -1, label: "-3 to -1%" },
      { min: -1, max: 1, label: "-1 to 1%" },
      { min: 1, max: 3, label: "1 to 3%" },
      { min: 3, max: 5, label: "3 to 5%" },
      { min: 5, max: Infinity, label: ">5%" },
    ];
    for (const r of ranges) {
      buckets.set(r.label, { count: 0, label: r.label });
    }
    for (const ts of data.timeSeries) {
      const pctError = ts.error; // Already in absolute units, compute as % of typical price
      for (const r of ranges) {
        if (ts.mape >= r.min && ts.mape < r.max) {
          const b = buckets.get(r.label)!;
          b.count++;
          break;
        }
      }
    }
    return [...buckets.values()];
  }, [data]);

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => d === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir(key === "mape" || key === "avgAbsError" ? "asc" : "desc");
    }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return "";
    return sortDir === "asc" ? " ▲" : " ▼";
  }

  if (loading) {
    return (
      <div className="viewLoading">
        <div className="viewLoadingSpinner" />
        <p>Computing accuracy metrics…</p>
      </div>
    );
  }

  if (!data || data.overall.totalPredictions === 0) {
    return (
      <div className="viewLoading">
        <p>No prediction accuracy data available yet. Run Actual_vs_Prediction.py after predictions have been made.</p>
      </div>
    );
  }

  const best5 = sortedTickers.filter((r) => r.totalPredictions >= 3).slice(0, 5);
  const worst5 = [...sortedTickers].filter((r) => r.totalPredictions >= 3).reverse().slice(0, 5);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const MAPETooltip = ({ active, payload }: any) => {
    if (!active || !payload?.[0]) return null;
    const d = payload[0].payload;
    return (
      <div className="pvTooltip">
        <div className="pvTooltipDate">{d.date}</div>
        <div className="pvTooltipRow"><span>7D MAPE</span><strong className="mono">{d.mape7d.toFixed(2)}%</strong></div>
        {d.mape30d !== null && (
          <div className="pvTooltipRow"><span>30D MAPE</span><strong className="mono">{d.mape30d.toFixed(2)}%</strong></div>
        )}
      </div>
    );
  };

  return (
    <div className="accuracyWrap">
      <div className="viewHeader">
        <h1 className="viewTitle">Prediction Accuracy</h1>
        <span className="viewSubtitle">{data.overall.totalPredictions} predictions analysed</span>
      </div>

      {/* Overall metrics */}
      <section className="statRow">
        <div className="tile">
          <div className="eyebrow">Mean Abs % Error</div>
          <div className={`value mono ${data.overall.mape < 2 ? "pvGood" : data.overall.mape < 5 ? "pvCaution" : "pvCritical"}`}>
            {data.overall.mape.toFixed(2)}%
          </div>
          <div className="sub">lower is better</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Direction Accuracy</div>
          <div className={`value mono ${data.overall.directionAccuracy > 60 ? "pvGood" : "pvCaution"}`}>
            {data.overall.directionAccuracy.toFixed(1)}%
          </div>
          <div className="sub">% of times predicted correct direction</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Avg Absolute Error</div>
          <div className="value mono">{fmtMoney(data.overall.avgError)}</div>
          <div className="sub">in rupees</div>
        </div>
        <div className="tile">
          <div className="eyebrow">Total Predictions</div>
          <div className="value mono">{data.overall.totalPredictions}</div>
          <div className="sub">{data.perTicker.length} tickers</div>
        </div>
      </section>

      {/* Rolling MAPE chart */}
      <div className="card" style={{ marginBottom: 16 }}>
        <div className="cardHead">
          <div className="cardTitle">Rolling MAPE Over Time</div>
        </div>
        <div className="cardBody">
          {rollingMAPE.length > 0 ? (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={rollingMAPE} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 10, fill: "var(--muted)" }}
                  interval={Math.max(1, Math.floor(rollingMAPE.length / 8))}
                />
                <YAxis tick={{ fontSize: 10, fill: "var(--muted)" }} width={40} />
                <Tooltip content={<MAPETooltip />} />
                <Line type="monotone" dataKey="mape7d" stroke="var(--accent)" strokeWidth={2} dot={false} name="7D MAPE" />
                <Line type="monotone" dataKey="mape30d" stroke="var(--caution)" strokeWidth={1.5} dot={false} strokeDasharray="4 2" connectNulls name="30D MAPE" />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div className="emptyState">Not enough data for rolling MAPE chart.</div>
          )}
        </div>
        <div className="legendRow">
          <span><span className="dot" style={{ background: "var(--accent)" }}></span>7-Day Rolling MAPE</span>
          <span><span className="dot" style={{ background: "var(--caution)" }}></span>30-Day Rolling MAPE</span>
        </div>
      </div>

      {/* Best / Worst predicted + Error distribution */}
      <div className="marketGridBottom">
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Best Predicted (Lowest MAPE)</div>
          </div>
          <div className="cardBody" style={{ padding: 0 }}>
            <table className="marketTable">
              <thead>
                <tr><th>Stock</th><th>MAPE</th><th>Dir. Accuracy</th><th>Count</th></tr>
              </thead>
              <tbody>
                {best5.map((r) => (
                  <tr key={r.ticker} onClick={() => onSelectTicker(r.ticker)} style={{ cursor: "pointer" }}>
                    <td><span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span></td>
                    <td className="pvGood mono">{r.mape.toFixed(2)}%</td>
                    <td className="mono">{r.directionAccuracy.toFixed(0)}%</td>
                    <td className="mono">{r.totalPredictions}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Worst Predicted (Highest MAPE)</div>
          </div>
          <div className="cardBody" style={{ padding: 0 }}>
            <table className="marketTable">
              <thead>
                <tr><th>Stock</th><th>MAPE</th><th>Dir. Accuracy</th><th>Count</th></tr>
              </thead>
              <tbody>
                {worst5.map((r) => (
                  <tr key={r.ticker} onClick={() => onSelectTicker(r.ticker)} style={{ cursor: "pointer" }}>
                    <td><span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span></td>
                    <td className="pvCritical mono">{r.mape.toFixed(2)}%</td>
                    <td className="mono">{r.directionAccuracy.toFixed(0)}%</td>
                    <td className="mono">{r.totalPredictions}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* Error distribution */}
      <div className="card" style={{ marginTop: 16, marginBottom: 16 }}>
        <div className="cardHead">
          <div className="cardTitle">Error Distribution</div>
        </div>
        <div className="cardBody">
          {errorDistribution.length > 0 ? (
            <ResponsiveContainer width="100%" height={160}>
              <ComposedChart data={errorDistribution} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
                <XAxis dataKey="label" tick={{ fontSize: 10, fill: "var(--muted)" }} />
                <YAxis tick={{ fontSize: 10, fill: "var(--muted)" }} width={30} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {errorDistribution.map((entry, index) => (
                    <Cell key={index} fill={index === 3 ? "var(--good)" : index <= 1 || index >= 5 ? "var(--critical)" : "var(--accent)"} opacity={0.7} />
                  ))}
                </Bar>
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <div className="emptyState">No error distribution data.</div>
          )}
        </div>
      </div>

      {/* Full per-ticker accuracy table */}
      <div className="card screenerTableCard">
        <div className="cardHead">
          <div className="cardTitle">Per-Ticker Accuracy</div>
        </div>
        <div className="screenerTableWrap">
          <table className="screenerTable">
            <thead>
              <tr>
                <th onClick={() => handleSort("ticker")} className="sortable">Ticker{sortIndicator("ticker")}</th>
                <th>Name</th>
                <th onClick={() => handleSort("mape")} className="sortable right">MAPE{sortIndicator("mape")}</th>
                <th onClick={() => handleSort("directionAccuracy")} className="sortable right">Direction{sortIndicator("directionAccuracy")}</th>
                <th onClick={() => handleSort("avgAbsError")} className="sortable right">Avg |Error|{sortIndicator("avgAbsError")}</th>
                <th onClick={() => handleSort("totalPredictions")} className="sortable right">Count{sortIndicator("totalPredictions")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedTickers.map((r) => (
                <tr key={r.ticker} onClick={() => onSelectTicker(r.ticker)} className="screenerRow">
                  <td><span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span></td>
                  <td className="screenerName">{r.name}</td>
                  <td className={`right mono ${r.mape < 2 ? "pvGood" : r.mape < 5 ? "" : "pvCritical"}`}>{r.mape.toFixed(2)}%</td>
                  <td className={`right mono ${r.directionAccuracy > 60 ? "pvGood" : ""}`}>{r.directionAccuracy.toFixed(0)}%</td>
                  <td className="right mono">{fmtMoney(r.avgAbsError)}</td>
                  <td className="right mono">{r.totalPredictions}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
