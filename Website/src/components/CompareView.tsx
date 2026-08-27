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
  Legend,
} from "recharts";
import type { ComparisonBundle, WatchlistRow } from "@/lib/types";
import { fmtMoney, fmtPct, fmtCompact } from "@/lib/format";

type Props = {
  watchlist: WatchlistRow[];
  onSelectTicker: (ticker: string) => void;
};

const COMPARE_COLORS = ["var(--accent)", "var(--good)", "var(--caution)"];
const RANGE_OPTIONS = [
  { label: "1M", days: 22 },
  { label: "3M", days: 64 },
  { label: "6M", days: 126 },
  { label: "1Y", days: 252 },
];

export default function CompareView({ watchlist, onSelectTicker }: Props) {
  const [selected, setSelected] = useState<string[]>([]);
  const [bundle, setBundle] = useState<ComparisonBundle | null>(null);
  const [loading, setLoading] = useState(false);
  const [rangeDays, setRangeDays] = useState(126);
  const [search, setSearch] = useState("");

  // Auto-load when selection changes
  useEffect(() => {
    if (selected.length < 2) {
      setBundle(null);
      return;
    }
    async function load() {
      setLoading(true);
      try {
        const res = await fetch(`/api/compare?tickers=${selected.join(",")}&range=${rangeDays}`);
        const data = await res.json();
        setBundle(data);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [selected, rangeDays]);

  function toggleTicker(ticker: string) {
    setSelected((prev) => {
      if (prev.includes(ticker)) return prev.filter((t) => t !== ticker);
      if (prev.length >= 3) return prev; // Max 3
      return [...prev, ticker];
    });
  }

  const filteredWatchlist = useMemo(() => {
    if (!search) return watchlist;
    const q = search.toLowerCase();
    return watchlist.filter(
      (r) => r.ticker.toLowerCase().includes(q) || r.name.toLowerCase().includes(q)
    );
  }, [watchlist, search]);

  // Build merged chart data
  const chartData = useMemo(() => {
    if (!bundle || bundle.tickers.length === 0) return [];
    // Use the first ticker's date range as the base
    const dateSet = new Set<string>();
    for (const t of bundle.tickers) {
      for (const p of t.series) dateSet.add(p.date);
    }
    const dates = [...dateSet].sort();

    return dates.map((date) => {
      const row: Record<string, string | number | null> = {
        date: new Date(date).toLocaleDateString("en-IN", { day: "numeric", month: "short" }),
      };
      for (const t of bundle.tickers) {
        const point = t.series.find((p) => p.date === date);
        row[t.ticker] = point ? point.normalised : null;
      }
      return row;
    });
  }, [bundle]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload) return null;
    return (
      <div className="pvTooltip">
        <div className="pvTooltipDate">{label}</div>
        {payload.map((p: { name: string; value: number; color: string }, i: number) => (
          <div key={i} className="pvTooltipRow">
            <span style={{ color: p.color }}>{p.name.replace(".NS", "")}</span>
            <strong className="mono">{p.value !== null ? fmtPct(p.value) : "—"}</strong>
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className="compareWrap">
      <div className="viewHeader">
        <h1 className="viewTitle">Stock Comparison</h1>
        <span className="viewSubtitle">Select 2–3 stocks to compare</span>
      </div>

      {/* Ticker selector */}
      <div className="compareSelector">
        <div className="compareSelectorLeft">
          <input
            className="screenerSearch"
            type="text"
            placeholder="Search ticker…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            style={{ maxWidth: 240 }}
          />
          <div className="compareSelectedChips">
            {selected.map((t, i) => (
              <button
                key={t}
                className="compareSelectedChip"
                style={{ borderColor: COMPARE_COLORS[i] }}
                onClick={() => toggleTicker(t)}
              >
                <span className="dot" style={{ background: COMPARE_COLORS[i] }}></span>
                {t.replace(".NS", "")}
                <span className="compareRemove">×</span>
              </button>
            ))}
            {selected.length < 2 && (
              <span className="muted" style={{ fontSize: 12 }}>
                {selected.length === 0 ? "Pick at least 2 stocks below" : "Pick 1 more stock"}
              </span>
            )}
          </div>
        </div>
        <div className="rangeToggle">
          {RANGE_OPTIONS.map((r) => (
            <button
              key={r.label}
              className={rangeDays === r.days ? "active" : ""}
              onClick={() => setRangeDays(r.days)}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>

      {/* Ticker picker grid */}
      <div className="comparePickerGrid">
        {filteredWatchlist.map((row) => {
          const isSelected = selected.includes(row.ticker);
          const colorIndex = selected.indexOf(row.ticker);
          return (
            <button
              key={row.ticker}
              className={`comparePickerItem ${isSelected ? "active" : ""}`}
              onClick={() => toggleTicker(row.ticker)}
              disabled={!isSelected && selected.length >= 3}
              style={isSelected ? { borderColor: COMPARE_COLORS[colorIndex], background: `${COMPARE_COLORS[colorIndex]}11` } : undefined}
            >
              <span className="mono" style={{ fontWeight: 700, fontSize: 12 }}>{row.ticker.replace(".NS", "")}</span>
              <span className="comparePickerPrice mono">{fmtMoney(row.price)}</span>
            </button>
          );
        })}
      </div>

      {/* Chart */}
      {loading && (
        <div className="viewLoading" style={{ minHeight: 200 }}>
          <div className="viewLoadingSpinner" />
        </div>
      )}

      {!loading && bundle && bundle.tickers.length >= 2 && (
        <>
          <div className="card" style={{ marginTop: 16 }}>
            <div className="cardHead">
              <div className="cardTitle">Normalised Price Performance</div>
            </div>
            <div className="cardBody">
              <ResponsiveContainer width="100%" height={320}>
                <LineChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 10, fill: "var(--muted)" }}
                    interval={Math.max(1, Math.floor(chartData.length / 8))}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: "var(--muted)" }}
                    tickFormatter={(v: number) => fmtPct(v)}
                    width={55}
                  />
                  <Tooltip content={<CustomTooltip />} />
                  {bundle.tickers.map((t, i) => (
                    <Line
                      key={t.ticker}
                      type="monotone"
                      dataKey={t.ticker}
                      stroke={COMPARE_COLORS[i]}
                      strokeWidth={2}
                      dot={false}
                      connectNulls
                      name={t.ticker}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="legendRow">
              {bundle.tickers.map((t, i) => (
                <span key={t.ticker}>
                  <span className="dot" style={{ background: COMPARE_COLORS[i] }}></span>
                  {t.ticker.replace(".NS", "")} — {t.name}
                </span>
              ))}
            </div>
          </div>

          {/* Side-by-side fundamentals */}
          <div className="card" style={{ marginTop: 16 }}>
            <div className="cardHead">
              <div className="cardTitle">Fundamentals Comparison</div>
            </div>
            <div className="cardBody" style={{ padding: 0 }}>
              <table className="compareTable">
                <thead>
                  <tr>
                    <th>Metric</th>
                    {bundle.tickers.map((t, i) => (
                      <th key={t.ticker}>
                        <span style={{ color: COMPARE_COLORS[i], fontWeight: 700 }}>{t.ticker.replace(".NS", "")}</span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Sector</td>
                    {bundle.tickers.map((t) => <td key={t.ticker}>{t.sector}</td>)}
                  </tr>
                  <tr>
                    <td>Market Cap</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker} className="mono">{t.marketCap ? fmtCompact(t.marketCap) : "—"}</td>
                    ))}
                  </tr>
                  <tr>
                    <td>P/E Ratio</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker} className="mono">{t.trailingPE !== null ? t.trailingPE.toFixed(1) : "—"}</td>
                    ))}
                  </tr>
                  <tr>
                    <td>Profit Margin</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker} className="mono">{t.profitMargins !== null ? (t.profitMargins * 100).toFixed(1) + "%" : "—"}</td>
                    ))}
                  </tr>
                  <tr>
                    <td>52W Change</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker}>
                        {t.change52Week !== null ? (
                          <span className={t.change52Week >= 0 ? "pvGood" : "pvCritical"}>
                            {fmtPct(t.change52Week * 100)}
                          </span>
                        ) : "—"}
                      </td>
                    ))}
                  </tr>
                  <tr>
                    <td>Predicted Return</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker}>
                        <span className={`chip ${t.predictedReturn >= 0 ? "up" : "down"}`}>
                          {fmtPct(t.predictedReturn)}
                        </span>
                      </td>
                    ))}
                  </tr>
                  <tr>
                    <td>Sentiment</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker}><span className={`chip ${t.sentiment.toLowerCase()}`}>{t.sentiment}</span></td>
                    ))}
                  </tr>
                  <tr>
                    <td>Signal</td>
                    {bundle.tickers.map((t) => (
                      <td key={t.ticker}><span className={`chip ${t.signal.toLowerCase()}`}>{t.signal}</span></td>
                    ))}
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
