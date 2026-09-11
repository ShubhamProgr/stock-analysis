"use client";

import { useState, useEffect } from "react";
import type { TopMoversData, TopMoverRow } from "@/lib/types";
import { fmtPct } from "@/lib/format";

type Props = {
  onSelectTicker: (ticker: string) => void;
  children?: React.ReactNode;
};

function MoverList({ title, items, color, onSelectTicker }: { title: string, items: TopMoverRow[], color: string, onSelectTicker: (t: string) => void }) {
  return (
    <div className="card">
      <div className="cardHead">
        <div className="cardTitle">{title}</div>
      </div>
      <div className="cardBody" style={{ padding: 0 }}>
        <table className="marketTable">
          <tbody>
            {items.map((r) => (
              <tr key={r.ticker} onClick={() => onSelectTicker(r.ticker)} style={{ cursor: "pointer" }}>
                <td>
                  <span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span>
                  <div className="marketTableName">{r.name}</div>
                </td>
                <td className="marketTableSector">{r.sector}</td>
                <td style={{ textAlign: "right", color }}>
                  <span className="mono">
                    {r.type === "volume" 
                      ? `${r.value.toFixed(1)}x` 
                      : fmtPct(r.value)}
                  </span>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr><td colSpan={3} style={{ textAlign: "center", color: "var(--muted)", padding: "16px" }}>No data</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function TopMovers({ onSelectTicker, children }: Props) {
  const [data, setData] = useState<TopMoversData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const res = await fetch("/api/movers");
        const json = await res.json();
        if (!cancelled) setData(json);
      } catch (err) {
        console.error(err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, []);

  if (loading || !data) {
    return (
      <div className="viewLoading" style={{ minHeight: 180 }}>
        <div className="viewLoadingSpinner" />
        <p>Loading market movers…</p>
      </div>
    );
  }

  return (
    <div style={{ marginTop: 14 }}>
      <div className="moversGrid">
        <MoverList title="🚀 Daily Gainers" items={data.dailyGainers} color="var(--good)" onSelectTicker={onSelectTicker} />
        <MoverList title="🔻 Daily Losers" items={data.dailyLosers} color="var(--critical)" onSelectTicker={onSelectTicker} />
      </div>

      <div className="moversGrid">
        <MoverList title="🔮 AI Predicted Gainers" items={data.predictedGainers} color="var(--good)" onSelectTicker={onSelectTicker} />
        <MoverList title="🔮 AI Predicted Losers" items={data.predictedLosers} color="var(--critical)" onSelectTicker={onSelectTicker} />
      </div>

      <div className="moversGrid">
        <MoverList title="📊 Unusual Volume Spikes" items={data.volumeSpikes} color="var(--accent)" onSelectTicker={onSelectTicker} />
        {children}
      </div>
    </div>
  );
}
