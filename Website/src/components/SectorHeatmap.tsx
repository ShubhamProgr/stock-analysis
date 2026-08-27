"use client";

import type { MarketOverviewRow } from "@/lib/types";
import { fmtPct, fmtMarketCap } from "@/lib/format";

type Props = {
  rows: MarketOverviewRow[];
  onSelectTicker: (ticker: string) => void;
};

/** Returns a CSS colour for a given predicted return percentage */
function returnColour(pct: number): string {
  if (pct > 2) return "var(--good)";
  if (pct > 0.5) return "rgba(53, 193, 94, 0.55)";
  if (pct > 0) return "rgba(53, 193, 94, 0.25)";
  if (pct > -0.5) return "rgba(232, 99, 95, 0.25)";
  if (pct > -2) return "rgba(232, 99, 95, 0.55)";
  return "var(--critical)";
}

function textColourForBg(pct: number): string {
  if (Math.abs(pct) > 2) return "#fff";
  return "var(--ink)";
}

export default function SectorHeatmap({ rows, onSelectTicker }: Props) {
  // Group by sector
  const bySector = new Map<string, MarketOverviewRow[]>();
  for (const r of rows) {
    const arr = bySector.get(r.sector) ?? [];
    arr.push(r);
    bySector.set(r.sector, arr);
  }

  // Sort sectors by total market cap (descending)
  const sectors = [...bySector.entries()]
    .map(([sector, items]) => ({
      sector,
      items: items.sort((a, b) => b.marketCap - a.marketCap),
      totalCap: items.reduce((s, i) => s + i.marketCap, 0),
    }))
    .sort((a, b) => b.totalCap - a.totalCap);

  return (
    <div className="heatmapContainer">
      {sectors.map(({ sector, items }) => (
        <div key={sector} className="heatmapSector">
          <div className="heatmapSectorLabel eyebrow">{sector}</div>
          <div className="heatmapGrid">
            {items.map((item) => (
              <button
                key={item.ticker}
                className="heatmapCell"
                style={{
                  background: returnColour(item.predictedReturn),
                  color: textColourForBg(item.predictedReturn),
                }}
                onClick={() => onSelectTicker(item.ticker)}
                title={`${item.name}\n${fmtPct(item.predictedReturn)} predicted\nSentiment: ${item.sentiment}\nMkt Cap: ₹${fmtMarketCap(item.marketCap)}`}
              >
                <span className="heatmapTicker">{item.ticker.replace(".NS", "")}</span>
                <span className="heatmapReturn">{fmtPct(item.predictedReturn)}</span>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
