"use client";

import { useRef, useState } from "react";
import type { PricePoint, SentimentPoint } from "@/lib/types";
import { dayLabel, fmtMoney, fmtVol } from "@/lib/format";

type Props = {
  series: PricePoint[];
  sentimentSeries: SentimentPoint[];
};

const W = 800;
const PRICE_TOP = 8;
const PRICE_H = 190;
const GAP = 26;
const SENT_TOP = PRICE_TOP + PRICE_H + GAP;
const SENT_H = 60;
const VIEW_H = SENT_TOP + SENT_H + 6;

function nearestSentiment(date: string, sentimentSeries: SentimentPoint[]): SentimentPoint | null {
  if (sentimentSeries.length === 0) return null;
  const t = new Date(date).getTime();
  let best = sentimentSeries[0];
  let bestDiff = Math.abs(new Date(best.date).getTime() - t);
  for (const s of sentimentSeries) {
    const diff = Math.abs(new Date(s.date).getTime() - t);
    if (diff < bestDiff) {
      best = s;
      bestDiff = diff;
    }
  }
  return best;
}

export default function PriceSentimentChart({ series, sentimentSeries }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (series.length < 2) {
    return <div className="emptyState">Not enough price history to chart yet.</div>;
  }

  const closes = series.map((d) => d.close);
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const pad = (max - min) * 0.08 || 1;
  const yMin = min - pad;
  const yMax = max + pad;

  const x = (i: number) => (i / (series.length - 1)) * W;
  const y = (v: number) => PRICE_TOP + PRICE_H - ((v - yMin) / (yMax - yMin)) * PRICE_H;

  const linePath = series.map((d, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(2)},${y(d.close).toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L${x(series.length - 1).toFixed(2)},${PRICE_TOP + PRICE_H} L${x(0).toFixed(2)},${PRICE_TOP + PRICE_H} Z`;

  const barW = Math.max(1.4, W / series.length - 1.4);
  const sentBars = series.map((d, i) => {
    const sp = nearestSentiment(d.date, sentimentSeries);
    if (!sp) return null;
    const cx = x(i);
    const norm = (sp.score - 50) / 50;
    const bh = Math.abs(norm) * (SENT_H / 2 - 2);
    const by = norm >= 0 ? SENT_TOP + SENT_H / 2 - bh : SENT_TOP + SENT_H / 2;
    const color = sp.score > 55 ? "var(--good)" : sp.score < 45 ? "var(--critical)" : "var(--muted)";
    return (
      <rect
        key={i}
        x={cx - barW / 2}
        y={by}
        width={barW}
        height={Math.max(1, bh)}
        fill={color}
        opacity={0.85}
      />
    );
  });

  const lastIdx = series.length - 1;

  function handleMove(evt: React.MouseEvent<SVGRectElement>) {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect) return;
    const px = ((evt.clientX - rect.left) / rect.width) * W;
    const idx = Math.max(0, Math.min(series.length - 1, Math.round((px / W) * (series.length - 1))));
    setHoverIdx(idx);
  }

  const hovered = hoverIdx !== null ? series[hoverIdx] : null;
  const hoveredSentiment = hovered ? nearestSentiment(hovered.date, sentimentSeries) : null;
  const tooltipLeftPct = hoverIdx !== null ? (x(hoverIdx) / W) * 100 : 0;
  const tooltipTopPct = hovered ? (y(hovered.close) / VIEW_H) * 100 : 0;

  return (
    <div className="chartWrap">
      <svg ref={svgRef} viewBox={`0 0 ${W} ${VIEW_H}`} preserveAspectRatio="none">
        {[0, 1, 2, 3].map((g) => (
          <line key={g} x1={0} y1={PRICE_TOP + (PRICE_H / 3) * g} x2={W} y2={PRICE_TOP + (PRICE_H / 3) * g} className="gridline" />
        ))}
        <defs>
          <linearGradient id="areaGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.22" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0" />
          </linearGradient>
        </defs>
        <path d={areaPath} fill="url(#areaGrad)" stroke="none" />
        <path d={linePath} fill="none" stroke="var(--accent)" strokeWidth={2} />
        <circle cx={x(lastIdx)} cy={y(series[lastIdx].close)} r={3.4} fill="var(--accent)" stroke="var(--surface)" strokeWidth={1.6} />

        <text x={4} y={SENT_TOP - 8} className="axisLabel">
          Sentiment (0–100, model-weighted, updated ~weekly)
        </text>
        <line x1={0} y1={SENT_TOP + SENT_H / 2} x2={W} y2={SENT_TOP + SENT_H / 2} className="gridline" />
        {sentBars}

        {hoverIdx !== null && (
          <line x1={x(hoverIdx)} y1={PRICE_TOP} x2={x(hoverIdx)} y2={SENT_TOP + SENT_H} className="crosshairLine" />
        )}
        <rect
          x={0}
          y={0}
          width={W}
          height={VIEW_H}
          fill="transparent"
          onMouseMove={handleMove}
          onMouseLeave={() => setHoverIdx(null)}
        />
      </svg>
      {hovered && (
        <div
          className="tooltip"
          style={{ left: `${tooltipLeftPct}%`, top: `${tooltipTopPct}%`, opacity: 1 }}
        >
          <b>{dayLabel(hovered.date)}</b>
          <br />
          Close {fmtMoney(hovered.close)}
          <br />
          {hoveredSentiment ? `Sentiment ${hoveredSentiment.score}` : "No sentiment reading"} · Vol {fmtVol(hovered.volume)}
        </div>
      )}
    </div>
  );
}
