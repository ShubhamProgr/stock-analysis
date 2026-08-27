"use client";

import { useMemo } from "react";
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Cell,
  ReferenceLine,
} from "recharts";
import type { OHLCPoint } from "@/lib/types";
import { fmtMoney, fmtVol } from "@/lib/format";

type Props = {
  data: OHLCPoint[];
  showSMA20?: boolean;
  showSMA50?: boolean;
  showBollinger?: boolean;
};

function sma(closes: number[], period: number): (number | null)[] {
  return closes.map((_, i) => {
    if (i < period - 1) return null;
    const slice = closes.slice(i - period + 1, i + 1);
    return slice.reduce((a, b) => a + b, 0) / period;
  });
}

function bollingerBands(closes: number[], period = 20, mult = 2) {
  const upper: (number | null)[] = [];
  const lower: (number | null)[] = [];
  for (let i = 0; i < closes.length; i++) {
    if (i < period - 1) {
      upper.push(null);
      lower.push(null);
      continue;
    }
    const slice = closes.slice(i - period + 1, i + 1);
    const mean = slice.reduce((a, b) => a + b, 0) / period;
    const std = Math.sqrt(slice.reduce((s, v) => s + (v - mean) ** 2, 0) / period);
    upper.push(mean + mult * std);
    lower.push(mean - mult * std);
  }
  return { upper, lower };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function CustomCandlestick(props: any) {
  const { x, y, width, height, payload } = props;
  if (!payload) return null;
  const { open, close, high, low } = payload;
  const isUp = close >= open;
  const color = isUp ? "var(--good)" : "var(--critical)";

  const bodyTop = Math.min(open, close);
  const bodyBottom = Math.max(open, close);
  const bodyHeight = Math.max(1, Math.abs(height));

  // Compute Y-scale from the chart
  const yScale = props.yAxis?.scale;
  if (!yScale) {
    return (
      <rect x={x} y={y} width={width} height={Math.max(1, Math.abs(height))} fill={color} rx={1} />
    );
  }

  const wickX = x + width / 2;
  const highY = yScale(high);
  const lowY = yScale(low);
  const openY = yScale(open);
  const closeY = yScale(close);
  const topY = Math.min(openY, closeY);
  const barHeight = Math.max(1, Math.abs(openY - closeY));

  return (
    <g>
      {/* Upper wick */}
      <line x1={wickX} y1={highY} x2={wickX} y2={topY} stroke={color} strokeWidth={1} />
      {/* Lower wick */}
      <line x1={wickX} y1={topY + barHeight} x2={wickX} y2={lowY} stroke={color} strokeWidth={1} />
      {/* Body */}
      <rect x={x} y={topY} width={width} height={barHeight} fill={isUp ? color : color} rx={1} opacity={isUp ? 0.9 : 0.9} />
    </g>
  );
}

export default function CandlestickChart({ data, showSMA20 = true, showSMA50 = true, showBollinger = false }: Props) {
  const chartData = useMemo(() => {
    const closes = data.map((d) => d.close);
    const sma20 = sma(closes, 20);
    const sma50 = sma(closes, 50);
    const bb = bollingerBands(closes);

    return data.map((d, i) => ({
      ...d,
      dateLabel: new Date(d.date).toLocaleDateString("en-IN", { day: "numeric", month: "short" }),
      sma20: sma20[i],
      sma50: sma50[i],
      bbUpper: bb.upper[i],
      bbLower: bb.lower[i],
      barRange: [Math.min(d.open, d.close), Math.max(d.open, d.close)],
      isUp: d.close >= d.open,
    }));
  }, [data]);

  if (data.length === 0) {
    return <div className="emptyState">No OHLC data available.</div>;
  }

  const allPrices = data.flatMap((d) => [d.high, d.low]);
  const yMin = Math.min(...allPrices) * 0.995;
  const yMax = Math.max(...allPrices) * 1.005;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const CustomTooltipContent = ({ active, payload }: any) => {
    if (!active || !payload?.[0]) return null;
    const d = payload[0].payload;
    return (
      <div className="pvTooltip">
        <div className="pvTooltipDate">{d.dateLabel}</div>
        <div className="pvTooltipRow"><span>Open</span><strong className="mono">{fmtMoney(d.open)}</strong></div>
        <div className="pvTooltipRow"><span>High</span><strong className="mono">{fmtMoney(d.high)}</strong></div>
        <div className="pvTooltipRow"><span>Low</span><strong className="mono">{fmtMoney(d.low)}</strong></div>
        <div className="pvTooltipRow"><span>Close</span><strong className="mono">{fmtMoney(d.close)}</strong></div>
        <div className="pvTooltipDivider" />
        <div className="pvTooltipRow"><span>Volume</span><strong className="mono">{fmtVol(d.volume)}</strong></div>
      </div>
    );
  };

  return (
    <div className="candlestickWrap">
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={chartData} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
          <XAxis
            dataKey="dateLabel"
            tick={{ fontSize: 10, fill: "var(--muted)" }}
            interval={Math.max(1, Math.floor(chartData.length / 8))}
            tickLine={false}
          />
          <YAxis
            domain={[yMin, yMax]}
            tick={{ fontSize: 10, fill: "var(--muted)" }}
            tickFormatter={(v: number) => fmtMoney(v)}
            width={80}
          />
          <Tooltip content={<CustomTooltipContent />} />

          {/* Bollinger Bands */}
          {showBollinger && (
            <>
              <Line type="monotone" dataKey="bbUpper" stroke="var(--caution)" strokeWidth={1} strokeDasharray="4 2" dot={false} />
              <Line type="monotone" dataKey="bbLower" stroke="var(--caution)" strokeWidth={1} strokeDasharray="4 2" dot={false} />
            </>
          )}

          {/* Candlestick bodies as bars */}
          <Bar dataKey="barRange" barSize={Math.max(2, Math.min(8, 400 / chartData.length))} isAnimationActive={false}>
            {chartData.map((entry, index) => (
              <Cell key={index} fill={entry.isUp ? "var(--good)" : "var(--critical)"} />
            ))}
          </Bar>

          {/* SMA overlays */}
          {showSMA20 && (
            <Line type="monotone" dataKey="sma20" stroke="var(--accent)" strokeWidth={1.5} dot={false} connectNulls />
          )}
          {showSMA50 && (
            <Line type="monotone" dataKey="sma50" stroke="var(--caution)" strokeWidth={1.5} dot={false} connectNulls />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Volume subplot */}
      <ResponsiveContainer width="100%" height={60}>
        <ComposedChart data={chartData} margin={{ top: 0, right: 8, bottom: 0, left: 0 }}>
          <XAxis dataKey="dateLabel" hide />
          <YAxis hide />
          <Bar dataKey="volume" isAnimationActive={false}>
            {chartData.map((entry, index) => (
              <Cell key={index} fill={entry.isUp ? "var(--good)" : "var(--critical)"} opacity={0.3} />
            ))}
          </Bar>
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
