"use client";

import { useMemo } from "react";
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceLine,
  Cell,
} from "recharts";
import type { OHLCPoint } from "@/lib/types";

type Props = {
  data: OHLCPoint[];
};

function computeRSI(closes: number[], period = 14): (number | null)[] {
  const rsi: (number | null)[] = [];
  for (let i = 0; i < closes.length; i++) {
    if (i < period) {
      rsi.push(null);
      continue;
    }
    let gains = 0;
    let losses = 0;
    for (let j = i - period + 1; j <= i; j++) {
      const diff = closes[j] - closes[j - 1];
      if (diff > 0) gains += diff;
      else losses -= diff;
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
    if (avgLoss === 0) {
      rsi.push(100);
    } else {
      const rs = avgGain / avgLoss;
      rsi.push(100 - 100 / (1 + rs));
    }
  }
  return rsi;
}

function computeMACD(closes: number[]) {
  const ema = (data: number[], span: number): number[] => {
    const k = 2 / (span + 1);
    const result = [data[0]];
    for (let i = 1; i < data.length; i++) {
      result.push(data[i] * k + result[i - 1] * (1 - k));
    }
    return result;
  };

  const ema12 = ema(closes, 12);
  const ema26 = ema(closes, 26);
  const macd = ema12.map((v, i) => (i < 25 ? null : v - ema26[i]));
  const macdValues = macd.filter((v) => v !== null) as number[];
  const signalLine = ema(macdValues, 9);

  // Pad signal to align with MACD
  const offset = macd.length - macdValues.length;
  const signal: (number | null)[] = new Array(offset).fill(null);
  for (let i = 0; i < signalLine.length; i++) {
    if (i < 8) {
      signal.push(null);
    } else {
      signal.push(signalLine[i]);
    }
  }

  const histogram = macd.map((v, i) => {
    if (v === null || signal[i] === null) return null;
    return v - (signal[i] as number);
  });

  return { macd, signal, histogram };
}

export default function TechnicalPanel({ data }: Props) {
  const chartData = useMemo(() => {
    const closes = data.map((d) => d.close);
    const rsi = computeRSI(closes);
    const { macd, signal, histogram } = computeMACD(closes);

    return data.map((d, i) => ({
      dateLabel: new Date(d.date).toLocaleDateString("en-IN", { day: "numeric", month: "short" }),
      rsi: rsi[i],
      macd: macd[i],
      signal: signal[i],
      histogram: histogram[i],
    }));
  }, [data]);

  if (data.length < 30) {
    return <div className="emptyState">Need at least 30 data points for technical indicators.</div>;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const RSITooltip = ({ active, payload }: any) => {
    if (!active || !payload?.[0]) return null;
    const d = payload[0].payload;
    return (
      <div className="pvTooltip">
        <div className="pvTooltipDate">{d.dateLabel}</div>
        <div className="pvTooltipRow">
          <span>RSI (14)</span>
          <strong className={`mono ${d.rsi > 70 ? "pvCritical" : d.rsi < 30 ? "pvGood" : ""}`}>
            {d.rsi !== null ? d.rsi.toFixed(1) : "—"}
          </strong>
        </div>
      </div>
    );
  };

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const MACDTooltip = ({ active, payload }: any) => {
    if (!active || !payload?.[0]) return null;
    const d = payload[0].payload;
    return (
      <div className="pvTooltip">
        <div className="pvTooltipDate">{d.dateLabel}</div>
        <div className="pvTooltipRow"><span>MACD</span><strong className="mono">{d.macd !== null ? d.macd.toFixed(2) : "—"}</strong></div>
        <div className="pvTooltipRow"><span>Signal</span><strong className="mono">{d.signal !== null ? d.signal.toFixed(2) : "—"}</strong></div>
        <div className="pvTooltipRow"><span>Histogram</span><strong className="mono">{d.histogram !== null ? d.histogram.toFixed(2) : "—"}</strong></div>
      </div>
    );
  };

  return (
    <div className="technicalPanelWrap">
      {/* RSI Chart */}
      <div className="techSubChart">
        <div className="techSubLabel eyebrow">RSI (14)</div>
        <ResponsiveContainer width="100%" height={120}>
          <ComposedChart data={chartData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
            <XAxis dataKey="dateLabel" hide />
            <YAxis domain={[0, 100]} ticks={[30, 50, 70]} tick={{ fontSize: 9, fill: "var(--muted)" }} width={30} />
            <Tooltip content={<RSITooltip />} />
            <ReferenceLine y={70} stroke="var(--critical)" strokeDasharray="3 3" strokeOpacity={0.5} />
            <ReferenceLine y={30} stroke="var(--good)" strokeDasharray="3 3" strokeOpacity={0.5} />
            <Line type="monotone" dataKey="rsi" stroke="var(--accent)" strokeWidth={1.5} dot={false} connectNulls />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="techSubLegend">
          <span className="pvCritical">Overbought (&gt;70)</span>
          <span className="pvGood">Oversold (&lt;30)</span>
        </div>
      </div>

      {/* MACD Chart */}
      <div className="techSubChart">
        <div className="techSubLabel eyebrow">MACD</div>
        <ResponsiveContainer width="100%" height={120}>
          <ComposedChart data={chartData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
            <XAxis dataKey="dateLabel" hide />
            <YAxis tick={{ fontSize: 9, fill: "var(--muted)" }} width={50} />
            <Tooltip content={<MACDTooltip />} />
            <ReferenceLine y={0} stroke="var(--muted)" strokeDasharray="3 3" />
            <Bar dataKey="histogram" isAnimationActive={false}>
              {chartData.map((entry, index) => (
                <Cell key={index} fill={(entry.histogram ?? 0) >= 0 ? "var(--good)" : "var(--critical)"} opacity={0.5} />
              ))}
            </Bar>
            <Line type="monotone" dataKey="macd" stroke="var(--accent)" strokeWidth={1.5} dot={false} connectNulls />
            <Line type="monotone" dataKey="signal" stroke="var(--critical)" strokeWidth={1} dot={false} connectNulls strokeDasharray="4 2" />
          </ComposedChart>
        </ResponsiveContainer>
        <div className="techSubLegend">
          <span style={{ color: "var(--accent)" }}>● MACD Line</span>
          <span style={{ color: "var(--critical)" }}>● Signal Line</span>
          <span style={{ color: "var(--muted)" }}>█ Histogram</span>
        </div>
      </div>
    </div>
  );
}
