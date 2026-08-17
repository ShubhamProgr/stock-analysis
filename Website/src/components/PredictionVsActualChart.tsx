"use client";

import { useState, useMemo } from "react";
import type { PredictionPoint } from "@/lib/types";
import { fmtMoney, dayLabel } from "@/lib/format";
import {
  ResponsiveContainer,
  ComposedChart,
  Area,
  Line,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RTooltip,
  Brush,
} from "recharts";

type Props = {
  predictionHistory: PredictionPoint[];
  ticker: string;
};

const RANGE_OPTIONS = [
  { label: "30D", count: 30 },
  { label: "90D", count: 90 },
  { label: "All", count: Infinity },
];

function computeStats(data: PredictionPoint[]) {
  if (data.length === 0) return null;

  const errors = data.map((d) => d.predicted - d.actual);
  const absErrors = errors.map((e) => Math.abs(e));
  const pctErrors = data.map((d) =>
    d.actual !== 0 ? Math.abs((d.predicted - d.actual) / d.actual) * 100 : 0
  );

  const mae = absErrors.reduce((a, b) => a + b, 0) / absErrors.length;
  const mape = pctErrors.reduce((a, b) => a + b, 0) / pctErrors.length;
  const rmse = Math.sqrt(
    errors.map((e) => e * e).reduce((a, b) => a + b, 0) / errors.length
  );

  // R² (coefficient of determination)
  const actualMean = data.reduce((a, d) => a + d.actual, 0) / data.length;
  const ssRes = data.reduce(
    (a, d) => a + (d.actual - d.predicted) ** 2,
    0
  );
  const ssTot = data.reduce((a, d) => a + (d.actual - actualMean) ** 2, 0);
  const r2 = ssTot === 0 ? 1 : 1 - ssRes / ssTot;

  // Direction accuracy
  let correctDirection = 0;
  for (let i = 1; i < data.length; i++) {
    const actualDir = data[i].actual - data[i - 1].actual;
    const predDir = data[i].predicted - data[i - 1].predicted;
    if ((actualDir >= 0 && predDir >= 0) || (actualDir < 0 && predDir < 0)) {
      correctDirection++;
    }
  }
  const dirAccuracy =
    data.length > 1 ? (correctDirection / (data.length - 1)) * 100 : 0;

  return { mae, mape, rmse, r2, dirAccuracy };
}

type CustomTooltipProps = {
  active?: boolean;
  payload?: Array<{
    dataKey: string;
    value: number;
    color: string;
    payload: {
      date: string;
      predicted: number;
      actual: number;
      error: number;
      errorPct: number;
    };
  }>;
  label?: string;
};

function CustomTooltip({ active, payload }: CustomTooltipProps) {
  if (!active || !payload || payload.length === 0) return null;
  const d = payload[0]?.payload;
  if (!d) return null;

  return (
    <div className="pvTooltip">
      <div className="pvTooltipDate">{dayLabel(d.date)}</div>
      <div className="pvTooltipRow">
        <span className="pvTooltipDot" style={{ background: "var(--accent)" }} />
        <span>Predicted</span>
        <strong>{fmtMoney(d.predicted)}</strong>
      </div>
      <div className="pvTooltipRow">
        <span className="pvTooltipDot" style={{ background: "var(--good)" }} />
        <span>Actual</span>
        <strong>{fmtMoney(d.actual)}</strong>
      </div>
      <div className="pvTooltipDivider" />
      <div className="pvTooltipRow">
        <span>Error</span>
        <strong
          className={
            Math.abs(d.errorPct) < 2
              ? "pvGood"
              : Math.abs(d.errorPct) < 5
              ? "pvCaution"
              : "pvCritical"
          }
        >
          {d.error >= 0 ? "+" : ""}
          {fmtMoney(d.error)} ({d.errorPct >= 0 ? "+" : ""}
          {d.errorPct.toFixed(2)}%)
        </strong>
      </div>
    </div>
  );
}

export default function PredictionVsActualChart({
  predictionHistory,
  ticker,
}: Props) {
  const [range, setRange] = useState(Infinity);

  const data = useMemo(() => {
    const sorted = [...predictionHistory]
      .filter((d) => d.actual > 0 && d.predicted > 0)
      .sort(
        (a, b) => new Date(a.date).getTime() - new Date(b.date).getTime()
      );
    const sliced =
      range === Infinity ? sorted : sorted.slice(-range);
    return sliced.map((d) => ({
      ...d,
      error: d.predicted - d.actual,
      errorPct:
        d.actual !== 0 ? ((d.predicted - d.actual) / d.actual) * 100 : 0,
    }));
  }, [predictionHistory, range]);

  const stats = useMemo(() => computeStats(data), [data]);

  if (predictionHistory.length === 0) {
    return (
      <div className="card pvCard">
        <div className="cardHead">
          <div className="cardTitle">Prediction vs Actual</div>
        </div>
        <div className="cardBody">
          <div className="emptyState">
            No prediction history available for {ticker.replace(".NS", "")}{" "}
            yet. Predictions will appear here once actual prices are
            recorded.
          </div>
        </div>
      </div>
    );
  }

  const formatXTick = (tick: string) => {
    const d = new Date(tick);
    return d.toLocaleDateString("en-IN", { month: "short", day: "numeric" });
  };

  return (
    <section className="pvSection" id="section-predictions">
      <div className="card pvCard">
        <div className="cardHead">
          <div className="cardTitle">
            Prediction vs Actual
            <span className="pvTickerBadge mono">
              {ticker.replace(".NS", "")}
            </span>
          </div>
          <div className="pvControls">
            <div className="pvRangeToggle">
              {RANGE_OPTIONS.map((r) => (
                <button
                  key={r.label}
                  className={range === r.count ? "active" : ""}
                  onClick={() => setRange(r.count)}
                >
                  {r.label}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Stats row */}
        {stats && (
          <div className="pvStatsRow">
            <div className="pvStat">
              <div className="pvStatLabel">MAPE</div>
              <div
                className={`pvStatValue ${
                  stats.mape < 3
                    ? "pvGood"
                    : stats.mape < 6
                    ? "pvCaution"
                    : "pvCritical"
                }`}
              >
                {stats.mape.toFixed(2)}%
              </div>
            </div>
            <div className="pvStat">
              <div className="pvStatLabel">MAE</div>
              <div className="pvStatValue">{fmtMoney(stats.mae)}</div>
            </div>
            <div className="pvStat">
              <div className="pvStatLabel">RMSE</div>
              <div className="pvStatValue">{fmtMoney(stats.rmse)}</div>
            </div>
            <div className="pvStat">
              <div className="pvStatLabel">R² Score</div>
              <div
                className={`pvStatValue ${
                  stats.r2 > 0.9
                    ? "pvGood"
                    : stats.r2 > 0.7
                    ? "pvCaution"
                    : "pvCritical"
                }`}
              >
                {stats.r2.toFixed(4)}
              </div>
            </div>
            <div className="pvStat">
              <div className="pvStatLabel">Direction</div>
              <div
                className={`pvStatValue ${
                  stats.dirAccuracy > 70
                    ? "pvGood"
                    : stats.dirAccuracy > 50
                    ? "pvCaution"
                    : "pvCritical"
                }`}
              >
                {stats.dirAccuracy.toFixed(1)}%
              </div>
            </div>
          </div>
        )}

        {/* Main chart */}
        <div className="pvChartWrap">
          <ResponsiveContainer width="100%" height={380}>
            <ComposedChart
              data={data}
              margin={{ top: 10, right: 24, bottom: 10, left: 16 }}
            >
              <defs>
                <linearGradient id="pvAreaPred" x1="0" y1="0" x2="0" y2="1">
                  <stop
                    offset="0%"
                    stopColor="var(--accent)"
                    stopOpacity={0.15}
                  />
                  <stop
                    offset="100%"
                    stopColor="var(--accent)"
                    stopOpacity={0}
                  />
                </linearGradient>
                <linearGradient id="pvAreaActual" x1="0" y1="0" x2="0" y2="1">
                  <stop
                    offset="0%"
                    stopColor="var(--good)"
                    stopOpacity={0.1}
                  />
                  <stop
                    offset="100%"
                    stopColor="var(--good)"
                    stopOpacity={0}
                  />
                </linearGradient>
              </defs>

              <CartesianGrid
                strokeDasharray="3 6"
                stroke="var(--grid)"
                vertical={false}
              />

              <XAxis
                dataKey="date"
                tickFormatter={formatXTick}
                tick={{ fill: "var(--muted)", fontSize: 11 }}
                axisLine={{ stroke: "var(--grid)" }}
                tickLine={false}
                interval="preserveStartEnd"
                minTickGap={60}
              />
              <YAxis
                tickFormatter={(v: number) => fmtMoney(v)}
                tick={{ fill: "var(--muted)", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                domain={["auto", "auto"]}
                width={80}
              />

              <RTooltip
                content={<CustomTooltip />}
                cursor={{
                  stroke: "var(--muted)",
                  strokeWidth: 1,
                  strokeDasharray: "4 4",
                }}
              />

              <Area
                type="monotone"
                dataKey="predicted"
                stroke="none"
                fill="url(#pvAreaPred)"
                animationDuration={800}
                animationEasing="ease-out"
              />
              <Area
                type="monotone"
                dataKey="actual"
                stroke="none"
                fill="url(#pvAreaActual)"
                animationDuration={800}
                animationEasing="ease-out"
              />

              <Line
                type="monotone"
                dataKey="predicted"
                stroke="var(--accent)"
                strokeWidth={2.5}
                dot={false}
                activeDot={{
                  r: 5,
                  stroke: "var(--accent)",
                  strokeWidth: 2,
                  fill: "var(--surface)",
                }}
                animationDuration={800}
                animationEasing="ease-out"
                name="Predicted"
              />
              <Line
                type="monotone"
                dataKey="actual"
                stroke="var(--good)"
                strokeWidth={2.5}
                dot={false}
                activeDot={{
                  r: 5,
                  stroke: "var(--good)",
                  strokeWidth: 2,
                  fill: "var(--surface)",
                }}
                animationDuration={800}
                animationEasing="ease-out"
                name="Actual"
              />

              {/* Scatter overlay for visual data points when zoomed in */}
              {data.length <= 60 && (
                <>
                  <Scatter
                    dataKey="predicted"
                    fill="var(--accent)"
                    r={2.5}
                    name="Predicted Points"
                    legendType="none"
                  />
                  <Scatter
                    dataKey="actual"
                    fill="var(--good)"
                    r={2.5}
                    name="Actual Points"
                    legendType="none"
                  />
                </>
              )}



              {/* Brush for zooming */}
              {data.length > 30 && (
                <Brush
                  dataKey="date"
                  height={28}
                  stroke="var(--border-2)"
                  fill="var(--surface-2)"
                  tickFormatter={formatXTick}
                  travellerWidth={8}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Error distribution chart */}
        <div className="pvErrorSection">
          <div className="pvErrorHead">
            <span className="pvErrorTitle">Prediction Error Distribution</span>
            <span className="pvErrorSub">
              {data.filter((d) => Math.abs(d.errorPct) < 2).length} of{" "}
              {data.length} predictions within ±2%
            </span>
          </div>
          <div className="pvErrorBars">
            {data.map((d, i) => {
              const barHeight = Math.min(
                100,
                Math.max(4, Math.abs(d.errorPct) * 10)
              );
              const isGood = Math.abs(d.errorPct) < 2;
              const isCaution =
                Math.abs(d.errorPct) >= 2 && Math.abs(d.errorPct) < 5;
              return (
                <div
                  key={i}
                  className="pvErrorBar"
                  title={`${dayLabel(d.date)}: ${d.errorPct >= 0 ? "+" : ""}${d.errorPct.toFixed(2)}%`}
                >
                  <div
                    className={`pvErrorBarInner ${
                      isGood ? "good" : isCaution ? "caution" : "critical"
                    }`}
                    style={{
                      height: `${barHeight}%`,
                      [d.errorPct >= 0 ? "bottom" : "top"]: "50%",
                    }}
                  />
                </div>
              );
            })}
          </div>
          <div className="pvErrorAxis">
            <span>Overestimated</span>
            <span className="pvErrorCenter">0%</span>
            <span>Underestimated</span>
          </div>
        </div>

        <div className="pvLegendRow">
          <span>
            <span className="dot" style={{ background: "var(--accent)" }} />
            Predicted Price
          </span>
          <span>
            <span className="dot" style={{ background: "var(--good)" }} />
            Actual Price
          </span>
          <span className="pvDataCount mono">
            {data.length} data point{data.length !== 1 ? "s" : ""}
          </span>
        </div>

        {/* Reference line showing overall mean */}
        {stats && data.length > 0 && (
          <div className="pvDisclaimer">
            Model accuracy metrics are computed from historical
            prediction-vs-actual data stored in your Supabase{" "}
            <span className="mono">prediction_vs_actual</span> table. Past
            performance does not guarantee future accuracy.
          </div>
        )}
      </div>
    </section>
  );
}
