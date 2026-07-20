import type { PricePoint, StrategySignal } from "./types";

function sma(values: number[], window: number): number | null {
  if (values.length < window) return null;
  const slice = values.slice(-window);
  return slice.reduce((a, b) => a + b, 0) / window;
}

function clampConfidence(n: number): number {
  return Math.max(5, Math.min(97, Math.round(n)));
}

/** Momentum breakout: 20D close vs 50D average, direction of the last 10 days. */
function momentumBreakout(series: PricePoint[]): StrategySignal {
  const closes = series.map((p) => p.close);
  const sma20 = sma(closes, 20);
  const sma50 = sma(closes, 50);
  const last = closes[closes.length - 1];

  if (sma20 === null || sma50 === null) {
    return {
      name: "Momentum breakout",
      horizon: "Swing · 2–10d",
      signal: "HOLD",
      confidence: 20,
      rationale: "Not enough price history yet to compute a 50-day average.",
    };
  }

  const gapPct = ((sma20 - sma50) / sma50) * 100;
  const priorWindow = closes.slice(-11, -1);
  const risingDays = priorWindow.filter((v, i) => i > 0 && v > priorWindow[i - 1]).length;

  let signal: StrategySignal["signal"] = "HOLD";
  if (gapPct > 0.8 && last > sma20) signal = "BUY";
  else if (gapPct < -0.8 && last < sma20) signal = "SELL";

  const confidence = clampConfidence(50 + gapPct * 6 + (risingDays - 5) * 3);
  const rationale =
    signal === "BUY"
      ? `20D average sits ${gapPct.toFixed(1)}% above the 50D average with price holding above the short-term trend.`
      : signal === "SELL"
      ? `20D average sits ${gapPct.toFixed(1)}% below the 50D average with price holding under the short-term trend.`
      : `20D and 50D averages are within ${Math.abs(gapPct).toFixed(1)}% of each other — no confirmed trend yet.`;

  return { name: "Momentum breakout", horizon: "Swing · 2–10d", signal, confidence, rationale };
}

/** Mean reversion: position within the 14-day high/low range. */
function meanReversion(series: PricePoint[]): StrategySignal {
  const window = series.slice(-14);
  if (window.length < 14) {
    return {
      name: "Mean reversion",
      horizon: "Intraday",
      signal: "HOLD",
      confidence: 20,
      rationale: "Not enough price history yet to compute a 14-day range.",
    };
  }
  const closes = window.map((p) => p.close);
  const last = closes[closes.length - 1];
  const hi = Math.max(...closes);
  const lo = Math.min(...closes);
  const range = hi - lo || 1;
  const position = (last - lo) / range; // 0 = at 14D low, 1 = at 14D high

  let signal: StrategySignal["signal"] = "HOLD";
  if (position < 0.18) signal = "BUY";
  else if (position > 0.82) signal = "SELL";

  const confidence = clampConfidence(
    signal === "HOLD" ? 45 - Math.abs(position - 0.5) * 20 : 55 + Math.abs(position - 0.5) * 70
  );
  const pct = Math.round(position * 100);
  const rationale =
    signal === "BUY"
      ? `Price sits near the bottom of its 14-day range (${pct}th percentile) — a bounce setup, not a trend call.`
      : signal === "SELL"
      ? `Price sits near the top of its 14-day range (${pct}th percentile) — stretched short-term, watch for a pullback.`
      : `Price sits mid-range (${pct}th percentile) of the last 14 days — no statistical extreme to fade.`;

  return { name: "Mean reversion", horizon: "Intraday", signal, confidence, rationale };
}

/** Sentiment-weighted: the model's own next-close prediction plus its FinBERT sentiment score. */
function sentimentWeighted(
  lastClose: number,
  predictedClose: number | null,
  sentimentLabel: string | null,
  sentimentScore: number | null,
  r2: number | null
): StrategySignal {
  if (predictedClose === null || sentimentLabel === null) {
    return {
      name: "Sentiment-weighted model signal",
      horizon: "Position · next update",
      signal: "HOLD",
      confidence: 20,
      rationale: "No recent model prediction available for this name yet.",
    };
  }
  const predictedChangePct = ((predictedClose - lastClose) / lastClose) * 100;
  const sentimentDir = sentimentLabel === "POSITIVE" ? 1 : sentimentLabel === "NEGATIVE" ? -1 : 0;
  const priceDir = predictedChangePct > 0.15 ? 1 : predictedChangePct < -0.15 ? -1 : 0;

  let signal: StrategySignal["signal"] = "HOLD";
  if (priceDir >= 0 && sentimentDir >= 0 && (priceDir === 1 || sentimentDir === 1)) signal = "BUY";
  else if (priceDir <= 0 && sentimentDir <= 0 && (priceDir === -1 || sentimentDir === -1)) signal = "SELL";

  const agree = priceDir !== 0 && priceDir === sentimentDir;
  const base = (r2 ?? 0.5) * 60 + (sentimentScore ?? 0.5) * 30;
  const confidence = clampConfidence(agree ? base + 10 : base - 10);

  const rationale = `Model projects a ${predictedChangePct >= 0 ? "+" : ""}${predictedChangePct.toFixed(
    2
  )}% next close; news sentiment reads ${sentimentLabel.toLowerCase()}${
    agree ? ", reinforcing the price signal" : ", diverging from the price signal"
  }.`;

  return { name: "Sentiment-weighted model signal", horizon: "Position · next update", signal, confidence, rationale };
}

export function buildStrategies(
  series: PricePoint[],
  lastClose: number,
  predictedClose: number | null,
  sentimentLabel: string | null,
  sentimentScore: number | null,
  r2: number | null
): StrategySignal[] {
  return [
    momentumBreakout(series),
    meanReversion(series),
    sentimentWeighted(lastClose, predictedClose, sentimentLabel, sentimentScore, r2),
  ];
}

export function compositeFromStrategies(strategies: StrategySignal[]): {
  action: StrategySignal["signal"];
  confidence: number;
} {
  const scores = { BUY: 0, HOLD: 0, SELL: 0 } as Record<StrategySignal["signal"], number>;
  let confSum = 0;
  for (const s of strategies) {
    scores[s.signal] += 1;
    confSum += s.confidence;
  }
  const action = (Object.keys(scores) as StrategySignal["signal"][]).sort(
    (a, b) => scores[b] - scores[a]
  )[0];
  return { action, confidence: Math.round(confSum / strategies.length) };
}
