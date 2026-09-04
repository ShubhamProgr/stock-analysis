"use client";

import { useState } from "react";

/* ─── Strategy definitions ─── */
interface StrategyInfo {
  id: string;
  name: string;
  horizon: string;
  tagline: string;
  what: string;
  shows: { buy: string; sell: string; hold: string };
  formula: { label: string; expression: string }[];
  decisionRules: { signal: "BUY" | "SELL" | "HOLD"; rule: string }[];
  confidenceFormula: string[];
  icon: React.ReactNode;
}

const STRATEGIES: StrategyInfo[] = [
  {
    id: "momentum",
    name: "Momentum Breakout",
    horizon: "Swing · 2–10 days",
    tagline: "Ride the trend when the short-term average pulls ahead of the long-term average.",
    what: "A trend-following indicator that measures whether the stock's short-term price trend (20-day moving average) is pulling strongly ahead of the medium-term baseline (50-day moving average). Think of it like checking whether a car is accelerating or slowing down — if the recent trajectory is decisively above the longer-term path, the stock is gaining momentum.",
    shows: {
      buy: "The stock has upward momentum — its 20-day average has climbed more than 0.8% above the 50-day average, and today's price sits above the short-term trend, confirming breakout strength.",
      sell: "The stock is breaking down — its 20-day average has dropped more than 0.8% below the 50-day average, with price falling below the short-term trend, confirming bearish momentum.",
      hold: "The two moving averages are within ±0.8% of each other, or the price contradicts the moving average relationship — no confirmed directional trend yet.",
    },
    formula: [
      { label: "20-Day SMA", expression: "Average of the last 20 closing prices" },
      { label: "50-Day SMA", expression: "Average of the last 50 closing prices" },
      { label: "Gap %", expression: "(SMA₂₀ − SMA₅₀) ÷ SMA₅₀ × 100" },
      { label: "Rising Days", expression: "Count of up-closes in the prior 10 trading sessions" },
    ],
    decisionRules: [
      { signal: "BUY", rule: "Gap % > +0.8% AND today's price is above the 20-day SMA" },
      { signal: "SELL", rule: "Gap % < −0.8% AND today's price is below the 20-day SMA" },
      { signal: "HOLD", rule: "Gap % is within ±0.8%, or price contradicts the moving average direction" },
    ],
    confidenceFormula: [
      "Confidence = 50 + (Gap% × 6) + ((Rising Days − 5) × 3)",
      "Higher gap percentage → stronger confidence",
      "More rising days (above 5) → adds confidence; fewer → subtracts",
      "Result is clamped between 5% and 97%",
    ],
    icon: (
      <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="8" fill="var(--accent-wash)" />
        <path d="M6 22L12 14L18 17L26 8" stroke="var(--accent)" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M22 8H26V12" stroke="var(--accent)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
  },
  {
    id: "mean-reversion",
    name: "Mean Reversion",
    horizon: "Intraday",
    tagline: "Fade the extremes — buy the dip, sell the rip within a 2-week range.",
    what: "A \"rubber band\" indicator. Stock prices tend to oscillate around a mean — when they stretch too far up (overbought) or too far down (oversold) relative to their recent 14-day trading range, they often snap back toward the middle. This strategy identifies those statistical extremes and bets on a reversal.",
    shows: {
      buy: "Price has dropped unusually low compared to the last 2 weeks — it's sitting in the bottom 18% of the 14-day range, which historically signals a bounce-back setup. This is not a trend call; it's a short-term reversion trade.",
      sell: "Price has jumped unusually high compared to the last 2 weeks — it's sitting in the top 18% of the 14-day range, suggesting it's stretched short-term and may pull back down toward the mean.",
      hold: "Price is comfortably in the middle of its recent 14-day range (between the 18th and 82nd percentile) — no statistical extreme to trade on, so the strategy sits this one out.",
    },
    formula: [
      { label: "14-Day High", expression: "Maximum closing price over the last 14 trading days" },
      { label: "14-Day Low", expression: "Minimum closing price over the last 14 trading days" },
      { label: "Range", expression: "14-Day High − 14-Day Low" },
      { label: "Position", expression: "(Current Price − 14D Low) ÷ Range   →   0.0 = at low, 1.0 = at high" },
    ],
    decisionRules: [
      { signal: "BUY", rule: "Position < 0.18 — price is in the bottom 18% of the 14-day range (oversold)" },
      { signal: "SELL", rule: "Position > 0.82 — price is in the top 18% of the 14-day range (overbought)" },
      { signal: "HOLD", rule: "0.18 ≤ Position ≤ 0.82 — price is mid-range, no extreme to fade" },
    ],
    confidenceFormula: [
      "If BUY or SELL:  Confidence = 55 + |Position − 0.5| × 70",
      "If HOLD:  Confidence = 45 − |Position − 0.5| × 20",
      "The closer to 0% or 100%, the more extreme → higher BUY/SELL confidence",
      "Result is clamped between 5% and 97%",
    ],
    icon: (
      <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="8" fill="var(--caution-wash)" />
        <path d="M6 16C10 8 14 24 18 16C22 8 26 24 26 16" stroke="var(--caution)" strokeWidth="2.2" strokeLinecap="round" />
        <line x1="6" y1="16" x2="26" y2="16" stroke="var(--caution)" strokeWidth="1" strokeDasharray="3 3" opacity="0.5" />
      </svg>
    ),
  },
  {
    id: "sentiment-weighted",
    name: "Sentiment-Weighted Model Signal",
    horizon: "Position · next update",
    tagline: "Cross-check the ML prediction with news sentiment — act when both agree.",
    what: "An AI double-check that fuses two independent sources of information: what the Machine Learning price model predicts the stock will do next, and what recent news headlines are saying (analysed by FinBERT AI sentiment). When both sources point in the same direction, the signal is stronger. When they disagree, the strategy advises caution — because conflicting information means higher uncertainty.",
    shows: {
      buy: "Both the AI price forecast and the news sentiment are positive (or at least one is positive with the other neutral) — the quantitative model and qualitative news are aligned in a bullish outlook.",
      sell: "Both the AI price forecast and the news sentiment are negative (or at least one is negative with the other neutral) — model and news both see weakness ahead.",
      hold: "The AI prediction and news sentiment disagree with each other (e.g., positive news but the model forecasts a drop, or vice-versa). Conflicting signals mean uncertainty, so the strategy advises staying out until they align.",
    },
    formula: [
      { label: "Forecast % Change", expression: "(Predicted Close − Current Price) ÷ Current Price × 100" },
      { label: "Price Direction", expression: "+1 if change > +0.15%,  −1 if < −0.15%,  else 0" },
      { label: "Sentiment Direction", expression: "+1 (Positive news),  −1 (Negative news),  0 (Neutral)" },
      { label: "Sentiment Score", expression: "FinBERT confidence score from 0.0 to 1.0" },
      { label: "Model R²", expression: "Historical regression accuracy of the ML price model" },
      { label: "Base Score", expression: "(Model R² × 60) + (Sentiment Score × 30)" },
    ],
    decisionRules: [
      { signal: "BUY", rule: "Both Price Direction ≥ 0 AND Sentiment Direction ≥ 0, with at least one being +1" },
      { signal: "SELL", rule: "Both Price Direction ≤ 0 AND Sentiment Direction ≤ 0, with at least one being −1" },
      { signal: "HOLD", rule: "Price and sentiment signals conflict (one positive, other negative), or data is missing" },
    ],
    confidenceFormula: [
      "If price forecast and sentiment agree:  Confidence = Base Score + 10",
      "If they diverge:  Confidence = Base Score − 10",
      "Higher model R² → more weight from the price prediction quality",
      "Higher sentiment score → more certainty from news analysis",
      "Result is clamped between 5% and 97%",
    ],
    icon: (
      <svg width="32" height="32" viewBox="0 0 32 32" fill="none">
        <rect width="32" height="32" rx="8" fill="var(--good-wash)" />
        <circle cx="12" cy="16" r="5" stroke="var(--good)" strokeWidth="1.8" />
        <circle cx="20" cy="16" r="5" stroke="var(--good)" strokeWidth="1.8" />
      </svg>
    ),
  },
];

export default function StrategyView() {
  const [selectedId, setSelectedId] = useState(STRATEGIES[0].id);

  const selected = STRATEGIES.find((s) => s.id === selectedId)!;

  return (
    <div className="strategyViewWrap">
      <div className="pageHead">
        <div className="tickerTitle">
          <span className="sym mono">Strategies</span>
          <span className="name">How trade signals are calculated</span>
        </div>
      </div>

      <p className="stratViewIntro">
        Each stock in your watchlist is evaluated against three independent strategy models.
        Their signals are combined into a <strong>Composite Signal</strong> via majority vote,
        with confidence averaged across all three. Select a strategy below to see how it works.
      </p>

      {/* ── Strategy selector cards ── */}
      <div className="stratPickerCards">
        {STRATEGIES.map((s, i) => (
          <button
            key={s.id}
            type="button"
            className={`stratPickerCard ${s.id === selectedId ? "active" : ""}`}
            onClick={() => setSelectedId(s.id)}
          >
            <div className="stratPickerCardHead">
              <div className="stratPickerIcon">{s.icon}</div>
              <span className="stratPickerNum">Strategy {i + 1} of {STRATEGIES.length}</span>
              {s.id === selectedId && (
                <span className="stratPickerActiveBadge">Active</span>
              )}
            </div>
            <div className="stratPickerName">{s.name}</div>
            <div className="stratPickerHorizon">{s.horizon}</div>
          </button>
        ))}
      </div>

      {/* ── Full strategy content ── */}
      <div className="stratContent" key={selected.id}>
        {/* Tagline */}
        <div className="stratContentTagline">{selected.tagline}</div>

        {/* What is it? */}
        <section className="stratContentSection">
          <h2 className="stratContentTitle">What is it?</h2>
          <p className="stratContentText">{selected.what}</p>
        </section>

        {/* What does it show? */}
        <section className="stratContentSection">
          <h2 className="stratContentTitle">What does it show?</h2>
          <div className="stratContentSignals">
            <div className="stratContentSignalCard buy">
              <span className="chip buy">BUY</span>
              <p>{selected.shows.buy}</p>
            </div>
            <div className="stratContentSignalCard sell">
              <span className="chip sell">SELL</span>
              <p>{selected.shows.sell}</p>
            </div>
            <div className="stratContentSignalCard hold">
              <span className="chip hold">HOLD</span>
              <p>{selected.shows.hold}</p>
            </div>
          </div>
        </section>

        {/* ── 2-Column Details Grid ── */}
        <div className="stratDetailsGrid">
          <div className="stratDetailsCol">
            {/* Formula breakdown */}
            <section className="stratContentSection">
              <h2 className="stratContentTitle">Formula breakdown</h2>
              <div className="stratContentFormulaTable">
                {selected.formula.map((f) => (
                  <div key={f.label} className="stratContentFormulaRow">
                    <div className="stratContentFormulaLabel">{f.label}</div>
                    <div className="stratContentFormulaExpr">{f.expression}</div>
                  </div>
                ))}
              </div>
            </section>

            {/* Confidence */}
            <section className="stratContentSection">
              <h2 className="stratContentTitle">Confidence calculation</h2>
              <ul className="stratContentConfList">
                {selected.confidenceFormula.map((line, i) => (
                  <li key={i}>{line}</li>
                ))}
              </ul>
            </section>
          </div>

          <div className="stratDetailsCol">
            {/* Decision rules */}
            <section className="stratContentSection">
              <h2 className="stratContentTitle">Decision rules</h2>
              <div className="stratContentRules">
                {selected.decisionRules.map((r) => (
                  <div key={r.signal} className="stratContentRule">
                    <span className={`chip ${r.signal.toLowerCase()}`} style={{ fontSize: 10 }}>{r.signal}</span>
                    <span>{r.rule}</span>
                  </div>
                ))}
              </div>
            </section>

            {/* Composite explainer */}
            <section className="stratContentSection">
              <h2 className="stratContentTitle">How the composite signal works</h2>
              <div className="stratContentComposite">
                <div className="stratContentCompositeRow">
                  <div className="stratContentCompositeLabel">Overall Action</div>
                  <div className="stratContentCompositeValue">
                    Majority vote across all 3 strategies — whichever of BUY, SELL, or HOLD gets 2+ votes wins.
                  </div>
                </div>
                <div className="stratContentCompositeRow">
                  <div className="stratContentCompositeLabel">Overall Confidence</div>
                  <div className="stratContentCompositeValue">
                    Simple arithmetic average of all three confidence scores.
                  </div>
                </div>
              </div>
            </section>
          </div>
        </div>
      </div>

      <div className="disclaimer" style={{ marginTop: 28 }}>
        These signals are computed client-side from your own price history (<span className="mono">stock_data</span>)
        and model output (<span className="mono">final_analysis</span>). They are illustrative tools
        for understanding the data — not investment advice.
      </div>
    </div>
  );
}
