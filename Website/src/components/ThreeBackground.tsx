"use client";

const CANDLE_MOVES = [
  24, -12, 30, 36, -16, 26, 18, -10, 34, 42, -18, 28,
  20, -14, 38, 30, -12, 44, 24, -10, 36, 48, -16, 42,
];

const CANDLES = CANDLE_MOVES.map((move, index) => {
  const open = 500 - index * 14 + (index % 3) * 8;
  const close = open - move;
  const high = Math.min(open, close) - (12 + (index % 4) * 5);
  const low = Math.max(open, close) + (14 + (index % 3) * 4);

  return {
    x: 74 + index * 46,
    open,
    close,
    high,
    low,
    rising: close < open,
  };
});

const TREND_POINTS = CANDLES.map((candle) => `${candle.x},${candle.close - 5}`).join(" ");

export default function ThreeBackground() {
  return (
    <div className="market-chart-background" aria-hidden="true">
      <div className="chart-scanline" />
      <svg className="market-chart-svg" viewBox="0 0 1200 720" preserveAspectRatio="xMidYMid slice">
        <defs>
          <linearGradient id="chartArea" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="#28b66f" stopOpacity="0.28" />
            <stop offset="1" stopColor="#28b66f" stopOpacity="0" />
          </linearGradient>
          <filter id="candleGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <g className="chart-grid">
          {[130, 230, 330, 430, 530, 630].map((y) => (
            <line key={`h-${y}`} x1="24" x2="1176" y1={y} y2={y} />
          ))}
          {[70, 250, 430, 610, 790, 970, 1150].map((x) => (
            <line key={`v-${x}`} x1={x} x2={x} y1="84" y2="670" />
          ))}
        </g>

        <polygon
          className="chart-area"
          points={`74,${CANDLES[0].close} ${TREND_POINTS} 1170,670 74,670`}
          fill="url(#chartArea)"
        />
        <polyline className="trend-line" points={TREND_POINTS} />

        <g className="candles">
          {CANDLES.map((candle, index) => {
            const bodyTop = Math.min(candle.open, candle.close);
            const bodyHeight = Math.max(Math.abs(candle.close - candle.open), 14);
            const color = candle.rising ? "#27c77a" : "#f05f61";

            return (
              <g
                key={candle.x}
                className={candle.rising ? "candle candle-up" : "candle candle-down"}
                style={{ animationDelay: `${index * 55}ms` }}
              >
                <line x1={candle.x} x2={candle.x} y1={candle.high} y2={candle.low} stroke={color} />
                <rect
                  x={candle.x - 13}
                  y={bodyTop}
                  width="26"
                  height={bodyHeight}
                  rx="3"
                  fill={color}
                  filter="url(#candleGlow)"
                />
                <rect
                  className="candle-core"
                  x={candle.x - 10}
                  y={bodyTop + 3}
                  width="20"
                  height={Math.max(bodyHeight - 6, 8)}
                  rx="2"
                  fill={color}
                />
              </g>
            );
          })}
        </g>

        <g className="chart-labels">
          <text x="70" y="112">NIFTY 50 / 1D</text>
          <text x="1040" y="112" className="label-positive">+18.42%</text>
          <text x="1040" y="640">09:30</text>
          <text x="1080" y="640">15:30</text>
        </g>
      </svg>
      <div className="chart-vignette" />
    </div>
  );
}