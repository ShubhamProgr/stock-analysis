"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import dynamic from "next/dynamic";

const ThreeBackground = dynamic(() => import("@/components/ThreeBackground"), {
  ssr: false,
});

/* ─── Feature Data ─── */
const FEATURES = [
  {
    label: "Interactive Candlestick Charts",
    desc: "Explore years of NSE price history with fully interactive candlestick overlays, volume bars, Bollinger Bands, RSI, MACD, and more. Zoom from 1M to 5Y seamlessly.",
    icon: "chart",
    accent: "#7da0de",
    glow: "rgba(125,160,222,0.15)",
  },
  {
    label: "NLP Sentiment Engine",
    desc: "Daily news ingestion from Indian financial media, scored by fine-tuned NLP models. Ticker-level sentiment from -1 to +1, with calendar heatmaps and price correlation overlays.",
    icon: "sentiment",
    accent: "#35c15e",
    glow: "rgba(53,193,94,0.15)",
  },
  {
    label: "XGBoost ML Predictions",
    desc: "Gradient-boosted models trained on 20+ technical indicators and sentiment scores. Next-day directional signals with confidence metrics and full backtesting visibility.",
    icon: "ml",
    accent: "#f0b429",
    glow: "rgba(240,180,41,0.15)",
  },
  {
    label: "Model Accuracy Tracking",
    desc: "Rolling accuracy, precision, recall, and F1 for every ticker. Prediction-vs-actual overlay charts let you assess model performance at a glance.",
    icon: "accuracy",
    accent: "#e8635f",
    glow: "rgba(232,99,95,0.15)",
  },
  {
    label: "Market Overview & Heatmaps",
    desc: "Macro NSE view with sector heatmaps, top gainers/losers, market breadth indicators, and a live ticker tape streaming real-time price changes.",
    icon: "market",
    accent: "#a78bfa",
    glow: "rgba(167,139,250,0.15)",
  },
  {
    label: "Compare & Screen Stocks",
    desc: "Multi-stock comparison with side-by-side charts. Screen NSE equities by PE ratio, market cap, ML signal strength, and fundamental metrics.",
    icon: "compare",
    accent: "#38bdf8",
    glow: "rgba(56,189,248,0.15)",
  },
];

const DASHBOARD_SECTIONS = [
  {
    title: "Stock Analysis View",
    desc: "The main analysis cockpit. Price charts with technical overlays, live sentiment scores, ML prediction signals, and news feed — all for a single ticker, all in one view.",
    tags: ["Candlestick", "Volume", "RSI", "MACD", "Sentiment", "ML Signal"],
    accent: "#7da0de",
  },
  {
    title: "Market Overview",
    desc: "Bird's eye view of the NSE. Sector heatmaps, top movers, market breadth bars, and the scrolling ticker tape keep you informed at a macro level.",
    tags: ["Sector Heatmap", "Top Gainers", "Top Losers", "Ticker Tape"],
    accent: "#a78bfa",
  },
  {
    title: "Accuracy & Backtest",
    desc: "See how the ML model performed historically. Rolling accuracy curves, prediction-vs-actual overlays, and per-ticker precision metrics.",
    tags: ["Rolling Accuracy", "Pred vs Actual", "Precision", "Recall"],
    accent: "#f0b429",
  },
  {
    title: "Screener & Compare",
    desc: "Filter the entire NSE universe by fundamentals, technicals, and ML signals. Compare multiple tickers side-by-side with synchronized charts.",
    tags: ["PE Ratio", "Market Cap", "Signal Strength", "Multi-Chart"],
    accent: "#38bdf8",
  },
];

const TECH_STACK = [
  { name: "Next.js 16", desc: "App Router SSR" },
  { name: "Supabase", desc: "Postgres + Realtime" },
  { name: "XGBoost", desc: "ML Predictions" },
  { name: "Python NLP", desc: "Sentiment Scoring" },
  { name: "Recharts", desc: "Data Visualization" },
  { name: "Three.js", desc: "3D Graphics" },
];

/* ─── Icon SVGs ─── */
function FeatureIcon({ type, color }: { type: string; color: string }) {
  const iconMap: Record<string, React.ReactNode> = {
    chart: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <path d="M4 22L10 13L16 17L24 6" stroke={color} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
        <circle cx="24" cy="6" r="2.5" fill={color} />
      </svg>
    ),
    sentiment: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <circle cx="14" cy="14" r="9" stroke={color} strokeWidth="1.8" />
        <path d="M8 18c0-3 2.5-5.5 6-3s5-0.5 5-3" stroke={color} strokeWidth="1.8" strokeLinecap="round" />
      </svg>
    ),
    ml: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <rect x="4" y="16" width="5" height="9" rx="1.5" fill={color} />
        <rect x="11.5" y="10" width="5" height="15" rx="1.5" fill={color} opacity="0.8" />
        <rect x="19" y="4" width="5" height="21" rx="1.5" fill={color} opacity="0.6" />
      </svg>
    ),
    accuracy: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <path d="M5 15L11 22L24 7" stroke={color} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
    ),
    market: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <rect x="3" y="3" width="9" height="9" rx="2.5" stroke={color} strokeWidth="1.8" />
        <rect x="16" y="3" width="9" height="9" rx="2.5" stroke={color} strokeWidth="1.8" />
        <rect x="3" y="16" width="9" height="9" rx="2.5" stroke={color} strokeWidth="1.8" />
        <rect x="16" y="16" width="9" height="9" rx="2.5" stroke={color} strokeWidth="1.8" />
      </svg>
    ),
    compare: (
      <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
        <path d="M3 22L9 14L15 18L22 8L26 12" stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M3 22L9 18L15 21L22 13L26 16" stroke={color} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" opacity="0.4" />
      </svg>
    ),
  };
  return <>{iconMap[type] || null}</>;
}

/* ─── Animated Counter Hook (bidirectional) ─── */
function useCountUp(target: number, duration = 2000, decimals = 0) {
  const [value, setValue] = useState(0);
  const ref = useRef<HTMLDivElement>(null);
  const animFrameRef = useRef<number>(0);

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          const start = performance.now();
          const tick = (now: number) => {
            const progress = Math.min((now - start) / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            setValue(parseFloat((eased * target).toFixed(decimals)));
            if (progress < 1) animFrameRef.current = requestAnimationFrame(tick);
          };
          animFrameRef.current = requestAnimationFrame(tick);
        } else {
          cancelAnimationFrame(animFrameRef.current);
          setValue(0);
        }
      },
      { threshold: 0.3 }
    );
    if (ref.current) observer.observe(ref.current);
    return () => {
      observer.disconnect();
      cancelAnimationFrame(animFrameRef.current);
    };
  }, [target, duration, decimals]);

  return { value, ref };
}

/* ─── Intersection Observer for Fade-In/Out ─── */
function useFadeIn(delay = 0) {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          timerRef.current = setTimeout(() => setVisible(true), delay);
        } else {
          if (timerRef.current) {
            clearTimeout(timerRef.current);
            timerRef.current = null;
          }
          setVisible(false);
        }
      },
      { threshold: 0.15 }
    );
    if (ref.current) observer.observe(ref.current);
    return () => {
      observer.disconnect();
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [delay]);

  return { ref, visible };
}

/* ─────────────────────────── LANDING PAGE ─────────────────────────── */
export default function LandingPage() {
  const [mounted, setMounted] = useState(false);
  const [hoveredFeature, setHoveredFeature] = useState<number | null>(null);
  const [hoveredDash, setHoveredDash] = useState<number | null>(null);

  const stat1 = useCountUp(500, 2000);
  const stat2 = useCountUp(84.3, 2000, 1);
  const stat3 = useCountUp(5, 1500);
  const stat4 = useCountUp(20, 1800);

  const heroFade = useFadeIn(100);
  const featureFade = useFadeIn(0);
  const dashFade = useFadeIn(0);
  const techFade = useFadeIn(0);
  const ctaFade = useFadeIn(0);

  useEffect(() => {
    setMounted(true);
  }, []);

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#0b100f",
        color: "#eef5ef",
        fontFamily: "'Inter','Helvetica Neue',Arial,sans-serif",
        overflowX: "hidden",
        position: "relative",
      }}
    >
      {/* ── 3D Background ── */}
      <ThreeBackground />

      {/* ── Gradient Overlays ── */}
      <div
        style={{
          position: "fixed",
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          background:
            "radial-gradient(ellipse 70% 55% at 50% 0%, rgba(39,199,122,0.11) 0%, transparent 68%), radial-gradient(ellipse 45% 35% at 10% 58%, rgba(240,95,97,0.08) 0%, transparent 70%)",
          pointerEvents: "none",
          zIndex: 1,
        }}
      />
      <div
        style={{
          position: "fixed",
          bottom: 0,
          left: 0,
          right: 0,
          height: "40vh",
          background:
            "linear-gradient(to top, #0b100f 0%, rgba(11,16,15,0.3) 42%, transparent 100%)",
          pointerEvents: "none",
          zIndex: 1,
        }}
      />

      {/* ━━━━━━━━━━━━━ NAV ━━━━━━━━━━━━━ */}
      <nav
        style={{
          position: "fixed",
          top: 0,
          left: 0,
          right: 0,
          zIndex: 100,
          background: "rgba(11,16,15,0.78)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          borderBottom: "1px solid rgba(242,241,236,0.06)",
        }}
      >
        <div
          style={{
            maxWidth: 1320,
            margin: "0 auto",
            padding: "14px 32px",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div
              style={{
                width: 34,
                height: 34,
                borderRadius: 9,
                background: "linear-gradient(135deg,#148655,#27c77a)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                boxShadow: "0 0 20px rgba(39,199,122,0.35)",
              }}
            >
              <svg width="18" height="18" viewBox="0 0 16 16" fill="none">
                <path
                  d="M2 12L6 7L9 9.5L14 3"
                  stroke="white"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </div>
            <div>
              <span style={{ fontWeight: 700, fontSize: 16, letterSpacing: "-0.02em", color: "#f2f1ec" }}>
                Stock Analytics
              </span>
              <span style={{ fontSize: 11, color: "#6b6960", marginLeft: 8 }}>by Shubham</span>
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
            <a href="#features" style={{ fontSize: 13, color: "#8b897f", textDecoration: "none", fontWeight: 500 }}>
              Features
            </a>
            <a href="#dashboard" style={{ fontSize: 13, color: "#8b897f", textDecoration: "none", fontWeight: 500 }}>
              Dashboard
            </a>
            <a href="#tech" style={{ fontSize: 13, color: "#8b897f", textDecoration: "none", fontWeight: 500 }}>
              Tech
            </a>
            <Link
              href="/dashboard"
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 7,
                background: "linear-gradient(135deg,#3b5ba5,#7da0de)",
                color: "#fff",
                borderRadius: 9,
                padding: "9px 20px",
                fontSize: 13,
                fontWeight: 600,
                textDecoration: "none",
                border: "1px solid rgba(255,255,255,0.18)",
                boxShadow: "0 0 24px rgba(125,160,222,0.3)",
                transition: "transform 0.2s ease, box-shadow 0.2s ease",
              }}
            >
              Launch Dashboard →
            </Link>
          </div>
        </div>
      </nav>

      {/* ━━━━━━━━━━━━━ HERO ━━━━━━━━━━━━━ */}
      <section
        ref={heroFade.ref}
        style={{
          position: "relative",
          zIndex: 10,
          maxWidth: 1320,
          margin: "0 auto",
          padding: "180px 24px 120px",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          textAlign: "center",
          opacity: heroFade.visible ? 1 : 0,
          transform: heroFade.visible ? "translateY(0)" : "translateY(40px)",
          transition: "opacity 1s ease, transform 1s ease",
        }}
      >
        {/* Pill badge */}
        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: 10,
            background: "rgba(125,160,222,0.06)",
            border: "1px solid rgba(125,160,222,0.15)",
            borderRadius: 100,
            padding: "7px 18px",
            fontSize: 11.5,
            fontWeight: 600,
            color: "#7da0de",
            letterSpacing: "0.07em",
            textTransform: "uppercase" as const,
            marginBottom: 28,
            backdropFilter: "blur(8px)",
          }}
        >
          <span
            style={{
              width: 7,
              height: 7,
              borderRadius: "50%",
              background: "#35c15e",
              boxShadow: "0 0 12px #35c15e",
              display: "inline-block",
              animation: "pulse-dot 2s ease-in-out infinite",
            }}
          />
          NSE Live · ML-Powered · Real-Time Sentiment
        </div>

        {/* Main heading */}
        <h1
          style={{
            fontSize: "clamp(48px,7vw,82px)",
            fontWeight: 800,
            lineHeight: 1.04,
            letterSpacing: "-0.04em",
            color: "#f2f1ec",
            margin: "0 0 24px",
            maxWidth: 900,
          }}
        >
          The Future of
          <br />
          <span
            style={{
              color: "#8de8b4",
            }}
          >
            Stock Intelligence
          </span>
        </h1>

        {/* Subtitle */}
        <p
          style={{
            fontSize: "clamp(17px,2vw,20px)",
            color: "#9b9a90",
            lineHeight: 1.7,
            margin: "0 auto 40px",
            maxWidth: 680,
          }}
        >
          Harness XGBoost ML predictions, NLP sentiment scoring, and comprehensive technical analysis
          on 500+ NSE equities — all in one premium, data-rich dashboard.
        </p>

        {/* CTA Buttons */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 16,
            marginBottom: 60,
            flexWrap: "wrap" as const,
          }}
        >
          <Link
            href="/dashboard"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 10,
              background: "linear-gradient(135deg,#148655 0%,#27c77a 100%)",
              color: "#fff",
              borderRadius: 12,
              padding: "15px 32px",
              fontSize: 15,
              fontWeight: 700,
              textDecoration: "none",
              border: "1px solid rgba(255,255,255,0.2)",
              boxShadow:
                "0 0 40px rgba(39,199,122,0.3),0 12px 32px rgba(0,0,0,0.5)",
              transition: "transform 0.2s ease, box-shadow 0.2s ease",
            }}
          >
            <svg width="18" height="18" viewBox="0 0 16 16" fill="none">
              <path
                d="M2 10L5 6L8 7.5L13 2"
                stroke="currentColor"
                strokeWidth="1.8"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
            Launch Dashboard →
          </Link>
          <a
            href="#features"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              background: "rgba(255,255,255,0.04)",
              color: "#f2f1ec",
              borderRadius: 12,
              padding: "15px 28px",
              fontSize: 15,
              fontWeight: 600,
              textDecoration: "none",
              border: "1px solid rgba(242,241,236,0.1)",
              backdropFilter: "blur(14px)",
              transition: "background 0.2s ease",
            }}
          >
            Explore Features ↓
          </a>
        </div>

        {/* ── Stats Bar ── */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(4,1fr)",
            gap: 0,
            maxWidth: 900,
            width: "100%",
            background: "rgba(20,24,32,0.7)",
            backdropFilter: "blur(24px)",
            WebkitBackdropFilter: "blur(24px)",
            border: "1px solid rgba(242,241,236,0.06)",
            borderRadius: 18,
            padding: "22px 0",
            boxShadow: "0 30px 70px -20px rgba(0,0,0,0.8)",
          }}
        >
          {[
            { ref: stat1.ref, val: `${stat1.value}+`, label: "NSE Equities" },
            { ref: stat2.ref, val: `${stat2.value}%`, label: "ML Accuracy" },
            { ref: stat3.ref, val: `${stat3.value}Y`, label: "History Depth" },
            { ref: stat4.ref, val: `${stat4.value}+`, label: "Indicators" },
          ].map((s, i) => (
            <div
              key={i}
              ref={s.ref}
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                gap: 4,
                padding: "0 20px",
                borderRight: i < 3 ? "1px solid rgba(242,241,236,0.06)" : "none",
              }}
            >
              <span
                style={{
                  fontSize: 28,
                  fontWeight: 800,
                  color: "#f2f1ec",
                  letterSpacing: "-0.03em",
                  fontFamily: "IBM Plex Mono,monospace",
                }}
              >
                {s.val}
              </span>
              <span
                style={{
                  fontSize: 10.5,
                  color: "#6b6960",
                  fontWeight: 600,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase" as const,
                }}
              >
                {s.label}
              </span>
            </div>
          ))}
        </div>

        {/* ── Floating Badges ── */}
        <div
          style={{
            position: "absolute",
            left: "5%",
            top: "68%",
            display: "inline-flex",
            alignItems: "center",
            gap: 7,
            padding: "7px 14px",
            background: "rgba(20,24,32,0.85)",
            backdropFilter: "blur(16px)",
            border: "1px solid rgba(53,193,94,0.25)",
            borderRadius: 20,
            fontSize: 11,
            fontFamily: "IBM Plex Mono, monospace",
            color: "#35c15e",
            boxShadow: "0 16px 40px rgba(0,0,0,0.6)",
            animation: "float-badge 5s ease-in-out infinite",
            zIndex: 5,
          }}
        >
          <span
            style={{
              width: 6,
              height: 6,
              borderRadius: "50%",
              background: "#35c15e",
              boxShadow: "0 0 8px #35c15e",
            }}
          />
          XGBOOST → BUY · 84.3%
        </div>
        <div
          style={{
            position: "absolute",
            right: "6%",
            top: "35%",
            display: "inline-flex",
            alignItems: "center",
            gap: 7,
            padding: "7px 16px",
            background: "rgba(20,24,32,0.9)",
            backdropFilter: "blur(16px)",
            border: "1px solid rgba(125,160,222,0.2)",
            borderRadius: 20,
            fontSize: 11,
            fontFamily: "IBM Plex Mono, monospace",
            color: "#7da0de",
            boxShadow: "0 16px 40px rgba(0,0,0,0.6)",
            animation: "float-badge 4.5s ease-in-out infinite 1.2s",
            zIndex: 5,
          }}
        >
          <span style={{ color: "#35c15e", fontSize: 13 }}>▲</span>
          RELIANCE.NS +2.4% · VOL 2.1x
        </div>
      </section>

      {/* ━━━━━━━━━━━━━ FEATURES ━━━━━━━━━━━━━ */}
      <section
        id="features"
        ref={featureFade.ref}
        style={{
          position: "relative",
          zIndex: 10,
          maxWidth: 1320,
          margin: "0 auto",
          padding: "100px 32px 80px",
          opacity: featureFade.visible ? 1 : 0,
          transform: featureFade.visible ? "translateY(0)" : "translateY(50px)",
          transition: "opacity 0.8s ease, transform 0.8s ease",
        }}
      >
        <div style={{ textAlign: "center", marginBottom: 64 }}>
          <div
            style={{
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase" as const,
              color: "#7da0de",
              marginBottom: 14,
            }}
          >
            Platform Features
          </div>
          <h2
            style={{
              fontSize: "clamp(30px,4.5vw,48px)",
              fontWeight: 800,
              letterSpacing: "-0.035em",
              lineHeight: 1.1,
              color: "#f2f1ec",
              margin: "0 0 16px",
            }}
          >
            Everything you need to
            <br />
            <span
              style={{
                background: "linear-gradient(135deg,#7da0de,#35c15e)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
                backgroundClip: "text",
              }}
            >
              trade with conviction
            </span>
          </h2>
          <p
            style={{
              fontSize: 17,
              color: "#9b9a90",
              lineHeight: 1.65,
              maxWidth: 580,
              margin: "0 auto",
            }}
          >
            A unified workspace combining ML predictions, real-time sentiment, and technical analysis for the NSE.
          </p>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill,minmax(340px,1fr))",
            gap: 16,
          }}
        >
          {FEATURES.map((f, i) => (
            <div
              key={i}
              onMouseEnter={() => setHoveredFeature(i)}
              onMouseLeave={() => setHoveredFeature(null)}
              style={{
                background: hoveredFeature === i ? "rgba(25,30,40,0.9)" : "rgba(18,22,30,0.75)",
                backdropFilter: "blur(16px)",
                WebkitBackdropFilter: "blur(16px)",
                border:
                  hoveredFeature === i
                    ? `1px solid ${f.accent}44`
                    : "1px solid rgba(242,241,236,0.06)",
                borderRadius: 18,
                padding: "30px 26px",
                cursor: "default",
                position: "relative",
                overflow: "hidden",
                transform: hoveredFeature === i ? "translateY(-8px) scale(1.01)" : "translateY(0) scale(1)",
                boxShadow:
                  hoveredFeature === i
                    ? `0 30px 60px -15px rgba(0,0,0,0.7), 0 0 50px -10px ${f.glow}`
                    : "0 12px 30px -18px rgba(0,0,0,0.5)",
                transition:
                  "transform 0.3s cubic-bezier(.22,.68,0,1.2), box-shadow 0.3s ease, border-color 0.25s ease, background 0.25s ease",
              }}
            >
              {/* Top accent line */}
              <div
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  right: 0,
                  height: 2,
                  background: `linear-gradient(90deg, transparent, ${f.accent}, transparent)`,
                  opacity: hoveredFeature === i ? 0.8 : 0.3,
                  transition: "opacity 0.3s ease",
                }}
              />
              {/* Corner glow */}
              <div
                style={{
                  position: "absolute",
                  top: -40,
                  right: -40,
                  width: 120,
                  height: 120,
                  borderRadius: "50%",
                  background: f.glow,
                  filter: "blur(40px)",
                  opacity: hoveredFeature === i ? 1 : 0,
                  transition: "opacity 0.3s ease",
                  pointerEvents: "none",
                }}
              />
              <div
                style={{
                  width: 52,
                  height: 52,
                  borderRadius: 14,
                  background: f.glow,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  marginBottom: 18,
                  border: `1px solid ${f.accent}22`,
                }}
              >
                <FeatureIcon type={f.icon} color={f.accent} />
              </div>
              <div
                style={{
                  fontSize: 17,
                  fontWeight: 700,
                  color: "#f2f1ec",
                  marginBottom: 8,
                  letterSpacing: "-0.01em",
                }}
              >
                {f.label}
              </div>
              <div style={{ fontSize: 14, color: "#9b9a90", lineHeight: 1.65 }}>
                {f.desc}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* ━━━━━━━━━━━━━ DASHBOARD DEEP-DIVE ━━━━━━━━━━━━━ */}
      <section
        id="dashboard"
        ref={dashFade.ref}
        style={{
          position: "relative",
          zIndex: 10,
          background: "rgba(14,18,24,0.5)",
          borderTop: "1px solid rgba(242,241,236,0.05)",
          borderBottom: "1px solid rgba(242,241,236,0.05)",
          padding: "100px 32px",
          opacity: dashFade.visible ? 1 : 0,
          transform: dashFade.visible ? "translateY(0)" : "translateY(50px)",
          transition: "opacity 0.8s ease, transform 0.8s ease",
        }}
      >
        <div style={{ maxWidth: 1320, margin: "0 auto" }}>
          <div style={{ textAlign: "center", marginBottom: 64 }}>
            <div
              style={{
                fontSize: 11,
                fontWeight: 700,
                letterSpacing: "0.12em",
                textTransform: "uppercase" as const,
                color: "#35c15e",
                marginBottom: 14,
              }}
            >
              Inside the Dashboard
            </div>
            <h2
              style={{
                fontSize: "clamp(30px,4.5vw,48px)",
                fontWeight: 800,
                letterSpacing: "-0.035em",
                lineHeight: 1.1,
                color: "#f2f1ec",
                margin: "0 0 16px",
              }}
            >
              Your complete
              <br />
              <span
                style={{
                  background: "linear-gradient(135deg,#35c15e,#7da0de)",
                  WebkitBackgroundClip: "text",
                  WebkitTextFillColor: "transparent",
                  backgroundClip: "text",
                }}
              >
                trading command center
              </span>
            </h2>
            <p
              style={{
                fontSize: 17,
                color: "#9b9a90",
                lineHeight: 1.65,
                maxWidth: 600,
                margin: "0 auto",
              }}
            >
              Multiple views, one dashboard. Switch between analysis, market overview, accuracy tracking, and screening — all powered by live data.
            </p>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fill,minmax(280px,1fr))",
              gap: 16,
            }}
          >
            {DASHBOARD_SECTIONS.map((d, i) => (
              <div
                key={i}
                onMouseEnter={() => setHoveredDash(i)}
                onMouseLeave={() => setHoveredDash(null)}
                style={{
                  background: "rgba(18,22,30,0.8)",
                  backdropFilter: "blur(16px)",
                  border: hoveredDash === i ? `1px solid ${d.accent}33` : "1px solid rgba(242,241,236,0.06)",
                  borderRadius: 18,
                  padding: "28px 24px",
                  position: "relative",
                  overflow: "hidden",
                  transform: hoveredDash === i ? "translateY(-4px)" : "translateY(0)",
                  boxShadow: hoveredDash === i
                    ? `0 24px 50px -12px rgba(0,0,0,0.6), 0 0 40px -10px ${d.accent}18`
                    : "0 8px 24px -12px rgba(0,0,0,0.4)",
                  transition: "all 0.3s cubic-bezier(.22,.68,0,1.2)",
                }}
              >
                {/* Number badge */}
                <div
                  style={{
                    position: "absolute",
                    top: 16,
                    right: 18,
                    fontSize: 48,
                    fontWeight: 900,
                    color: d.accent,
                    opacity: 0.06,
                    fontFamily: "IBM Plex Mono,monospace",
                    lineHeight: 1,
                  }}
                >
                  0{i + 1}
                </div>
                <div
                  style={{
                    width: 4,
                    height: 32,
                    borderRadius: 2,
                    background: d.accent,
                    marginBottom: 18,
                    opacity: 0.7,
                  }}
                />
                <div
                  style={{
                    fontSize: 18,
                    fontWeight: 700,
                    color: "#f2f1ec",
                    marginBottom: 10,
                    letterSpacing: "-0.01em",
                  }}
                >
                  {d.title}
                </div>
                <div
                  style={{
                    fontSize: 14,
                    color: "#9b9a90",
                    lineHeight: 1.65,
                    marginBottom: 18,
                  }}
                >
                  {d.desc}
                </div>
                <div style={{ display: "flex", flexWrap: "wrap" as const, gap: 6 }}>
                  {d.tags.map((tag) => (
                    <span
                      key={tag}
                      style={{
                        fontSize: 10.5,
                        fontWeight: 600,
                        color: d.accent,
                        background: `${d.accent}12`,
                        border: `1px solid ${d.accent}22`,
                        borderRadius: 6,
                        padding: "3px 9px",
                        fontFamily: "IBM Plex Mono,monospace",
                        letterSpacing: "0.03em",
                      }}
                    >
                      {tag}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Live Preview Card */}
          <div
            style={{
              marginTop: 40,
              background: "rgba(18,22,30,0.85)",
              backdropFilter: "blur(24px)",
              border: "1px solid rgba(242,241,236,0.08)",
              borderRadius: 20,
              overflow: "hidden",
              boxShadow: "0 40px 80px -20px rgba(0,0,0,0.8), 0 0 60px -15px rgba(125,160,222,0.08)",
            }}
          >
            {/* Terminal titlebar */}
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                padding: "12px 20px",
                background: "rgba(14,18,24,0.8)",
                borderBottom: "1px solid rgba(242,241,236,0.06)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: "#e8635f", display: "inline-block", opacity: 0.8 }} />
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: "#f0b429", display: "inline-block", opacity: 0.8 }} />
                <span style={{ width: 10, height: 10, borderRadius: "50%", background: "#35c15e", display: "inline-block", opacity: 0.8 }} />
                <span style={{ marginLeft: 12, fontSize: 11, fontFamily: "IBM Plex Mono, monospace", color: "#6b6960", letterSpacing: "0.05em", textTransform: "uppercase" as const }}>
                  Dashboard Live Preview · RELIANCE.NS
                </span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, fontFamily: "IBM Plex Mono, monospace", color: "#35c15e" }}>
                <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#35c15e", boxShadow: "0 0 8px #35c15e", display: "inline-block" }} />
                CONNECTED
              </div>
            </div>

            {/* Dashboard mock content */}
            <div style={{ padding: "24px 28px 20px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
                <div>
                  <div style={{ fontFamily: "IBM Plex Mono,monospace", fontWeight: 700, fontSize: 16, color: "#f2f1ec", letterSpacing: "0.02em" }}>
                    RELIANCE.NS
                  </div>
                  <div style={{ fontSize: 12, color: "#6b6960", marginTop: 3 }}>
                    Reliance Industries Ltd · NSE Bluechip
                  </div>
                </div>
                <div style={{ textAlign: "right" as const }}>
                  <div style={{ fontFamily: "IBM Plex Mono,monospace", fontWeight: 700, fontSize: 24, color: "#f2f1ec" }}>
                    ₹2,941.50
                  </div>
                  <div style={{ fontSize: 13, color: "#35c15e", fontWeight: 600 }}>
                    ▲ +1.24% today
                  </div>
                </div>
              </div>

              {/* Mini chart SVG */}
              <svg width="100%" height="120" viewBox="0 0 700 120" preserveAspectRatio="none" style={{ display: "block", marginBottom: 16 }}>
                <defs>
                  <linearGradient id="preview-area" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#7da0de" stopOpacity="0.2" />
                    <stop offset="100%" stopColor="#7da0de" stopOpacity="0" />
                  </linearGradient>
                </defs>
                <path
                  d="M0,90 L35,85 L70,88 L105,78 L140,70 L175,73 L210,65 L245,55 L280,58 L315,48 L350,42 L385,45 L420,38 L455,30 L490,33 L525,25 L560,20 L595,22 L630,15 L665,10 L700,8 L700,120 L0,120 Z"
                  fill="url(#preview-area)"
                />
                <path
                  d="M0,90 L35,85 L70,88 L105,78 L140,70 L175,73 L210,65 L245,55 L280,58 L315,48 L350,42 L385,45 L420,38 L455,30 L490,33 L525,25 L560,20 L595,22 L630,15 L665,10 L700,8"
                  fill="none"
                  stroke="#7da0de"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  style={{
                    strokeDasharray: 2000,
                    strokeDashoffset: mounted ? 0 : 2000,
                    transition: "stroke-dashoffset 2s ease 0.5s",
                  }}
                />
                <circle cx="700" cy="8" r="4" fill="#7da0de" opacity={mounted ? 1 : 0} style={{ transition: "opacity 0.5s ease 2.5s" }} />
              </svg>

              {/* Indicator chips */}
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" as const, marginBottom: 16 }}>
                {[
                  { label: "Sentiment", value: "+0.78", color: "#35c15e", tag: "Bullish" },
                  { label: "RSI", value: "62.4", color: "#7da0de", tag: "Neutral" },
                  { label: "ML Signal", value: "BUY", color: "#35c15e", tag: "84.3%" },
                ].map((ind) => (
                  <div
                    key={ind.label}
                    style={{
                      flex: 1,
                      minWidth: 140,
                      background: "rgba(255,255,255,0.02)",
                      border: "1px solid rgba(242,241,236,0.06)",
                      borderRadius: 10,
                      padding: "10px 14px",
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <span style={{ fontSize: 11, fontWeight: 600, color: "#6b6960", textTransform: "uppercase" as const, letterSpacing: "0.05em" }}>
                      {ind.label}
                    </span>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <span style={{ fontFamily: "IBM Plex Mono,monospace", fontSize: 13, fontWeight: 700, color: ind.color }}>
                        {ind.value}
                      </span>
                      <span
                        style={{
                          fontSize: 9.5,
                          fontWeight: 700,
                          background: `${ind.color}15`,
                          color: ind.color,
                          padding: "2px 7px",
                          borderRadius: 4,
                        }}
                      >
                        {ind.tag}
                      </span>
                    </div>
                  </div>
                ))}
              </div>

              {/* XGBoost signal bar */}
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  padding: "12px 16px",
                  background: "rgba(53,193,94,0.06)",
                  borderRadius: 10,
                  border: "1px solid rgba(53,193,94,0.15)",
                }}
              >
                <span style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 13, fontWeight: 700, color: "#35c15e", fontFamily: "IBM Plex Mono,monospace" }}>
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                    <path d="M2 10L5 6L7.5 8L11 3" stroke="#35c15e" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                  XGBoost → STRONG BUY
                </span>
                <span style={{ fontSize: 12, color: "#6b6960", fontFamily: "IBM Plex Mono,monospace" }}>
                  Confidence: 84.3% · 30d Accuracy: 76%
                </span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ━━━━━━━━━━━━━ TECH STACK ━━━━━━━━━━━━━ */}
      <section
        id="tech"
        ref={techFade.ref}
        style={{
          position: "relative",
          zIndex: 10,
          maxWidth: 1320,
          margin: "0 auto",
          padding: "100px 32px 80px",
          opacity: techFade.visible ? 1 : 0,
          transform: techFade.visible ? "translateY(0)" : "translateY(50px)",
          transition: "opacity 0.8s ease, transform 0.8s ease",
        }}
      >
        <div style={{ textAlign: "center", marginBottom: 60 }}>
          <div
            style={{
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: "0.12em",
              textTransform: "uppercase" as const,
              color: "#f0b429",
              marginBottom: 14,
            }}
          >
            Technology
          </div>
          <h2
            style={{
              fontSize: "clamp(30px,4.5vw,48px)",
              fontWeight: 800,
              letterSpacing: "-0.035em",
              lineHeight: 1.1,
              color: "#f2f1ec",
              margin: "0 0 16px",
            }}
          >
            Built with
            <br />
            <span
              style={{
                background: "linear-gradient(135deg,#f0b429,#e8635f)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
                backgroundClip: "text",
              }}
            >
              cutting-edge tech
            </span>
          </h2>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill,minmax(180px,1fr))",
            gap: 12,
            maxWidth: 800,
            margin: "0 auto",
          }}
        >
          {TECH_STACK.map((t, i) => (
            <div
              key={i}
              style={{
                background: "rgba(18,22,30,0.7)",
                border: "1px solid rgba(242,241,236,0.06)",
                borderRadius: 14,
                padding: "22px 18px",
                textAlign: "center",
                transition: "border-color 0.2s ease, transform 0.2s ease",
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLElement).style.borderColor = "rgba(242,241,236,0.15)";
                (e.currentTarget as HTMLElement).style.transform = "translateY(-4px)";
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLElement).style.borderColor = "rgba(242,241,236,0.06)";
                (e.currentTarget as HTMLElement).style.transform = "translateY(0)";
              }}
            >
              <div style={{ fontSize: 16, fontWeight: 700, color: "#f2f1ec", marginBottom: 4 }}>
                {t.name}
              </div>
              <div style={{ fontSize: 12, color: "#6b6960", fontWeight: 500 }}>
                {t.desc}
              </div>
            </div>
          ))}
        </div>

        {/* How It Works Flow */}
        <div
          style={{
            marginTop: 60,
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            gap: 0,
            flexWrap: "wrap" as const,
          }}
        >
          {[
            { step: "Data Ingestion", desc: "500+ NSE tickers scraped daily", icon: "📊" },
            { step: "NLP Processing", desc: "Sentiment scored from news", icon: "🧠" },
            { step: "Feature Engineering", desc: "20+ technical indicators", icon: "⚙️" },
            { step: "XGBoost Training", desc: "Model retrained daily", icon: "🤖" },
            { step: "Dashboard Delivery", desc: "Real-time signals & charts", icon: "🚀" },
          ].map((s, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center" }}>
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  gap: 8,
                  padding: "16px 20px",
                  minWidth: 140,
                }}
              >
                <div style={{ fontSize: 28 }}>{s.icon}</div>
                <div style={{ fontSize: 13, fontWeight: 700, color: "#f2f1ec", textAlign: "center" }}>{s.step}</div>
                <div style={{ fontSize: 11, color: "#6b6960", textAlign: "center", lineHeight: 1.4 }}>{s.desc}</div>
              </div>
              {i < 4 && (
                <div
                  style={{
                    width: 40,
                    height: 2,
                    background: "linear-gradient(90deg, rgba(125,160,222,0.3), rgba(53,193,94,0.3))",
                    borderRadius: 1,
                    flexShrink: 0,
                  }}
                />
              )}
            </div>
          ))}
        </div>
      </section>

      {/* ━━━━━━━━━━━━━ FINAL CTA ━━━━━━━━━━━━━ */}
      <section
        ref={ctaFade.ref}
        style={{
          position: "relative",
          zIndex: 10,
          padding: "100px 32px 80px",
          textAlign: "center",
          opacity: ctaFade.visible ? 1 : 0,
          transform: ctaFade.visible ? "translateY(0)" : "translateY(50px)",
          transition: "opacity 0.8s ease, transform 0.8s ease",
        }}
      >
        <div
          style={{
            maxWidth: 700,
            margin: "0 auto",
            background: "rgba(18,22,30,0.6)",
            backdropFilter: "blur(24px)",
            border: "1px solid rgba(242,241,236,0.06)",
            borderRadius: 24,
            padding: "60px 40px",
            boxShadow: "0 40px 80px -20px rgba(0,0,0,0.6), 0 0 80px -20px rgba(125,160,222,0.06)",
          }}
        >
          <h2
            style={{
              fontSize: "clamp(28px,4vw,42px)",
              fontWeight: 800,
              letterSpacing: "-0.03em",
              lineHeight: 1.1,
              color: "#f2f1ec",
              margin: "0 0 16px",
            }}
          >
            Ready to trade
            <br />
            <span
              style={{
                background: "linear-gradient(135deg,#7da0de,#35c15e)",
                WebkitBackgroundClip: "text",
                WebkitTextFillColor: "transparent",
                backgroundClip: "text",
              }}
            >
              smarter?
            </span>
          </h2>
          <p style={{ fontSize: 16, color: "#9b9a90", lineHeight: 1.65, marginBottom: 32, maxWidth: 480, margin: "0 auto 32px" }}>
            Access the full dashboard with live NSE data, ML predictions, and sentiment analysis — completely free.
          </p>
          <Link
            href="/dashboard"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 10,
              background: "linear-gradient(135deg,#3b5ba5 0%,#7da0de 100%)",
              color: "#fff",
              borderRadius: 12,
              padding: "16px 36px",
              fontSize: 16,
              fontWeight: 700,
              textDecoration: "none",
              border: "1px solid rgba(255,255,255,0.2)",
              boxShadow: "0 0 50px rgba(125,160,222,0.4), 0 16px 40px rgba(0,0,0,0.5)",
              transition: "transform 0.2s ease, box-shadow 0.2s ease",
            }}
          >
            <svg width="18" height="18" viewBox="0 0 16 16" fill="none">
              <path d="M2 10L5 6L8 7.5L13 2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            Launch Dashboard →
          </Link>
        </div>
      </section>

      {/* ━━━━━━━━━━━━━ FOOTER ━━━━━━━━━━━━━ */}
      <footer
        style={{
          position: "relative",
          zIndex: 10,
          borderTop: "1px solid rgba(242,241,236,0.05)",
          background: "rgba(10,12,16,0.95)",
          padding: "32px 32px",
        }}
      >
        <div
          style={{
            maxWidth: 1320,
            margin: "0 auto",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 24,
            flexWrap: "wrap" as const,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div
              style={{
                width: 30,
                height: 30,
                borderRadius: 8,
                background: "linear-gradient(135deg,#3b5ba5,#7da0de)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
              }}
            >
              <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                <path d="M2 12L6 7L9 9.5L14 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </div>
            <div>
              <div style={{ fontWeight: 700, fontSize: 14, color: "#f2f1ec" }}>Stock Analytics</div>
              <div style={{ fontSize: 11, color: "#6b6960" }}>by Shubham · NSE Equities</div>
            </div>
          </div>
          <div
            style={{
              display: "flex",
              alignItems: "center",
              gap: 6,
              fontSize: 12,
              color: "#6b6960",
              fontFamily: "IBM Plex Mono,monospace",
              flexWrap: "wrap" as const,
            }}
          >
            Supabase Postgres · XGBoost ML · NLP Sentiment · Three.js
          </div>
          <Link
            href="/dashboard"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 6,
              background: "linear-gradient(135deg,#3b5ba5,#7da0de)",
              color: "#fff",
              borderRadius: 8,
              padding: "9px 20px",
              fontSize: 13,
              fontWeight: 600,
              textDecoration: "none",
              border: "1px solid rgba(255,255,255,0.18)",
            }}
          >
            Go to Dashboard →
          </Link>
        </div>
      </footer>

      {/* ── Animation Styles (injected via style tag) ── */}
      <style jsx global>{`
        @keyframes pulse-dot {
          0%, 100% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.5; transform: scale(1.3); }
        }
        @keyframes float-badge {
          0%, 100% { transform: translateY(0); }
          50% { transform: translateY(-10px); }
        }
      `}</style>
    </div>
  );
}