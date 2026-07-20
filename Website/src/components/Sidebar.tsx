"use client";

import type { WatchlistRow } from "@/lib/types";
import { fmtMoney, fmtPct, sparklinePath } from "@/lib/format";

type Props = {
  watchlist: WatchlistRow[];
  currentTicker: string;
  onSelectTicker: (ticker: string) => void;
  activeNav: string;
  onSelectNav: (id: string) => void;
};

const NAV_ITEMS = [
  {
    id: "section-overview",
    label: "Overview",
    icon: <path d="M1 8L4 4L7 6L13 1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />,
  },
  {
    id: "section-sentiment",
    label: "Sentiment",
    icon: (
      <>
        <circle cx="7" cy="7" r="5.5" stroke="currentColor" strokeWidth="1.5" />
        <path d="M7 4v3l2 1.5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </>
    ),
  },
  {
    id: "section-strategy",
    label: "Strategy",
    icon: (
      <>
        <rect x="1.5" y="7" width="2.4" height="5.5" rx="0.6" fill="currentColor" />
        <rect x="5.8" y="3.5" width="2.4" height="9" rx="0.6" fill="currentColor" />
        <rect x="10" y="1" width="2.4" height="11.5" rx="0.6" fill="currentColor" />
      </>
    ),
  },
];

export default function Sidebar({ watchlist, currentTicker, onSelectTicker, activeNav, onSelectNav }: Props) {
  return (
    <aside className="rail">
      <div className="brand">
        <div className="brandMark">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M2 12L6 7L9 9.5L14 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <div>
          <div className="brandName">Northline</div>
          <div className="brandSub">Market Intelligence</div>
        </div>
      </div>

      <nav className="navPrimary">
        {NAV_ITEMS.map((item) => (
          <div
            key={item.id}
            className={`navItem${activeNav === item.id ? " active" : ""}`}
            onClick={() => onSelectNav(item.id)}
          >
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              {item.icon}
            </svg>
            {item.label}
          </div>
        ))}
        <div className="navItem disabled" title="Not built yet in this preview">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <rect x="1" y="2" width="12" height="10" rx="1.5" stroke="currentColor" strokeWidth="1.5" />
            <path d="M1 5.5H13" stroke="currentColor" strokeWidth="1.5" />
          </svg>
          Journal
          <span className="soonTag">Soon</span>
        </div>
      </nav>

      <div>
        <div className="railSectionLabel eyebrow">Watchlist · {watchlist.length}</div>
        <div className="watchlist">
          {watchlist.map((row) => {
            const up = row.spark.length > 1 && row.spark[row.spark.length - 1] >= row.spark[0];
            return (
              <button
                key={row.ticker}
                className={`watchRow${row.ticker === currentTicker ? " active" : ""}`}
                onClick={() => onSelectTicker(row.ticker)}
              >
                <span className="watchId">
                  <span className="sym mono">{row.ticker.replace(".NS", "")}</span>
                  <span className="name">{row.name}</span>
                </span>
                <svg width="46" height="20" viewBox="0 0 46 20">
                  <path
                    d={sparklinePath(row.spark, 46, 20)}
                    fill="none"
                    stroke={up ? "var(--good)" : "var(--critical)"}
                    strokeWidth="1.6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
                <span style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 3 }}>
                  <span className="watchPrice mono">{fmtMoney(row.price)}</span>
                  <span className={`watchChg ${row.changePct >= 0 ? "up" : "down"}`}>{fmtPct(row.changePct)}</span>
                </span>
              </button>
            );
          })}
        </div>
      </div>

      <div className="railFoot">
        Live from your Supabase Postgres — <span className="mono">stock_data</span>,{" "}
        <span className="mono">final_analysis</span>, <span className="mono">News</span>.
      </div>
    </aside>
  );
}
