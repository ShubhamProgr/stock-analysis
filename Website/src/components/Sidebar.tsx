"use client";

import type { WatchlistRow, DashboardView } from "@/lib/types";
import { fmtMoney, fmtPct, sparklinePath } from "@/lib/format";

type Props = {
  watchlist: WatchlistRow[];
  currentTicker: string;
  onSelectTicker: (ticker: string) => void;
  activeNav: string;
  onSelectNav: (id: string) => void;
  activeView: DashboardView;
  onSelectView: (view: DashboardView) => void;
};

const VIEW_ITEMS: { id: DashboardView; label: string; icon: React.ReactNode }[] = [
  {
    id: "stock",
    label: "Stock Analysis",
    icon: <path d="M1 8L4 4L7 6L13 1" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />,
  },
  {
    id: "market",
    label: "Market Overview",
    icon: (
      <>
        <rect x="1" y="1" width="5" height="5" rx="1" stroke="currentColor" strokeWidth="1.3" />
        <rect x="8" y="1" width="5" height="5" rx="1" stroke="currentColor" strokeWidth="1.3" />
        <rect x="1" y="8" width="5" height="5" rx="1" stroke="currentColor" strokeWidth="1.3" />
        <rect x="8" y="8" width="5" height="5" rx="1" stroke="currentColor" strokeWidth="1.3" />
      </>
    ),
  },
  {
    id: "screener",
    label: "Screener",
    icon: (
      <>
        <path d="M1 3h12M1 7h9M1 11h6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
      </>
    ),
  },
  {
    id: "accuracy",
    label: "Accuracy",
    icon: (
      <>
        <circle cx="7" cy="7" r="5.5" stroke="currentColor" strokeWidth="1.3" />
        <circle cx="7" cy="7" r="2.5" stroke="currentColor" strokeWidth="1.3" />
        <circle cx="7" cy="7" r="0.8" fill="currentColor" />
      </>
    ),
  },
  {
    id: "compare",
    label: "Compare",
    icon: (
      <>
        <path d="M1 10L5 5L9 7L13 2" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
        <path d="M1 12L5 9L9 11L13 6" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" opacity="0.5" />
      </>
    ),
  },
];

const STOCK_NAV_ITEMS = [
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

export default function Sidebar({ watchlist, currentTicker, onSelectTicker, activeNav, onSelectNav, activeView, onSelectView }: Props) {
  return (
    <aside className="rail">
      <div className="brand">
        <div className="brandMark">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M2 12L6 7L9 9.5L14 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <div>
          <div className="brandName">Stock Analytics</div>
          <div className="brandSub">By Shubham</div>
        </div>
      </div>

      {/* View-level navigation */}
      <nav className="navPrimary">
        {VIEW_ITEMS.map((item) => (
          <div
            key={item.id}
            className={`navItem${activeView === item.id ? " active" : ""}`}
            onClick={() => onSelectView(item.id)}
          >
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              {item.icon}
            </svg>
            {item.label}
          </div>
        ))}
      </nav>

      {/* Stock sub-navigation (only when in stock view) */}
      {activeView === "stock" && (
        <>
          <div className="railDivider" />
          <nav className="navPrimary">
            <div className="railSectionLabel eyebrow" style={{ padding: "0 10px 4px" }}>Stock sections</div>
            {STOCK_NAV_ITEMS.map((item) => (
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
          </nav>

          <div>
            <div className="railSectionLabel eyebrow">Watchlist · {watchlist.length}</div>
            <div className="watchlist">
              {watchlist.map((row) => {
                const up = row.spark.length > 1 && row.spark[row.spark.length - 1] >= row.spark[0];
                return (
                  <button
                    key={row.ticker}
                    type="button"
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
        </>
      )}

      <div className="railFoot">
        Live from your Supabase Postgres — <span className="mono">stock_data</span>,{" "}
        <span className="mono">final_analysis</span>, <span className="mono">News</span>.
      </div>
    </aside>
  );
}
