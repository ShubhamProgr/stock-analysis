type TickerItem = { label: string; value: string; changePct: number };

const FALLBACK_ITEMS: TickerItem[] = [
  { label: "NIFTY 50", value: "24,812.35", changePct: 0.22 },
  { label: "SENSEX", value: "81,455.10", changePct: 0.31 },
  { label: "BANK NIFTY", value: "52,904.60", changePct: -0.15 },
  { label: "INDIA VIX", value: "13.42", changePct: -2.33 },
  { label: "10Y G-SEC", value: "6.82%", changePct: 0.27 }
];

export function TickerRibbon({ items = FALLBACK_ITEMS }: { items?: TickerItem[] }) {
  const doubled = [...items, ...items];
  return (
    <div className="ticker-ribbon">
      <div className="ticker-ribbon__track">
        {doubled.map((item, i) => (
          <span key={`${item.label}-${i}`} className="flex items-center gap-2 text-text-muted">
            <strong className="text-text-primary">{item.label}</strong>
            <span>{item.value}</span>
            <em className={`not-italic ${item.changePct >= 0 ? "stat-up" : "stat-down"}`}>
              {item.changePct >= 0 ? "+" : ""}
              {item.changePct.toFixed(2)}%
            </em>
          </span>
        ))}
      </div>
    </div>
  );
}
