import type { WatchlistRow } from "@/lib/types";
import { fmtMoney, fmtPct } from "@/lib/format";

export default function TickerTape({ watchlist }: { watchlist: WatchlistRow[] }) {
  const items = watchlist.slice(0, 24).map((row) => (
    <span key={row.ticker}>
      <b>{row.ticker.replace(".NS", "")}</b> {fmtMoney(row.price)}{" "}
      <span className={`chip ${row.changePct >= 0 ? "up" : "down"}`} style={{ padding: "1px 6px" }}>
        {fmtPct(row.changePct)}
      </span>
    </span>
  ));
  return (
    <div className="tapeWrap">
      <div className="tape mono">
        {items}
        {items}
      </div>
    </div>
  );
}
