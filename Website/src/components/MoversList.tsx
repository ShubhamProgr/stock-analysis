"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { formatINR } from "@/lib/format";

type MoverRow = {
  ticker: string;
  company: string;
  predicted: number;
  actual: number;
  changePct: number;
};

export function MoversList({ direction }: { direction: "up" | "down" }) {
  const [rows, setRows] = useState<MoverRow[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch("/api/movers")
      .then((res) => {
        if (!res.ok) throw new Error("Prediction comparison data is unavailable.");
        return res.json();
      })
      .then((data: MoverRow[]) => {
        if (cancelled) return;
        const sorted = [...data].sort((a, b) =>
          direction === "up" ? b.changePct - a.changePct : a.changePct - b.changePct
        );
        setRows(sorted.slice(0, 5));
      })
      .catch((err) => !cancelled && setError(err.message));
    return () => {
      cancelled = true;
    };
  }, [direction]);

  if (error) return <div className="text-sm text-text-muted">{error}</div>;
  if (!rows) return <div className="text-sm text-text-muted">Loading model comparisons...</div>;
  if (rows.length === 0) return <div className="text-sm text-text-muted">No prediction vs actual records yet.</div>;

  return (
    <div className="flex flex-col divide-y divide-ink-600/50">
      {rows.map((row) => (
        <Link
          key={row.ticker}
          href={`/company/${encodeURIComponent(row.company.toLowerCase())}`}
          className="flex items-center justify-between gap-3 py-3 hover:bg-ink-700/40"
        >
          <div>
            <strong className="block font-mono text-sm">{row.ticker}</strong>
            <small className="text-text-muted">{row.company}</small>
          </div>
          <div className="text-right">
            <strong className="block font-mono">{formatINR(row.predicted)}</strong>
            <small className={row.changePct >= 0 ? "stat-up" : "stat-down"}>
              {row.changePct >= 0 ? "+" : ""}
              {row.changePct.toFixed(2)}%
            </small>
          </div>
        </Link>
      ))}
    </div>
  );
}
