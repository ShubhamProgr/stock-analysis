"use client";

import { useState, useEffect } from "react";
import type { PeerComparisonRow } from "@/lib/types";
import { fmtCompact, fmtPct } from "@/lib/format";

type Props = {
  ticker: string;
  onSelectTicker: (ticker: string) => void;
};

type SortKey = "marketCap" | "totalRevenue" | "profitMargins" | "grossMargins" | "operatingMargins" | "trailingPE" | "change52Week";

export default function PeerComparison({ ticker, onSelectTicker }: Props) {
  const [peers, setPeers] = useState<PeerComparisonRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState<SortKey>("marketCap");
  const [sortAsc, setSortAsc] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const res = await fetch(`/api/peers/${encodeURIComponent(ticker)}`);
        const data = await res.json();
        if (!cancelled) setPeers(data.peers ?? []);
      } catch (err) {
        console.error(err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [ticker]);

  if (loading) {
    return (
      <div className="card">
        <div className="cardHead"><div className="cardTitle">Sector Peers</div></div>
        <div className="cardBody"><div className="viewLoading"><div className="viewLoadingSpinner" /><p>Loading peers…</p></div></div>
      </div>
    );
  }

  if (peers.length === 0) return null;

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortAsc(!sortAsc);
    } else {
      setSortKey(key);
      setSortAsc(false);
    }
  }

  const sorted = [...peers].sort((a, b) => {
    const av = a[sortKey] ?? -Infinity;
    const bv = b[sortKey] ?? -Infinity;
    return sortAsc ? (av as number) - (bv as number) : (bv as number) - (av as number);
  });

  const currentSector = peers.find(p => p.isCurrent)?.industry ?? "Sector";
  const arrow = sortAsc ? " ↑" : " ↓";

  const cols: { key: SortKey; label: string; fmt: (v: number | null) => string }[] = [
    { key: "marketCap", label: "Mkt Cap", fmt: (v) => v ? fmtCompact(v) : "—" },
    { key: "totalRevenue", label: "Revenue", fmt: (v) => v ? fmtCompact(v) : "—" },
    { key: "profitMargins", label: "Profit %", fmt: (v) => v !== null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "grossMargins", label: "Gross %", fmt: (v) => v !== null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "operatingMargins", label: "Op %", fmt: (v) => v !== null ? `${(v * 100).toFixed(1)}%` : "—" },
    { key: "trailingPE", label: "P/E", fmt: (v) => v ? v.toFixed(1) : "—" },
    { key: "change52Week", label: "52W Δ", fmt: (v) => v !== null ? fmtPct(v * 100) : "—" },
  ];

  return (
    <div className="card peerCard">
      <div className="cardHead">
        <div className="cardTitle">
          Sector Peers
          <span className="pvTickerBadge mono" style={{ marginLeft: 8 }}>{currentSector}</span>
          <span style={{ fontWeight: 400, fontSize: 12, color: "var(--muted)", marginLeft: 8 }}>
            {peers.length} companies
          </span>
        </div>
      </div>
      <div className="cardBody" style={{ padding: 0, overflowX: "auto" }}>
        <table className="peerTable">
          <thead>
            <tr>
              <th>Company</th>
              {cols.map(c => (
                <th key={c.key} className="peerSortable" onClick={() => handleSort(c.key)}>
                  {c.label}{sortKey === c.key ? arrow : ""}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row) => (
              <tr
                key={row.ticker}
                className={row.isCurrent ? "peerRowCurrent" : ""}
                onClick={() => { if (!row.isCurrent) onSelectTicker(row.ticker); }}
                style={{ cursor: row.isCurrent ? "default" : "pointer" }}
              >
                <td>
                  <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <span className="mono" style={{ fontWeight: 700, fontSize: 12 }}>
                      {row.ticker.replace(".NS", "")}
                      {row.isCurrent && <span className="peerCurrentBadge">You</span>}
                    </span>
                    <span style={{ fontSize: 11, color: "var(--muted)", lineHeight: 1.2 }}>{row.name}</span>
                  </div>
                </td>
                {cols.map(c => {
                  const val = row[c.key];
                  const isChange = c.key === "change52Week";
                  return (
                    <td key={c.key} className="mono" style={isChange ? { color: (val ?? 0) >= 0 ? "var(--good)" : "var(--critical)" } : undefined}>
                      {c.fmt(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
