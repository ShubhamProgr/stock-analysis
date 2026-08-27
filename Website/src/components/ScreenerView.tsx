"use client";

import { useState, useEffect, useMemo } from "react";
import type { ScreenerRow } from "@/lib/types";
import { fmtMoney, fmtPct, fmtCompact } from "@/lib/format";

type Props = {
  onSelectTicker: (ticker: string) => void;
};

type SortKey = "ticker" | "price" | "predictedReturn" | "sentiment" | "r2" | "trailingPE" | "profitMargins" | "change52Week" | "marketCap";
type SortDir = "asc" | "desc";

const SIGNAL_FILTERS = ["ALL", "BUY", "HOLD", "SELL"] as const;
const SENTIMENT_FILTERS = ["ALL", "POSITIVE", "NEUTRAL", "NEGATIVE"] as const;

export default function ScreenerView({ onSelectTicker }: Props) {
  const [rows, setRows] = useState<ScreenerRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [sortKey, setSortKey] = useState<SortKey>("predictedReturn");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  const [search, setSearch] = useState("");
  const [sectorFilter, setSectorFilter] = useState("ALL");
  const [signalFilter, setSignalFilter] = useState<typeof SIGNAL_FILTERS[number]>("ALL");
  const [sentimentFilter, setSentimentFilter] = useState<typeof SENTIMENT_FILTERS[number]>("ALL");

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/screener");
        const data = await res.json();
        setRows(data.rows ?? []);
      } catch (err) {
        console.error(err);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  const sectors = useMemo(() => {
    const s = new Set(rows.map((r) => r.sector));
    return ["ALL", ...Array.from(s).sort()];
  }, [rows]);

  const filtered = useMemo(() => {
    let result = rows;

    if (search) {
      const q = search.toLowerCase();
      result = result.filter(
        (r) => r.ticker.toLowerCase().includes(q) || r.name.toLowerCase().includes(q)
      );
    }
    if (sectorFilter !== "ALL") {
      result = result.filter((r) => r.sector === sectorFilter);
    }
    if (signalFilter !== "ALL") {
      result = result.filter((r) => r.signal === signalFilter);
    }
    if (sentimentFilter !== "ALL") {
      result = result.filter((r) => r.sentiment === sentimentFilter);
    }

    // Sort
    result = [...result].sort((a, b) => {
      let av: number | string = 0;
      let bv: number | string = 0;
      switch (sortKey) {
        case "ticker": av = a.ticker; bv = b.ticker; break;
        case "price": av = a.price; bv = b.price; break;
        case "predictedReturn": av = a.predictedReturn; bv = b.predictedReturn; break;
        case "sentiment": av = a.sentimentScore; bv = b.sentimentScore; break;
        case "r2": av = a.r2; bv = b.r2; break;
        case "trailingPE": av = a.trailingPE ?? 0; bv = b.trailingPE ?? 0; break;
        case "profitMargins": av = a.profitMargins ?? 0; bv = b.profitMargins ?? 0; break;
        case "change52Week": av = a.change52Week ?? 0; bv = b.change52Week ?? 0; break;
        case "marketCap": av = a.marketCap ?? 0; bv = b.marketCap ?? 0; break;
      }
      if (typeof av === "string") {
        return sortDir === "asc" ? av.localeCompare(bv as string) : (bv as string).localeCompare(av);
      }
      return sortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });

    return result;
  }, [rows, search, sectorFilter, signalFilter, sentimentFilter, sortKey, sortDir]);

  function handleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return "";
    return sortDir === "asc" ? " ▲" : " ▼";
  }

  if (loading) {
    return (
      <div className="viewLoading">
        <div className="viewLoadingSpinner" />
        <p>Loading screener…</p>
      </div>
    );
  }

  return (
    <div className="screenerWrap">
      <div className="viewHeader">
        <h1 className="viewTitle">Stock Screener</h1>
        <span className="viewSubtitle">{filtered.length} of {rows.length} stocks</span>
      </div>

      {/* Filters */}
      <div className="screenerFilters">
        <input
          className="screenerSearch"
          type="text"
          placeholder="Search ticker or company…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <select
          className="screenerSelect"
          value={sectorFilter}
          onChange={(e) => setSectorFilter(e.target.value)}
        >
          {sectors.map((s) => (
            <option key={s} value={s}>{s === "ALL" ? "All Sectors" : s}</option>
          ))}
        </select>
        <div className="screenerChipGroup">
          {SIGNAL_FILTERS.map((s) => (
            <button
              key={s}
              className={`screenerChip ${signalFilter === s ? "active" : ""} ${s.toLowerCase()}`}
              onClick={() => setSignalFilter(s)}
            >
              {s === "ALL" ? "All Signals" : s}
            </button>
          ))}
        </div>
        <div className="screenerChipGroup">
          {SENTIMENT_FILTERS.map((s) => (
            <button
              key={s}
              className={`screenerChip ${sentimentFilter === s ? "active" : ""}`}
              onClick={() => setSentimentFilter(s)}
            >
              {s === "ALL" ? "All Sentiment" : s}
            </button>
          ))}
        </div>
      </div>

      {/* Table */}
      <div className="card screenerTableCard">
        <div className="screenerTableWrap">
          <table className="screenerTable">
            <thead>
              <tr>
                <th onClick={() => handleSort("ticker")} className="sortable">Ticker{sortIndicator("ticker")}</th>
                <th>Sector</th>
                <th onClick={() => handleSort("price")} className="sortable right">Price{sortIndicator("price")}</th>
                <th onClick={() => handleSort("predictedReturn")} className="sortable right">Pred. Return{sortIndicator("predictedReturn")}</th>
                <th>Signal</th>
                <th>Sentiment</th>
                <th onClick={() => handleSort("r2")} className="sortable right">R²{sortIndicator("r2")}</th>
                <th onClick={() => handleSort("trailingPE")} className="sortable right">P/E{sortIndicator("trailingPE")}</th>
                <th onClick={() => handleSort("profitMargins")} className="sortable right">Profit Margin{sortIndicator("profitMargins")}</th>
                <th onClick={() => handleSort("change52Week")} className="sortable right">52W Chg{sortIndicator("change52Week")}</th>
                <th onClick={() => handleSort("marketCap")} className="sortable right">Mkt Cap{sortIndicator("marketCap")}</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => (
                <tr key={r.ticker} onClick={() => onSelectTicker(r.ticker)} className="screenerRow">
                  <td>
                    <span className="mono" style={{ fontWeight: 700 }}>{r.ticker.replace(".NS", "")}</span>
                    <span className="screenerName">{r.name}</span>
                  </td>
                  <td className="screenerSector">{r.sector}</td>
                  <td className="right mono">{fmtMoney(r.price)}</td>
                  <td className="right">
                    <span className={`chip ${r.predictedReturn >= 0 ? "up" : "down"}`}>
                      {fmtPct(r.predictedReturn)}
                    </span>
                  </td>
                  <td><span className={`chip ${r.signal.toLowerCase()}`}>{r.signal}</span></td>
                  <td><span className={`chip ${r.sentiment.toLowerCase()}`}>{r.sentiment}</span></td>
                  <td className="right mono">{r.r2.toFixed(3)}</td>
                  <td className="right mono">{r.trailingPE !== null ? r.trailingPE.toFixed(1) : "—"}</td>
                  <td className="right mono">
                    {r.profitMargins !== null ? (r.profitMargins * 100).toFixed(1) + "%" : "—"}
                  </td>
                  <td className="right">
                    {r.change52Week !== null ? (
                      <span className={r.change52Week >= 0 ? "pvGood" : "pvCritical"}>
                        {fmtPct(r.change52Week * 100)}
                      </span>
                    ) : "—"}
                  </td>
                  <td className="right mono">{r.marketCap ? fmtCompact(r.marketCap) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
