"use client";

import { useEffect, useState } from "react";
import type { DailySentimentRow } from "@/lib/types";

type Props = {
  ticker: string;
};

export default function SentimentCalendar({ ticker }: Props) {
  const [data, setData] = useState<DailySentimentRow[]>([]);
  const [loading, setLoading] = useState(true);

  const [rangeDays, setRangeDays] = useState(90);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      try {
        const res = await fetch(`/api/sentiment/calendar/${encodeURIComponent(ticker)}?days=${rangeDays}`);
        const json = await res.json();
        if (!cancelled && !json.error) {
          setData(json);
        }
      } catch (err) {
        console.error(err);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [ticker, rangeDays]);

  if (loading) {
    return (
      <div className="card sentimentCard">
        <div className="cardHead">
          <div className="cardTitle">Sentiment Heatmap</div>
        </div>
        <div className="cardBody" style={{ minHeight: 120, display: "flex", alignItems: "center", justifyContent: "center" }}>
          <div className="viewLoadingSpinner" />
        </div>
      </div>
    );
  }

  // Group by week (Sunday to Saturday)
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(endDate.getDate() - (rangeDays - 1));

  // Pre-fill a map of all dates
  const dates = new Map<string, DailySentimentRow | null>();
  for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
    dates.set(d.toISOString().split("T")[0], null);
  }

  // Populate data
  for (const d of data) {
    if (dates.has(d.date)) {
      dates.set(d.date, d);
    }
  }

  // Convert to 2D array: weeks[cols][rows(7)]
  const weeks: (DailySentimentRow | null | undefined)[][] = [];
  let currentWeek: (DailySentimentRow | null | undefined)[] = [];
  let currentDate = new Date(startDate);

  // Pad beginning if start date is not Sunday
  const startDay = currentDate.getDay();
  for (let i = 0; i < startDay; i++) {
    currentWeek.push(undefined);
  }

  for (const [dateStr, row] of dates.entries()) {
    currentWeek.push(row);
    if (currentWeek.length === 7) {
      weeks.push(currentWeek);
      currentWeek = [];
    }
  }

  // Pad end if necessary
  if (currentWeek.length > 0) {
    while (currentWeek.length < 7) {
      currentWeek.push(undefined);
    }
    weeks.push(currentWeek);
  }

  // Color mapping
  const getColor = (row: DailySentimentRow | null | undefined) => {
    if (row === undefined) return "transparent";
    if (row === null) return "var(--surface-2)";
    
    if (row.sentiment === "POSITIVE") {
      if (row.score > 0.4) return "var(--good)";
      if (row.score > 0.1) return "rgba(53, 193, 94, 0.6)";
      return "rgba(53, 193, 94, 0.3)";
    }
    if (row.sentiment === "NEGATIVE") {
      if (row.score < -0.4) return "var(--critical)";
      if (row.score < -0.1) return "rgba(232, 99, 95, 0.6)";
      return "rgba(232, 99, 95, 0.3)";
    }
    return "rgba(255, 255, 255, 0.15)";
  };

  return (
    <div className="card sentimentCard">
      <div className="cardHead">
        <div className="cardTitle">Sentiment Heatmap</div>
        <div className="rangeToggleSmall">
          {[
            { label: "90D", days: 90 },
            { label: "180D", days: 180 },
            { label: "1Y", days: 365 },
          ].map((r) => (
            <button
              key={r.days}
              className={rangeDays === r.days ? "active" : ""}
              onClick={() => setRangeDays(r.days)}
            >
              {r.label}
            </button>
          ))}
        </div>
      </div>
      <div className="cardBody">
        <div className="sentimentCalendarWrap">
          <div className="sentimentCalendar">
            {weeks.map((week, wIdx) => (
              <div key={wIdx} className="sentimentWeek">
                {week.map((day, dIdx) => (
                  <div
                    key={dIdx}
                    className="sentimentDay"
                    style={{ backgroundColor: getColor(day) }}
                    title={day ? `${day.date}: ${day.sentiment} (${day.score.toFixed(2)}) - ${day.articleCount} articles` : "No data"}
                  />
                ))}
              </div>
            ))}
          </div>
        </div>
        <div className="sentimentLegend">
          <span className="muted">Neg</span>
          <div className="sentimentDay" style={{ backgroundColor: "var(--critical)" }} />
          <div className="sentimentDay" style={{ backgroundColor: "rgba(232, 99, 95, 0.4)" }} />
          <div className="sentimentDay" style={{ backgroundColor: "var(--surface-2)" }} />
          <div className="sentimentDay" style={{ backgroundColor: "rgba(53, 193, 94, 0.4)" }} />
          <div className="sentimentDay" style={{ backgroundColor: "var(--good)" }} />
          <span className="muted">Pos</span>
        </div>
      </div>
    </div>
  );
}
