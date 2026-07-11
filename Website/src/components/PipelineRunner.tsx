"use client";

import { useState } from "react";
import { PIPELINE_JOBS, type PipelineJob } from "@/lib/pipeline-jobs";

const JOB_LABELS: Record<PipelineJob, { title: string; script: string; blurb: string }> = {
  stock_data_daily: { title: "Daily Prices", script: "Stock_Data_Daily.py", blurb: "Pull the latest 3 days of OHLCV." },
  stock_data_5y: { title: "5-Year History", script: "Stock_Data_5Y.py", blurb: "Backfill 10 years of daily OHLCV." },
  company_info: { title: "Fundamentals", script: "Company_Data.py", blurb: "Refresh sector, margins, market cap." },
  news_extractor: { title: "News Feed", script: "News_Extractor.py", blurb: "Scrape latest per-company headlines." },
  sentiment_analyzer: { title: "Sentiment", script: "Sentiment_Analyzer.py", blurb: "Score headlines with FinBERT." },
  final_analysis: { title: "ML Prediction", script: "Final_Analysis.py", blurb: "Train + predict next close per ticker." },
  prediction_vs_actual: { title: "Prediction vs Actual", script: "Actual_vs_Prediction.py", blurb: "Reconcile forecasts against real closes." }
};

export function PipelineRunner() {
  const [status, setStatus] = useState<Record<string, string>>({});
  const [pending, setPending] = useState<string | null>(null);

  async function run(job: PipelineJob) {
    setPending(job);
    setStatus((s) => ({ ...s, [job]: "Starting..." }));
    try {
      const res = await fetch("/api/admin/trigger", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job })
      });
      const body = await res.json();
      setStatus((s) => ({ ...s, [job]: res.ok ? body.message ?? "Started." : body.error ?? "Failed to start." }));
    } catch {
      setStatus((s) => ({ ...s, [job]: "Network error while starting the job." }));
    } finally {
      setPending(null);
    }
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {PIPELINE_JOBS.map((job) => {
        const meta = JOB_LABELS[job];
        return (
          <article key={job} className="panel flex flex-col justify-between gap-3 p-4">
            <div>
              <h3 className="font-display font-semibold">{meta.title}</h3>
              <p className="mt-1 text-sm text-text-muted">{meta.blurb}</p>
              <code className="mt-2 block text-xs text-text-faint">{meta.script}</code>
            </div>
            <button
              onClick={() => run(job)}
              disabled={pending === job}
              className="rounded-lg border border-ink-600 bg-ink-700/60 px-3 py-2 text-sm font-medium hover:border-signal-amber disabled:opacity-50"
            >
              {pending === job ? "Starting..." : "Run script"}
            </button>
            {status[job] && <p className="text-xs text-text-muted">{status[job]}</p>}
          </article>
        );
      })}
    </div>
  );
}
