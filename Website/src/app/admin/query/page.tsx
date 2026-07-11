"use client";

import { useState } from "react";

export default function AdminQueryPage() {
  const [sql, setSql] = useState("select * from final_analysis order by prediction_date desc limit 25;");
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function runQuery(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/admin/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql })
      });
      const body = await res.json();
      if (!res.ok) throw new Error(body.error ?? "Query failed.");
      setColumns(body.columns);
      setRows(body.rows);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Query failed.");
      setColumns([]);
      setRows([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto max-w-5xl px-6 py-8">
      <span className="text-xs uppercase tracking-wide text-signal-amber">Protected workspace</span>
      <h1 className="mt-2 font-display text-2xl font-bold">Read-only SQL console</h1>
      <p className="mt-2 text-text-muted">
        SELECT-only. Statements run on a read-only transaction with a 5s timeout, and an implicit LIMIT 200 is added if
        you don&apos;t specify one.
      </p>

      <form onSubmit={runQuery} className="panel mt-6 p-6">
        <label htmlFor="query" className="mb-2 block text-sm text-text-muted">
          SQL query
        </label>
        <textarea
          id="query"
          rows={6}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          className="w-full rounded-lg border border-ink-600 bg-ink-900 px-3 py-2 font-mono text-sm outline-none focus:border-signal-amber"
        />
        <div className="mt-4 flex items-center justify-between">
          <span className="text-xs text-text-faint">Only statements beginning with SELECT are accepted.</span>
          <button
            type="submit"
            disabled={loading}
            className="rounded-lg bg-signal-amber px-4 py-2 text-sm font-semibold text-ink-950 hover:brightness-110 disabled:opacity-50"
          >
            {loading ? "Running..." : "Execute query"}
          </button>
        </div>
        {error && <div className="mt-4 rounded-lg bg-signal-down/10 px-3 py-2 text-sm text-signal-down">{error}</div>}
      </form>

      {rows.length > 0 && (
        <div className="panel mt-6 overflow-x-auto p-6">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="text-text-faint">
                {columns.map((c) => (
                  <th key={c} className="pb-2 pr-4">
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-ink-600/50">
              {rows.map((row, i) => (
                <tr key={i}>
                  {columns.map((c) => (
                    <td key={c} className="py-2 pr-4 font-mono">
                      {String(row[c] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
