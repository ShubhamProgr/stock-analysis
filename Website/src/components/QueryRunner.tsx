"use client";

import { useState } from "react";

export default function QueryRunner() {
  const [query, setQuery] = useState("SELECT * FROM final_analysis LIMIT 5;");
  const [data, setData] = useState<{ fields: string[]; rows: any[] } | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleRunQuery() {
    setLoading(true);
    setError(null);
    setData(null);

    try {
      const res = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });

      const json = await res.json();
      if (!res.ok) throw new Error(json.error || "Query failed");

      setData(json);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card queryRunner">
      <div className="cardHead">
        <div className="cardTitle">SQL Query Runner</div>
      </div>
      <div className="cardBody">
        <p className="description" style={{ fontSize: "0.85rem", opacity: 0.8, marginBottom: 12 }}>
          Run ad-hoc SELECT queries directly against your Postgres database to extract custom insights.
        </p>
        
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            style={{
              width: "100%",
              height: 100,
              background: "rgba(0,0,0,0.2)",
              border: "1px solid rgba(255,255,255,0.1)",
              color: "var(--fg)",
              padding: 12,
              borderRadius: 8,
              fontFamily: "monospace",
              resize: "vertical",
            }}
          />
          
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ color: "var(--critical)", fontSize: "0.85rem" }}>
              {error}
            </span>
            <button 
              onClick={handleRunQuery}
              disabled={loading}
              style={{
                background: "var(--accent)",
                color: "#000",
                border: "none",
                padding: "8px 16px",
                borderRadius: 6,
                fontWeight: 600,
                cursor: loading ? "not-allowed" : "pointer",
                opacity: loading ? 0.7 : 1,
              }}
            >
              {loading ? "Running..." : "Run Query"}
            </button>
          </div>
        </div>

        {data && (
          <div style={{ marginTop: 24, overflowX: "auto" }}>
            <p style={{ fontSize: "0.8rem", opacity: 0.6, marginBottom: 8 }}>
              {data.rows.length} rows returned
            </p>
            <table className="strategyTable" style={{ width: "100%", textAlign: "left", fontSize: "0.85rem" }}>
              <thead>
                <tr>
                  {data.fields.map((field) => (
                    <th key={field} style={{ borderBottom: "1px solid rgba(255,255,255,0.1)", padding: "8px 12px" }}>
                      {field}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.rows.map((row, i) => (
                  <tr key={i}>
                    {data.fields.map((field) => (
                      <td key={field} style={{ padding: "8px 12px", borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
                        {row[field] !== null ? String(row[field]) : "NULL"}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
