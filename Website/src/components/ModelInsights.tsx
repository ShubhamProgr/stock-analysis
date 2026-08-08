import type { TickerBundle } from "@/lib/types";

export default function ModelInsights({ bundle }: { bundle: TickerBundle }) {
  const analysis = bundle.analysis;

  if (!analysis || !analysis.modelType) {
    return (
      <div className="card">
        <div className="cardHead">
          <div className="cardTitle">ML Model Insights</div>
        </div>
        <div className="cardBody">
          <p style={{ opacity: 0.7, fontSize: "0.9rem" }}>
            No advanced ML insights available for {bundle.ticker} on {analysis?.predictionDate ? String(analysis.predictionDate) : "this date"}. 
            Model_Type or Top_Features not found in the database.
          </p>
        </div>
      </div>
    );
  }

  const features = analysis.topFeatures ? Object.entries(analysis.topFeatures) : [];

  return (
    <div className="card">
      <div className="cardHead">
        <div className="cardTitle">ML Model Insights</div>
      </div>
      <div className="cardBody" style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
          <div style={{ background: "rgba(0,0,0,0.2)", padding: 12, borderRadius: 8, border: "1px solid rgba(255,255,255,0.05)" }}>
            <div style={{ fontSize: "0.75rem", opacity: 0.6, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Architecture</div>
            <div style={{ fontSize: "1.1rem", fontWeight: 600, color: "var(--accent)" }}>{analysis.modelType}</div>
          </div>
          
          <div style={{ background: "rgba(0,0,0,0.2)", padding: 12, borderRadius: 8, border: "1px solid rgba(255,255,255,0.05)" }}>
            <div style={{ fontSize: "0.75rem", opacity: 0.6, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>CV RMSE (5-Fold)</div>
            <div style={{ fontSize: "1.1rem", fontWeight: 600 }}>{analysis.cvRmse !== null ? analysis.cvRmse.toFixed(2) : "N/A"}</div>
          </div>
          
          <div style={{ background: "rgba(0,0,0,0.2)", padding: 12, borderRadius: 8, border: "1px solid rgba(255,255,255,0.05)" }}>
            <div style={{ fontSize: "0.75rem", opacity: 0.6, textTransform: "uppercase", letterSpacing: 1, marginBottom: 4 }}>Test R² Score</div>
            <div style={{ fontSize: "1.1rem", fontWeight: 600, color: analysis.r2 > 0.5 ? "var(--good)" : "inherit" }}>
              {analysis.r2.toFixed(3)}
            </div>
          </div>
        </div>

        {features.length > 0 && (
          <div>
            <h4 style={{ fontSize: "0.9rem", margin: "0 0 12px 0", opacity: 0.8 }}>Top Driving Features</h4>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {features.map(([name, importance], idx) => (
                <div key={name} style={{ display: "flex", alignItems: "center", gap: 12 }}>
                  <div style={{ width: 120, fontSize: "0.85rem", opacity: 0.8 }}>{name}</div>
                  <div style={{ flex: 1, height: 8, background: "rgba(255,255,255,0.1)", borderRadius: 4, overflow: "hidden" }}>
                    <div 
                      style={{ 
                        height: "100%", 
                        width: `${Math.max(2, (importance as number) * 100 * 2)}%`, // Scaled for visibility
                        background: "var(--accent)",
                        opacity: 1 - (idx * 0.15)
                      }} 
                    />
                  </div>
                  <div style={{ width: 40, fontSize: "0.75rem", textAlign: "right", opacity: 0.6, fontFamily: "monospace" }}>
                    {((importance as number) * 100).toFixed(1)}%
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

      </div>
    </div>
  );
}
