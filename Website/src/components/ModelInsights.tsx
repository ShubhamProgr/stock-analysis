import type { TickerBundle } from "@/lib/types";
import { fmtMoney, fmtPct } from "@/lib/format";

export default function ModelInsights({ bundle }: { bundle: TickerBundle }) {
  const analysis = bundle.analysis;

  if (!analysis || !analysis.modelType) {
    return (
      <section className="modelInsightsSection" id="section-ml-insights">
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle" style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <span>ML Model Insights</span>
              <span className="pvTickerBadge mono">{bundle.ticker.replace(".NS", "")}</span>
            </div>
          </div>
          <div className="cardBody">
            <p style={{ opacity: 0.7, fontSize: "0.9rem", margin: 0 }}>
              No advanced ML insights available for {bundle.ticker} on{" "}
              {analysis?.predictionDate ? String(analysis.predictionDate) : "this date"}. Model Type or Top Features
              not found in the database.
            </p>
          </div>
        </div>
      </section>
    );
  }

  const features = analysis.topFeatures ? Object.entries(analysis.topFeatures) : [];
  const predReturnPct =
    analysis.lastClose > 0
      ? ((analysis.predictedClose - analysis.lastClose) / analysis.lastClose) * 100
      : 0;

  return (
    <section className="modelInsightsSection" id="section-ml-insights">
      <div className="card">
        <div className="cardHead">
          <div className="cardTitle" style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span>ML Model Insights</span>
            <span className="pvTickerBadge mono">{analysis.modelType}</span>
            <span className="pvTickerBadge mono" style={{ opacity: 0.8 }}>
              {bundle.ticker.replace(".NS", "")}
            </span>
          </div>
          {analysis.predictionDate && (
            <div className="modelInsightsDate">
              Evaluation Date:{" "}
              <strong className="mono" style={{ color: "var(--ink)" }}>
                {new Date(analysis.predictionDate).toLocaleDateString("en-IN", {
                  day: "numeric",
                  month: "short",
                  year: "numeric",
                })}
              </strong>
            </div>
          )}
        </div>

        <div className="cardBody">
          <div className="modelInsightsGrid">
            {/* Left: Model Performance & Architecture KPI tiles */}
            <div className="modelInsightsKpiCol">
              <div className="modelInsightsKpiGrid">
                <div className="modelKpiTile">
                  <div className="modelKpiLabel">Architecture</div>
                  <div className="modelKpiValue mono" style={{ color: "var(--accent)" }}>
                    {analysis.modelType}
                  </div>
                  <div className="modelKpiSub">Gradient Boosted Trees</div>
                </div>

                <div className="modelKpiTile">
                  <div className="modelKpiLabel">Test R² Score</div>
                  <div
                    className={`modelKpiValue mono ${
                      analysis.r2 >= 0.7
                        ? "pvGood"
                        : analysis.r2 >= 0.4
                        ? "pvCaution"
                        : "pvCritical"
                    }`}
                  >
                    {analysis.r2.toFixed(3)}
                  </div>
                  <div className="modelKpiSub">
                    {analysis.r2 >= 0.8
                      ? "High Variance Explained"
                      : analysis.r2 >= 0.5
                      ? "Moderate Fit"
                      : "High Variance"}
                  </div>
                </div>

                <div className="modelKpiTile">
                  <div className="modelKpiLabel">CV RMSE (5-Fold)</div>
                  <div className="modelKpiValue mono">
                    {analysis.cvRmse !== null ? analysis.cvRmse.toFixed(2) : "N/A"}
                  </div>
                  <div className="modelKpiSub">Validation Loss</div>
                </div>

                <div className="modelKpiTile">
                  <div className="modelKpiLabel">Predicted Next Close</div>
                  <div className="modelKpiValue mono">
                    {fmtMoney(analysis.predictedClose)}
                  </div>
                  <div
                    className={`modelKpiSub ${
                      predReturnPct >= 0 ? "pvGood" : "pvCritical"
                    }`}
                  >
                    {fmtPct(predReturnPct)} vs last close
                  </div>
                </div>
              </div>
            </div>

            {/* Right: Top Driving Features */}
            <div className="modelInsightsFeaturesCol">
              <div className="modelFeaturesHeader">
                <div className="modelFeaturesTitle">Top Driving Features</div>
                <div className="modelFeaturesSubtitle">
                  Relative feature importance in decision splits
                </div>
              </div>

              {features.length > 0 ? (
                <div className="modelFeaturesList">
                  {features.map(([name, importance], idx) => {
                    const pct = (importance as number) * 100;
                    const maxPct = Math.max(
                      ...features.map(([, imp]) => (imp as number) * 100)
                    );
                    const relWidth = maxPct > 0 ? (pct / maxPct) * 100 : 0;
                    return (
                      <div key={name} className="modelFeatureRow">
                        <span className="modelFeatureName mono" title={name}>
                          {name}
                        </span>
                        <div className="modelFeatureTrack">
                          <div
                            className="modelFeatureBar"
                            style={{
                              width: `${Math.max(3, relWidth)}%`,
                              opacity: Math.max(0.45, 1 - idx * 0.12),
                            }}
                          />
                        </div>
                        <span className="modelFeaturePct mono">
                          {pct.toFixed(1)}%
                        </span>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="emptyState" style={{ padding: "16px 0" }}>
                  No feature importance breakdown available.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
