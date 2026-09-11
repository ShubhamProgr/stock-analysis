"use client";

import type { CompanyInfo, HealthScore } from "@/lib/types";
import { fmtCompact, fmtPct } from "@/lib/format";

function MarginBar({ label, value, color }: { label: string; value: number | null; color: string }) {
  const pct = value !== null ? Math.round(value * 100) : null;
  return (
    <div className="fundMarginRow">
      <span className="fundMarginLabel">{label}</span>
      <div className="fundMarginTrack">
        <div
          className="fundMarginFill"
          style={{
            width: `${Math.max(2, Math.min(100, (pct ?? 0)))}%`,
            background: color,
          }}
        />
      </div>
      <span className="fundMarginValue mono">{pct !== null ? `${pct}%` : "—"}</span>
    </div>
  );
}

function HealthGauge({ score }: { score: HealthScore }) {
  const radius = 54;
  const stroke = 8;
  const circumference = Math.PI * radius; // half circle
  const offset = circumference - (score.overall / 100) * circumference;

  const gradeColor =
    score.overall >= 75 ? "var(--good)" :
    score.overall >= 50 ? "var(--caution)" :
    "var(--critical)";

  return (
    <div className="healthGauge">
      <svg width="130" height="80" viewBox="0 0 130 80">
        {/* Background track */}
        <path
          d="M 11 70 A 54 54 0 0 1 119 70"
          fill="none"
          stroke="var(--grid)"
          strokeWidth={stroke}
          strokeLinecap="round"
        />
        {/* Filled arc */}
        <path
          d="M 11 70 A 54 54 0 0 1 119 70"
          fill="none"
          stroke={gradeColor}
          strokeWidth={stroke}
          strokeLinecap="round"
          strokeDasharray={`${circumference}`}
          strokeDashoffset={offset}
          style={{ transition: "stroke-dashoffset 0.8s ease" }}
        />
      </svg>
      <div className="healthGaugeLabel">
        <span className="healthGaugeGrade" style={{ color: gradeColor }}>{score.grade}</span>
        <span className="healthGaugeScore mono">{score.overall}/100</span>
      </div>
    </div>
  );
}

function SubScore({ label, value }: { label: string; value: number }) {
  const color = value >= 70 ? "var(--good)" : value >= 40 ? "var(--caution)" : "var(--critical)";
  return (
    <div className="healthSubScore">
      <div className="healthSubScoreHeader">
        <span className="healthSubScoreLabel">{label}</span>
        <span className="healthSubScoreValue mono" style={{ color }}>{value}</span>
      </div>
      <div className="healthSubScoreTrack">
        <div className="healthSubScoreFill" style={{ width: `${value}%`, background: color }} />
      </div>
    </div>
  );
}

export default function CompanyFundamentals({
  info,
  healthScore,
}: {
  info: CompanyInfo | null;
  healthScore: HealthScore | null;
}) {
  if (!info) return null;

  const debtToCash =
    info.totalCash && info.totalDebt && info.totalCash > 0
      ? (info.totalDebt / info.totalCash).toFixed(2)
      : null;

  const floatPct =
    info.floatShares && info.sharesOutstanding && info.sharesOutstanding > 0
      ? ((info.floatShares / info.sharesOutstanding) * 100).toFixed(1)
      : null;

  return (
    <section className="fundSection">
      <div className="fundGrid">
        {/* Health Score Card */}
        <div className="card fundHealthCard">
          <div className="cardHead">
            <div className="cardTitle">Financial Health</div>
          </div>
          {healthScore ? (
            <div className="cardBody fundHealthBody">
              <HealthGauge score={healthScore} />
              <div className="healthSubScores">
                <SubScore label="Margins" value={healthScore.marginQuality} />
                <SubScore label="Leverage" value={healthScore.leverage} />
                <SubScore label="Momentum" value={healthScore.momentum} />
                <SubScore label="Valuation" value={healthScore.valuation} />
              </div>
            </div>
          ) : (
            <div className="cardBody" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: 140, color: "var(--muted)", fontSize: 12.5 }}>
              Health metrics pending quarterly filing data
            </div>
          )}
        </div>

        {/* Margins Breakdown */}
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Margins &amp; Profitability</div>
          </div>
          <div className="cardBody">
            <MarginBar label="Gross Margin" value={info.grossMargins} color="var(--accent)" />
            <MarginBar label="Operating Margin" value={info.operatingMargins} color="var(--caution)" />
            <MarginBar label="Profit Margin" value={info.profitMargins} color="var(--good)" />
            {info.totalRevenue && (
              <div className="fundKpi">
                <span className="fundKpiLabel">Total Revenue</span>
                <span className="fundKpiValue mono">{fmtCompact(info.totalRevenue)}</span>
              </div>
            )}
          </div>
        </div>

        {/* Balance Sheet */}
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Balance Sheet</div>
          </div>
          <div className="cardBody">
            <div className="fundBalanceGrid">
              <div className="fundBalanceItem">
                <span className="fundBalanceLabel">Total Cash</span>
                <span className="fundBalanceValue mono pvGood">
                  {info.totalCash ? fmtCompact(info.totalCash) : "—"}
                </span>
              </div>
              <div className="fundBalanceItem">
                <span className="fundBalanceLabel">Total Debt</span>
                <span className="fundBalanceValue mono pvCritical">
                  {info.totalDebt ? fmtCompact(info.totalDebt) : "—"}
                </span>
              </div>
            </div>
            {/* Visual cash vs debt bar */}
            {info.totalCash !== null && info.totalDebt !== null && (
              <div className="fundCashDebtBar">
                <div
                  className="fundCashDebtCash"
                  style={{
                    width: `${Math.max(5, (info.totalCash / (info.totalCash + info.totalDebt)) * 100)}%`,
                  }}
                />
                <div
                  className="fundCashDebtDebt"
                  style={{
                    width: `${Math.max(5, (info.totalDebt / (info.totalCash + info.totalDebt)) * 100)}%`,
                  }}
                />
              </div>
            )}
            <div className="fundCashDebtLegend">
              <span><span className="dot" style={{ background: "var(--good)" }}></span>Cash</span>
              <span><span className="dot" style={{ background: "var(--critical)" }}></span>Debt</span>
              {debtToCash && <span className="mono" style={{ marginLeft: "auto", fontSize: 11, color: "var(--muted)" }}>D/C Ratio: {debtToCash}x</span>}
            </div>
          </div>
        </div>

        {/* Company Profile */}
        <div className="card">
          <div className="cardHead">
            <div className="cardTitle">Company Profile</div>
          </div>
          <div className="cardBody">
            <div className="fundProfileGrid">
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Sector</span>
                <span className="fundProfileValue">{info.sector ?? "—"}</span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Industry</span>
                <span className="fundProfileValue">{info.industry ?? "—"}</span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Market Cap</span>
                <span className="fundProfileValue mono">{info.marketCap ? fmtCompact(info.marketCap) : "—"}</span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">P/E Ratio</span>
                <span className="fundProfileValue mono">{info.trailingPE ? info.trailingPE.toFixed(2) : "—"}</span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">52W Change</span>
                <span className={`fundProfileValue mono ${(info.change52Week ?? 0) >= 0 ? "pvGood" : "pvCritical"}`}>
                  {info.change52Week ? fmtPct(info.change52Week * 100) : "—"}
                </span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Employees</span>
                <span className="fundProfileValue mono">
                  {info.fullTimeEmployees ? info.fullTimeEmployees.toLocaleString("en-IN") : "—"}
                </span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Shares Outstanding</span>
                <span className="fundProfileValue mono">
                  {info.sharesOutstanding ? fmtCompact(info.sharesOutstanding) : "—"}
                </span>
              </div>
              <div className="fundProfileRow">
                <span className="fundProfileLabel">Float %</span>
                <span className="fundProfileValue mono">{floatPct ? `${floatPct}%` : "—"}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
