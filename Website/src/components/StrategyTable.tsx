import type { StrategySignal } from "@/lib/types";

export default function StrategyTable({ strategies }: { strategies: StrategySignal[] }) {
  return (
    <>
      <div className="cardHead strategyCard" id="section-strategy">
        <div className="cardTitle">Trade strategy signals</div>
      </div>
      <div className="cardBody" style={{ paddingTop: 0, overflowX: "auto" }}>
        <table className="strategies">
          <thead>
            <tr>
              <th>Strategy</th>
              <th>Signal</th>
              <th>Confidence</th>
              <th>Rationale</th>
            </tr>
          </thead>
          <tbody>
            {strategies.map((s) => (
              <tr key={s.name}>
                <td>
                  <div className="stratName">{s.name}</div>
                  <div className="stratHorizon">{s.horizon}</div>
                </td>
                <td>
                  <span className={`chip ${s.signal.toLowerCase()}`}>{s.signal}</span>
                </td>
                <td>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <div className="confBarTrack">
                      <div className="confBarFill" style={{ width: `${s.confidence}%` }} />
                    </div>
                    <span className="mono" style={{ fontSize: 11.5 }}>
                      {s.confidence}%
                    </span>
                  </div>
                </td>
                <td className="rationale">{s.rationale}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="disclaimer">
        Signals are derived from your own price history and model output (momentum &amp; range position from{" "}
        <span className="mono">stock_data</span>, direction &amp; sentiment from{" "}
        <span className="mono">final_analysis</span>) — illustrative, not investment advice.
      </div>
    </>
  );
}
