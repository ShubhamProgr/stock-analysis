import type { TickerBundle } from "@/lib/types";
import { compositeFromStrategies } from "@/lib/signals";
import { fmtMoney, fmtPct, fmtVol } from "@/lib/format";

export default function StatTiles({
  bundle,
  predictionLabel,
}: {
  bundle: TickerBundle;
  predictionLabel: string | null;
}) {
  const composite = compositeFromStrategies(bundle.strategies);
  const latestSentiment = bundle.sentimentSeries[bundle.sentimentSeries.length - 1] ?? null;
  const priorSentiment = bundle.sentimentSeries[bundle.sentimentSeries.length - 2] ?? null;
  const sentimentDelta = latestSentiment && priorSentiment ? latestSentiment.score - priorSentiment.score : null;
  const volVsAvg = ((bundle.volume - bundle.avgVolume30d) / (bundle.avgVolume30d || 1)) * 100;

  return (
    <section className="statRow" id="section-overview">
      <div className="tile">
        <div className="eyebrow">{predictionLabel ? `Prediction - ${predictionLabel}` : "Prediction"}</div>
        <div className="value mono">{fmtMoney(bundle.price)}</div>
        <div className="sub">
          <span className={`chip ${bundle.changePct >= 0 ? "up" : "down"}`}>{fmtPct(bundle.changePct)}</span> today
        </div>
      </div>
      <div className="tile">
        <div className="eyebrow">Sentiment score</div>
        <div className="value mono">{latestSentiment ? latestSentiment.score : "—"}</div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 7 }}>
          <div className="gaugeTrack">
            <div className="gaugeFill" style={{ width: `${latestSentiment?.score ?? 0}%` }} />
          </div>
        </div>
        <div className="sub">
          {sentimentDelta === null
            ? "No prior reading yet"
            : `${sentimentDelta >= 0 ? "+" : ""}${sentimentDelta} pts vs prior reading`}
        </div>
      </div>
      <div className="tile">
        <div className="eyebrow">Composite signal</div>
        <div className="value" style={{ marginTop: 2 }}>
          <span className={`chip ${composite.action.toLowerCase()}`} style={{ fontSize: 13, padding: "4px 10px" }}>
            {composite.action}
          </span>
        </div>
        <div className="sub">{composite.confidence}% average confidence across strategies</div>
      </div>
      <div className="tile">
        <div className="eyebrow">Volume vs 30D avg</div>
        <div className="value mono">{fmtVol(bundle.volume)}</div>
        <div className="sub">
          {volVsAvg >= 0 ? "+" : ""}
          {volVsAvg.toFixed(0)}% vs 30D avg
        </div>
      </div>
    </section>
  );
}
