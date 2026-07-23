import type { PredictionData } from "@/lib/types";
import { fmtMoney, fmtPct } from "@/lib/format";

export default function TickerTape({ predictions }: { predictions: PredictionData[] }) {
  if (!predictions || predictions.length === 0) return null;

  const items = predictions.slice(0, 24).map((pred, index) => (
    <span key={`${pred.Ticker}-${index}`}>
      <b>{pred.Ticker.replace(".NS", "")}</b> {fmtMoney(pred.Predicted_Closing_Price)}{" "}
      {pred.Predicted_Return_Pct !== null && (
        <span className={`chip ${pred.Predicted_Return_Pct >= 0 ? "up" : "down"}`} style={{ padding: "1px 6px" }}>
          {fmtPct(pred.Predicted_Return_Pct)}
        </span>
      )}
    </span>
  ));

  return (
    <div className="tapeWrap">
      <div className="tape mono">
        {items}
        {items}
      </div>
    </div>
  );
}