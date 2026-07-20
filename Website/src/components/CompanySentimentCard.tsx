import type { CompanySentiment } from "@/lib/types";

export default function CompanySentimentCard({ sentiment }: { sentiment: CompanySentiment | null }) {
  return (
    <div className="card sentimentCard" id="section-sentiment">
      <div className="cardHead">
        <div className="cardTitle">Company sentiment</div>
        {sentiment && <span className="eyebrow">{sentiment.articleCount} articles analyzed</span>}
      </div>
      <div className="cardBody">
        {sentiment ? (
          <>
            <div className="scoreRow">
              <span className="scoreValue mono">{sentiment.score}</span>
              <span
                className={`chip ${
                  sentiment.sentiment.toLowerCase() === "positive"
                    ? "up"
                    : sentiment.sentiment.toLowerCase() === "negative"
                    ? "down"
                    : "neutral"
                }`}
              >
                {sentiment.sentiment}
              </span>
            </div>
            <div className="sub" style={{ fontSize: 11.5, color: "var(--muted)", marginTop: 6 }}>
              FinBERT sentiment aggregated across recent news for this company. Score is 0–100, model-weighted, not a
              per-article breakdown.
            </div>
          </>
        ) : (
          <div className="emptyState">No FinBERT sentiment computed for this ticker yet.</div>
        )}
      </div>
    </div>
  );
}
