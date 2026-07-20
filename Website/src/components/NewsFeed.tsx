import type { NewsItem } from "@/lib/types";

function relativeTime(iso: string): string {
  const diffMs = Date.now() - new Date(iso).getTime();
  const hours = Math.round(diffMs / 3600000);
  if (hours < 1) return "just now";
  if (hours < 24) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
}

export default function NewsFeed({ news }: { news: NewsItem[] }) {
  return (
    <div className="card" style={{ marginTop: 14 }}>
      <div className="cardHead">
        <div className="cardTitle">News feed</div>
        <span className="eyebrow">Auto-matched by company name</span>
      </div>
      <div className="cardBody">
        {news.length === 0 ? (
          <div className="emptyState">No matched articles for this name yet.</div>
        ) : (
          <div className="newsList">
            {news.map((n, i) => (
              <div className="newsItem" key={i}>
                <div className="newsMeta">
                  <span className="newsSource">{n.source}</span>
                  <span className="newsTime">{relativeTime(n.publicationDate)}</span>
                </div>
                <a className="newsHead" href={n.link} target="_blank" rel="noopener noreferrer">
                  {n.headline}
                </a>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
