import { query } from "./db";
import { buildStrategies } from "./signals";
import type {
  CompanyInfo,
  CompanySentiment,
  NewsItem,
  PredictionPoint,
  PricePoint,
  SentimentPoint,
  TickerBundle,
  WatchlistRow,
} from "./types";

const STOP_WORDS = new Set(["limited", "ltd", "inc", "corporation", "corp", "co", "company", "plc", "the"]);

function significantWords(s: string): string[] {
  return s
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter((w) => w.length > 2 && !STOP_WORDS.has(w));
}

export async function getWatchlist(limit = 96): Promise<WatchlistRow[]> {
  const tickers = await query<{ ticker: string; longname: string | null }>(
    `SELECT "Ticker" as ticker, "longName" as longname
     FROM company_info
     ORDER BY "Ticker"
     LIMIT $1`,
    [limit]
  );

  const rows = await query<{ ticker: string; date: string; close: string }>(
    `WITH ranked AS (
       SELECT "Ticker" as ticker, "Date" as date, "Close" as close,
              ROW_NUMBER() OVER (PARTITION BY "Ticker" ORDER BY "Date" DESC) rn
       FROM stock_data
       WHERE "Close" > 0
     )
     SELECT ticker, date, close FROM ranked WHERE rn <= 30 ORDER BY ticker, date ASC`,
    []
  );

  const byTicker = new Map<string, { date: string; close: number }[]>();
  for (const r of rows) {
    const arr = byTicker.get(r.ticker) ?? [];
    arr.push({ date: r.date, close: parseFloat(r.close) });
    byTicker.set(r.ticker, arr);
  }

  const nameByTicker = new Map(tickers.map((t) => [t.ticker, t.longname ?? t.ticker]));

  const out: WatchlistRow[] = [];
  for (const [ticker, points] of byTicker) {
    if (points.length < 2) continue;
    const last = points[points.length - 1].close;
    const prev = points[points.length - 2].close;
    out.push({
      ticker,
      name: nameByTicker.get(ticker) ?? ticker,
      price: last,
      changePct: ((last - prev) / prev) * 100,
      spark: points.map((p) => p.close),
    });
  }
  out.sort((a, b) => a.ticker.localeCompare(b.ticker));
  return out;
}

export async function getPriceSeries(ticker: string, days: number): Promise<PricePoint[]> {
  const rows = await query<{ date: string; close: string; volume: string }>(
    `SELECT "Date" as date, "Close" as close, "Volume" as volume
     FROM stock_data
     WHERE "Ticker" = $1 AND "Close" > 0
     ORDER BY "Date" DESC
     LIMIT $2`,
    [ticker, days]
  );
  return rows
    .map((r) => ({ date: r.date, close: parseFloat(r.close), volume: parseInt(r.volume, 10) || 0 }))
    .reverse();
}

export async function getSentimentSeries(ticker: string): Promise<SentimentPoint[]> {
  const rows = await query<{ date: string; sentiment: string; score: string }>(
    `SELECT "Prediction_Date" as date, "Sentiment" as sentiment, "Sentiment_Score" as score
     FROM final_analysis
     WHERE "Ticker" = $1
     ORDER BY "Prediction_Date" ASC`,
    [ticker]
  );
  return rows.map((r) => ({
    date: r.date,
    sentiment: r.sentiment,
    score: Math.round(parseFloat(r.score) * 100),
  }));
}

export async function getLatestAnalysis(ticker: string) {
  const rows = await query<{
    prediction_date: string;
    predicted_closing_price: string;
    last_close: string;
    r2_score: string;
    sentiment: string;
    sentiment_score: string;
  }>(
    `SELECT "Prediction_Date" as prediction_date, "Predicted_Closing_Price" as predicted_closing_price,
            "Last_Close" as last_close, "R2_Score" as r2_score,
            "Sentiment" as sentiment, "Sentiment_Score" as sentiment_score
     FROM final_analysis
     WHERE "Ticker" = $1
     ORDER BY "Prediction_Date" DESC
     LIMIT 1`,
    [ticker]
  );
  const row = rows[0];
  if (!row) return null;
  return {
    predictionDate: row.prediction_date,
    predictedClose: parseFloat(row.predicted_closing_price),
    lastClose: parseFloat(row.last_close),
    r2: parseFloat(row.r2_score),
    sentiment: row.sentiment,
    sentimentScore: parseFloat(row.sentiment_score),
  };
}

export async function getPredictionHistory(ticker: string): Promise<PredictionPoint[]> {
  const rows = await query<{ date: string; predicted: string; actual: string }>(
    `SELECT "Date" as date, "Predicted_Closing_Price" as predicted, "Actual_Closing_Price" as actual
     FROM prediction_vs_actual
     WHERE "Ticker" = $1
     ORDER BY "Date" ASC`,
    [ticker]
  );
  return rows.map((r) => ({ date: r.date, predicted: parseFloat(r.predicted), actual: parseFloat(r.actual) }));
}

export async function getCompanyInfo(ticker: string): Promise<CompanyInfo | null> {
  const rows = await query<{
    ticker: string;
    longname: string | null;
    sector: string | null;
    industry: string | null;
    marketcap: string | null;
    trailingpe: string | null;
    profitmargins: string | null;
    change52week: string | null;
  }>(
    `SELECT "Ticker" as ticker, "longName" as longname, sector, industry,
            "marketCap" as marketcap, "trailingPE" as trailingpe,
            "profitMargins" as profitmargins, "52WeekChange" as change52week
     FROM company_info WHERE "Ticker" = $1`,
    [ticker]
  );
  const row = rows[0];
  if (!row) return null;
  return {
    ticker: row.ticker,
    longName: row.longname,
    sector: row.sector,
    industry: row.industry,
    marketCap: row.marketcap ? parseFloat(row.marketcap) : null,
    trailingPE: row.trailingpe ? parseFloat(row.trailingpe) : null,
    profitMargins: row.profitmargins ? parseFloat(row.profitmargins) : null,
    change52Week: row.change52week ? parseFloat(row.change52week) : null,
  };
}

export async function getCompanySentiment(ticker: string): Promise<CompanySentiment | null> {
  const rows = await query<{
    company: string;
    ticker: string;
    articlecount: number;
    sentiment: string;
    score: string;
    paragraph: string;
  }>(
    `SELECT "Company" as company, "Ticker" as ticker, "ArticleCount" as articlecount,
            "Sentiment" as sentiment, "Score" as score, "Paragraph" as paragraph
     FROM company_finbert_sentiments WHERE "Ticker" = $1`,
    [ticker]
  );
  const row = rows[0];
  if (!row) return null;
  return {
    company: row.company,
    ticker: row.ticker,
    articleCount: row.articlecount,
    sentiment: row.sentiment,
    score: Math.round(parseFloat(row.score) * 100),
    excerpt: row.paragraph?.slice(0, 220) ?? "",
  };
}

export async function getNewsForTicker(ticker: string, companySlug: string, limit = 8): Promise<NewsItem[]> {
  const words = significantWords(companySlug);
  if (words.length === 0) return [];
  const firstWord = words[0];

  const candidates = await query<{ source: string; publicationdate: string; content: string; link: string; company: string }>(
    `SELECT "Source" as source, "PublicationDate" as publicationdate, "Content" as content,
            "Link" as link, "Company" as company
     FROM "News"
     WHERE lower("Company") LIKE '%' || $1 || '%'
     ORDER BY "PublicationDate" DESC
     LIMIT 300`,
    [firstWord]
  );

  const scored = candidates
    .map((c) => {
      const companyWords = new Set(significantWords(c.company));
      const overlap = words.filter((w) => companyWords.has(w)).length;
      return { ...c, overlap };
    })
    .filter((c) => c.overlap >= Math.min(2, words.length))
    .sort((a, b) => b.overlap - a.overlap || (a.publicationdate < b.publicationdate ? 1 : -1))
    .slice(0, limit);

  return scored.map((c) => ({
    source: c.source,
    publicationDate: c.publicationdate,
    headline: c.content,
    link: c.link,
  }));
}

export async function getTickerBundle(ticker: string, rangeDays: number): Promise<TickerBundle | null> {
  const [series, sentimentSeries, companyInfo, companySentiment, analysis, predictionHistory] = await Promise.all([
    getPriceSeries(ticker, Math.max(rangeDays, 60)),
    getSentimentSeries(ticker),
    getCompanyInfo(ticker),
    getCompanySentiment(ticker),
    getLatestAnalysis(ticker),
    getPredictionHistory(ticker),
  ]);

  if (series.length === 0) return null;

  const trimmedSeries = series.slice(-rangeDays);
  const last = series[series.length - 1];
  const prev = series[series.length - 2] ?? last;
  const changePct = ((last.close - prev.close) / prev.close) * 100;

  const vols = series.slice(-30).map((p) => p.volume);
  const avgVolume30d = vols.reduce((a, b) => a + b, 0) / (vols.length || 1);

  const companySlug = companySentiment?.company ?? companyInfo?.longName ?? ticker;
  const news = await getNewsForTicker(ticker, companySlug);

  const strategies = buildStrategies(
    series,
    last.close,
    analysis?.predictedClose ?? null,
    analysis?.sentiment ?? null,
    analysis ? analysis.sentimentScore : null,
    analysis?.r2 ?? null
  );

  return {
    ticker,
    name: companyInfo?.longName ?? ticker,
    price: last.close,
    changePct,
    volume: last.volume,
    avgVolume30d,
    series: trimmedSeries,
    sentimentSeries,
    companyInfo,
    companySentiment,
    news,
    strategies,
    predictionHistory,
  };
}
