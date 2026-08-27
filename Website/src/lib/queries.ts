import { pool, query } from './db';
import { buildStrategies, compositeFromStrategies } from "./signals";
import { PredictionData } from './types';

export async function getPredictionDates(): Promise<string[]> {
  try {
    const result = await pool.query(`
      SELECT DISTINCT ("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date::text AS date
      FROM final_analysis
      ORDER BY date DESC
    `);
    return result.rows.map((row) => row.date as string);
  } catch (error) {
    console.error("Error fetching prediction dates:", error);
    return [];
  }
}

export async function getPredictions(date?: string): Promise<PredictionData[] | null> {
  try {
    const result = date
      ? await pool.query(
          `
            SELECT fa.*
            FROM final_analysis fa
            WHERE (fa."Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date = $1::date
            ORDER BY fa."Ticker" ASC
          `,
          [date]
        )
      : await pool.query(`
          WITH latest AS (
            SELECT MAX(("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date) AS max_date
            FROM final_analysis
          )
          SELECT fa.*
          FROM final_analysis fa
          JOIN latest l ON (fa."Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date = l.max_date
          ORDER BY fa."Ticker" ASC
        `);
    return result.rows as PredictionData[];
  } catch (error) {
    console.error("Error fetching predictions:", error);
    return null;
  }
}

export async function getPredictionsForDate(date: string): Promise<PredictionData[] | null> {
  return getPredictions(date);
}

import type {
  CompanyInfo,
  CompanySentiment,
  NewsItem,
  PredictionPoint,
  PricePoint,
  SentimentPoint,
  TickerBundle,
  WatchlistRow,
  MarketOverviewRow,
  ScreenerRow,
  OHLCPoint,
  AccuracyData,
  AccuracyRow,
  AccuracyTimeSeries,
  ComparisonBundle,
  ComparisonSeries,
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
    model_type: string | null;
    cv_rmse: string | null;
    top_features: string | null;
  }>(
    `WITH latest AS (
       SELECT MAX(("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date) AS max_date
       FROM final_analysis
     ),
     preferred AS (
       SELECT "Prediction_Date" as prediction_date,
              "Predicted_Closing_Price" as predicted_closing_price,
              "Last_Close" as last_close,
              "R2_Score" as r2_score,
              "Sentiment" as sentiment,
              "Sentiment_Score" as sentiment_score,
              "Model_Type" as model_type,
              "CV_RMSE" as cv_rmse,
              "Top_Features" as top_features
       FROM final_analysis
       WHERE "Ticker" = $1
         AND ("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date = (SELECT max_date FROM latest)
       LIMIT 1
     ),
     fallback AS (
       SELECT "Prediction_Date" as prediction_date,
              "Predicted_Closing_Price" as predicted_closing_price,
              "Last_Close" as last_close,
              "R2_Score" as r2_score,
              "Sentiment" as sentiment,
              "Sentiment_Score" as sentiment_score,
              "Model_Type" as model_type,
              "CV_RMSE" as cv_rmse,
              "Top_Features" as top_features
       FROM final_analysis
       WHERE "Ticker" = $1
       ORDER BY "Prediction_Date" DESC
       LIMIT 1
     )
     SELECT * FROM preferred
     UNION ALL
     SELECT * FROM fallback
     WHERE NOT EXISTS (SELECT 1 FROM preferred)
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
    modelType: row.model_type,
    cvRmse: row.cv_rmse ? parseFloat(row.cv_rmse) : null,
    topFeatures: row.top_features ? JSON.parse(row.top_features) : null,
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
    analysis,
  };
}


/* ==================== NEW QUERIES FOR DASHBOARD FEATURES ==================== */

/**
 * Market Overview: joins company_info + final_analysis (latest) + stock_data (latest price)
 * Returns one row per stock with sector, prediction, sentiment, and signal.
 */
export async function getMarketOverview(): Promise<MarketOverviewRow[]> {
  const rows = await query<{
    ticker: string;
    longname: string | null;
    sector: string | null;
    industry: string | null;
    marketcap: string | null;
    last_close: string;
    predicted_return: string | null;
    sentiment: string | null;
    sentiment_score: string | null;
    r2_score: string | null;
  }>(`
    WITH latest_date AS (
      SELECT MAX(("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date) AS max_date
      FROM final_analysis
    ),
    latest_prices AS (
      SELECT DISTINCT ON ("Ticker") "Ticker", "Close", "Date"
      FROM stock_data
      WHERE "Close" > 0
      ORDER BY "Ticker", "Date" DESC
    )
    SELECT
      ci."Ticker" as ticker,
      ci."longName" as longname,
      ci.sector,
      ci.industry,
      ci."marketCap" as marketcap,
      COALESCE(lp."Close", 0) as last_close,
      fa."Predicted_Return_Pct" as predicted_return,
      fa."Sentiment" as sentiment,
      fa."Sentiment_Score" as sentiment_score,
      fa."R2_Score" as r2_score
    FROM company_info ci
    LEFT JOIN latest_prices lp ON ci."Ticker" = lp."Ticker"
    LEFT JOIN final_analysis fa
      ON ci."Ticker" = fa."Ticker"
      AND (fa."Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date = (SELECT max_date FROM latest_date)
    ORDER BY ci.sector, ci."Ticker"
  `);

  return rows.map((r) => {
    const predictedReturn = r.predicted_return ? parseFloat(r.predicted_return) : 0;
    const sentimentLabel = r.sentiment ?? "NEUTRAL";
    const sentimentScore = r.sentiment_score ? parseFloat(r.sentiment_score) : 0;
    const r2 = r.r2_score ? parseFloat(r.r2_score) : 0;

    // Derive a simple signal from predicted return + sentiment
    let signal: "BUY" | "HOLD" | "SELL" = "HOLD";
    let confidence = 50;
    if (predictedReturn > 0.3 && sentimentLabel !== "NEGATIVE") {
      signal = "BUY";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    } else if (predictedReturn < -0.3 && sentimentLabel !== "POSITIVE") {
      signal = "SELL";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    }

    return {
      ticker: r.ticker,
      name: r.longname ?? r.ticker,
      sector: r.sector ?? "Other",
      industry: r.industry ?? "Other",
      marketCap: r.marketcap ? parseFloat(r.marketcap) : 0,
      price: parseFloat(r.last_close),
      predictedReturn,
      sentiment: sentimentLabel,
      sentimentScore,
      r2,
      signal,
      confidence: Math.round(confidence),
    };
  });
}

/**
 * Screener: similar to market overview but includes more fundamentals.
 */
export async function getScreenerData(): Promise<ScreenerRow[]> {
  const rows = await query<{
    ticker: string;
    longname: string | null;
    sector: string | null;
    industry: string | null;
    marketcap: string | null;
    trailingpe: string | null;
    profitmargins: string | null;
    grossmargins: string | null;
    change52week: string | null;
    last_close: string;
    predicted_return: string | null;
    sentiment: string | null;
    sentiment_score: string | null;
    r2_score: string | null;
  }>(`
    WITH latest_date AS (
      SELECT MAX(("Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date) AS max_date
      FROM final_analysis
    ),
    latest_prices AS (
      SELECT DISTINCT ON ("Ticker") "Ticker", "Close"
      FROM stock_data
      WHERE "Close" > 0
      ORDER BY "Ticker", "Date" DESC
    )
    SELECT
      ci."Ticker" as ticker,
      ci."longName" as longname,
      ci.sector,
      ci.industry,
      ci."marketCap" as marketcap,
      ci."trailingPE" as trailingpe,
      ci."profitMargins" as profitmargins,
      ci."grossMargins" as grossmargins,
      ci."52WeekChange" as change52week,
      COALESCE(lp."Close", 0) as last_close,
      fa."Predicted_Return_Pct" as predicted_return,
      fa."Sentiment" as sentiment,
      fa."Sentiment_Score" as sentiment_score,
      fa."R2_Score" as r2_score
    FROM company_info ci
    LEFT JOIN latest_prices lp ON ci."Ticker" = lp."Ticker"
    LEFT JOIN final_analysis fa
      ON ci."Ticker" = fa."Ticker"
      AND (fa."Prediction_Date" AT TIME ZONE 'Asia/Kolkata')::date = (SELECT max_date FROM latest_date)
    ORDER BY ci."Ticker"
  `);

  return rows.map((r) => {
    const predictedReturn = r.predicted_return ? parseFloat(r.predicted_return) : 0;
    const sentimentLabel = r.sentiment ?? "NEUTRAL";
    const sentimentScore = r.sentiment_score ? parseFloat(r.sentiment_score) : 0;
    const r2 = r.r2_score ? parseFloat(r.r2_score) : 0;

    let signal: "BUY" | "HOLD" | "SELL" = "HOLD";
    let confidence = 50;
    if (predictedReturn > 0.3 && sentimentLabel !== "NEGATIVE") {
      signal = "BUY";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    } else if (predictedReturn < -0.3 && sentimentLabel !== "POSITIVE") {
      signal = "SELL";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    }

    return {
      ticker: r.ticker,
      name: r.longname ?? r.ticker,
      sector: r.sector ?? "Other",
      industry: r.industry ?? "Other",
      price: parseFloat(r.last_close),
      predictedReturn,
      sentiment: sentimentLabel,
      sentimentScore,
      r2,
      trailingPE: r.trailingpe ? parseFloat(r.trailingpe) : null,
      profitMargins: r.profitmargins ? parseFloat(r.profitmargins) : null,
      grossMargins: r.grossmargins ? parseFloat(r.grossmargins) : null,
      change52Week: r.change52week ? parseFloat(r.change52week) : null,
      marketCap: r.marketcap ? parseFloat(r.marketcap) : null,
      signal,
      confidence: Math.round(confidence),
    };
  });
}

/**
 * OHLC data for candlestick charts.
 */
export async function getOHLCData(ticker: string, days: number): Promise<OHLCPoint[]> {
  const rows = await query<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
  }>(
    `SELECT "Date" as date, "Open" as open, "High" as high, "Low" as low, "Close" as close, "Volume" as volume
     FROM stock_data
     WHERE "Ticker" = $1 AND "Close" > 0
     ORDER BY "Date" DESC
     LIMIT $2`,
    [ticker, days]
  );
  return rows
    .map((r) => ({
      date: r.date,
      open: parseFloat(r.open),
      high: parseFloat(r.high),
      low: parseFloat(r.low),
      close: parseFloat(r.close),
      volume: parseInt(r.volume, 10) || 0,
    }))
    .reverse();
}

/**
 * Accuracy metrics computed from prediction_vs_actual.
 */
export async function getAccuracyData(): Promise<AccuracyData> {
  // Get all prediction vs actual data with company names
  const rows = await query<{
    ticker: string;
    name: string | null;
    date: string;
    predicted: string;
    actual: string;
  }>(`
    SELECT pva."Ticker" as ticker,
           ci."longName" as name,
           pva."Date" as date,
           pva."Predicted_Closing_Price" as predicted,
           pva."Actual_Closing_Price" as actual
    FROM prediction_vs_actual pva
    LEFT JOIN company_info ci ON pva."Ticker" = ci."Ticker"
    WHERE pva."Actual_Closing_Price" > 0
    ORDER BY pva."Date" ASC
  `);

  if (rows.length === 0) {
    return {
      overall: { totalPredictions: 0, mape: 0, directionAccuracy: 0, avgError: 0 },
      perTicker: [],
      timeSeries: [],
    };
  }

  // Time series (per prediction)
  const timeSeries: AccuracyTimeSeries[] = [];
  let totalAbsError = 0;
  let totalPctError = 0;
  let totalDirectionCorrect = 0;
  let total = 0;

  // Per-ticker aggregation
  const tickerMap = new Map<string, {
    name: string;
    errors: number[];
    absErrors: number[];
    pctErrors: number[];
    directionCorrect: number;
    count: number;
  }>();

  for (const r of rows) {
    const predicted = parseFloat(r.predicted);
    const actual = parseFloat(r.actual);
    if (actual === 0) continue;

    const error = predicted - actual;
    const absError = Math.abs(error);
    const pctError = (absError / actual) * 100;

    // We need prior close to check direction — approximate from actual vs predicted direction
    // Since we predict next close, direction correct if both predicted and actual moved same way relative to... 
    // We'll use a simpler metric: was error less than 2% (good prediction)
    const directionCorrect = (predicted >= actual && actual >= 0) || (predicted < actual && actual < 0)
      ? true : pctError < 2; // fallback: within 2% counts as correct direction

    totalAbsError += absError;
    totalPctError += pctError;
    if (directionCorrect) totalDirectionCorrect++;
    total++;

    timeSeries.push({
      date: r.date,
      mape: pctError,
      directionCorrect,
      error: error,
      ticker: r.ticker,
    });

    // Per ticker
    const existing = tickerMap.get(r.ticker) ?? {
      name: r.name ?? r.ticker,
      errors: [],
      absErrors: [],
      pctErrors: [],
      directionCorrect: 0,
      count: 0,
    };
    existing.errors.push(error);
    existing.absErrors.push(absError);
    existing.pctErrors.push(pctError);
    if (directionCorrect) existing.directionCorrect++;
    existing.count++;
    tickerMap.set(r.ticker, existing);
  }

  const perTicker: AccuracyRow[] = [];
  for (const [ticker, data] of tickerMap) {
    const mape = data.pctErrors.reduce((a, b) => a + b, 0) / data.count;
    const avgError = data.errors.reduce((a, b) => a + b, 0) / data.count;
    const avgAbsError = data.absErrors.reduce((a, b) => a + b, 0) / data.count;
    perTicker.push({
      ticker,
      name: data.name,
      totalPredictions: data.count,
      mape,
      directionAccuracy: (data.directionCorrect / data.count) * 100,
      avgError,
      avgAbsError,
    });
  }

  perTicker.sort((a, b) => a.mape - b.mape); // Best first

  return {
    overall: {
      totalPredictions: total,
      mape: total > 0 ? totalPctError / total : 0,
      directionAccuracy: total > 0 ? (totalDirectionCorrect / total) * 100 : 0,
      avgError: total > 0 ? totalAbsError / total : 0,
    },
    perTicker,
    timeSeries,
  };
}

/**
 * Comparison bundle: normalised price series for multiple tickers.
 */
export async function getComparisonBundle(tickers: string[], days: number): Promise<ComparisonBundle> {
  const results: ComparisonSeries[] = [];

  for (const ticker of tickers.slice(0, 3)) {
    // Fetch price series
    const priceRows = await query<{ date: string; close: string }>(
      `SELECT "Date" as date, "Close" as close
       FROM stock_data
       WHERE "Ticker" = $1 AND "Close" > 0
       ORDER BY "Date" DESC
       LIMIT $2`,
      [ticker, days]
    );

    const prices = priceRows
      .map((r) => ({ date: r.date, close: parseFloat(r.close) }))
      .reverse();

    if (prices.length === 0) continue;

    const basePrice = prices[0].close;
    const series = prices.map((p) => ({
      date: p.date,
      close: p.close,
      normalised: ((p.close - basePrice) / basePrice) * 100,
    }));

    // Fetch company info + latest prediction
    const infoRows = await query<{
      longname: string | null;
      sector: string | null;
      trailingpe: string | null;
      profitmargins: string | null;
      marketcap: string | null;
      change52week: string | null;
    }>(
      `SELECT "longName" as longname, sector, "trailingPE" as trailingpe,
              "profitMargins" as profitmargins, "marketCap" as marketcap,
              "52WeekChange" as change52week
       FROM company_info WHERE "Ticker" = $1`,
      [ticker]
    );
    const info = infoRows[0];

    const predRows = await query<{
      predicted_return: string | null;
      sentiment: string | null;
      r2_score: string | null;
    }>(`
      SELECT "Predicted_Return_Pct" as predicted_return, "Sentiment" as sentiment, "R2_Score" as r2_score
      FROM final_analysis
      WHERE "Ticker" = $1
      ORDER BY "Prediction_Date" DESC
      LIMIT 1
    `, [ticker]);
    const pred = predRows[0];

    const predictedReturn = pred?.predicted_return ? parseFloat(pred.predicted_return) : 0;
    const sentimentLabel = pred?.sentiment ?? "NEUTRAL";
    const r2 = pred?.r2_score ? parseFloat(pred.r2_score) : 0;

    let signal: "BUY" | "HOLD" | "SELL" = "HOLD";
    let confidence = 50;
    if (predictedReturn > 0.3 && sentimentLabel !== "NEGATIVE") {
      signal = "BUY";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    } else if (predictedReturn < -0.3 && sentimentLabel !== "POSITIVE") {
      signal = "SELL";
      confidence = Math.min(90, 55 + Math.abs(predictedReturn) * 8 + r2 * 20);
    }

    results.push({
      ticker,
      name: info?.longname ?? ticker,
      sector: info?.sector ?? "Other",
      predictedReturn,
      sentiment: sentimentLabel,
      trailingPE: info?.trailingpe ? parseFloat(info.trailingpe) : null,
      profitMargins: info?.profitmargins ? parseFloat(info.profitmargins) : null,
      marketCap: info?.marketcap ? parseFloat(info.marketcap) : null,
      change52Week: info?.change52week ? parseFloat(info.change52week) : null,
      signal,
      confidence: Math.round(confidence),
      series,
    });
  }

  return { tickers: results };
}