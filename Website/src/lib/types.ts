export type PricePoint = {
  date: string;
  close: number;
  volume: number;
};

export type DailySentimentRow = {
  date: string; // YYYY-MM-DD
  sentiment: string;
  score: number;
  articleCount: number;
};

export type SentimentHistoryRow = {
  date: string;
  sentiment: string;
  score: number; // 0-100
};

export type SentimentPoint = {
  date: string;
  sentiment: string;
  score: number; // 0-100
};

export type WatchlistRow = {
  ticker: string;
  name: string;
  price: number;
  changePct: number;
  spark: number[];
};

export type CompanyInfo = {
  ticker: string;
  longName: string | null;
  sector: string | null;
  industry: string | null;
  marketCap: number | null;
  trailingPE: number | null;
  profitMargins: number | null;
  change52Week: number | null;
  totalRevenue: number | null;
  grossMargins: number | null;
  operatingMargins: number | null;
  totalCash: number | null;
  totalDebt: number | null;
  fullTimeEmployees: number | null;
  sharesOutstanding: number | null;
  floatShares: number | null;
};

/** Computed financial health score for a company */
export type HealthScore = {
  overall: number;       // 0–100
  grade: string;         // A+ to F
  marginQuality: number; // 0–100 sub-score
  leverage: number;      // 0–100 sub-score
  momentum: number;      // 0–100 sub-score
  valuation: number;     // 0–100 sub-score
};

/** Peer comparison row for sector-level benchmarking */
export type PeerComparisonRow = {
  ticker: string;
  name: string;
  industry: string;
  marketCap: number | null;
  totalRevenue: number | null;
  profitMargins: number | null;
  grossMargins: number | null;
  operatingMargins: number | null;
  trailingPE: number | null;
  change52Week: number | null;
  totalCash: number | null;
  totalDebt: number | null;
  isCurrent: boolean;
};

export type CompanySentiment = {
  company: string;
  ticker: string;
  articleCount: number;
  sentiment: string;
  score: number;
  excerpt: string;
};

export type NewsItem = {
  source: string;
  publicationDate: string;
  headline: string;
  link: string;
};

export type StrategySignal = {
  name: string;
  horizon: string;
  signal: "BUY" | "HOLD" | "SELL";
  confidence: number;
  rationale: string;
};

export type PredictionPoint = {
  date: string;
  predicted: number;
  actual: number;
};

export type TickerBundle = {
  ticker: string;
  name: string;
  price: number;
  changePct: number;
  volume: number;
  avgVolume30d: number;
  series: PricePoint[];
  sentimentSeries: SentimentPoint[];
  companyInfo: CompanyInfo | null;
  companySentiment: CompanySentiment | null;
  news: NewsItem[];
  strategies: StrategySignal[];
  predictionHistory: PredictionPoint[];
  analysis: {
    predictionDate: string;
    predictedClose: number;
    lastClose: number;
    r2: number;
    sentiment: string;
    sentimentScore: number;
    modelType: string | null;
    cvRmse: number | null;
    topFeatures: Record<string, number> | null;
  } | null;
  healthScore: HealthScore | null;
};

export interface PredictionData {
  Company: string;
  Ticker: string;
  Prediction_Date: string;
  Predicted_Closing_Price: number;
  Predicted_Return_Pct: number | null;  // <-- Add this new line
  Last_Close: number;
  Last_Close_Date: string;
  MAE: number;
  MSE: number;
  RMSE: number;
  R2_Score: number;
  Sentiment: string;
  Sentiment_Score: number;
  Model_Type?: string;
  CV_RMSE?: number;
  Top_Features?: string;
}

/* ==================== New types for dashboard features ==================== */

/** Market Overview — one row per stock, grouped by sector for the heatmap */
export type MarketOverviewRow = {
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  marketCap: number;
  price: number;
  predictedReturn: number;
  sentiment: string;
  sentimentScore: number;
  r2: number;
  signal: "BUY" | "HOLD" | "SELL";
  confidence: number;
};

export type TopMoverRow = {
  ticker: string;
  name: string;
  sector: string;
  value: number; // Represents the % change, or volume multiple
  type: "daily" | "predicted" | "volume";
};

export type TopMoversData = {
  dailyGainers: TopMoverRow[];
  dailyLosers: TopMoverRow[];
  predictedGainers: TopMoverRow[];
  predictedLosers: TopMoverRow[];
  volumeSpikes: TopMoverRow[];
};

/** Screener — enriched row with fundamentals + prediction + signal */
export type ScreenerRow = {
  ticker: string;
  name: string;
  sector: string;
  industry: string;
  price: number;
  predictedReturn: number;
  sentiment: string;
  sentimentScore: number;
  r2: number;
  trailingPE: number | null;
  profitMargins: number | null;
  grossMargins: number | null;
  change52Week: number | null;
  marketCap: number | null;
  signal: "BUY" | "HOLD" | "SELL";
  confidence: number;
};

/** OHLC data point for candlestick charts */
export type OHLCPoint = {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

/** Accuracy metrics — per-ticker and aggregate */
export type AccuracyRow = {
  ticker: string;
  name: string;
  totalPredictions: number;
  mape: number;
  directionAccuracy: number;
  avgError: number;
  avgAbsError: number;
  rmse: number;
  r2: number;
};

export type AccuracyTimeSeries = {
  date: string;
  mape: number;
  directionCorrect: boolean;
  error: number;
  ticker: string;
};

export type AccuracyData = {
  overall: {
    totalPredictions: number;
    mape: number;
    directionAccuracy: number;
    avgError: number;
    rmse?: number;
  };
  perTicker: AccuracyRow[];
  timeSeries: AccuracyTimeSeries[];
};

/** Comparison — normalised price series for multiple tickers */
export type ComparisonSeries = {
  ticker: string;
  name: string;
  sector: string;
  predictedReturn: number;
  sentiment: string;
  trailingPE: number | null;
  profitMargins: number | null;
  marketCap: number | null;
  change52Week: number | null;
  signal: "BUY" | "HOLD" | "SELL";
  confidence: number;
  series: { date: string; close: number; normalised: number }[];
};

export type ComparisonBundle = {
  tickers: ComparisonSeries[];
};

/** Active view in the dashboard */
export type DashboardView = "stock" | "market" | "screener" | "accuracy" | "compare" | "strategy";
