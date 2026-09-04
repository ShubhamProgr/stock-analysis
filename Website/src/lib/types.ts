export type PricePoint = {
  date: string;
  close: number;
  volume: number;
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
