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
};
