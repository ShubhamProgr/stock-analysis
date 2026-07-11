export const PIPELINE_JOBS = [
  "stock_data_daily",
  "stock_data_5y",
  "company_info",
  "news_extractor",
  "sentiment_analyzer",
  "final_analysis",
  "prediction_vs_actual"
] as const;

export type PipelineJob = (typeof PIPELINE_JOBS)[number];