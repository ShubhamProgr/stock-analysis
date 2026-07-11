// Single source of truth for tracked tickers + display names on the frontend.
// Keep this in sync with the ticker_to_company map in Final_Analysis.py and
// Sentiment_Analyzer.py on the pipeline side.
export const TICKER_COMPANY_MAP: Record<string, string> = {
  "RELIANCE.NS": "Reliance",
  "TCS.NS": "TCS",
  "INFY.NS": "Infosys",
  "HDFCBANK.NS": "HDFC Bank",
  "ICICIBANK.NS": "ICICI Bank",
  "KOTAKBANK.NS": "Kotak Bank",
  "HCLTECH.NS": "HCL Technologies",
  "LT.NS": "L&T",
  "ITC.NS": "ITC",
  "SBIN.NS": "SBI",
  "BHARTIARTL.NS": "Bharti Airtel",
  "ASIANPAINT.NS": "Asian Paints",
  "BAJFINANCE.NS": "Bajaj Finance",
  "BAJAJFINSV.NS": "Bajaj Finserv",
  "HINDUNILVR.NS": "Hindustan Unilever",
  "MARUTI.NS": "Maruti Suzuki",
  "NESTLEIND.NS": "Nestle India",
  "NTPC.NS": "NTPC",
  "ONGC.NS": "ONGC",
  "POWERGRID.NS": "Power Grid",
  "TITAN.NS": "Titan",
  "ULTRACEMCO.NS": "UltraTech Cement",
  "WIPRO.NS": "Wipro",
  "TECHM.NS": "Tech Mahindra",
  "SUNPHARMA.NS": "Sun Pharma",
  "ADANIENT.NS": "Adani Enterprises",
  "DIVISLAB.NS": "Divi's Labs",
  "EICHERMOT.NS": "Eicher Motors",
  "APOLLOHOSP.NS": "Apollo Hospitals",
  "GRASIM.NS": "Grasim",
  "JSWSTEEL.NS": "JSW Steel",
  "TATASTEEL.NS": "Tata Steel",
  "DRREDDY.NS": "Dr Reddy's",
  "HEROMOTOCO.NS": "Hero MotoCorp",
  "CIPLA.NS": "Cipla",
  "COALINDIA.NS": "Coal India",
  "HDFCLIFE.NS": "HDFC Life",
  "HINDALCO.NS": "Hindalco",
  "INDUSINDBK.NS": "IndusInd Bank",
  "BAJAJ-AUTO.NS": "Bajaj Auto",
  "BRITANNIA.NS": "Britannia",
  "SBILIFE.NS": "SBI Life",
  "UPL.NS": "UPL",
  "AXISBANK.NS": "Axis Bank",
  "SHREECEM.NS": "Shree Cement",
  "TATACONSUM.NS": "Tata Consumer",
  "M&M.NS": "Mahindra & Mahindra",
  "HAL.NS": "HAL",
  "DLF.NS": "DLF",
  "LTIM.NS": "LTIMindtree"
  // ...extend with the remaining tickers from Company_Data.py as needed
};

export function companyNameToTicker(name: string): string | null {
  const normalized = name.trim().toLowerCase();
  const match = Object.entries(TICKER_COMPANY_MAP).find(
    ([, company]) => company.toLowerCase() === normalized
  );
  return match ? match[0] : null;
}

export function tickerLabel(ticker: string): string {
  return TICKER_COMPANY_MAP[ticker] ?? ticker;
}

export const ALL_TICKERS = Object.keys(TICKER_COMPANY_MAP);
