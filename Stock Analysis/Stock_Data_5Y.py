from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import time

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    supabase_host = os.getenv("SUPABASE_DB_HOST")
    supabase_port = os.getenv("SUPABASE_DB_PORT", "5432")
    supabase_name = os.getenv("SUPABASE_DB_NAME", "postgres")
    supabase_user = os.getenv("SUPABASE_DB_USER", "postgres")
    supabase_password = os.getenv("SUPABASE_DB_PASSWORD")
    supabase_sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")
    if not all([supabase_host, supabase_password]):
        raise RuntimeError("Set DATABASE_URL or SUPABASE_DB_* environment variables")
    DATABASE_URL = (
        f"postgresql+psycopg2://{supabase_user}:{supabase_password}"
        f"@{supabase_host}:{supabase_port}/{supabase_name}?sslmode={supabase_sslmode}"
    )

excel_output_path = os.getenv("STOCK_DATA", os.path.join(os.environ.get("USERPROFILE",""), "Documents", "Stock_Data.xlsx"))

tickers = [
    'RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS',
    'KOTAKBANK.NS', 'HCLTECH.NS', 'LT.NS', 'ITC.NS', 'SBIN.NS',
    'BHARTIARTL.NS', 'ASIANPAINT.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'HINDUNILVR.NS',
    'MARUTI.NS', 'NESTLEIND.NS', 'NTPC.NS', 'ONGC.NS', 'POWERGRID.NS',
    'TITAN.NS', 'ULTRACEMCO.NS', 'WIPRO.NS', 'TECHM.NS', 'SUNPHARMA.NS',
    'ADANIENT.NS', 'DIVISLAB.NS', 'EICHERMOT.NS', 'APOLLOHOSP.NS', 'GRASIM.NS',
    'JSWSTEEL.NS', 'TATASTEEL.NS', 'DRREDDY.NS', 'HEROMOTOCO.NS', 'CIPLA.NS',
    'COALINDIA.NS', 'HDFCLIFE.NS', 'HINDALCO.NS', 'INDUSINDBK.NS', 'BAJAJ-AUTO.NS',
    'BRITANNIA.NS', 'SBILIFE.NS', 'UPL.NS', 'AXISBANK.NS', 'SHREECEM.NS',
    'TATACONSUM.NS', 'M&M.NS', 'HAL.NS', 'DLF.NS', 'LTIM.NS','ABB.NS', 'ADANIGREEN.NS',
    'ADANIPOWER.NS', 'ADANIPORTS.NS', 'AMBUJACEM.NS', 'BAJAJHLDNG.NS', 'BANKBARODA.NS',
    'BPCL.NS', 'BOSCHLTD.NS', 'CANBK.NS', 'ACC.NS', 'DMART.NS', 'BANDHANBNK.NS', 'BIOCON.NS',
    'CHOLAFIN.NS', 'COLPAL.NS', 'GAIL.NS', 'GODREJCP.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
    'INDHOTEL.NS', 'INDUSTOWER.NS', 'NAUKRI.NS', 'INDIGO.NS', 'LICI.NS', 'MARICO.NS',
    'MPHASIS.NS', 'MUTHOOTFIN.NS', 'PAYTM.NS', 'PIIND.NS', 'PIDILITIND.NS', 'SBICARD.NS',
    'SRF.NS', 'MOTHERSON.NS', 'SIEMENS.NS', 'TATAPOWER.NS', 'TORNTPHARM.NS', 'MCDOWELL-N.NS',
    'VEDL.NS', 'ZOMATO.NS', 'PETRONET.NS', 'PGHH.NS', 'POLYCAB.NS', 'ICICISENSX.NS', 'HAVELLS.NS',
    'CONCOR.NS', 'IRCTC.NS', 'TRENT.NS', 'TVSMOTOR.NS', 'JUBLFOOD.NS'
]

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_data (
            "Ticker" TEXT NOT NULL,
            "Date" DATE NOT NULL,
            "Open" DOUBLE PRECISION,
            "High" DOUBLE PRECISION,
            "Low" DOUBLE PRECISION,
            "Close" DOUBLE PRECISION,
            "Index" DOUBLE PRECISION,
            "Volume" BIGINT,
            PRIMARY KEY ("Ticker", "Date")
        )
    """))

def download_with_retry(ticker, period='30d', max_retries=3, initial_wait=2):
    """
    Download stock data with exponential backoff retry logic.
    
    Args:
        ticker: Stock ticker symbol
        period: Time period for download (default: '3d')
        max_retries: Maximum number of retry attempts (default: 3)
        initial_wait: Initial wait time in seconds (default: 2)
    
    Returns:
        DataFrame with stock data or None if all retries failed
    """
    for attempt in range(max_retries):
        try:
            data = yf.download(ticker, period=period, auto_adjust=False)
            return data
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = initial_wait * (2 ** attempt)  # Exponential backoff
                print(f"Retry {attempt + 1}/{max_retries - 1} for {ticker} after {wait_time}s (Error: {str(e)[:60]}...)")
                time.sleep(wait_time)
            else:
                print(f" Failed to fetch data for {ticker} after {max_retries} attempts: {e}")
                return None


all_data = pd.DataFrame()

for ticker in tickers:
    print(f"Downloading 10 Year stock data for {ticker}")
    data = download_with_retry(ticker, period='10y')
    
    if data is None or data.empty:
        print(f"No data for {ticker}")
        continue
    
    try:
        if data is None:
            print(f"No data retrieved for {ticker}")
            continue
            
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data.reset_index()
        data['Ticker'] = ticker
        data['Date'] = pd.to_datetime(data['Date']).dt.date

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

        with engine.begin() as conn:
            for _, row in data.iterrows():
                try:
                    conn.execute(text("""
                        INSERT INTO stock_data ("Ticker", "Date", "Open", "High", "Low", "Close", "Index", "Volume")
                        VALUES (:ticker, :date, :open, :high, :low, :close, :index, :volume)
                        ON CONFLICT ("Ticker", "Date") DO UPDATE SET
                            "Open" = EXCLUDED."Open",
                            "High" = EXCLUDED."High",
                            "Low" = EXCLUDED."Low",
                            "Close" = EXCLUDED."Close",
                            "Index" = EXCLUDED."Index",
                            "Volume" = EXCLUDED."Volume"
                    """), {
                        "ticker": row['Ticker'],
                        "date": row['Date'],
                        "open": float(row.get('Open', 0.0)),
                        "high": float(row.get('High', 0.0)),
                        "low": float(row.get('Low', 0.0)),
                        "close": float(row.get('Close', 0.0)),
                        "index": 0.0,
                        "volume": int(row.get('Volume', 0)),
                    })
                except Exception as e:
                    print(f" Error inserting {ticker} {row.get('Date')}: {e}")

        all_data = pd.concat([all_data, data], ignore_index=True)
        print(f" Inserted data for {ticker}")

    except Exception as e:
        print(f" Error processing {ticker}: {e}")

if isinstance(all_data.columns, pd.MultiIndex):
    all_data.columns = [' '.join(col).strip() for col in all_data.columns.values]

all_data.reset_index(drop=True, inplace=True)

try:
    all_data.to_excel(excel_output_path, index=False)
    print(f" Excel file saved at: {excel_output_path}")
except Exception as e:
    print(f" Excel saving error: {e}")