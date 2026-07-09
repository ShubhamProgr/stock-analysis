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

# 1. Ensure the production table uses clean lowercase names matching your earlier logs
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_data (
            ticker TEXT NOT NULL,
            date DATE NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            index DOUBLE PRECISION DEFAULT 0.0,
            volume BIGINT,
            PRIMARY KEY (ticker, date)
        )
    """))

def download_with_retry(ticker, period='10y', max_retries=3, initial_wait=2):
    for attempt in range(max_retries):
        try:
            time.sleep(5) 
            data = yf.download(ticker, period=period, auto_adjust=False, progress=False)
            if data is None or data.empty:
                raise ValueError("Empty data returned. Likely rate-limited.")
                
            return data
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = initial_wait * (2 ** attempt)
                print(f"Retry {attempt + 1} for {ticker} after {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch data for {ticker}: {e}")
                return None

# Collect all data into a local list first
all_data_list = []

print(f"Starting data download for {len(tickers)} tickers...")
for ticker in tickers:
    data = download_with_retry(ticker, period='10y')
    
    if data is None or data.empty:
        continue
    
    # Flatten multi-index columns if present
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
        
    data = data.reset_index()
    data['ticker'] = ticker
    
    # Map raw headers to match lowercase database columns exactly
    data = data.rename(columns={
        'Date': 'date',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume'
    })
    
    # Filter only the columns we actually want to keep
    keep_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
    data = data[[col for col in keep_cols if col in data.columns]]
    data['index'] = 0.0
    
    all_data_list.append(data)
    print(f"Downloaded {ticker}")

# 2. Bulk process everything in memory and upsert in batches
if all_data_list:
    final_df = pd.concat(all_data_list, ignore_index=True)
    
    # Sanitize data types
    final_df['date'] = pd.to_datetime(final_df['date']).dt.date
    for col in ['open', 'high', 'low', 'close']:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0.0)
    final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce').fillna(0).astype(int)

    print(f"Total rows to sync: {len(final_df)}. Performing bulk upsert...")

    # Upload using a staging table block to handle ON CONFLICT instantly
    with engine.begin() as conn:
        # Create an exact temp table duplicate
        conn.execute(text("CREATE TEMP TABLE temp_stock_data (LIKE stock_data INCLUDING DEFAULTS) ON COMMIT DROP;"))
        
        # Stream the data in bulk via pandas to the temporary staging table
        final_df.to_sql('temp_stock_data', con=conn, if_exists='append', index=False, method='multi', chunksize=10000)
        
        # Use Postgres high-performance internal upsert from staging to production
        conn.execute(text("""
            INSERT INTO stock_data (ticker, date, open, high, low, close, index, volume)
            SELECT ticker, date, open, high, low, close, index, volume FROM temp_stock_data
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                index = EXCLUDED.index,
                volume = EXCLUDED.volume;
        """))
        
    print("Database sync completed successfully.")
else:
    print("No data collected to upload.")