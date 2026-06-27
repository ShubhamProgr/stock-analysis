from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
import numpy
from sqlalchemy import create_engine, text

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

table_name = "Company_Info"

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

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
    'TATACONSUM.NS', 'M&M.NS', 'HAL.NS', 'DLF.NS', 'LTIM.NS','ABB.NS', 'ADANIGREEN.NS', 'ADANIPOWER.NS', 'ADANIPORTS.NS', 'AMBUJACEM.NS',
    'BAJAJHLDNG.NS', 'BANKBARODA.NS', 'BPCL.NS', 'BOSCHLTD.NS', 'CANBK.NS',
    'ACC.NS', 'DMART.NS', 'BANDHANBNK.NS', 'BIOCON.NS', 'CHOLAFIN.NS',
    'COLPAL.NS', 'GAIL.NS', 'GODREJCP.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
    'INDHOTEL.NS', 'INDUSTOWER.NS', 'NAUKRI.NS', 'INDIGO.NS', 'LICI.NS',
    'MARICO.NS', 'MPHASIS.NS', 'MUTHOOTFIN.NS', 'PAYTM.NS', 'PIIND.NS',
    'PIDILITIND.NS', 'SBICARD.NS', 'SRF.NS', 'MOTHERSON.NS', 'SIEMENS.NS',
    'TATAPOWER.NS', 'TORNTPHARM.NS', 'MCDOWELL-N.NS', 'VEDL.NS', 'ZOMATO.NS',
    'PETRONET.NS', 'PGHH.NS', 'POLYCAB.NS', 'ICICISENSX.NS', 'HAVELLS.NS',
    'CONCOR.NS', 'IRCTC.NS', 'TRENT.NS', 'TVSMOTOR.NS', 'JUBLFOOD.NS'
]

fetch_fields = [
    "symbol", "longName", "sector", "industry", "fullTimeEmployees", "marketCap",
    "totalRevenue", "grossMargins", "operatingMargins", "profitMargins",
    "totalCash", "totalDebt", "52WeekChange",
    "sharesOutstanding", "floatShares", "trailingPE"
]

data = []
for ticker in tickers:
    print(f"Fetching data for: {ticker}")
    try:
        info = yf.Ticker(ticker).info
        row = {field: info.get(field, None) for field in fetch_fields}
        data.append(row)
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

df = pd.DataFrame(data)
df.rename(columns={'symbol': 'Ticker'}, inplace=True)
df.replace([numpy.nan, numpy.inf, -numpy.inf], None, inplace=True)

with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name.lower()} (
            "Ticker" TEXT PRIMARY KEY,
            "longName" TEXT,
            "sector" TEXT,
            "industry" TEXT,
            "fullTimeEmployees" INTEGER,
            "marketCap" BIGINT,
            "totalRevenue" BIGINT,
            "grossMargins" DOUBLE PRECISION,
            "operatingMargins" DOUBLE PRECISION,
            "profitMargins" DOUBLE PRECISION,
            "totalCash" BIGINT,
            "totalDebt" BIGINT,
            "52WeekChange" DOUBLE PRECISION,
            "sharesOutstanding" BIGINT,
            "floatShares" BIGINT,
            "trailingPE" DOUBLE PRECISION
        )
    """))

delete_query = text('DELETE FROM company_info WHERE "Ticker" = :ticker')
insert_query = text("""
INSERT INTO company_info (
    "Ticker", "longName", "sector", "industry", "fullTimeEmployees", "marketCap",
    "totalRevenue", "grossMargins", "operatingMargins", "profitMargins",
    "totalCash", "totalDebt", "52WeekChange",
    "sharesOutstanding", "floatShares", "trailingPE"
) VALUES (
    :ticker, :long_name, :sector, :industry, :full_time_employees, :market_cap,
    :total_revenue, :gross_margins, :operating_margins, :profit_margins,
    :total_cash, :total_debt, :week52_change,
    :shares_outstanding, :float_shares, :trailing_pe
)
ON CONFLICT ("Ticker") DO UPDATE SET
    "longName" = EXCLUDED."longName",
    "sector" = EXCLUDED."sector",
    "industry" = EXCLUDED."industry",
    "fullTimeEmployees" = EXCLUDED."fullTimeEmployees",
    "marketCap" = EXCLUDED."marketCap",
    "totalRevenue" = EXCLUDED."totalRevenue",
    "grossMargins" = EXCLUDED."grossMargins",
    "operatingMargins" = EXCLUDED."operatingMargins",
    "profitMargins" = EXCLUDED."profitMargins",
    "totalCash" = EXCLUDED."totalCash",
    "totalDebt" = EXCLUDED."totalDebt",
    "52WeekChange" = EXCLUDED."52WeekChange",
    "sharesOutstanding" = EXCLUDED."sharesOutstanding",
    "floatShares" = EXCLUDED."floatShares",
    "trailingPE" = EXCLUDED."trailingPE"
""")

with engine.begin() as conn:
    for _, row in df.iterrows():
        ticker = row['Ticker']
        if pd.isna(ticker):
            print("Skipping row with NULL Ticker")
            continue

        conn.execute(delete_query, {"ticker": ticker})
        conn.execute(insert_query, {
            "ticker": row['Ticker'],
            "long_name": row['longName'] if pd.notna(row['longName']) else None,
            "sector": row['sector'] if pd.notna(row['sector']) else None,
            "industry": row['industry'] if pd.notna(row['industry']) else None,
            "full_time_employees": int(row['fullTimeEmployees']) if pd.notna(row['fullTimeEmployees']) else None,
            "market_cap": int(row['marketCap']) if pd.notna(row['marketCap']) else None,
            "total_revenue": int(row['totalRevenue']) if pd.notna(row['totalRevenue']) else None,
            "gross_margins": float(row['grossMargins']) if pd.notna(row['grossMargins']) else None,
            "operating_margins": float(row['operatingMargins']) if pd.notna(row['operatingMargins']) else None,
            "profit_margins": float(row['profitMargins']) if pd.notna(row['profitMargins']) else None,
            "total_cash": int(row['totalCash']) if pd.notna(row['totalCash']) else None,
            "total_debt": int(row['totalDebt']) if pd.notna(row['totalDebt']) else None,
            "week52_change": float(row['52WeekChange']) if pd.notna(row['52WeekChange']) else None,
            "shares_outstanding": int(row['sharesOutstanding']) if pd.notna(row['sharesOutstanding']) else None,
            "float_shares": int(row['floatShares']) if pd.notna(row['floatShares']) else None,
            "trailing_pe": float(row['trailingPE']) if pd.notna(row['trailingPE']) else None,
        })

print("Data inserted into Supabase Postgres successfully.")
