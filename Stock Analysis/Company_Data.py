from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
import pyodbc
import numpy

load_dotenv()

MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

table_name = "Company_Info"

conn_str = (
    f"DRIVER={{{MSSQL_DRIVER}}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

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

cursor.execute(f"""
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'
)
BEGIN
    CREATE TABLE {table_name} (
        [Ticker] NVARCHAR(50) PRIMARY KEY,
        [longName] NVARCHAR(255),
        [sector] NVARCHAR(100),
        [industry] NVARCHAR(100),
        [fullTimeEmployees] INT,
        [marketCap] BIGINT,
        [totalRevenue] BIGINT,
        [grossMargins] FLOAT,
        [operatingMargins] FLOAT,
        [profitMargins] FLOAT,
        [totalCash] BIGINT,
        [totalDebt] BIGINT,
        [52WeekChange] FLOAT,
        [sharesOutstanding] BIGINT,
        [floatShares] BIGINT,
        [trailingPE] FLOAT
    )
END
""")
conn.commit()

delete_query = f"DELETE FROM {table_name} WHERE [Ticker] = ?"
insert_query = f"""
INSERT INTO {table_name} (
    [Ticker], [longName], [sector], [industry], [fullTimeEmployees], [marketCap],
    [totalRevenue], [grossMargins], [operatingMargins], [profitMargins],
    [totalCash], [totalDebt], [52WeekChange],
    [sharesOutstanding], [floatShares], [trailingPE]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    ticker = row['Ticker']
    
    # Skip rows with NULL Ticker
    if pd.isna(ticker):
        print(f"⚠️ Skipping row with NULL Ticker")
        continue
    
    cursor.execute(delete_query, ticker)
    
    values = (
        row['Ticker'],
        row['longName'] if pd.notna(row['longName']) else None,
        row['sector'] if pd.notna(row['sector']) else None,
        row['industry'] if pd.notna(row['industry']) else None,
        int(row['fullTimeEmployees']) if pd.notna(row['fullTimeEmployees']) else None,
        int(row['marketCap']) if pd.notna(row['marketCap']) else None,
        int(row['totalRevenue']) if pd.notna(row['totalRevenue']) else None,
        float(row['grossMargins']) if pd.notna(row['grossMargins']) else None,
        float(row['operatingMargins']) if pd.notna(row['operatingMargins']) else None,
        float(row['profitMargins']) if pd.notna(row['profitMargins']) else None,
        int(row['totalCash']) if pd.notna(row['totalCash']) else None,
        int(row['totalDebt']) if pd.notna(row['totalDebt']) else None,
        float(row['52WeekChange']) if pd.notna(row['52WeekChange']) else None,
        int(row['sharesOutstanding']) if pd.notna(row['sharesOutstanding']) else None,
        int(row['floatShares']) if pd.notna(row['floatShares']) else None,
        float(row['trailingPE']) if pd.notna(row['trailingPE']) else None
    )
    cursor.execute(insert_query, *values)

conn.commit()
cursor.close()
conn.close()
print("✅ Data inserted into SQL Server successfully.")
