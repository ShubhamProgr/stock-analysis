from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
import pyodbc
import numpy

load_dotenv()

MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

table_name = "Company_Info"

conn_str = (
    f"DRIVER={{{MSSQL_DRIVER}}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
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

cursor.execute(f"DELETE FROM {table_name}")
conn.commit()

insert_query = f"""
INSERT INTO {table_name} (
    [Ticker], [longName], [sector], [industry], [fullTimeEmployees], [marketCap],
    [totalRevenue], [grossMargins], [operatingMargins], [profitMargins],
    [totalCash], [totalDebt], [52WeekChange],
    [sharesOutstanding], [floatShares], [trailingPE]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    values = tuple(row[field] for field in [
        "Ticker", "longName", "sector", "industry", "fullTimeEmployees",
        "marketCap", "totalRevenue", "grossMargins", "operatingMargins", "profitMargins",
        "totalCash", "totalDebt", "52WeekChange",
        "sharesOutstanding", "floatShares", "trailingPE"
    ])
    cursor.execute(insert_query, *values)

conn.commit()
cursor.close()
conn.close()
print("✅ Data inserted into SQL Server successfully.")
