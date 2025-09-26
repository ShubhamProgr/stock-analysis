import yfinance as yf
import pandas as pd
import pyodbc
import numpy

# ------------------------------
# SQL Server connection setup
# ------------------------------
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)
table_name = "Company_Info"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ------------------------------
# List of tickers
# ------------------------------
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
    
]    # more tickers can be added as needed

# ------------------------------
# Fields to fetch from Yahoo Finance
# ------------------------------
fetch_fields = [
    "symbol", "longName", "sector", "industry", "fullTimeEmployees", "marketCap",
    "totalRevenue", "grossMargins", "operatingMargins", "profitMargins",   
    "totalCash", "totalDebt", "52WeekChange", 
    "sharesOutstanding", "floatShares", "trailingPE"
]

# ------------------------------
# Fetch company info
# ------------------------------
data = []
for ticker in tickers:
    print(f"Fetching data for: {ticker}")
    try:
        info = yf.Ticker(ticker).info
        row = {field: info.get(field, None) for field in fetch_fields}
        data.append(row)
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

# ------------------------------
# Convert to DataFrame and rename column
# ------------------------------
df = pd.DataFrame(data)
df.rename(columns={'symbol': 'Ticker'}, inplace=True)
df.replace([numpy.nan, numpy.inf, -numpy.inf], None, inplace=True)

# ------------------------------
# Create SQL table if it does not exist
# ------------------------------
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

# ------------------------------
# Clear existing table data (optional)
# ------------------------------
cursor.execute(f"DELETE FROM {table_name}")
conn.commit()

# ------------------------------
# Fields for SQL insertion (matches DataFrame)
# ------------------------------
insert_fields = [
    "Ticker", "longName", "sector", "industry", "fullTimeEmployees",
    "marketCap", "totalRevenue", "grossMargins", "operatingMargins", "profitMargins",  # ⚡ Added
    "totalCash", "totalDebt", "52WeekChange",
    "sharesOutstanding", "floatShares", "trailingPE"
]

# ------------------------------
# Insert data into SQL Server
# ------------------------------
insert_query = f"""
IF NOT EXISTS (SELECT 1 FROM {table_name} WHERE Ticker = ?)
BEGIN
    INSERT INTO {table_name} (
        [Ticker], [longName], [sector], [industry], [fullTimeEmployees], [marketCap],
        [totalRevenue], [grossMargins], [operatingMargins], [profitMargins],           -- ⚡ Added
        [totalCash], [totalDebt], [52WeekChange],
        [sharesOutstanding], [floatShares], [trailingPE]
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
END
"""

for _, row in df.iterrows():
    values = tuple(row[field] for field in insert_fields)
    cursor.execute(insert_query, values[0], *values)

# ------------------------------
# Commit and close
# ------------------------------
conn.commit()
cursor.close()
conn.close()
print("✅ Data inserted into SQL Server successfully.")
