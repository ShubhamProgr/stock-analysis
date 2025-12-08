from dotenv import load_dotenv
import os
import yfinance as yf
import pandas as pd
import pyodbc

load_dotenv()

MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

excel_output_path = os.getenv("STOCK_DATA")

conn_str = (
    f"DRIVER={{{MSSQL_DRIVER}}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
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

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='StockData' AND xtype='U')
    CREATE TABLE StockData (
        [Ticker] NVARCHAR(50),
        [Date] DATE,
        [Open] FLOAT,
        [High] FLOAT,
        [Low] FLOAT,
        [Close] FLOAT,
        [Index] FLOAT,
        [Volume] BIGINT
    )
""")
conn.commit()

all_data = pd.DataFrame()

for ticker in tickers:
    print(f"📥 Downloading 5-year data for {ticker}")

    try:
        data = yf.download(ticker, period="5y", auto_adjust=False)

        if data.empty:
            print(f"⚠️ No data for {ticker}")
            continue

        data.reset_index(inplace=True)
        data["Ticker"] = ticker
        data.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in numeric_cols:
            data[col] = pd.to_numeric(data[col], errors="coerce").fillna(0)

        for _, row in data.iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (
                        SELECT 1 FROM StockData WHERE Ticker = ? AND [Date] = ?
                    )
                    INSERT INTO StockData (Ticker, [Date], [Open], [High], [Low], [Close], [Index], [Volume])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row["Ticker"], row["Date"],  
                row["Ticker"], row["Date"], 
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
                float(0),  
                int(row["Volume"])
                )
            except Exception as e:
                print(f"❌ Insert error for {ticker}: {e}")

        conn.commit()
        all_data = pd.concat([all_data, data], ignore_index=True)
        print(f"✅ Data inserted for {ticker}")

    except Exception as e:
        print(f"❌ Failed to fetch {ticker}: {e}")

cursor.close()
conn.close()

try:
    all_data.to_excel(excel_output_path, index=False)
    print(f"📁 Excel saved: {excel_output_path}")
except Exception as e:
    print(f"❌ Excel save error: {e}")
