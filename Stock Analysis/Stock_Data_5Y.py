import yfinance as yf
import pandas as pd
import pyodbc
import os

# ✅ List of stock tickers
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


# ✅ Database connection string (uses raw string for backslashes)
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)

# ✅ Connect to SQL Server
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ✅ Create table if not exists (escape reserved keywords)
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

# ✅ Fetch and insert stock data
all_data = pd.DataFrame()

for ticker in tickers:
    print(f"📥 Downloading data for {ticker}")
    try:
        data = yf.download(ticker,period='5Y')
        data.reset_index(inplace=True)  # Convert index to column
        data['Ticker'] = ticker  # Add Ticker column
        data.to_excel('all1.xlsx')
        data = pd.read_excel('all1.xlsx', skiprows=3)
        data.columns = ['Index', 'Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'Ticker']
        data.to_excel('all.xlsx')

        # Conversion to appropriate Data Types
        data['Open'] = pd.to_numeric(data['Open'], errors='coerce').fillna(0)
        data['High'] = pd.to_numeric(data['High'], errors='coerce').fillna(0)
        data['Low'] = pd.to_numeric(data['Low'], errors='coerce').fillna(0)
        data['Close'] = pd.to_numeric(data['Close'], errors='coerce').fillna(0)
        data['Index'] = pd.to_numeric(data['Index'], errors='coerce').fillna(0)
        data['Volume'] = pd.to_numeric(data['Volume'], errors='coerce').fillna(0).astype('int64')

        for i, row in data.iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (
                        SELECT 1 FROM StockData WHERE Ticker = ? AND [Date] = ?
                    )
                    INSERT INTO StockData (Ticker, [Date], [Open], [High], [Low], [Close], [Index], [Volume])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['Ticker'],
                    row['Date'],
                    row['Ticker'],
                    row['Date'],
                    (row['Open']),
                    (row['High']),
                    (row['Low']),
                    (row['Close']),
                    (row['Index']),
                    int(row['Volume'])
                ))
            except Exception as e:
                print(f"❌ Error inserting row for {ticker} at index {i}: {e}")

        conn.commit()
        all_data = pd.concat([all_data, data], ignore_index=True)
        print(f"✅ Inserted data for {ticker}")

    except Exception as e:
        print(f"❌ Failed to fetch data for {ticker}: {e}")

cursor.close()
conn.close()

# ✅ Clean column headers
if isinstance(all_data.columns, pd.MultiIndex):
    all_data.columns = [' '.join(col).strip() for col in all_data.columns.values]

all_data.reset_index(drop=True, inplace=True)

# ✅ Save to Excel in Documents folder
excel_path = os.path.join(os.environ['USERPROFILE'], 'Documents', 'Stock_Data.xlsx')
try:
    all_data.to_excel(excel_path, index=False)
    print(f"📁 Excel file saved at: {excel_path}")
except Exception as e:
    print(f"❌ Excel saving error: {e}") 