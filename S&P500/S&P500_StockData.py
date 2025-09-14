import yfinance as yf
import pandas as pd
import pyodbc
import os
import requests

# ✅ Get live S&P 500 tickers from Wikipedia
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/138.0.7204.185 Safari/537.36"
}

response = requests.get(sp500_url, headers=headers)
response.raise_for_status()  # raises error if request fails
sp500_table = pd.read_html(response.text)[0]
tickers = sp500_table['Symbol'].str.replace('.', '-', regex=False).tolist()

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
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='S&P500 StockData' AND xtype='U')
    CREATE TABLE [S&P500 StockData] (
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
        data = yf.download(ticker, period='1Y')
        if data.empty:
            print(f"❌ No data for {ticker}, skipping...")
            continue
        data.reset_index(inplace=True)
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
                        SELECT 1 FROM [S&P500 StockData] WHERE [Ticker] = ? AND [Date] = ?
                    )
                    INSERT INTO [S&P500 StockData] (Ticker, [Date], [Open], [High], [Low], [Close], [Index], [Volume])
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
excel_path = os.path.join(os.environ['USERPROFILE'], 'Documents', 'S&P500_StockData.xlsx')
try:
    all_data.to_excel(excel_path, index=False)
    print(f"📁 Excel file saved at: {excel_path}")
except Exception as e:
    print(f"❌ Excel saving error: {e}") 