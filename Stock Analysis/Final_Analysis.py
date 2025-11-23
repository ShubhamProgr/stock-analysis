import pyodbc
import pandas as pd
import numpy as np
import warnings
from datetime import timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from datetime import datetime, date, time

warnings.filterwarnings("ignore")

ticker_to_company = {
    'RELIANCE.NS': 'reliance', 'TCS.NS': 'tcs', 'INFY.NS': 'infosys', 'HDFC.NS': 'hdfc bank',
    'ICICIBANK.NS': 'icici bank', 'KOTAKBANK.NS': 'kotak bank', 'HCLTECH.NS': 'hcl',
    'LT.NS': 'l&t', 'ITC.NS': 'itc', 'SBIN.NS': 'sbi', 'BHARTIARTL.NS': 'bharti airtel',
    'ASIANPAINT.NS': 'asian paints', 'BAJFINANCE.NS': 'bajaj finance', 'BAJAJFINSV.NS': 'bajaj finserv',
    'HINDUNILVR.NS': 'hindustan unilever', 'MARUTI.NS': 'maruti', 'NESTLEIND.NS': 'nestle',
    'NTPC.NS': 'ntpc', 'ONGC.NS': 'ongc', 'POWERGRID.NS': 'power grid', 'TITAN.NS': 'titan',
    'ULTRACEMCO.NS': 'ultratech cement', 'WIPRO.NS': 'wipro', 'TECHM.NS': 'tech mahindra',
    'SUNPHARMA.NS': 'sun pharma', 'ADANIENT.NS': 'adani enterprises', 'DIVISLAB.NS': 'divis labs',
    'EICHERMOT.NS': 'eicher motors', 'APOLLOHOSP.NS': 'apollo hospitals', 'GRASIM.NS': 'grasim',
    'JSWSTEEL.NS': 'jsw steel', 'TATASTEEL.NS': 'tata steel', 'DRREDDY.NS': 'dr reddy',
    'HEROMOTOCO.NS': 'hero motocorp', 'CIPLA.NS': 'cipla', 'COALINDIA.NS': 'coal india',
    'HDFCLIFE.NS': 'hdfc life', 'HINDALCO.NS': 'hindalco', 'INDUSINDBK.NS': 'indusind',
    'BAJAJ-AUTO.NS': 'bajaj auto', 'BRITANNIA.NS': 'britannia', 'SBILIFE.NS': 'sbi life',
    'UPL.NS': 'upl', 'AXISBANK.NS': 'axis bank', 'SHREECEM.NS': 'shree cement',
    'TATACONSUM.NS': 'tata consumer', 'M&M.NS': 'mahindra', 'HAL.NS': 'hal', 'DLF.NS': 'dlf'
}

conn = pyodbc.connect(
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)

nse_holidays_2025 = {
    pd.Timestamp("2025-01-26"), pd.Timestamp("2025-03-14"), pd.Timestamp("2025-04-18"),
    pd.Timestamp("2025-08-15"), pd.Timestamp("2025-10-02"), pd.Timestamp("2025-10-21"),
    pd.Timestamp("2025-11-14"), pd.Timestamp("2025-12-25")
}

def get_next_trading_day(input_date):
    next_day = input_date + timedelta(days=1)
    while next_day.weekday() >= 5 or next_day in nse_holidays_2025:
        next_day += timedelta(days=1)
    return next_day

results = []

for ticker, company in ticker_to_company.items():
    try:
        query = """
            SELECT [Date], [Open], [High], [Low], [Close], [Volume]
            FROM StockData
            WHERE [Ticker] = ?
            ORDER BY [Date] ASC
        """
        df = pd.read_sql(query, conn, params=[ticker])
        if len(df) < 10:
            continue

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')

        now = datetime.now()
        market_close_time = time(15, 30)
        today = pd.Timestamp(date.today())
        if df['Date'].iloc[-1].date() == today.date() and now.time() < market_close_time:
            df = df[df['Date'] < today]

        if len(df) < 10:
            continue

        df = df.dropna()

        sent_query = """
            SELECT TOP 1 Sentiment, Score
            FROM Company_FinBERT_Sentiments
            WHERE Ticker = ?
            ORDER BY Score DESC
        """
        sent_df = pd.read_sql(sent_query, conn, params=[ticker])
        if not sent_df.empty:
            sentiment_label = sent_df['Sentiment'].iloc[0]
            sentiment_score = sent_df['Score'].iloc[0]
        else:
            sentiment_label, sentiment_score = "NEUTRAL", 0.0

        df['Sentiment_Score'] = sentiment_score

        X = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Sentiment_Score']]
        y_reg = df['Close'].shift(-1).dropna()
        X = X.iloc[:-1]

        if len(X) != len(y_reg):
            continue

        X_train, X_test, y_reg_train, y_reg_test = train_test_split(X, y_reg, test_size=0.2, shuffle=False)

        reg = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_leaf=3,
            random_state=42
        )
        reg.fit(X_train, y_reg_train)

        latest_data = X.iloc[[-1]]
        predicted_price = reg.predict(latest_data)[0]

        y_reg_pred = reg.predict(X_test)
        mae = mean_absolute_error(y_reg_test, y_reg_pred)
        mse = mean_squared_error(y_reg_test, y_reg_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_reg_test, y_reg_pred)

        results.append({
            'Company': company,
            'Ticker': ticker,
            'Prediction_Date': get_next_trading_day(df.iloc[-1]['Date']),
            'Predicted_Closing_Price': round(predicted_price, 2),
            'Last_Close': df.iloc[-1]['Close'],
            'Last_Close_Date': df.iloc[-1]['Date'],
            'MAE': round(mae, 4),
            'MSE': round(mse, 4),
            'RMSE': round(rmse, 4),
            'R2_Score': round(r2, 4),
            'Sentiment': sentiment_label,
            'Sentiment_Score': sentiment_score
        })

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        continue

final_df = pd.DataFrame(results)
prediction_date = final_df.iloc[-1]['Prediction_Date'].strftime('%Y_%m_%d')
output_path = fr'C:\Users\PC\Documents\Final_Analysis_{prediction_date}.xlsx'
final_df.to_excel(output_path, index=False)
print(f"✅ Final Analysis saved to {output_path}")

table_name = 'Final_Analysis'
create_table_sql = f"""
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'
)
BEGIN
    CREATE TABLE {table_name} (
        Company VARCHAR(100),
        Ticker VARCHAR(20),
        Prediction_Date DATE,
        Predicted_Closing_Price FLOAT,
        Last_Close FLOAT,
        Last_Close_Date DATE,
        MAE FLOAT,
        MSE FLOAT,
        RMSE FLOAT,
        R2_Score FLOAT,
        Sentiment VARCHAR(50),
        Sentiment_Score FLOAT
    );
END
"""
cursor = conn.cursor()
cursor.execute(create_table_sql)
conn.commit()

insert_sql = f"""
IF NOT EXISTS (
    SELECT 1 FROM {table_name} WHERE [Ticker] = ? AND [Prediction_Date] = ?
)
INSERT INTO {table_name} (
    [Company], [Ticker], [Prediction_Date],
    [Predicted_Closing_Price], [Last_Close], [Last_Close_Date],
    [MAE], [MSE], [RMSE], [R2_Score], [Sentiment], [Sentiment_Score]
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in final_df.iterrows():
    values = (
        row['Ticker'],
        row['Prediction_Date'],
        row['Company'],
        row['Ticker'],
        row['Prediction_Date'].date(),
        row['Predicted_Closing_Price'],
        row['Last_Close'],
        row['Last_Close_Date'].date(),
        row['MAE'],
        row['MSE'],
        row['RMSE'],
        row['R2_Score'],
        row['Sentiment'],
        row['Sentiment_Score']
    )
    conn.execute(insert_sql, values)

conn.commit()
cursor.close()
conn.close()
print(f"✅ Final Analysis inserted into SQL Server table {table_name}")
