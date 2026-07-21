from dotenv import load_dotenv
from datetime import timedelta, datetime, date, time
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
import pandas as pd
import numpy as np
import warnings
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

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

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

ticker_to_company = {
    'RELIANCE.NS': 'reliance', 'TCS.NS': 'tcs', 'INFY.NS': 'infosys', 'HDFCBANK.NS': 'hdfc bank',
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
    'TATACONSUM.NS': 'tata consumer', 'M&M.NS': 'mahindra', 'HAL.NS': 'hal', 'DLF.NS': 'dlf',
    'LTIM.NS': 'ltim', 'ABB.NS': 'abb', 'ADANIGREEN.NS': 'adani green energy',
    'ADANIPOWER.NS': 'adani power', 'ADANIPORTS.NS': 'adani ports', 'AMBUJACEM.NS': 'ambuja cements',
    'BAJAJHLDNG.NS': 'bajaj holdings', 'BANKBARODA.NS': 'bank of baroda', 'BPCL.NS': 'bpcl',
    'BOSCHLTD.NS': 'bosch', 'CANBK.NS': 'canara bank', 'ACC.NS': 'acc', 'DMART.NS': 'dmart',
    'BANDHANBNK.NS': 'bandhan bank', 'BIOCON.NS': 'biocon', 'CHOLAFIN.NS': 'chola finance',
    'COLPAL.NS': 'colgate', 'GAIL.NS': 'gail', 'GODREJCP.NS': 'godrej consumer',
    'ICICIGI.NS': 'icici general', 'ICICIPRULI.NS': 'icici prudential', 'INDHOTEL.NS': 'indian hotels',
    'INDUSTOWER.NS': 'indus towers', 'NAUKRI.NS': 'naukri', 'INDIGO.NS': 'indigo',
    'LICI.NS': 'lici', 'MARICO.NS': 'marico', 'MPHASIS.NS': 'mphasis',
    'MUTHOOTFIN.NS': 'muthoot finance', 'PAYTM.NS': 'paytm', 'PIIND.NS': 'pi industries',
    'PIDILITIND.NS': 'pidilite', 'SBICARD.NS': 'sbi card', 'SRF.NS': 'srf',
    'MOTHERSON.NS': 'motherson sumi', 'SIEMENS.NS': 'siemens', 'TATAPOWER.NS': 'tata power',
    'TORNTPHARM.NS': 'torrent pharma', 'MCDOWELL-N.NS': 'mcdowell', 'VEDL.NS': 'vedanta',
    'ZOMATO.NS': 'zomato', 'PETRONET.NS': 'petronet lgm', 'PGHH.NS': 'procter gamble',
    'POLYCAB.NS': 'polycab', 'ICICISENSX.NS': 'icici securities', 'HAVELLS.NS': 'havells',
    'CONCOR.NS': 'concor', 'IRCTC.NS': 'irctc', 'TRENT.NS': 'trent', 'TVSMOTOR.NS': 'tvs motor',
    'JUBLFOOD.NS': 'jubilant foodworks'
}

nse_holidays_2026 = {
    pd.Timestamp("2026-01-26"),  
    pd.Timestamp("2026-03-03"),  
    pd.Timestamp("2026-03-26"),  
    pd.Timestamp("2026-03-31"),  
    pd.Timestamp("2026-04-03"),  
    pd.Timestamp("2026-04-14"),  
    pd.Timestamp("2026-05-01"),  
    pd.Timestamp("2026-05-28"),  
    pd.Timestamp("2026-06-26"),  
    pd.Timestamp("2026-09-14"),  
    pd.Timestamp("2026-10-02"),  
    pd.Timestamp("2026-10-20"),  
    pd.Timestamp("2026-11-10"),  
    pd.Timestamp("2026-11-24"),  
    pd.Timestamp("2026-12-25")   
}

def get_next_trading_day(input_date):
    next_day = input_date + timedelta(days=1)
    while next_day.weekday() >= 5 or next_day in nse_holidays_2026:
        next_day += timedelta(days=1)
    return next_day

results = []

for ticker, company in ticker_to_company.items():
    try:
        query = """
            SELECT "Date", "Open", "High", "Low", "Close", "Volume"
            FROM stock_data
            WHERE "Ticker" = :ticker
            ORDER BY "Date" ASC
        """
        df = pd.read_sql(text(query), engine, params={"ticker": ticker})
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
            SELECT "Sentiment", "Score"
            FROM company_finbert_sentiments
            WHERE "Ticker" = :ticker
            ORDER BY "Score" DESC
            LIMIT 1
        """
        sent_df = pd.read_sql(text(sent_query), engine, params={"ticker": ticker})
        if not sent_df.empty:
            sentiment_label = sent_df['Sentiment'].iloc[0]
            sentiment_score = sent_df['Score'].iloc[0]
        else:
            sentiment_label, sentiment_score = "NEUTRAL", 0.0

        df['Sentiment_Score'] = sentiment_score

        # --- KEY CHANGE: TARGET ENGINEERING ---
        # Calculate daily percentage returns
        df['Daily_Return'] = df['Close'].pct_change()
        
        # Shift the returns up by 1 to act as our target (predicting TOMORROW'S return)
        df['Target_Return'] = df['Daily_Return'].shift(-1)
        
        X = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Sentiment_Score']]
        y_reg = df['Target_Return'].dropna()
        X = X.iloc[1:-1] # Drop the first row (NaN return) and last row (NaN target)
        
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

        # --- KEY CHANGE: PREDICTION LOGIC ---
        latest_data = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Sentiment_Score']].iloc[[-1]]
        predicted_return = reg.predict(latest_data)[0]
        
        last_close = df.iloc[-1]['Close']
        predicted_price = last_close * (1 + predicted_return)

        # Re-convert testing targets to absolute prices to calculate accurate MAE/MSE in rupees
        y_reg_pred_return = reg.predict(X_test)
        y_test_prices = X_test['Close'] * (1 + y_reg_test)
        y_pred_prices = X_test['Close'] * (1 + y_reg_pred_return)

        mae = mean_absolute_error(y_test_prices, y_pred_prices)
        mse = mean_squared_error(y_test_prices, y_pred_prices)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test_prices, y_pred_prices)

        results.append({
            'Company': company,
            'Ticker': ticker,
            'Prediction_Date': get_next_trading_day(df.iloc[-1]['Date']),
            'Predicted_Closing_Price': round(predicted_price, 2),
            'Predicted_Return_Pct': round(predicted_return * 100, 4), # Saving percentage as a nice readable number (e.g., 1.25 for 1.25%)
            'Last_Close': last_close,
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

if final_df.empty:
    print("No results to save. Exiting.")
else:
    prediction_date = final_df.iloc[-1]['Prediction_Date']
    if isinstance(prediction_date, pd.Timestamp):
        prediction_date_str = prediction_date.strftime('%Y_%m_%d')
    else:
        prediction_date_str = pd.to_datetime(prediction_date).strftime('%Y_%m_%d')

    output_path = f'data/Final_Analysis_{prediction_date_str}.xlsx'
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_excel(output_path, index=False)
    print(f"Final Analysis saved to {output_path}")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS final_analysis (
                "Company" TEXT,
                "Ticker" TEXT,
                "Prediction_Date" DATE,
                "Predicted_Closing_Price" DOUBLE PRECISION,
                "Predicted_Return_Pct" DOUBLE PRECISION,
                "Last_Close" DOUBLE PRECISION,
                "Last_Close_Date" DATE,
                "MAE" DOUBLE PRECISION,
                "MSE" DOUBLE PRECISION,
                "RMSE" DOUBLE PRECISION,
                "R2_Score" DOUBLE PRECISION,
                "Sentiment" TEXT,
                "Sentiment_Score" DOUBLE PRECISION,
                PRIMARY KEY ("Ticker", "Prediction_Date")
            )
        """))

        insert_sql = text("""
            INSERT INTO final_analysis (
                "Company", "Ticker", "Prediction_Date",
                "Predicted_Closing_Price", "Predicted_Return_Pct", "Last_Close", "Last_Close_Date",
                "MAE", "MSE", "RMSE", "R2_Score", "Sentiment", "Sentiment_Score"
            )
            VALUES (
                :company, :ticker, :prediction_date,
                :predicted_closing_price, :predicted_return_pct, :last_close, :last_close_date,
                :mae, :mse, :rmse, :r2_score, :sentiment, :sentiment_score
            )
            ON CONFLICT ("Ticker", "Prediction_Date") DO UPDATE SET
                "Company" = EXCLUDED."Company",
                "Predicted_Closing_Price" = EXCLUDED."Predicted_Closing_Price",
                "Predicted_Return_Pct" = EXCLUDED."Predicted_Return_Pct",
                "Last_Close" = EXCLUDED."Last_Close",
                "Last_Close_Date" = EXCLUDED."Last_Close_Date",
                "MAE" = EXCLUDED."MAE",
                "MSE" = EXCLUDED."MSE",
                "RMSE" = EXCLUDED."RMSE",
                "R2_Score" = EXCLUDED."R2_Score",
                "Sentiment" = EXCLUDED."Sentiment",
                "Sentiment_Score" = EXCLUDED."Sentiment_Score"
        """)

        for _, row in final_df.iterrows():
            ticker = row['Ticker']
            prediction_date = row['Prediction_Date'].date() if hasattr(row['Prediction_Date'], "date") else pd.to_datetime(row['Prediction_Date']).date()
            conn.execute(insert_sql, {
                "company": row['Company'],
                "ticker": ticker,
                "prediction_date": prediction_date,
                "predicted_closing_price": row['Predicted_Closing_Price'],
                "predicted_return_pct": row['Predicted_Return_Pct'],
                "last_close": row['Last_Close'],
                "last_close_date": row['Last_Close_Date'].date() if hasattr(row['Last_Close_Date'], "date") else pd.to_datetime(row['Last_Close_Date']).date(),
                "mae": row['MAE'],
                "mse": row['MSE'],
                "rmse": row['RMSE'],
                "r2_score": row['R2_Score'],
                "sentiment": row['Sentiment'],
                "sentiment_score": row['Sentiment_Score']
            })

    print("Final Analysis inserted into Supabase Postgres table final_analysis")