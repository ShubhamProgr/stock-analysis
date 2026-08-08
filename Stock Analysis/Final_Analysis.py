from dotenv import load_dotenv
from datetime import timedelta, datetime, time
from zoneinfo import ZoneInfo
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
import pandas as pd
import numpy as np
import json
import warnings
from sqlalchemy import create_engine, text
import yfinance as yf

try:
    from xgboost import XGBRegressor
    USE_XGBOOST = True
except ImportError:
    from sklearn.ensemble import GradientBoostingRegressor
    USE_XGBOOST = False

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


# ==================== Technical Indicators ====================
def compute_technical_indicators(df):
    """Compute normalized/stationary technical indicators from OHLCV data."""
    close = df['Close'].astype(float)
    high = df['High'].astype(float)
    low = df['Low'].astype(float)
    volume = df['Volume'].astype(float)

    # --- Price-to-SMA ratios (normalized, trend signal) ---
    sma_5 = close.rolling(5).mean()
    sma_20 = close.rolling(20).mean()
    df['Price_to_SMA5'] = close / sma_5
    df['Price_to_SMA20'] = close / sma_20

    # --- MACD (momentum) ---
    ema_12 = close.ewm(span=12, adjust=False).mean()
    ema_26 = close.ewm(span=26, adjust=False).mean()
    df['MACD'] = ema_12 - ema_26
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

    # --- RSI 14-period (bounded 0-100, overbought/oversold) ---
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta.where(delta < 0, 0.0))
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df['RSI_14'] = 100 - (100 / (1 + rs))

    # --- Bollinger Bands (volatility squeeze/breakout) ---
    bb_sma = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_upper = bb_sma + 2 * bb_std
    bb_lower = bb_sma - 2 * bb_std
    df['BB_Width'] = (bb_upper - bb_lower) / bb_sma
    df['BB_Position'] = (close - bb_lower) / (bb_upper - bb_lower)

    # --- ATR as percentage of close (normalized volatility) ---
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['ATR_Pct'] = true_range.rolling(14).mean() / close

    # --- Volume ratio (spike detection) ---
    vol_ma_20 = volume.rolling(20).mean()
    df['Volume_Ratio'] = volume / vol_ma_20

    # --- OBV percentage change (volume-price divergence) ---
    obv = (np.sign(close.diff()) * volume).fillna(0).cumsum()
    df['OBV_Pct_Change'] = obv.pct_change(5)

    # --- Lagged returns (momentum at different horizons) ---
    df['Daily_Return'] = close.pct_change()
    df['Return_5d'] = close.pct_change(5)
    df['Return_20d'] = close.pct_change(20)

    # --- Intraday range as percentage (intraday volatility) ---
    df['HL_Pct'] = (high - low) / close

    return df


# ==================== Fetch Nifty50 for Market Regime Features ====================
print("Fetching Nifty50 index data for market regime features...")
try:
    nifty_raw = yf.download("^NSEI", period="5y", progress=False, timeout=15)
    if isinstance(nifty_raw.columns, pd.MultiIndex):
        nifty_raw.columns = nifty_raw.columns.get_level_values(0)
    nifty_data = nifty_raw[['Close']].copy()
    nifty_data['Nifty_Return_5d'] = nifty_data['Close'].pct_change(5)
    nifty_data['Nifty_Return_20d'] = nifty_data['Close'].pct_change(20)
    nifty_data['Nifty_Volatility_20d'] = nifty_data['Close'].pct_change().rolling(20).std()
    nifty_data = nifty_data.drop(columns=['Close']).reset_index()
    nifty_data['Date'] = pd.to_datetime(nifty_data['Date']).dt.normalize()
    has_nifty = True
    print(f"  Nifty50 data loaded: {len(nifty_data)} trading days")
except Exception as e:
    print(f"  Could not load Nifty50 data: {e}. Skipping market regime features.")
    has_nifty = False
    nifty_data = None

model_type = "XGBoost" if USE_XGBOOST else "GradientBoosting"
print(f"Using model: {model_type}\n")


# ==================== Feature Column Definitions ====================
FEATURE_COLS = [
    # Technical indicators (15 features — all normalized/stationary)
    'Daily_Return', 'Return_5d', 'Return_20d',
    'RSI_14',
    'MACD', 'MACD_Signal', 'MACD_Hist',
    'BB_Width', 'BB_Position',
    'ATR_Pct',
    'Volume_Ratio', 'OBV_Pct_Change',
    'HL_Pct',
    'Price_to_SMA5', 'Price_to_SMA20',
    # Sentiment (4 features — date-aligned)
    'Sentiment_Score', 'Positive_Count', 'Negative_Count', 'Neutral_Count',
    # Company fundamentals (5 features — cross-sectional context)
    'trailingPE', 'profitMargins', 'grossMargins', 'operatingMargins', '52WeekChange',
    # Market regime (3 features — broad market context)
    'Nifty_Return_5d', 'Nifty_Return_20d', 'Nifty_Volatility_20d',
]


# ==================== Main Prediction Loop ====================
results = []

for ticker, company in ticker_to_company.items():
    try:
        # --- 1. Load stock data ---
        query = """
            SELECT "Date", "Open", "High", "Low", "Close", "Volume"
            FROM stock_data
            WHERE "Ticker" = :ticker
            ORDER BY "Date" ASC
        """
        df = pd.read_sql(text(query), engine, params={"ticker": ticker})

        if len(df) < 50:
            print(f"Skipping {ticker}: only {len(df)} rows (need 50+)")
            continue

        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date')

        # Exclude today's row if market hasn't closed yet
        now = datetime.now(ZoneInfo("Asia/Kolkata"))
        market_close_time = time(15, 30)
        today = pd.Timestamp(now.date())
        if df['Date'].iloc[-1].date() == today.date() and now.time() < market_close_time:
            df = df[df['Date'] < today]

        if len(df) < 50:
            continue

        # --- 2. Compute technical indicators ---
        df = compute_technical_indicators(df)

        # --- 3. Load daily sentiment and merge by date ---
        try:
            sent_query = """
                SELECT "Date", "Score", "Positive_Count", "Negative_Count", "Neutral_Count"
                FROM company_daily_sentiments
                WHERE "Ticker" = :ticker
                ORDER BY "Date" ASC
            """
            sent_df = pd.read_sql(text(sent_query), engine, params={"ticker": ticker})
            if not sent_df.empty:
                sent_df['Date'] = pd.to_datetime(sent_df['Date'])
                df = df.merge(sent_df, on='Date', how='left')
                for col in ['Score', 'Positive_Count', 'Negative_Count', 'Neutral_Count']:
                    df[col] = df[col].ffill().fillna(0)
                df.rename(columns={'Score': 'Sentiment_Score'}, inplace=True)
            else:
                df['Sentiment_Score'] = 0.0
                df['Positive_Count'] = 0
                df['Negative_Count'] = 0
                df['Neutral_Count'] = 0
        except Exception:
            # Table may not exist on first run
            df['Sentiment_Score'] = 0.0
            df['Positive_Count'] = 0
            df['Negative_Count'] = 0
            df['Neutral_Count'] = 0

        # --- 4. Load company fundamentals ---
        try:
            fund_query = """
                SELECT "trailingPE", "profitMargins", "grossMargins", "operatingMargins",
                       "52WeekChange"
                FROM company_info
                WHERE "Ticker" = :ticker
                LIMIT 1
            """
            fund_df = pd.read_sql(text(fund_query), engine, params={"ticker": ticker})
            if not fund_df.empty:
                for col in fund_df.columns:
                    val = fund_df[col].iloc[0]
                    df[col] = float(val) if pd.notna(val) else 0.0
            else:
                for col in ['trailingPE', 'profitMargins', 'grossMargins', 'operatingMargins', '52WeekChange']:
                    df[col] = 0.0
        except Exception:
            for col in ['trailingPE', 'profitMargins', 'grossMargins', 'operatingMargins', '52WeekChange']:
                df[col] = 0.0

        # --- 5. Merge Nifty50 market regime data ---
        if has_nifty and nifty_data is not None:
            df['Date_norm'] = df['Date'].dt.normalize()
            nifty_merge = nifty_data.rename(columns={'Date': 'Date_norm'})
            df = df.merge(nifty_merge, on='Date_norm', how='left')
            df.drop(columns=['Date_norm'], inplace=True)
            for col in ['Nifty_Return_5d', 'Nifty_Return_20d', 'Nifty_Volatility_20d']:
                df[col] = df[col].ffill().fillna(0)
        else:
            df['Nifty_Return_5d'] = 0.0
            df['Nifty_Return_20d'] = 0.0
            df['Nifty_Volatility_20d'] = 0.0

        # --- 6. Target engineering ---
        df['Target_Return'] = df['Daily_Return'].shift(-1)
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Ensure all feature columns exist
        for col in FEATURE_COLS:
            if col not in df.columns:
                df[col] = 0.0

        # Separate latest row (no future target) for prediction
        latest_row = df.dropna(subset=FEATURE_COLS).iloc[[-1]]

        # Training data: rows that have both features and a known target
        train_df = df.dropna(subset=FEATURE_COLS + ['Target_Return']).copy()

        if len(train_df) < 50:
            print(f"Skipping {ticker}: only {len(train_df)} trainable rows after indicator computation")
            continue

        X = train_df[FEATURE_COLS]
        y = train_df['Target_Return']
        close_prices = train_df['Close']  # Keep Close separately for price conversion

        # --- 7. Cross-validation with TimeSeriesSplit ---
        n_splits = max(2, min(5, len(X) // 50))
        tscv = TimeSeriesSplit(n_splits=n_splits)
        cv_rmse_scores = []

        for train_idx, val_idx in tscv.split(X):
            X_cv_train = X.iloc[train_idx]
            y_cv_train = y.iloc[train_idx]
            X_cv_val = X.iloc[val_idx]
            y_cv_val = y.iloc[val_idx]
            cv_close = close_prices.iloc[val_idx]

            if USE_XGBOOST:
                cv_model = XGBRegressor(
                    n_estimators=200, max_depth=5, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.8,
                    random_state=42, verbosity=0, n_jobs=-1
                )
            else:
                cv_model = GradientBoostingRegressor(
                    n_estimators=200, max_depth=5, learning_rate=0.05,
                    subsample=0.8, random_state=42
                )

            cv_model.fit(X_cv_train, y_cv_train)
            cv_pred_return = cv_model.predict(X_cv_val)

            cv_actual_price = cv_close.values * (1 + y_cv_val.values)
            cv_pred_price = cv_close.values * (1 + cv_pred_return)
            fold_rmse = np.sqrt(mean_squared_error(cv_actual_price, cv_pred_price))
            cv_rmse_scores.append(fold_rmse)

        avg_cv_rmse = np.mean(cv_rmse_scores)

        # --- 8. Train-test split for reported metrics (80/20, no shuffle) ---
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
        close_test = close_prices.iloc[split_idx:]

        if USE_XGBOOST:
            eval_model = XGBRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, verbosity=0, n_jobs=-1
            )
        else:
            eval_model = GradientBoostingRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, random_state=42
            )

        eval_model.fit(X_train, y_train)
        y_pred_return = eval_model.predict(X_test)
        y_test_prices = close_test.values * (1 + y_test.values)
        y_pred_prices = close_test.values * (1 + y_pred_return)

        mae = mean_absolute_error(y_test_prices, y_pred_prices)
        mse = mean_squared_error(y_test_prices, y_pred_prices)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_test_prices, y_pred_prices)

        # --- 9. Retrain on ALL data for the final production prediction ---
        if USE_XGBOOST:
            final_model = XGBRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, verbosity=0, n_jobs=-1
            )
        else:
            final_model = GradientBoostingRegressor(
                n_estimators=200, max_depth=5, learning_rate=0.05,
                subsample=0.8, random_state=42
            )

        final_model.fit(X, y)

        # --- 10. Feature importance ---
        importances = dict(zip(FEATURE_COLS, final_model.feature_importances_))
        top_features = dict(sorted(importances.items(), key=lambda x: x[1], reverse=True)[:5])
        top_features_str = json.dumps({k: round(v, 4) for k, v in top_features.items()})

        # --- 11. Predict next trading day ---
        latest_features = latest_row[FEATURE_COLS]
        predicted_return = final_model.predict(latest_features)[0]

        # Clip to realistic daily range (±10% for large-cap Indian stocks)
        predicted_return = float(np.clip(predicted_return, -0.10, 0.10))

        last_close = float(latest_row.iloc[-1]['Close'])
        predicted_price = last_close * (1 + predicted_return)

        # --- 12. Get latest sentiment label for output ---
        sentiment_label, sentiment_score = "NEUTRAL", 0.0
        try:
            latest_sent_query = """
                SELECT "Sentiment", "Score"
                FROM company_daily_sentiments
                WHERE "Ticker" = :ticker
                ORDER BY "Date" DESC
                LIMIT 1
            """
            latest_sent = pd.read_sql(text(latest_sent_query), engine, params={"ticker": ticker})
            if not latest_sent.empty:
                sentiment_label = latest_sent['Sentiment'].iloc[0]
                sentiment_score = float(latest_sent['Score'].iloc[0])
        except Exception:
            # Fallback to old table if daily sentiments don't exist yet
            try:
                old_sent_query = """
                    SELECT "Sentiment", "Score"
                    FROM company_finbert_sentiments
                    WHERE "Ticker" = :ticker
                    ORDER BY "Score" DESC
                    LIMIT 1
                """
                old_sent = pd.read_sql(text(old_sent_query), engine, params={"ticker": ticker})
                if not old_sent.empty:
                    sentiment_label = old_sent['Sentiment'].iloc[0]
                    sentiment_score = float(old_sent['Score'].iloc[0])
            except Exception:
                pass

        results.append({
            'Company': company,
            'Ticker': ticker,
            'Prediction_Date': get_next_trading_day(latest_row.iloc[-1]['Date']),
            'Predicted_Closing_Price': round(predicted_price, 2),
            'Predicted_Return_Pct': round(predicted_return * 100, 4),
            'Last_Close': last_close,
            'Last_Close_Date': latest_row.iloc[-1]['Date'],
            'MAE': round(mae, 4),
            'MSE': round(mse, 4),
            'RMSE': round(rmse, 4),
            'R2_Score': round(r2, 4),
            'Sentiment': sentiment_label,
            'Sentiment_Score': sentiment_score,
            'Model_Type': model_type,
            'CV_RMSE': round(avg_cv_rmse, 4),
            'Top_Features': top_features_str,
        })

        print(f"  {ticker}: Predicted={predicted_price:.2f}, R2={r2:.4f}, CV_RMSE={avg_cv_rmse:.2f}")

    except Exception as e:
        print(f"Error processing {ticker}: {e}")
        continue


# ==================== Save Results ====================
final_df = pd.DataFrame(results)

if final_df.empty:
    print("No results to save. Exiting.")
else:
    with engine.begin() as conn:
        # Create table (includes new columns for fresh installs)
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
                "Model_Type" TEXT,
                "CV_RMSE" DOUBLE PRECISION,
                "Top_Features" TEXT,
                PRIMARY KEY ("Ticker", "Prediction_Date")
            )
        """))

        # Add new columns to existing tables that were created before this update
        for col_def in ['"Model_Type" TEXT', '"CV_RMSE" DOUBLE PRECISION', '"Top_Features" TEXT']:
            try:
                conn.execute(text(f'ALTER TABLE final_analysis ADD COLUMN IF NOT EXISTS {col_def}'))
            except Exception:
                pass

        insert_sql = text("""
            INSERT INTO final_analysis (
                "Company", "Ticker", "Prediction_Date",
                "Predicted_Closing_Price", "Predicted_Return_Pct", "Last_Close", "Last_Close_Date",
                "MAE", "MSE", "RMSE", "R2_Score", "Sentiment", "Sentiment_Score",
                "Model_Type", "CV_RMSE", "Top_Features"
            )
            VALUES (
                :company, :ticker, :prediction_date,
                :predicted_closing_price, :predicted_return_pct, :last_close, :last_close_date,
                :mae, :mse, :rmse, :r2_score, :sentiment, :sentiment_score,
                :model_type, :cv_rmse, :top_features
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
                "Sentiment_Score" = EXCLUDED."Sentiment_Score",
                "Model_Type" = EXCLUDED."Model_Type",
                "CV_RMSE" = EXCLUDED."CV_RMSE",
                "Top_Features" = EXCLUDED."Top_Features"
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
                "sentiment_score": row['Sentiment_Score'],
                "model_type": row['Model_Type'],
                "cv_rmse": row['CV_RMSE'],
                "top_features": row['Top_Features'],
            })

    print(f"\nFinal Analysis inserted into Supabase Postgres table final_analysis ({len(results)} rows)")
    print(f"Model used: {model_type} | Features: {len(FEATURE_COLS)}")
