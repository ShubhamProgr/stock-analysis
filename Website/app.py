from dotenv import load_dotenv
from flask import Flask, render_template, jsonify, request, redirect, url_for, make_response
from datetime import datetime
from flask_apscheduler import APScheduler
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from io import BytesIO
from pytz import timezone
from typing import Optional
from sqlalchemy import create_engine, text
import subprocess
import os 
import uuid
import smtplib
import pandas as pd
import time
import yfinance as yf
import logging
import threading
import sys
import threading

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)


def build_database_url():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    host = os.getenv("SUPABASE_DB_HOST")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    database = os.getenv("SUPABASE_DB_NAME", "postgres")
    user = os.getenv("SUPABASE_DB_USER", "postgres")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    if not host or not password:
        raise RuntimeError("Set DATABASE_URL or SUPABASE_DB_* variables in .env")

    return (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{database}?sslmode={sslmode}"
    )

class Config:
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")

app.config.from_object(Config)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

DATABASE_URL = build_database_url()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SCRIPT_DIR = os.getenv("SCRIPT_DIR")
if not SCRIPT_DIR:
    SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Stock Analysis'))

ticker_map = {
    'reliance': 'RELIANCE.NS', 'tcs': 'TCS.NS', 'infy': 'INFY.NS', 'hdfcbank': 'HDFCBANK.NS',
    'icicibank': 'ICICIBANK.NS', 'kotakbank': 'KOTAKBANK.NS', 'hcltech': 'HCLTECH.NS',
    'lt': 'LT.NS', 'itc': 'ITC.NS', 'sbin': 'SBIN.NS', 'bhartiartl': 'BHARTIARTL.NS',
    'asianpaint': 'ASIANPAINT.NS', 'bajfinance': 'BAJFINANCE.NS', 'bajajfinsv': 'BAJAJFINSV.NS',
    'hindunilvr': 'HINDUNILVR.NS', 'maruti': 'MARUTI.NS', 'nestleind': 'NESTLEIND.NS',
    'ntpc': 'NTPC.NS', 'ongc': 'ONGC.NS', 'powergrid': 'POWERGRID.NS', 'titan': 'TITAN.NS',
    'ultracemco': 'ULTRACEMCO.NS', 'wipro': 'WIPRO.NS', 'techm': 'TECHM.NS',
    'sunpharma': 'SUNPHARMA.NS', 'adanient': 'ADANIENT.NS', 'divislab': 'DIVISLAB.NS',
    'eichermot': 'EICHERMOT.NS', 'apollohosp': 'APOLLOHOSP.NS', 'grasim': 'GRASIM.NS',
    'jswsteel': 'JSWSTEEL.NS', 'tatasteel': 'TATASTEEL.NS', 'drreddy': 'DRREDDY.NS',
    'heromotoco': 'HEROMOTOCO.NS', 'cipla': 'CIPLA.NS', 'coalindia': 'COALINDIA.NS',
    'hdfclife': 'HDFCLIFE.NS', 'hindalco': 'HINDALCO.NS', 'indusindbk': 'INDUSINDBK.NS',
    'bajaj-auto': 'BAJAJ-AUTO.NS', 'britannia': 'BRITANNIA.NS', 'sbilife': 'SBILIFE.NS',
    'upl': 'UPL.NS', 'axisbank': 'AXISBANK.NS', 'shreecem': 'SHREECEM.NS',
    'tataconsum': 'TATACONSUM.NS', 'm&m': 'M&M.NS', 'hal': 'HAL.NS', 'dlf': 'DLF.NS'
}

def execute_script_in_background(script_name):
    try:
        logger.info(f"Starting background script: {script_name}")
        script_path = os.path.join(SCRIPT_DIR, script_name)
        subprocess.run(["python", script_path], check=True)
        logger.info(f"Background script {script_name} finished successfully!")
    except Exception as e:
        logger.error(f"Script {script_name} failed: {str(e)}")

@app.route('/run_program', methods=['POST'])
def run_program():
    data = request.get_json()
    program_id = data.get('program')
    
    script_mapping = {
        'prediction': 'Actual_vs_Prediction.py',
        'final': 'Final_Analysis.py',
        'news': 'News_Extractor.py',
        'sentiment': 'Sentiment_Analyzer.py',
        'stockdata': 'Stock_Data_Daily.py',
        'stockdata5y': 'Stock_Data_5Y.py'
    }
    
    target_script = script_mapping.get(program_id)
    
    if target_script:
        thread = threading.Thread(target=execute_script_in_background, args=(target_script,))
        thread.start()
        return jsonify({"status": "started", "message": f"{target_script} is running."})
    else:
        return jsonify({"status": "error", "message": "Unknown program ID"}), 400

@app.route("/")
def index():
    try:
        logger.info("Loading index page")
        predictions, prediction_date = get_predictions()
        company_list = list(ticker_map.keys())
        my_date = get_my_date()
        logger.info(f"Index loaded successfully. Predictions: {len(predictions)}")
        return render_template("index.html", predictions=predictions, prediction_date=prediction_date, my_date=my_date, company_list=company_list)
    except Exception as e:
        logger.error(f"Error loading index: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

@app.route("/company")
def company_redirect():
    company_name = request.args.get("company", "").lower()
    return redirect(url_for("company_page", company_name=company_name))

@app.route("/company/<company_name>")
def company_page(company_name):
    try:
        logger.info(f"Loading company page for: {company_name}")
        company_name = company_name.lower()
        ticker = ticker_map.get(company_name)
        if not ticker:
            logger.warning(f"Company not found: {company_name}")
            return render_template("company_not_found.html", company_name=company_name)

        df_info = pd.read_sql(text("""
            SELECT "longName","sector","marketCap","profitMargins","52WeekChange"
            FROM company_info
            WHERE "Ticker" = :t
        """), engine, params={"t": ticker})

        company_data = None
        if not df_info.empty:
            r = df_info.iloc[0]
            company_data = {
                "longname": r["longName"],
                "sector": r["sector"],
                "marketcap": float(r["marketCap"]) if r["marketCap"] is not None else None,
                "profitmargins": float(r["profitMargins"]) if r["profitMargins"] is not None else None,
                "week52change": float(r["52WeekChange"]) if r["52WeekChange"] is not None else None
            }

        df_stock = pd.read_sql(text("""
            SELECT "Date" AS trade_date,"Open" AS open,"High" AS high,"Low" AS low,"Close" AS close
            FROM stock_data
            WHERE "Ticker" = :t
            ORDER BY "Date"
        """), engine, params={"t": ticker})

        candle_chart_data = [
            {
                "x": row.trade_date.strftime("%Y-%m-%d"),
                "o": float(row.open),
                "h": float(row.high),
                "l": float(row.low),
                "c": float(row.close)
            }
            for row in df_stock.itertuples()
        ]

        df_pva = pd.read_sql(text("""
            SELECT "Predicted_Closing_Price","Actual_Closing_Price"
            FROM prediction_vs_actual
            WHERE "Company" = :c
            ORDER BY "Date" DESC
            LIMIT 1
        """), engine, params={"c": company_name})

        if not df_pva.empty:
            predicted_price = float(df_pva.iloc[0]['Predicted_Closing_Price'])
            actual_price = float(df_pva.iloc[0]['Actual_Closing_Price'])
            min_y = min(predicted_price, actual_price) - 50
            max_y = max(predicted_price, actual_price) + 50
        else:
            predicted_price = actual_price = 0
            min_y, max_y = 0, 100

        prediction_bar_data = {
            "predicted": round(predicted_price, 2),
            "actual": round(actual_price, 2),
            "min_y": round(min_y, 2),
            "max_y": round(max_y, 2)
        }

        df_final = pd.read_sql(text("""
            SELECT "Predicted_Closing_Price","Prediction_Date"
            FROM final_analysis
            WHERE "Company" = :c
            ORDER BY "Prediction_Date" DESC
            LIMIT 1
        """), engine, params={"c": company_name})

        latest_prediction = {"closing_price": "N/A", "date": "N/A"}
        if not df_final.empty:
            latest_prediction = {
                "closing_price": round(float(df_final.iloc[0]['Predicted_Closing_Price']), 2),
                "date": df_final.iloc[0]['Prediction_Date'].strftime("%Y-%m-%d")
            }

        logger.info(f"Company page loaded successfully for: {company_name}")
        return render_template(
            "company_info.html",
            company_name=company_name.upper(),
            company_data=company_data,
            candle_chart_data=candle_chart_data,
            prediction_bar_data=prediction_bar_data,
            latest_prediction=latest_prediction
        )
    except Exception as e:
        logger.error(f"Error loading company page for {company_name}: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

def get_predictions():
    try:
        predictions = []
        df_date = pd.read_sql('SELECT MAX("Prediction_Date") AS d FROM final_analysis', engine)
        if df_date.empty or df_date.iloc[0]["d"] is None:
            logger.warning("No predictions found in final_analysis")
            return predictions, ""
        latest_date = df_date.iloc[0]["d"]
        df = pd.read_sql(text("""
            SELECT "Ticker","Predicted_Closing_Price"
            FROM final_analysis
            WHERE "Prediction_Date" = :d
        """), engine, params={"d": latest_date})
        for r in df.itertuples():
            predictions.append({"ticker": r.Ticker, "price": r.Predicted_Closing_Price})
        logger.info(f"Retrieved {len(predictions)} predictions for date {latest_date}")
        return predictions, latest_date.strftime("%d %B %Y")
    except Exception as e:
        logger.error(f"Error in get_predictions: {str(e)}", exc_info=True)
        return [], ""

def get_my_date():
    try:
        df = pd.read_sql('SELECT MAX("Date") AS d FROM prediction_vs_actual', engine)
        result = df.iloc[0]["d"] if not df.empty else None
        logger.info(f"Retrieved my_date: {result}")
        return result
    except Exception as e:
        logger.warning(f"prediction_vs_actual table may not exist or is empty: {str(e)}")
        return None

@app.route('/prediction-vs-actual')
def prediction_vs_actual():
    try:
        logger.info("Loading prediction vs actual data")
        df_date = pd.read_sql('SELECT MAX("Date") AS d FROM prediction_vs_actual', engine)
        if df_date.empty or df_date.iloc[0]["d"] is None:
            logger.warning("No data found in prediction_vs_actual")
            return jsonify([])
        latest_date = df_date.iloc[0]["d"]
        df = pd.read_sql(text("""
            SELECT "Company","Ticker","Predicted_Closing_Price","Actual_Closing_Price"
            FROM prediction_vs_actual
            WHERE "Date" = :d
        """), engine, params={"d": latest_date})
        result = [
            {
                "company": r.Company,
                "ticker": r.Ticker,
                "predicted": round(r.Predicted_Closing_Price, 2),
                "actual": round(r.Actual_Closing_Price, 2)
            }
            for r in df.itertuples()
        ]
        logger.info(f"Retrieved {len(result)} prediction vs actual records")
        return jsonify(result)
    except Exception as e:
        logger.warning(f"prediction_vs_actual table may not exist: {str(e)}")
        return jsonify([])

VALID_KEY = os.getenv("VALID_KEY", "217621")

@app.route("/key", methods=["GET", "POST"])
def key_check():
    if request.method == "POST":
        entered_key = request.form.get("access_key")
        if entered_key == VALID_KEY:
            return redirect(url_for("query_page"))
        return render_template("key.html", error="Please recheck credentials")
    return render_template("key.html")

@app.route("/query", methods=["GET", "POST"])
def query_page():
    results, columns, error = None, None, None
    if request.method == "POST":
        q = request.form.get("query")
        try:
            # Only allow SELECT queries for security
            if q.strip().upper().startswith("SELECT"):
                df = pd.read_sql(text(q), engine)
                results = df.values.tolist()
                columns = df.columns.tolist()
            else:
                error = "Only SELECT queries are allowed"
        except Exception as e:
            error = f"Query error: {str(e)}"
    return render_template("query.html", results=results, columns=columns, error=error)

@app.route("/api/live-price/<ticker>")
def live_price(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="1d", interval="1m")
    if data.empty:
        return jsonify({"error": "No data available"}), 404
    data = data.reset_index()
    return jsonify([
        {"time": row["Datetime"].strftime("%H:%M"), "price": round(row["Close"], 2)}
        for _, row in data.iterrows()
    ])

def execute_scripts_sequentially(script_paths):
    """
    Runs a list of Python scripts one by one in the background.
    Output goes directly to Render Logs in real-time.
    """
    print(f"\n[BACKGROUND THREAD] Starting pipeline: {len(script_paths)} scripts...", flush=True)
    
    for script in script_paths:
        print(f"\n---> [STARTING] {script} <---", flush=True)
        try:
            subprocess.run([sys.executable, script], check=True)
            print(f"---> [SUCCESS] Finished {script} <---", flush=True)
        
        except subprocess.CalledProcessError as e:
            print(f"\n!!! [CRITICAL ERROR] Failed while running {script} !!!", flush=True)
            print("Pipeline halted. Check logs above for the specific error.", flush=True)
            break
            
        except Exception as e:
            print(f"\n!!! [UNEXPECTED ERROR] {e} !!!", flush=True)
            break

    print("\n[BACKGROUND THREAD] Pipeline Execution Completed.", flush=True)

@app.route('/admin/trigger-daily', methods=['POST', 'GET'])
def trigger_daily_pipeline():
    """Triggers the daily update workflow"""
    scripts_to_run = [
        "Stock Analysis/Stock_Data_Daily.py",
        "Stock Analysis/Company_Data.py",
        "Stock Analysis/News_Extractor.py",
        "Stock Analysis/Sentiment_Analyzer.py",
        "Stock Analysis/Actual_vs_Prediction.py",
        "Stock Analysis/Final_Analysis.py"
    ]
    
    thread = threading.Thread(target=execute_scripts_sequentially, args=(scripts_to_run,))
    thread.daemon = True 
    thread.start()
    
    return jsonify({
        "status": "success", 
        "message": "Daily Pipeline started in the cloud! You can safely close your laptop. Check Render logs to watch the progress."
    }), 202

@app.route('/admin/trigger-5y', methods=['POST', 'GET'])
def trigger_5y_pipeline():
    """Triggers the historical data download"""
    scripts_to_run = [
        "Stock Analysis/Stock_Data_5Y.py"
    ]
    
    thread = threading.Thread(target=execute_scripts_sequentially, args=(scripts_to_run,))
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "status": "success", 
        "message": "5Y Data fetching started in the background. Check Render logs."
    }), 202

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
