from google import genai
from dotenv import load_dotenv
from flask import Flask, render_template, jsonify, request, redirect, url_for, make_response
from datetime import datetime
from flask_apscheduler import APScheduler
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from io import BytesIO
from pytz import timezone
import subprocess
import os
import json
import uuid
import smtplib
import pandas as pd
import time
from typing import Optional
from sqlalchemy import create_engine, text
import yfinance as yf

load_dotenv()

app = Flask(__name__)

class Config:
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")

app.config.from_object(Config)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL")
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

@app.route("/")
def index():
    predictions, prediction_date = get_predictions()
    company_list = list(ticker_map.keys())
    my_date = get_my_date()
    return render_template("index.html", predictions=predictions, prediction_date=prediction_date, my_date=my_date, company_list=company_list)

@app.route("/company")
def company_redirect():
    company_name = request.args.get("company", "").lower()
    return redirect(url_for("company_page", company_name=company_name))

@app.route("/company/<company_name>")
def company_page(company_name):
    company_name = company_name.lower()
    ticker = ticker_map.get(company_name)
    if not ticker:
        return render_template("company_not_found.html", company_name=company_name)

    df_info = pd.read_sql(text("""
        SELECT "longName","sector","marketCap","profitMargins","52WeekChange"
        FROM "Company_Info"
        WHERE "Ticker"=:t
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
        SELECT "trade_date","open","high","low","close"
        FROM "StockData"
        WHERE "symbol"=:t
        ORDER BY "trade_date"
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
        FROM "Prediction_vs_Actual"
        WHERE "Company"=:c
        ORDER BY "Date" DESC
        LIMIT 1
    """), engine, params={"c": company_name})

    if not df_pva.empty:
        predicted_price = float(df_pva.iloc[0][0])
        actual_price = float(df_pva.iloc[0][1])
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
        FROM "Final_Analysis"
        WHERE "Company"=:c
        ORDER BY "Prediction_Date" DESC
        LIMIT 1
    """), engine, params={"c": company_name})

    latest_prediction = {"closing_price": "N/A", "date": "N/A"}
    if not df_final.empty:
        latest_prediction = {
            "closing_price": round(float(df_final.iloc[0][0]), 2),
            "date": df_final.iloc[0][1].strftime("%Y-%m-%d")
        }

    return render_template(
        "company_info.html",
        company_name=company_name.upper(),
        company_data=company_data,
        candle_chart_data=candle_chart_data,
        prediction_bar_data=prediction_bar_data,
        latest_prediction=latest_prediction
    )

def get_predictions():
    predictions = []
    df_date = pd.read_sql('SELECT MAX("Prediction_Date") AS d FROM "Final_Analysis"', engine)
    if df_date.empty or df_date.iloc[0]["d"] is None:
        return predictions, ""
    latest_date = df_date.iloc[0]["d"]
    df = pd.read_sql(text("""
        SELECT "Ticker","Predicted_Closing_Price"
        FROM "Final_Analysis"
        WHERE "Prediction_Date"=:d
    """), engine, params={"d": latest_date})
    for r in df.itertuples():
        predictions.append({"ticker": r.Ticker, "price": r.Predicted_Closing_Price})
    return predictions, latest_date.strftime("%d %B %Y")

def get_my_date():
    df = pd.read_sql('SELECT MAX("Date") AS d FROM "Prediction_vs_Actual"', engine)
    return df.iloc[0]["d"] if not df.empty else None

@app.route('/prediction-vs-actual')
def prediction_vs_actual():
    df_date = pd.read_sql('SELECT MAX("Date") AS d FROM "Prediction_vs_Actual"', engine)
    if df_date.empty or df_date.iloc[0]["d"] is None:
        return jsonify([])
    latest_date = df_date.iloc[0]["d"]
    df = pd.read_sql(text("""
        SELECT "Company","Ticker","Predicted_Closing_Price","Actual_Closing_Price"
        FROM "Prediction_vs_Actual"
        WHERE "Date"=:d
    """), engine, params={"d": latest_date})
    return jsonify([
        {
            "company": r.Company,
            "ticker": r.Ticker,
            "predicted": round(r.Predicted_Closing_Price, 2),
            "actual": round(r.Actual_Closing_Price, 2)
        }
        for r in df.itertuples()
    ])

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
            df = pd.read_sql(text(q), engine)
            results = df.values.tolist()
            columns = df.columns.tolist()
        except Exception as e:
            error = str(e)
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
