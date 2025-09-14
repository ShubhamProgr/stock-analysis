from flask import Flask, render_template, jsonify, request, redirect, url_for, flash
from datetime import datetime
from flask_apscheduler import APScheduler
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from io import BytesIO
import subprocess
import os
import yfinance as yf
import pyodbc
import json
import uuid
import smtplib
import pandas as pd

app = Flask(__name__)

class Config:
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = "Asia/Kolkata"

app.config.from_object(Config)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

# ✅ Database connection string (uses raw string for backslashes)
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)
# Absolute path to the Stock Analysis folder
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
my_date  = '01/01/2025'

@app.route("/")
def index():
    predictions, prediction_date = get_predictions()
    my_date = get_my_date()
    company_list = list(ticker_map.keys())  

    return render_template(
        "index.html",
        predictions=predictions,
        prediction_date=prediction_date,
        my_date=my_date,
        company_list=company_list
    )

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

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # --- 1. Company Info ---
        cursor.execute("""
            SELECT [longName], [sector], [marketCap], [profitMargins], [52WeekChange]
            FROM Company_Info
            WHERE [Ticker] = ?
        """, ticker)
        info = cursor.fetchone()
        if info:
            company_data = {
                "longname": info[0],
                "sector": info[1],
                "marketcap": float(info[2]) if info[2] else None,
                "profitmargins": float(info[3]) if info[3] else None,
                "week52change": float(info[4]) if info[4] else None
            }
        else:
            company_data = None

        # --- 2. Candlestick Data ---
        cursor.execute("""
            SELECT [Date], [Open], [High], [Low], [Close]
            FROM StockData
            WHERE [Ticker] = ?
            ORDER BY [Date] ASC
        """, ticker)
        rows = cursor.fetchall()
        if rows:
            candle_chart_data = [
                {
                    "x": row.Date.strftime("%Y-%m-%d"),
                    "o": float(row.Open),
                    "h": float(row.High),
                    "l": float(row.Low),
                    "c": float(row.Close)
                }
                for row in rows
            ]
        else:
            candle_chart_data = [
                {"x": "2025-08-20", "o": 100, "h": 110, "l": 95, "c": 105},
                {"x": "2025-08-21", "o": 105, "h": 115, "l": 100, "c": 110},
            ]

        # --- 3. Prediction vs Actual ---
        cursor.execute("""
            SELECT TOP 1 [Predicted_Closing_Price], [Actual_Closing_Price]
            FROM Prediction_vs_Actual
            WHERE [Company] = ?
            ORDER BY [Date] DESC
        """, company_name)
        result = cursor.fetchone()

        if result:
            predicted_price = float(result.Predicted_Closing_Price)
            actual_price = float(result.Actual_Closing_Price)
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

        # --- 4. Latest Prediction ---
        cursor.execute("""
            SELECT TOP 1 [Predicted_Closing_Price], [Prediction_Date]
            FROM Final_Analysis
            WHERE [Company] = ?
            ORDER BY [Prediction_Date] DESC
        """, company_name)
        latest = cursor.fetchone()
        if latest:
            latest_prediction = {
                "closing_price": round(float(latest.Predicted_Closing_Price), 2),
                "date": latest.Prediction_Date.strftime("%Y-%m-%d")
            }
        else:
            latest_prediction = {"closing_price": "N/A", "date": "N/A"}

        return render_template(
            "company_info.html",
            company_name=company_name.upper(),
            company_data=company_data,
            candle_chart_data=candle_chart_data,
            prediction_bar_data=prediction_bar_data,
            latest_prediction=latest_prediction
        )

    except Exception as e:
        return f"<h2>Error loading data for {company_name}: {e}</h2>"

    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass

def get_predictions():
    predictions = []
    prediction_date = ""
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get latest Prediction Date
        cursor.execute("SELECT MAX(Prediction_Date) FROM Final_Analysis")
        latest_date_row = cursor.fetchone()

        if latest_date_row:
            latest_date = latest_date_row[0]  # datetime object
            prediction_date = latest_date.strftime("%d %B %Y")  # Example: 05 August 2025

        # Fetch predictions for the most recent Prediction Date
        cursor.execute("""
            SELECT Ticker, Predicted_Closing_Price
            FROM Final_Analysis
            WHERE Prediction_Date = ?
        """, latest_date)
        
        for row in cursor.fetchall():
            predictions.append({
                "ticker": row[0],
                "price": row[1]
            })

    except Exception as e:
        print(f"[get_predictions] error: {e}")

    finally:
        try: cursor.close()
        except: pass
        try: conn.close()
        except: pass

    return predictions, prediction_date


def get_my_date():
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get latest date from Prediction_vs_Actual table
        cursor.execute("SELECT MAX(Date) FROM Prediction_vs_Actual")
        latest_date_row = cursor.fetchone()
        latest_date = latest_date_row[0] if latest_date_row else None
        
        return latest_date


@app.route('/prediction-vs-actual')
def prediction_vs_actual():
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Get latest date from Prediction_vs_Actual table
        cursor.execute("SELECT MAX(Date) FROM Prediction_vs_Actual")
        latest_date_row = cursor.fetchone()
        latest_date = latest_date_row[0] if latest_date_row else None
        
        if not latest_date:
            return jsonify([])

        # Get prediction vs actual values for the latest date
        cursor.execute("""
            SELECT Company, Ticker, Predicted_Closing_Price, Actual_Closing_Price 
            FROM Prediction_vs_Actual 
            WHERE Date = ?
        """, latest_date)

        data = [
    {
        'company': row.Company,
        'ticker': row.Ticker,
        'predicted': round(row.Predicted_Closing_Price, 2),
        'actual': round(row.Actual_Closing_Price, 2)
    }
    for row in cursor.fetchall()
]
        return jsonify(data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
    'TATACONSUM.NS', 'M&M.NS', 'HAL.NS', 'DLF.NS'
    ] 

@app.route('/run/prediction')
def run_actual_vs_prediction():
    try:
        subprocess.run(
            ["python", os.path.join(SCRIPT_DIR, "Actual_vs_Prediction.py")], check=True
        )
        return "<h2>✅ Actual vs Prediction ran successfully.</h2>"
    except Exception as e:
        return f"<h2>❌ Error running Actual vs Prediction: {e}</h2>"

@app.route('/run/final')
def run_final_analysis():
    try:
        subprocess.run(
            ["python", os.path.join(SCRIPT_DIR, "Final_Analysis.py")], check=True
        )
        return "<h2>✅ Final Analysis ran successfully.</h2>"
    except Exception as e:
        return f"<h2>❌ Error running Final Analysis: {e}</h2>"

@app.route('/run/news')
def run_news_extractor():
    try:
        subprocess.run(
            ["python", os.path.join(SCRIPT_DIR, "News_Extractor.py")], check=True
        )
        return "<h2>✅ News Extractor ran successfully.</h2>"
    except Exception as e:
        return f"<h2>❌ Error running News Extractor: {e}</h2>"

@app.route('/run/sentiment')
def run_sentiment_analyzer():
    try:
        subprocess.run(
            ["python", os.path.join(SCRIPT_DIR, "Sentiment_Analyzer.py")], check=True
        )
        return "<h2>✅ Sentiment Analyzer ran successfully.</h2>"
    except Exception as e:
        return f"<h2>❌ Error running Sentiment Analyzer: {e}</h2>"

@app.route('/run/stockdata')
def run_stock_data():
    try:
        subprocess.run(
            ["python", os.path.join(SCRIPT_DIR, "Stock_Data_Daily.py")], check=True
        )
        return "<h2>✅ Latest Stock Data Inserted.</h2>"
    except Exception as e:
        return f"<h2>❌ Error running Stock Data Daily: {e}</h2>"
# --- QUERY ---

# --- Email setup ---
EMAIL_SENDER = "sbahuguna2007@gmail.com"
EMAIL_PASSWORD = "rgdf glsr mmxr bgeb"  
EMAIL_RECEIVER = "sbahuguna2007@gmail.com"

# --- /query route ---
@app.route("/query", methods=["GET", "POST"])
def query_page():
    results, columns, error, message = None, None, None, None
    scheduled_queries = []

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # --- Fetch existing scheduled queries for modal ---
        cursor.execute("""
            SELECT Id, Sender, Receiver, Query, Status, ScheduledTime 
            FROM ScheduledQueries 
            ORDER BY ScheduledTime DESC
        """)
        rows = cursor.fetchall()
        for row in rows:
            scheduled_queries.append({
                "id": str(row[0]),
                "sender": row[1],
                "receiver": row[2],
                "query": row[3],
                "status": row[4],
                "scheduled_time": row[5].strftime("%Y-%m-%d %H:%M") if row[5] else "-"
            })

        if request.method == "POST":
            query = request.form.get("query")
            schedule_type = request.form.get("schedule_type", "now")
            schedule_time = request.form.get("run_schedule_time")
            action = request.form.get("action")

            # --- Schedule query ---
            if schedule_type != "now" and action == "schedule":
                if not schedule_time:
                    error = "Schedule time is required."
                else:
                    try:
                        run_at = datetime.strptime(schedule_time, "%Y-%m-%dT%H:%M")
                        job_id = str(uuid.uuid4())

                        # Schedule APScheduler job
                        scheduler.add_job(
                            func=lambda q=query, job_id=job_id: run_scheduled_query(q, job_id),
                            trigger="date",
                            run_date=run_at,
                            id=job_id
                        )

                        # Insert into DB
                        cursor.execute("""
                            INSERT INTO ScheduledQueries (Id, Sender, Receiver, Query, Status, ScheduledTime)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, job_id, EMAIL_SENDER, EMAIL_RECEIVER, query, "Scheduled", run_at)
                        conn.commit()

                        message = f"✅ Query scheduled for {run_at.strftime('%Y-%m-%d %H:%M')}"
                        scheduled_queries.insert(0, {
                            "id": job_id,
                            "sender": EMAIL_SENDER,
                            "receiver": EMAIL_RECEIVER,
                            "query": query,
                            "status": "Scheduled",
                            "scheduled_time": run_at.strftime("%Y-%m-%d %H:%M")
                        })
                    except Exception as e:
                        error = f"Error scheduling query: {e}"

            # --- Run immediately ---
            else:
                try:
                    cursor.execute(query)
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        results = cursor.fetchall()
                    else:
                        conn.commit()
                        results = [["Query executed successfully."]]
                        columns = ["Message"]
                except Exception as e:
                    error = f"Error executing query: {e}"

    except Exception as e:
        error = f"DB connection error: {e}"

    finally:
        try:
            conn.close()
        except:
            pass

    return render_template(
        "query.html",
        results=results,
        columns=columns,
        error=error,
        message=message,
        scheduled_queries=scheduled_queries
    )

def send_email(subject, body_html, attachment=None):
    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER

        # Attach the HTML body
        msg.attach(MIMEText(body_html, "html"))

        # Attach file if provided
        if attachment:
            msg.attach(attachment)

        # Send via SMTP
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

    except Exception as e:
        print(f"❌ Error sending email: {e}")

# --- Run scheduled query and update DB ---
def run_scheduled_query(query, job_id):
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute(query)

        # Handle SELECT queries
        if cursor.description:
            try:
                rows = cursor.fetchall()
                if rows:
                    columns = [col[0] for col in cursor.description]
                    df = pd.DataFrame.from_records(rows, columns=columns)

                    # Save to Excel in memory
                    excel_buffer = BytesIO()
                    df.to_excel(excel_buffer, index=False)
                    excel_buffer.seek(0)

                    # Attach Excel file
                    part = MIMEApplication(excel_buffer.read(), Name=f"Query_{job_id}.xlsx")
                    part['Content-Disposition'] = f'attachment; filename="Query_{job_id}.xlsx"'

                    # Send email with attachment
                    send_email("Scheduled Query Results", "<p> Report </p>", attachment=part)
                else:
                    send_email("Scheduled Query Results", "<p> Report </p>")
            except pyodbc.ProgrammingError:
                send_email("Scheduled Query Results", "<p> Report </p>")
        else:
            # Non-SELECT statements
            conn.commit()
            send_email("Scheduled Query Results", "<p> Report </p>")

        # Update status
        cursor.execute("UPDATE ScheduledQueries SET Status = ? WHERE Id = ?", "Ran", job_id)
        conn.commit()

    except Exception as e:
        try:
            cursor.execute("UPDATE ScheduledQueries SET Status = ? WHERE Id = ?", "Failed", job_id)
            conn.commit()
        except:
            pass
        send_email("Scheduled Query Failed", f"<p style='color:red;'>❌ {e}</p>")

    finally:
        conn.close()

# --- Fetch scheduled queries ---

@app.route("/get-scheduled-queries")
def get_scheduled_queries():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT Id, Sender, Receiver, Query, Status, ScheduledTime FROM ScheduledQueries ORDER BY ScheduledTime DESC")
    rows = cursor.fetchall()
    conn.close()

    data = []
    for row in rows:
        data.append({
            "id": str(row[0]),
            "sender": row[1],
            "receiver": row[2],
            "query": row[3],
            "status": row[4],
            "scheduled_time": row[5].strftime("%Y-%m-%d %H:%M") if row[5] else "-"
        })
    return jsonify(data)

# ---------- Analytics Page ----------
@app.route("/analytics")
def analytics_page():
    company_query = request.args.get("company", "").lower()
    
    # Default top 5 companies
    default_companies = ["reliance", "tcs", "infy", "hdfcbank", "icicibank"]

    # Determine which companies to show
    if company_query in ticker_map:
        companies = [company_query]
    else:
        companies = default_companies

    stock_data = {}

    for comp in companies:
        ticker = ticker_map[comp]
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m")
            hist = hist.reset_index()
            datetime_col = "Datetime" if "Datetime" in hist.columns else "Date"
            hist[datetime_col] = pd.to_datetime(hist[datetime_col])

            # Prepare OHLC data for candlestick chart
            ohlc = [
                {
                    "x": row[datetime_col].strftime("%H:%M"),
                    "o": round(row["Open"], 2),
                    "h": round(row["High"], 2),
                    "l": round(row["Low"], 2),
                    "c": round(row["Close"], 2)
                }
                for _, row in hist.iterrows()
            ]
            stock_data[comp] = {"ohlc": ohlc}

        except Exception as e:
            print(f"[analytics] Error fetching {comp} ({ticker}): {e}")
            stock_data[comp] = {"ohlc": []}

    return render_template(
        "analytics.html",
        companies=companies,
        stock_data=stock_data
    )

# ---------- Single Ticker Per-Minute ----------
@app.route("/api/live-price/<ticker>")
def live_price(ticker):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d", interval="1m")
        if data.empty:
            return jsonify({"error": "No data available"}), 404

        data = data.reset_index()
        datetime_col = "Datetime" if "Datetime" in data.columns else "Date"
        data[datetime_col] = pd.to_datetime(data[datetime_col])

        ohlc = [
            {
                "time": row[datetime_col].strftime("%H:%M"),
                "o": round(row["Open"], 2),
                "h": round(row["High"], 2),
                "l": round(row["Low"], 2),
                "c": round(row["Close"], 2)
            }
            for _, row in data.iterrows()
        ]
        return jsonify(ohlc)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
# ---------- Multiple Tickers Per-Minute ----------
@app.route("/api/live-prices")
def live_prices():
    tickers = request.args.get("tickers", "").split(",")
    results = {}

    for t in tickers:
        try:
            stock = yf.Ticker(t)
            data = stock.history(period="1d", interval="1m")
            if data.empty:
                results[t] = {"error": "No data available"}
                continue

            data = data.reset_index()
            datetime_col = "Datetime" if "Datetime" in data.columns else "Date"
            data[datetime_col] = pd.to_datetime(data[datetime_col])

            prices = [
                {"time": row[datetime_col].strftime("%H:%M"), "price": round(row["Close"], 2)}
                for _, row in data.iterrows()
            ]
            results[t] = prices
        except Exception as e:
            results[t] = {"error": str(e)}

    return jsonify(results)

# --- KEY CHECKING LOGIC ---

VALID_KEY = '217621'
@app.route("/key",methods=["GET","POST"])
def key_check():
    if request.method=="POST":
        entered_key = request.form.get("access_key")
        if entered_key == VALID_KEY:
            return redirect(url_for("query_page"))
        else:
            return render_template("key.html", error="Please recheck credentials")
    return render_template("key.html")

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host="0.0.0.0", port=5000, debug=True)

