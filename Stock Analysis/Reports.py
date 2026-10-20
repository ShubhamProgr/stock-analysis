import pandas as pd
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file if it exists (for local testing)
load_dotenv(find_dotenv())


def load_paper_portfolio_section(engine):
    """Build an email section from the two paper simulation tables."""
    with engine.connect() as connection:
        table_exists = connection.execute(
            text("SELECT to_regclass('paper_simulation_daily_totals')")
        ).scalar()
        if table_exists is None:
            return "Paper portfolio: simulation tables have not been initialized yet."

        latest = connection.execute(text('''
            SELECT simulation_day, trading_date, total_value, profit_loss
            FROM paper_simulation_daily_totals
            ORDER BY trading_date DESC
            LIMIT 1
        ''')).mappings().first()
        if latest is None:
            return "Paper portfolio: no simulation days have been recorded yet."

        history = connection.execute(text('''
            SELECT simulation_day, trading_date, total_value
            FROM paper_simulation_daily_totals
            ORDER BY simulation_day
        ''')).mappings().all()

        latest_investment_date = connection.execute(text('''
            SELECT MAX(investment_date) FROM paper_simulation_holdings
        ''')).scalar()
        holdings = connection.execute(text('''
            SELECT ticker, amount, result, profit_loss, next_day
            FROM paper_simulation_holdings
            WHERE investment_date = :investment_date
            ORDER BY ticker
        '''), {"investment_date": latest_investment_date}).mappings().all() if latest_investment_date else []

    initial_cash = float(history[0]["total_value"]) if history else float(latest["total_value"])
    overall_pnl = float(latest["total_value"]) - initial_cash
    lines = [
        f"Paper simulation - Day {latest['simulation_day']} ({latest['trading_date']})",
        f"Portfolio value: INR {float(latest['total_value']):,.2f}",
        f"Day profit/loss: INR {float(latest['profit_loss']):+,.2f}",
        f"Total profit/loss from INR {initial_cash:,.2f}: INR {overall_pnl:+,.2f}",
        "",
        "Daily totals:",
    ]
    lines.extend(
        f"Day {row['simulation_day']} ({row['trading_date']}): INR {float(row['total_value']):,.2f}"
        for row in history
    )
    lines.extend(["", f"Holdings (investment date: {latest_investment_date}):"])
    if holdings:
        lines.append("Ticker | Invested | Result | Profit/loss")
        for row in holdings:
            result = f"INR {float(row['result']):,.2f}" if row["result"] is not None else "Pending"
            pnl = f"INR {float(row['profit_loss']):+,.2f}" if row["profit_loss"] is not None else "Pending"
            lines.append(
                f"{row['ticker']} | INR {float(row['amount']):,.2f} | {result} | {pnl}"
            )
        pending_date = next((row["next_day"] for row in holdings if row["result"] is None), None)
        if pending_date:
            lines.append(f"Results pending for: {pending_date}")
    else:
        lines.append("No holdings have been selected yet.")
    return "\n".join(lines)

def generate_and_send_report():
    # 1. Load Data from Supabase Postgres
    db_url = os.environ.get("DATABASE_URL") 
    if not db_url:
        raise ValueError("DATABASE_URL not found in environment variables.")

    # Create the connection engine
    engine = create_engine(db_url)
    
    query = """
    SELECT "Ticker", "Prediction_Date", "Predicted_Closing_Price", "Predicted_Return_Pct" 
    FROM final_analysis
    WHERE "Prediction_Date" = (SELECT MAX("Prediction_Date") FROM final_analysis)
    ORDER BY "Predicted_Return_Pct" DESC NULLS LAST
    """
    
    # Read directly into pandas using the SQLAlchemy engine
    df = pd.read_sql_query(query, engine)

    # 2. Filter for Max Date and Sort by Return
    max_date = df['Prediction_Date'].max()
    filtered_df = df[df['Prediction_Date'] == max_date]
    
    # Sort highest return at the top
    sorted_df = filtered_df.sort_values(by='Predicted_Return_Pct', ascending=False)

    # 3. Create Temporary Excel File
    excel_filename = f"Predictions_{max_date}.xlsx"
    sorted_df.to_excel(excel_filename, index=False)
    print(f"Created temporary file: {excel_filename}")

    # 4. Email Configuration
    # Pull configuration securely from environment variables
    SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp-relay.brevo.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
    
    BREVO_LOGIN = os.environ.get("BREVO_LOGIN") 
    BREVO_PASSWORD = os.environ.get("BREVO_PASSWORD") 
    SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
    
    # Parse a comma-separated string of emails into a list
    receiver_emails_env = os.environ.get("RECEIVER_EMAILS")
    if not receiver_emails_env:
        raise ValueError("RECEIVER_EMAILS not found in environment variables.")
    RECEIVER_EMAILS = [email.strip() for email in receiver_emails_env.split(",")]

    # Validate that all required email configurations are present
    if not all([BREVO_LOGIN, BREVO_PASSWORD, SENDER_EMAIL]):
        raise ValueError(" Missing email credentials or sender configuration in environment variables.")

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECEIVER_EMAILS)
    msg['Subject'] = f"Daily Stock Report - {max_date}"

    paper_portfolio_section = load_paper_portfolio_section(engine)
    body = (
        f"Hello,\n\nAttached is the stock prediction report for {max_date}.\n\n"
        f"{paper_portfolio_section}"
    )
    msg.attach(MIMEText(body, 'plain'))

    with open(excel_filename, "rb") as f:
        attach = MIMEApplication(f.read(), _subtype="xlsx")
        attach.add_header('Content-Disposition', 'attachment', filename=excel_filename)
        msg.attach(attach)

    # Send Email
    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(BREVO_LOGIN, BREVO_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("Email sent successfully to all recipients.")
    except Exception as e:
        print(f"Failed to send email: {e}")

    # 5. Clean Up 
    if os.path.exists(excel_filename):
        os.remove(excel_filename)
        print("Temporary Excel file deleted securely.")

if __name__ == "__main__":
    generate_and_send_report()
