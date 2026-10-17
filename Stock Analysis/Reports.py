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
    """Build an email section from the latest persisted paper-trading result."""
    with engine.connect() as connection:
        table_exists = connection.execute(
            text("SELECT to_regclass('paper_portfolio_daily_results')")
        ).scalar()
        if table_exists is None:
            return "Paper portfolio: no completed simulation results yet."

        latest = connection.execute(text('''
            SELECT trading_date, invested_value, cash, total_value, day_pnl
            FROM paper_portfolio_daily_results
            WHERE account_id = :account_id
            ORDER BY trading_date DESC
            LIMIT 1
        '''), {"account_id": "default"}).mappings().first()
        account = connection.execute(text('''
            SELECT initial_cash, pending_prediction_date
            FROM paper_portfolio_account
            WHERE account_id = :account_id
        '''), {"account_id": "default"}).mappings().first()

        if account is None:
            return "Paper portfolio: simulation account has not been initialized yet."
        initial_cash = float(account["initial_cash"])
        if latest is None:
            pending_date = account["pending_prediction_date"]
            pending_line = f"Pending prediction date: {pending_date}\n" if pending_date else ""
            return (
                "Paper portfolio: no completed simulation session yet.\n"
                f"Starting balance: INR {initial_cash:,.2f}\n{pending_line}"
            )

        investments = connection.execute(text('''
            SELECT "Ticker" AS ticker,
                   "Amount" AS amount,
                   "Result" AS result,
                   "Profit_Loss" AS profit_loss
            FROM paper_investment_records
            WHERE "Next_Day" = :trading_date
              AND "Result" IS NOT NULL
            ORDER BY "Ticker"
        '''), {"trading_date": latest["trading_date"]}).mappings().all()

    overall_pnl = float(latest["total_value"]) - initial_cash
    lines = [
        f"Paper portfolio result for {latest['trading_date']}",
        f"Invested: INR {float(latest['invested_value']):,.2f}",
        f"Cash: INR {float(latest['cash']):,.2f}",
        f"Total value: INR {float(latest['total_value']):,.2f}",
        f"Day P/L: INR {float(latest['day_pnl']):+,.2f}",
        f"Overall P/L from INR {initial_cash:,.2f}: INR {overall_pnl:+,.2f}",
        "",
        "Investments with results for this session:",
    ]
    if investments:
        lines.append("Ticker | Invested | Result value | P/L")
        for row in investments:
            lines.append(
                f"{row['ticker']} | INR {float(row['amount']):,.2f} | "
                f"INR {float(row['result']):,.2f} | INR {float(row['profit_loss']):+,.2f}"
            )
    else:
        lines.append("No new positions were opened for this session.")

    pending_date = account["pending_prediction_date"]
    if pending_date:
        lines.extend(["", f"Next paper-trade date: {pending_date}"])
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
