import pandas as pd
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file if it exists (for local testing)
load_dotenv(find_dotenv())

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

    body = f"Hello,\n \nAttached is the stock prediction report for {max_date}."
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