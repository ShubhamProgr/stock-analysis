import pandas as pd
import re
import pyodbc
import math
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

input_file = r"C:\Users\PC\Documents\News.xlsx"
df = pd.read_excel(input_file)

company_tickers = {
    "reliance": "RELIANCE.NS",
    "tcs": "TCS.NS",
    "infosys": "INFY.NS",
    "hdfc bank": "HDFCBANK.NS",
    "icici bank": "ICICIBANK.NS",
    "kotak bank": "KOTAKBANK.NS",
    "hcl": "HCLTECH.NS",
    "l&t": "LT.NS",
    "itc": "ITC.NS",
    "sbi": "SBIN.NS",
    "bharti airtel": "BHARTIARTL.NS",
    "asian paints": "ASIANPAINT.NS",
    "bajaj finance": "BAJFINANCE.NS",
    "bajaj finserv": "BAJAJFINSV.NS",
    "hindustan unilever": "HINDUNILVR.NS",
    "maruti": "MARUTI.NS",
    "nestle": "NESTLEIND.NS",
    "ntpc": "NTPC.NS",
    "ongc": "ONGC.NS",
    "hdfc life": "HDFCLIFE.NS",
    "sbi life": "SBILIFE.NS",
    "sun pharma": "SUNPHARMA.NS",
    "dr reddy": "DRREDDY.NS",
    "divis labs": "DIVISLAB.NS",
    "cipla": "CIPLA.NS",
    "wipro": "WIPRO.NS",
    "tech mahindra": "TECHM.NS",
    "tata motors": "TATAMOTORS.NS",
    "tata steel": "TATASTEEL.NS",
    "jsw steel": "JSWSTEEL.NS",
    "ultratech cement": "ULTRACEMCO.NS",
    "grasim": "GRASIM.NS",
    "indusind": "INDUSINDBK.NS",
    "axis bank": "AXISBANK.NS",
    "mahindra": "M&M.NS",
    "hero motocorp": "HEROMOTOCO.NS",
    "bajaj auto": "BAJAJ-AUTO.NS",
    "eicher motors": "EICHERMOT.NS",
    "britannia": "BRITANNIA.NS",
    "tata consumer": "TATACONSUM.NS",
    "upl": "UPL.NS",
    "apollo hospitals": "APOLLOHOSP.NS",
    "bpcl": "BPCL.NS",
    "coal india": "COALINDIA.NS",
    "hindalco": "HINDALCO.NS",
    "shree cement": "SHREECEM.NS",
    "adani enterprises": "ADANIENT.NS",
    "power grid": "POWERGRID.NS"
}

article_counts = df.groupby("Company").size().to_dict()

required_columns = ["Company", "Content"]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in Excel!")

company_paragraphs = df.groupby("Company")["Content"].apply(lambda x: " ".join(x)).to_dict()

cleaned_paragraphs = {}
for company, text in company_paragraphs.items():
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    cleaned_paragraphs[company] = text.strip()

finbert_model = "yiyanghkust/finbert-tone"
tokenizer = BertTokenizer.from_pretrained(finbert_model)
model = BertForSequenceClassification.from_pretrained(finbert_model)
sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

def chunk_text(text, words_per_chunk=100):
    words = text.split()
    return [" ".join(words[i:i+words_per_chunk]) for i in range(0, len(words), words_per_chunk)]

results = []

for company, paragraph in cleaned_paragraphs.items():
    if paragraph.strip() == "":
        continue
    
    chunks = chunk_text(paragraph, words_per_chunk=100)
    
    cumulative_scores = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
    
    for chunk in chunks:
        sentiment = sentiment_analyzer(chunk[:512])[0]
        label = sentiment['label'].upper()
        score = sentiment['score']
        cumulative_scores[label] += score
    
    overall_label = max(cumulative_scores, key=cumulative_scores.get)
    overall_score = cumulative_scores[overall_label] / len(chunks)
    
    results.append({
        "Company": company,
        "Ticker": company_tickers.get(company.lower(), None),
        "ArticleCount": article_counts.get(company, 0),
        "Paragraph": paragraph,
        "Sentiment": overall_label,
        "Score": overall_score
    })

conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("""
IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='Company_FinBERT_Sentiments' AND xtype='U'
)
CREATE TABLE Company_FinBERT_Sentiments (
    [Company] NVARCHAR(255),
    [Ticker] NVARCHAR(50),
    [ArticleCount] INT,
    [Paragraph] NVARCHAR(MAX),
    [Sentiment] NVARCHAR(50),
    [Score] FLOAT
)
""")
conn.commit()

cursor.execute("DELETE FROM Company_FinBERT_Sentiments")
conn.commit()

for row in results:
    cursor.execute("""
    INSERT INTO Company_FinBERT_Sentiments ([Company], [Ticker], [ArticleCount], [Paragraph], [Sentiment], [Score])
    VALUES (?, ?, ?, ?, ?, ?)
""",
    row["Company"],
    row["Ticker"],
    row["ArticleCount"],
    row["Paragraph"],
    row["Sentiment"],
    row["Score"]
)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Table 'Company_FinBERT_Sentiments' cleared and reinserted {len(results)} rows")
