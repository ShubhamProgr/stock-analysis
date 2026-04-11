from dotenv import load_dotenv
import os
import pandas as pd
import re
import pyodbc
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

load_dotenv()

# ==================== Configuration ====================
input_file = os.getenv("NEWS_FILE")
SENTIMENT_OUTPUT_FILE = os.getenv("SENTIMENT_OUTPUT_FILE")
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_DRIVER = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

# Company to Ticker mapping
COMPANY_TICKERS = {
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
    "power grid": "POWERGRID.NS",
    "titan": "TITAN.NS",
    "adani ports": "ADANIPORTS.NS",
    "cholamandalam": "CHOLAFIN.NS",
    "colgate": "COLPAL.NS",
    "godrej consumer": "GODREJCP.NS",
    "icici lombard": "ICICIGI.NS",
    "icici prudential": "ICICIPRULI.NS",
    "indian hotels": "INDHOTEL.NS",
    "indus towers": "INDUSTOWER.NS",
    "info edge": "NAUKRI.NS",
    "lic": "LICI.NS",
    "ltimindtree": "LTIM.NS",
    "ltim": "LTIM.NS",
    "pg hygiene": "PGHH.NS",
    "abb": "ABB.NS",
    "adani green": "ADANIGREEN.NS",
    "adani power": "ADANIPOWER.NS",
    "ambuja cement": "AMBUJACEM.NS",
    "bajaj holdings": "BAJAJHLDNG.NS",
    "bank of baroda": "BANKBARODA.NS",
    "bosch": "BOSCHLTD.NS",
    "canara bank": "CANBK.NS",
    "acc": "ACC.NS",
    "dmart": "DMART.NS",
    "bandhan bank": "BANDHANBNK.NS",
    "biocon": "BIOCON.NS",
    "gail": "GAIL.NS",
    "marico": "MARICO.NS",
    "mphasis": "MPHASIS.NS",
    "muthoot finance": "MUTHOOTFIN.NS",
    "paytm": "PAYTM.NS",
    "pi industries": "PIIND.NS",
    "pidilite": "PIDILITIND.NS",
    "sbi cards": "SBICARD.NS",
    "srf": "SRF.NS",
    "motherson": "MOTHERSON.NS",
    "siemens": "SIEMENS.NS",
    "tata power": "TATAPOWER.NS",
    "torrent pharma": "TORNTPHARM.NS",
    "united spirits": "MCDOWELL-N.NS",
    "vedanta": "VEDL.NS",
    "zomato": "ZOMATO.NS",
    "petronet": "PETRONET.NS",
    "polycab": "POLYCAB.NS",
    "havells": "HAVELLS.NS",
    "concor": "CONCOR.NS",
    "irctc": "IRCTC.NS",
    "trent": "TRENT.NS",
    "tvs motor": "TVSMOTOR.NS",
    "jubilant food": "JUBLFOOD.NS",
    "hal": "HAL.NS",
    "dlf": "DLF.NS"
}

# ==================== Helper Functions ====================
def get_ticker(company_name):
    """Get ticker for a company name, handling case and whitespace variations."""
    company_normalized = company_name.strip().lower()
    return COMPANY_TICKERS.get(company_normalized, None)

def chunk_text(text, words_per_chunk=100):
    """Split text into chunks for processing."""
    words = text.split()
    if not words:
        return []
    return [" ".join(words[i:i+words_per_chunk]) for i in range(0, len(words), words_per_chunk)]

def clean_text(text):
    """Clean text by removing extra whitespace and special characters."""
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def analyze_sentiment(chunks, sentiment_analyzer):
    """Analyze sentiment for a list of text chunks."""
    cumulative_scores = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
    
    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            sentiment = sentiment_analyzer(chunk[:512])[0]
            label = sentiment['label'].upper()
            score = sentiment['score']
            cumulative_scores[label] += score
        except Exception as e:
            print(f"⚠️ Error analyzing chunk: {str(e)[:60]}")
            continue
    
    # Calculate overall sentiment
    total_chunks = len(chunks)
    if total_chunks == 0:
        return "NEUTRAL", 0.0
    
    overall_label = max(cumulative_scores, key=cumulative_scores.get)
    overall_score = cumulative_scores[overall_label] / total_chunks
    
    return overall_label, overall_score

# ==================== Main Process ====================
try:
    # Load data
    print("📂 Loading Excel file...")
    df = pd.read_excel(input_file)
    
    # Validate columns
    required_columns = ["Company", "Content"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"❌ Column '{col}' not found in Excel!")
    
    print(f"✅ Loaded {len(df)} rows from Excel\n")
    
    # Count articles per company
    article_counts = df.groupby("Company").size().to_dict()
    
    # Group and clean content
    company_paragraphs = df.groupby("Company")["Content"].apply(lambda x: " ".join(x)).to_dict()
    
    cleaned_paragraphs = {}
    for company, text in company_paragraphs.items():
        cleaned_paragraphs[company] = clean_text(text)
    
    # Load sentiment model
    print("🤖 Loading FinBERT sentiment model...")
    finbert_model = "yiyanghkust/finbert-tone"
    tokenizer = BertTokenizer.from_pretrained(finbert_model)
    model = BertForSequenceClassification.from_pretrained(finbert_model)
    sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    print("✅ Model loaded\n")
    
    # Analyze sentiment for each company
    print("📊 Analyzing sentiments...")
    results = []
    missing_tickers = []
    
    for company, paragraph in cleaned_paragraphs.items():
        if not paragraph.strip():
            print(f"⚠️ Skipping empty content for {company}")
            continue
        
        # Get ticker
        ticker = get_ticker(company)
        if ticker is None:
            missing_tickers.append(company)
            print(f"❌ No ticker mapping for: '{company}'")
        
        # Chunk and analyze
        chunks = chunk_text(paragraph, words_per_chunk=100)
        if not chunks:
            print(f"⚠️ No valid chunks for {company}")
            continue
        
        overall_label, overall_score = analyze_sentiment(chunks, sentiment_analyzer)
        
        results.append({
            "Company": company,
            "Ticker": ticker,
            "ArticleCount": article_counts.get(company, 0),
            "Paragraph": paragraph,
            "Sentiment": overall_label,
            "Score": round(overall_score, 4)
        })
        
        print(f"✅ {company}: {overall_label} (Score: {overall_score:.4f})")
    
    # Export to Excel
    print(f"\n📁 Saving to Excel...")
    results_df = pd.DataFrame(results)
    results_df.to_excel(SENTIMENT_OUTPUT_FILE, index=False, engine='openpyxl')
    print(f"✅ Excel saved: {SENTIMENT_OUTPUT_FILE} ({len(results)} rows)")
    
    # Save to SQL Server
    print(f"\n🗄️ Syncing to SQL Server...")
    conn_str = (
        f"DRIVER={{{MSSQL_DRIVER}}};"
        f"SERVER={MSSQL_SERVER};"
        f"DATABASE={MSSQL_DATABASE};"
        f"Trusted_Connection=yes;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Create table with PRIMARY KEY
    cursor.execute("""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Company_FinBERT_Sentiments'
    )
    CREATE TABLE Company_FinBERT_Sentiments (
        [Company] NVARCHAR(255) NOT NULL PRIMARY KEY,
        [Ticker] NVARCHAR(50),
        [ArticleCount] INT,
        [Paragraph] NVARCHAR(MAX),
        [Sentiment] NVARCHAR(50),
        [Score] FLOAT
    )
    """)
    conn.commit()
    
    # Insert data (delete then insert per company for consistency)
    delete_query = "DELETE FROM Company_FinBERT_Sentiments WHERE [Company] = ?"
    insert_query = """
    INSERT INTO Company_FinBERT_Sentiments 
    ([Company], [Ticker], [ArticleCount], [Paragraph], [Sentiment], [Score])
    VALUES (?, ?, ?, ?, ?, ?)
    """
    
    for row in results:
        cursor.execute(delete_query, row["Company"])
        cursor.execute(insert_query,
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
    print(f"✅ SQL Server synced: {len(results)} rows")
    
    # Summary
    print("\n" + "="*70)
    print("📋 SUMMARY")
    print("="*70)
    print(f"✅ Total companies processed: {len(results)}")
    print(f"❌ Companies with missing tickers: {len(missing_tickers)}")
    
    if missing_tickers:
        print(f"\n⚠️ Missing ticker mappings for:")
        for company in missing_tickers:
            print(f"   - {company}")
    
    print(f"\n✅ Sentiment analysis complete!")
    print("="*70)

except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
