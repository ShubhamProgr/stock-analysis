from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import numpy as np
import re
from datetime import datetime
from sqlalchemy import create_engine, text
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

load_dotenv(find_dotenv())

# ==================== Configuration ====================
input_file = os.getenv("NEWS_FILE")
SENTIMENT_OUTPUT_FILE = os.getenv("SENTIMENT_OUTPUT_FILE")

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

def analyze_article_sentiment(article_text, sentiment_analyzer):
    """Analyze sentiment for a single article. Returns (label, confidence_score)."""
    chunks = chunk_text(article_text, words_per_chunk=100)
    if not chunks:
        return "NEUTRAL", 0.0

    cumulative_scores = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}
    valid_chunks = 0

    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            sentiment = sentiment_analyzer(chunk[:512])[0]
            label = sentiment['label'].upper()
            score = sentiment['score']
            cumulative_scores[label] += score
            valid_chunks += 1
        except Exception as e:
            print(f"  Error analyzing chunk: {str(e)[:60]}")
            continue

    if valid_chunks == 0:
        return "NEUTRAL", 0.0

    overall_label = max(cumulative_scores, key=cumulative_scores.get)
    overall_score = cumulative_scores[overall_label] / valid_chunks

    return overall_label, overall_score

# ==================== Main Process ====================
try:
    print(" Loading raw news from Supabase 'News' table...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        df = pd.read_sql('SELECT "Company", "Content", "PublicationDate" FROM "News"', conn)

    if df.empty:
        raise ValueError(" No news data found in the 'News' table!")

    print(f" Loaded {len(df)} rows from the database\n")

    # Load sentiment model
    print(" Loading FinBERT sentiment model...")
    finbert_model = "yiyanghkust/finbert-tone"
    tokenizer = BertTokenizer.from_pretrained(finbert_model)
    model = BertForSequenceClassification.from_pretrained(finbert_model)
    sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    print(" Model loaded\n")

    # ==================== Score Each Article Individually ====================
    print(" Analyzing sentiment per article...")
    article_results = []
    missing_tickers = set()

    for idx, row in df.iterrows():
        company = row['Company']
        content = clean_text(str(row['Content']))
        pub_date = row['PublicationDate']

        if not content.strip():
            continue

        # Get ticker
        ticker = get_ticker(company)
        if ticker is None:
            missing_tickers.add(company)

        # Parse publication date
        try:
            if hasattr(pub_date, 'date'):
                article_date = pub_date.date()
            else:
                article_date = pd.to_datetime(pub_date).date()
        except Exception:
            article_date = datetime.now().date()

        # Score this individual article
        label, score = analyze_article_sentiment(content, sentiment_analyzer)

        # Convert to signed score: POSITIVE → +score, NEGATIVE → -score, NEUTRAL → 0
        if label == "POSITIVE":
            signed_score = score
        elif label == "NEGATIVE":
            signed_score = -score
        else:
            signed_score = 0.0

        article_results.append({
            "Company": company,
            "Ticker": ticker,
            "Date": article_date,
            "Sentiment": label,
            "Raw_Score": score,
            "Signed_Score": signed_score,
        })

        if (idx + 1) % 25 == 0:
            print(f"  Processed {idx + 1}/{len(df)} articles...")

    print(f" Scored {len(article_results)} articles individually\n")

    if not article_results:
        raise ValueError("No articles could be scored!")

    article_df = pd.DataFrame(article_results)
    today = datetime.now().date()

    # ==================== Aggregate by Company + Date ====================
    print(" Aggregating daily sentiments...")
    daily_sentiments = []

    for (company, date), group in article_df.groupby(["Company", "Date"]):
        ticker = group["Ticker"].iloc[0]
        pos_count = int((group["Sentiment"] == "POSITIVE").sum())
        neg_count = int((group["Sentiment"] == "NEGATIVE").sum())
        neu_count = int((group["Sentiment"] == "NEUTRAL").sum())
        article_count = len(group)

        # Average signed score for the day
        avg_signed_score = group["Signed_Score"].mean()

        # Derive daily label from signed score
        if avg_signed_score > 0.05:
            daily_label = "POSITIVE"
        elif avg_signed_score < -0.05:
            daily_label = "NEGATIVE"
        else:
            daily_label = "NEUTRAL"

        daily_sentiments.append({
            "Company": company,
            "Ticker": ticker,
            "Date": date,
            "Sentiment": daily_label,
            "Score": round(avg_signed_score, 4),
            "Positive_Count": pos_count,
            "Negative_Count": neg_count,
            "Neutral_Count": neu_count,
            "Article_Count": article_count,
        })

    daily_df = pd.DataFrame(daily_sentiments)
    print(f" Generated {len(daily_df)} daily sentiment records\n")

    # ==================== Recency-Weighted Overall Sentiment ====================
    print(" Computing recency-weighted overall sentiments...")
    overall_sentiments = []

    for company, group in article_df.groupby("Company"):
        ticker = group["Ticker"].iloc[0]
        article_count = len(group)

        # Exponential decay: articles from today have weight 1.0,
        # articles from 7 days ago have weight ~0.5, 30 days ago ~0.05
        group = group.copy()
        group["days_ago"] = (today - group["Date"]).apply(lambda d: d.days if hasattr(d, 'days') else 0)
        group["weight"] = np.exp(-0.1 * group["days_ago"])

        # Weighted average of signed scores
        if group["weight"].sum() > 0:
            weighted_score = np.average(group["Signed_Score"], weights=group["weight"])
        else:
            weighted_score = 0.0

        if weighted_score > 0.05:
            overall_label = "POSITIVE"
        elif weighted_score < -0.05:
            overall_label = "NEGATIVE"
        else:
            overall_label = "NEUTRAL"

        # Reconstruct a combined paragraph for backward compatibility
        paragraph = " ".join(df[df["Company"] == company]["Content"].dropna().astype(str).tolist())

        overall_sentiments.append({
            "Company": company,
            "Ticker": ticker,
            "ArticleCount": article_count,
            "Paragraph": paragraph,
            "Sentiment": overall_label,
            "Score": round(abs(weighted_score), 4),
        })

        print(f"  {company}: {overall_label} (weighted score: {weighted_score:.4f})")

    # ==================== Write Daily Sentiments to New Table ====================
    print(f"\n Syncing daily sentiments to Supabase Postgres...")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_daily_sentiments (
                "Company" TEXT,
                "Ticker" TEXT,
                "Date" DATE,
                "Sentiment" TEXT,
                "Score" DOUBLE PRECISION,
                "Positive_Count" INTEGER,
                "Negative_Count" INTEGER,
                "Neutral_Count" INTEGER,
                "Article_Count" INTEGER,
                PRIMARY KEY ("Ticker", "Date")
            )
        """))

        # Clear old data and re-insert to stay in sync with the News table
        conn.execute(text("DELETE FROM company_daily_sentiments"))

        insert_daily = text("""
            INSERT INTO company_daily_sentiments
            ("Company", "Ticker", "Date", "Sentiment", "Score",
             "Positive_Count", "Negative_Count", "Neutral_Count", "Article_Count")
            VALUES (:company, :ticker, :date, :sentiment, :score,
                    :pos_count, :neg_count, :neu_count, :article_count)
            ON CONFLICT ("Ticker", "Date") DO UPDATE SET
                "Company" = EXCLUDED."Company",
                "Sentiment" = EXCLUDED."Sentiment",
                "Score" = EXCLUDED."Score",
                "Positive_Count" = EXCLUDED."Positive_Count",
                "Negative_Count" = EXCLUDED."Negative_Count",
                "Neutral_Count" = EXCLUDED."Neutral_Count",
                "Article_Count" = EXCLUDED."Article_Count"
        """)

        for row in daily_sentiments:
            if row["Ticker"] is None:
                continue
            conn.execute(insert_daily, {
                "company": row["Company"],
                "ticker": row["Ticker"],
                "date": row["Date"],
                "sentiment": row["Sentiment"],
                "score": row["Score"],
                "pos_count": row["Positive_Count"],
                "neg_count": row["Negative_Count"],
                "neu_count": row["Neutral_Count"],
                "article_count": row["Article_Count"],
            })

    print(f" Daily sentiments synced: {len([r for r in daily_sentiments if r['Ticker']])} rows")

    # ==================== Write Overall Sentiments (Backward Compatible) ====================
    print(f" Syncing overall sentiments to company_finbert_sentiments...")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_finbert_sentiments (
                "Company" TEXT PRIMARY KEY,
                "Ticker" TEXT,
                "ArticleCount" INTEGER,
                "Paragraph" TEXT,
                "Sentiment" TEXT,
                "Score" DOUBLE PRECISION
            )
        """))

        insert_query = text("""
            INSERT INTO company_finbert_sentiments
            ("Company", "Ticker", "ArticleCount", "Paragraph", "Sentiment", "Score")
            VALUES (:company, :ticker, :article_count, :paragraph, :sentiment, :score)
            ON CONFLICT ("Company") DO UPDATE SET
                "Ticker" = EXCLUDED."Ticker",
                "ArticleCount" = EXCLUDED."ArticleCount",
                "Paragraph" = EXCLUDED."Paragraph",
                "Sentiment" = EXCLUDED."Sentiment",
                "Score" = EXCLUDED."Score"
        """)

        for row in overall_sentiments:
            conn.execute(insert_query, {
                "company": row["Company"],
                "ticker": row["Ticker"],
                "article_count": row["ArticleCount"],
                "paragraph": row["Paragraph"],
                "sentiment": row["Sentiment"],
                "score": row["Score"],
            })

    print(f" Overall sentiments synced: {len(overall_sentiments)} rows")

    # Summary
    print("\n" + "="*70)
    print(" SUMMARY")
    print("="*70)
    print(f" Total articles scored individually: {len(article_results)}")
    print(f" Daily sentiment records created: {len(daily_df)}")
    print(f" Companies with overall sentiment: {len(overall_sentiments)}")
    print(f" Companies with missing tickers: {len(missing_tickers)}")

    if missing_tickers:
        print(f"\n Missing ticker mappings for:")
        for company in sorted(missing_tickers):
            print(f"   - {company}")

    print(f"\n Sentiment analysis complete!")
    print("="*70)

except Exception as e:
    print(f"\n ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
