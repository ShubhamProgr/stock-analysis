import pandas as pd
import re
import pyodbc
from transformers import BertTokenizer, BertForSequenceClassification, pipeline

# --------------------  Load News Excel --------------------
input_file = r"C:\Users\PC\Documents\S&P500_News.xlsx"
df = pd.read_excel(input_file)

# Ensure required columns exist
required_columns = ["Company", "Content"]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in Excel!")

# --------------------  Fetch S&P 500 companies from Wikipedia --------------------
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500_table = pd.read_html(sp500_url)[0]
sp500_companies = sp500_table[['Security', 'Symbol']].rename(columns={'Security':'Company', 'Symbol':'Ticker'})
sp500_companies['Company'] = sp500_companies['Company'].str.lower().str.strip()

# Create mapping: company name → ticker
company_to_ticker = dict(zip(sp500_companies['Company'], sp500_companies['Ticker']))

# Lowercase news company names for matching
df['Company_lower'] = df['Company'].str.lower().str.strip()

# Filter news to only S&P 500 companies
df = df[df['Company_lower'].isin(company_to_ticker.keys())]

# --------------------  Aggregate content per company --------------------
article_counts = df.groupby("Company_lower").size().to_dict()
company_paragraphs = df.groupby("Company_lower")["Content"].apply(lambda x: " ".join(x)).to_dict()

# --------------------  Clean text --------------------
cleaned_paragraphs = {}
for company, text in company_paragraphs.items():
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)  # remove punctuation
    cleaned_paragraphs[company] = text.strip()

# --------------------  Load FinBERT --------------------
finbert_model = "yiyanghkust/finbert-tone"
tokenizer = BertTokenizer.from_pretrained(finbert_model)
model = BertForSequenceClassification.from_pretrained(finbert_model)
sentiment_analyzer = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

# --------------------  Split text into chunks --------------------
def chunk_text(text, words_per_chunk=100):
    words = text.split()
    return [" ".join(words[i:i+words_per_chunk]) for i in range(0, len(words), words_per_chunk)]

# --------------------  Analyze sentiment per company --------------------
results = []

for company, paragraph in cleaned_paragraphs.items():
    if paragraph.strip() == "":
        continue

    chunks = chunk_text(paragraph, words_per_chunk=100)
    cumulative_scores = {"POSITIVE": 0, "NEGATIVE": 0, "NEUTRAL": 0}

    for chunk in chunks:
        sentiment = sentiment_analyzer(chunk[:512])[0]  # truncate to 512 tokens
        label = sentiment['label'].upper()
        score = sentiment['score']
        cumulative_scores[label] += score

    overall_label = max(cumulative_scores, key=cumulative_scores.get)
    overall_score = cumulative_scores[overall_label] / len(chunks)

    results.append({
        "Company": company,
        "Ticker": company_to_ticker.get(company),
        "ArticleCount": article_counts.get(company, 0),
        "Paragraph": paragraph,
        "Sentiment": overall_label,
        "Score": overall_score
    })

# --------------------  Save results to SQL Server --------------------
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=DESKTOP-UDR6P21\SQLEXPRESS;"  # change your server
    r"Database=Market_data;"
    r"UID=sa;"
    r"PWD=a;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Create table if not exists
cursor.execute("""
IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='S&P500_FinBERT_Sentiments' AND xtype='U'
)
CREATE TABLE [S&P500_FinBERT_Sentiments] (
    [Company] NVARCHAR(255),
    [Ticker] NVARCHAR(50),
    [ArticleCount] INT,
    [Paragraph] NVARCHAR(MAX),
    [Sentiment] NVARCHAR(50),
    [Score] FLOAT
)
""")
conn.commit()

# Clear old data
cursor.execute("DELETE FROM [S&P500_FinBERT_Sentiments]")
conn.commit()

# Batch insert results
insert_values = [(r["Company"], r["Ticker"], r["ArticleCount"], r["Paragraph"], r["Sentiment"], r["Score"]) for r in results]
cursor.executemany("""
INSERT INTO [S&P500_FinBERT_Sentiments] (Company, Ticker, ArticleCount, Paragraph, Sentiment, Score)
VALUES (?, ?, ?, ?, ?, ?)
""", insert_values)

conn.commit()
cursor.close()
conn.close()

print(f"✅ Table S&P500_FinBERT_Sentiments cleared and inserted {len(results)} rows")
