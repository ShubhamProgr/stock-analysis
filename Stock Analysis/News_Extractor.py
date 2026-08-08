from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import pandas as pd
import time
import re
import requests

load_dotenv()

output_file = os.getenv("NEWS_FILE")

company_aliases = {
    "reliance": ["reliance", "reliance industries", "ril"],
    "tcs": ["tcs", "tata consultancy services"],
    "infosys": ["infosys"],
    "hdfc bank": ["hdfc bank", "hdfc"],
    "icici bank": ["icici bank", "icici"],
    "kotak bank": ["kotak bank", "kotak mahindra bank", "kotak"],
    "hcl": ["hcl", "hcl technologies"],
    "l&t": ["l&t", "larsen and toubro", "larsen & toubro"],
    "itc": ["itc", "itc ltd"],
    "sbi": ["sbi", "state bank of india"],
    "bharti airtel": ["bharti airtel", "airtel"],
    "asian paints": ["asian paints"],
    "bajaj finance": ["bajaj finance"],
    "bajaj finserv": ["bajaj finserv"],
    "hindustan unilever": ["hindustan unilever", "hul"],
    "maruti": ["maruti", "maruti suzuki"],
    "nestle": ["nestle", "nestle india"],
    "ntpc": ["ntpc"],
    "ongc": ["ongc", "oil and natural gas corporation"],
    "power grid": ["power grid", "power grid corporation"],
    "titan": ["titan", "titan company"],
    "ultratech cement": ["ultratech cement", "ultratech"],
    "wipro": ["wipro", "wiproltd"],
    "tech mahindra": ["tech mahindra"],
    "sun pharma": ["sun pharma", "sun pharmaceutical"],
    "adani enterprises": ["adani enterprises", "adanient"],
    "divis labs": ["divis labs", "divi's", "divi’s laboratories"],
    "eicher motors": ["eicher motors", "eicher", "royal enfield"],
    "apollo hospitals": ["apollo hospitals"],
    "grasim": ["grasim", "grasim industries"],
    "jsw steel": ["jsw steel", "jsw"],
    "tata steel": ["tata steel"],
    "dr reddy": ["dr reddy", "dr reddy's", "dr reddy's laboratories"],
    "hero motocorp": ["hero motocorp"],
    "cipla": ["cipla"],
    "coal india": ["coal india"],
    "hdfc life": ["hdfc life", "hdfc life insurance"],
    "hindalco": ["hindalco", "hindalco industries"],
    "indusind": ["indusind", "indusind bank"],
    "bajaj auto": ["bajaj auto"],
    "britannia": ["britannia", "britannia industries"],
    "sbi life": ["sbi life", "sbi life insurance"],
    "upl": ["upl", "united phosphorous"],
    "axis bank": ["axis bank", "axis"],
    "shree cement": ["shree cement"],
    "tata consumer": ["tata consumer", "tata consumer products"],
    "mahindra": ["mahindra", "mahindra and mahindra", "m&m"],
    "hal": ["hal", "hindustan aeronautics", "hindustan aeronautics limited"],
    "dlf": ["dlf", "dlf limited"],
    "adani ports": ["adani ports", "adani ports and sez", "adaniports"],
    "abb": ["abb", "abb india"],
    "adani green": ["adani green", "adani green energy"],
    "adani power": ["adani power"],
    "ambuja cement": ["ambuja cement", "ambuja cements"],
    "bajaj holdings": ["bajaj holdings", "bajaj holdings & investment"],
    "bank of baroda": ["bank of baroda", "bob"],
    "bpcl": ["bpcl", "bharat petroleum"],
    "bosch": ["bosch", "bosch india", "bosch ltd"],
    "canara bank": ["canara bank", "canbank", "canara"],
    "acc": ["acc", "acc cement"],
    "dmart": ["dmart", "avenue supermarts"],
    "bandhan bank": ["bandhan bank", "bandhan"],
    "biocon": ["biocon"],
    "cholamandalam": ["cholamandalam", "chola finance", "cholamandalam investment"],
    "colgate": ["colgate", "colpal", "colgate palmolive"],
    "gail": ["gail", "gail india"],
    "godrej consumer": ["godrej consumer", "godrej consumer products", "godrejcp"],
    "icici lombard": ["icici lombard", "icicigi"],
    "icici prudential": ["icici prudential", "icici prudential life", "icicipru"],
    "indian hotels": ["indian hotels", "taj hotels", "indhotels"],
    "indus towers": ["indus towers"],
    "info edge": ["info edge", "naukri", "infoedge"],
    "indigo": ["indigo", "interglobe aviation"],
    "lic": ["lic", "life insurance corporation"],
    "ltimindtree": ["ltimindtree", "ltim", "lti mindtree"],
    "marico": ["marico"],
    "mphasis": ["mphasis"],
    "muthoot finance": ["muthoot finance", "muthoot"],
    "paytm": ["paytm", "one97"],
    "pi industries": ["pi industries", "piind"],
    "pidilite": ["pidilite", "pidilite industries"],
    "sbi cards": ["sbi cards", "sbi card", "sbi cards and payment"],
    "srf": ["srf"],
    "motherson": ["motherson", "samvardhana motherson"],
    "siemens": ["siemens"],
    "tata power": ["tata power"],
    "torrent pharma": ["torrent pharma", "torrent pharmaceuticals"],
    "united spirits": ["united spirits", "mcdowell", "mcdowell-n"],
    "vedanta": ["vedanta", "vedl"],
    "zomato": ["zomato"],
    "petronet": ["petronet", "petronet lng"],
    "pg hygiene": ["pg hygiene", "p&g hygiene", "procter and gamble hygiene"],
    "polycab": ["polycab", "polycab india"],
    "havells": ["havells", "havells india"],
    "concor": ["concor", "container corporation of india"],
    "irctc": ["irctc", "indian railway catering"],
    "trent": ["trent"],
    "tvs motor": ["tvs", "tvs motor"],
    "jubilant food": ["jubilant food", "jubilant foodworks", "domino's", "dominos"]
}

gnews_api_key = os.getenv("GNEWS_API_KEY")

all_articles = []
ist = timezone(timedelta(hours=5,minutes=30))
cutoff_date = (datetime.now(ist) - timedelta(days=30)).replace(tzinfo=None)

if not gnews_api_key:
    print("WARNING: GNEWS_API_KEY not found in environment variables. Please set it in .env")
else:
    # Build batched queries (GNews max query length is 512 chars)
    queries = []
    current_query = ""
    for company, aliases in company_aliases.items():
        # Using the primary alias for the search query to keep it concise
        alias_str = f'"{aliases[0]}"'
        if current_query:
            proposed = f"{current_query} OR {alias_str}"
            if len(proposed) > 400:  # keep safely under 512
                queries.append(current_query)
                current_query = alias_str
            else:
                current_query = proposed
        else:
            current_query = alias_str
    if current_query:
        queries.append(current_query)

    print(f"Formulated {len(queries)} batched queries for GNews API...")

    for q in queries:
        # We request max 100 articles per batched query to get as much data as possible
        url = f"https://gnews.io/api/v4/search?q={q}&lang=en&country=in&max=100&apikey={gnews_api_key}"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            
            if "articles" in data:
                for entry in data["articles"]:
                    title = entry.get("title", "").strip()
                    link = entry.get("url", "").strip()
                    content = entry.get("content", "").strip()
                    
                    # If content is short/missing, fallback to description, then title
                    if not content or len(content) < 50:
                        content = entry.get("description", "").strip()
                    if not content:
                        content = title

                    source_name = entry.get("source", {}).get("name", "GNews")
                    published_at = entry.get("publishedAt", "")
                    
                    try:
                        pub_date_obj = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                        pub_date_obj = pub_date_obj.astimezone(ist).replace(tzinfo=None)
                    except:
                        pub_date_obj = datetime.now(ist)
                        
                    if pub_date_obj < cutoff_date:
                        continue
                        
                    # Find which company this article is about since we batched them
                    combined_text = (title + " " + entry.get("description", "") + " " + content).lower()
                    matched_companies = [
                        company for company, aliases in company_aliases.items()
                        if any(alias.lower() in combined_text for alias in aliases)
                    ]
                    
                    # Clean up overlaps (e.g., "tata" vs "tata steel")
                    cleaned_companies = []
                    for company in matched_companies:
                        if not any(company != other and company in other for other in matched_companies):
                            cleaned_companies.append(company)
                            
                    for company in cleaned_companies:
                        all_articles.append({
                            "Company": company.strip().lower(),
                            "Content": content,
                            "PublicationDate": pub_date_obj,
                            "Source": source_name,
                            "Link": link
                        })
            else:
                print(f"Error or missing articles in response: {data}")
        except Exception as e:
            print(f"Request failed for query chunk: {e}")
            
        time.sleep(1) # Be nice to the API


# ==================== Database Sync & Cleanup ====================
if not all_articles:
    print("No new articles matched your companies today.")
else:
    print(f"Scraped {len(all_articles)} raw articles. Syncing to database...")
    
    # Database connection setup
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        supabase_host = os.getenv("SUPABASE_DB_HOST")
        supabase_port = os.getenv("SUPABASE_DB_PORT", "5432")
        supabase_name = os.getenv("SUPABASE_DB_NAME", "postgres")
        supabase_user = os.getenv("SUPABASE_DB_USER", "postgres")
        supabase_password = os.getenv("SUPABASE_DB_PASSWORD")
        supabase_sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")
        DATABASE_URL = (
            f"postgresql+psycopg2://{supabase_user}:{supabase_password}"
            f"@{supabase_host}:{supabase_port}/{supabase_name}?sslmode={supabase_sslmode}"
        )
    
    from sqlalchemy import create_engine, text
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.begin() as conn:
        # 1. Create the News table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS "News" (
                "Company" TEXT,
                "Content" TEXT,
                "PublicationDate" TIMESTAMP,
                "Source" TEXT,
                "Link" TEXT,
                UNIQUE ("Content", "Link")
            )
        """))

        # 2. Insert new articles (ignore if we already scraped this exact article)
        insert_query = text("""
            INSERT INTO "News" ("Company", "Content", "PublicationDate", "Source", "Link")
            VALUES (:Company, :Content, :PublicationDate, :Source, :Link)
            ON CONFLICT ("Content", "Link") DO NOTHING
        """)
        
        for article in all_articles:
            conn.execute(insert_query, article)

        # 3. The 7-Day Memory Clean: Delete anything older than 7 days
        conn.execute(text("""
            DELETE FROM "News"
            WHERE "PublicationDate" < NOW() - INTERVAL '30 days'
        """))

    print("Successfully synced to the 'News' table and cleared old memory!")