from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv, find_dotenv
import os
import time
import re
import requests
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

load_dotenv(find_dotenv())

output_file = os.getenv("NEWS_FILE")

# Mapping of canonical company name to valid aliases
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
    "divis labs": ["divis labs", "divi's", "divi’s laboratories", "divis laboratories"],
    "eicher motors": ["eicher motors", "eicher", "royal enfield"],
    "apollo hospitals": ["apollo hospitals"],
    "grasim": ["grasim", "grasim industries"],
    "jsw steel": ["jsw steel", "jsw"],
    "tata steel": ["tata steel"],
    "tata motors": ["tata motors"],
    "dr reddy": ["dr reddy", "dr reddy's", "dr reddy's laboratories", "dr reddys"],
    "hero motocorp": ["hero motocorp"],
    "cipla": ["cipla"],
    "coal india": ["coal india"],
    "hdfc life": ["hdfc life", "hdfc life insurance"],
    "hindalco": ["hindalco", "hindalco industries"],
    "indusind": ["indusind", "indusind bank"],
    "bajaj auto": ["bajaj auto"],
    "britannia": ["britannia", "britannia industries"],
    "sbi life": ["sbi life", "sbi life insurance"],
    "upl": ["upl", "united phosphorous", "upl limited"],
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
    "acc": ["acc", "acc cement", "acc limited"],
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
    "pg hygiene": ["pg hygiene", "p&g hygiene", "procter and gamble hygiene", "procter & gamble"],
    "polycab": ["polycab", "polycab india"],
    "havells": ["havells", "havells india"],
    "concor": ["concor", "container corporation of india"],
    "irctc": ["irctc", "indian railway catering"],
    "trent": ["trent"],
    "tvs motor": ["tvs", "tvs motor"],
    "jubilant food": ["jubilant food", "jubilant foodworks", "domino's", "dominos"]
}

# Dedicated, high-precision search query terms to prevent starvation and false results
company_search_terms = {
    "reliance": 'Reliance Industries stock OR shares',
    "tcs": 'Tata Consultancy Services OR TCS shares',
    "infosys": 'Infosys stock OR shares',
    "hdfc bank": 'HDFC Bank stock OR shares',
    "icici bank": 'ICICI Bank stock OR shares',
    "kotak bank": 'Kotak Mahindra Bank stock OR shares',
    "hcl": 'HCL Technologies OR HCLTech shares',
    "l&t": 'Larsen & Toubro OR "L&T" stock OR shares',
    "itc": 'ITC Limited OR "ITC" shares OR results',
    "sbi": 'State Bank of India OR SBIN stock OR shares',
    "bharti airtel": 'Bharti Airtel stock OR shares',
    "asian paints": 'Asian Paints stock OR shares',
    "bajaj finance": 'Bajaj Finance stock OR shares',
    "bajaj finserv": 'Bajaj Finserv stock OR shares',
    "hindustan unilever": 'Hindustan Unilever OR HUL stock',
    "maruti": 'Maruti Suzuki stock OR shares',
    "nestle": 'Nestle India stock OR shares',
    "ntpc": 'NTPC stock OR shares',
    "ongc": 'ONGC stock OR shares',
    "power grid": 'Power Grid Corporation stock OR shares',
    "titan": 'Titan Company stock OR shares',
    "ultratech cement": 'UltraTech Cement stock OR shares',
    "wipro": 'Wipro stock OR shares',
    "tech mahindra": 'Tech Mahindra stock OR shares',
    "sun pharma": 'Sun Pharma stock OR shares',
    "adani enterprises": 'Adani Enterprises stock OR shares',
    "divis labs": 'Divis Laboratories OR "Divi\'s" stock OR shares',
    "eicher motors": 'Eicher Motors OR "Royal Enfield" shares',
    "apollo hospitals": 'Apollo Hospitals stock OR shares',
    "grasim": 'Grasim Industries stock OR shares',
    "jsw steel": 'JSW Steel stock OR shares',
    "tata steel": 'Tata Steel stock OR shares',
    "tata motors": 'Tata Motors stock OR shares',
    "dr reddy": 'Dr Reddys Laboratories stock OR shares',
    "hero motocorp": 'Hero MotoCorp stock OR shares',
    "cipla": 'Cipla stock OR shares',
    "coal india": 'Coal India stock OR shares',
    "hdfc life": 'HDFC Life Insurance stock OR shares',
    "hindalco": 'Hindalco Industries stock OR shares',
    "indusind": 'IndusInd Bank stock OR shares',
    "bajaj auto": 'Bajaj Auto stock OR shares',
    "britannia": 'Britannia Industries stock OR shares',
    "sbi life": 'SBI Life Insurance stock OR shares',
    "upl": 'UPL Limited stock OR shares',
    "axis bank": 'Axis Bank stock OR shares',
    "shree cement": 'Shree Cement stock OR shares',
    "tata consumer": 'Tata Consumer Products stock OR shares',
    "mahindra": 'Mahindra & Mahindra OR "M&M" stock OR shares',
    "hal": 'Hindustan Aeronautics OR "HAL" shares OR defence',
    "dlf": 'DLF Limited stock OR shares',
    "adani ports": 'Adani Ports stock OR shares',
    "abb": 'ABB India stock OR shares',
    "adani green": 'Adani Green Energy stock OR shares',
    "adani power": 'Adani Power stock OR shares',
    "ambuja cement": 'Ambuja Cements stock OR shares',
    "bajaj holdings": 'Bajaj Holdings stock OR shares',
    "bank of baroda": 'Bank of Baroda stock OR shares',
    "bpcl": 'Bharat Petroleum OR BPCL stock OR shares',
    "bosch": 'Bosch Limited stock OR shares',
    "canara bank": 'Canara Bank stock OR shares',
    "acc": 'ACC Cement OR "ACC Limited" stock OR shares',
    "dmart": 'Avenue Supermarts OR DMart stock OR shares',
    "bandhan bank": 'Bandhan Bank stock OR shares',
    "biocon": 'Biocon stock OR shares',
    "cholamandalam": 'Cholamandalam Investment OR "Chola Finance" shares',
    "colgate": 'Colgate Palmolive India stock OR shares',
    "gail": 'GAIL India stock OR shares',
    "godrej consumer": 'Godrej Consumer Products stock OR shares',
    "icici lombard": 'ICICI Lombard stock OR shares',
    "icici prudential": 'ICICI Prudential Life stock OR shares',
    "indian hotels": 'Indian Hotels OR "Taj Hotels" stock OR shares',
    "indus towers": 'Indus Towers stock OR shares',
    "info edge": 'Info Edge OR Naukri stock OR shares',
    "indigo": 'InterGlobe Aviation OR IndiGo stock OR shares',
    "lic": 'Life Insurance Corporation OR "LIC" stock OR shares',
    "ltimindtree": 'LTIMindtree stock OR shares',
    "marico": 'Marico stock OR shares',
    "mphasis": 'Mphasis stock OR shares',
    "muthoot finance": 'Muthoot Finance stock OR shares',
    "paytm": 'Paytm OR One97 stock OR shares',
    "pi industries": 'PI Industries stock OR shares',
    "pidilite": 'Pidilite Industries stock OR shares',
    "sbi cards": 'SBI Cards stock OR shares',
    "srf": 'SRF Limited stock OR shares',
    "motherson": 'Samvardhana Motherson stock OR shares',
    "siemens": 'Siemens India stock OR shares',
    "tata power": 'Tata Power stock OR shares',
    "torrent pharma": 'Torrent Pharmaceuticals stock OR shares',
    "united spirits": 'United Spirits OR McDowell stock OR shares',
    "vedanta": 'Vedanta Limited stock OR shares',
    "zomato": 'Zomato stock OR shares',
    "petronet": 'Petronet LNG stock OR shares',
    "pg hygiene": 'Procter & Gamble Hygiene OR "PGHH" stock OR shares',
    "polycab": 'Polycab India stock OR shares',
    "havells": 'Havells India stock OR shares',
    "concor": 'Container Corporation of India OR CONCOR stock OR shares',
    "irctc": 'IRCTC stock OR shares',
    "trent": 'Trent Limited OR "Trent" stock OR shares',
    "tvs motor": 'TVS Motor stock OR shares',
    "jubilant food": 'Jubilant FoodWorks stock OR shares'
}

# Contextual guardrails for short or ambiguous symbols to reject false positives
AMBIGUOUS_SYMBOLS_CONTEXT = {
    "acc": ["cement", "shares", "stock", "results", "adani", "amalgamated", "quarter", "profit", "loss"],
    "hal": ["aerospace", "defence", "defense", "tejas", "aircraft", "shares", "stock", "hindustan aeronautics", "order", "contract"],
    "itc": ["tobacco", "cigarette", "fmcg", "hotels", "shares", "stock", "dividend", "q1", "q2", "q3", "q4", "results", "quarter"],
    "upl": ["crop", "chemical", "agro", "seeds", "shares", "stock", "results", "united phosphorus", "quarter", "pat"],
    "titan": ["tata", "watch", "jewel", "eyewear", "shares", "stock", "results", "q1", "q2", "q3", "q4", "quarter", "tanishq"],
    "abb": ["power", "automation", "electrification", "shares", "stock", "results", "order", "grid"],
    "dlf": ["realty", "real estate", "developer", "property", "shares", "stock", "residential", "commercial"],
    "bob": ["bank", "baroda", "shares", "stock", "lending", "npa", "quarter"],
    "srf": ["chemical", "packaging", "technical textiles", "shares", "stock", "results", "refrigerant"],
    "trent": ["westside", "zudio", "tata", "retail", "shares", "stock", "results", "stores", "fashion"]
}

def is_company_in_text(company, text):
    """
    Check if a company is legitimately mentioned in text using regex word boundaries
    and contextual disambiguation for ambiguous short symbols.
    """
    if not text:
        return False
    text_lower = text.lower()
    aliases = company_aliases.get(company, [company])

    matched = False
    for alias in aliases:
        pattern = rf"\b{re.escape(alias.lower())}\b"
        if re.search(pattern, text_lower):
            matched = True
            break

    if not matched:
        return False

    # Check contextual requirements for ambiguous short symbols
    if company in AMBIGUOUS_SYMBOLS_CONTEXT:
        required_contexts = AMBIGUOUS_SYMBOLS_CONTEXT[company]
        if not any(ctx in text_lower for ctx in required_contexts):
            return False

    return True

def normalize_headline(title):
    """Clean headline for deduplication by removing publisher names and non-alphanumeric chars."""
    if not title:
        return ""
    # Strip trailing publisher tags like " - The Economic Times", " | NDTV Profit"
    cleaned = re.sub(r'\s*[-|–—]\s*[^-|–—]+$', '', title).strip()
    cleaned = re.sub(r'[^\w\s]', '', cleaned.lower())
    return " ".join(cleaned.split())

def extract_and_sync_news():
    ist = timezone(timedelta(hours=5, minutes=30))
    cutoff_date = (datetime.now(ist) - timedelta(days=30)).replace(tzinfo=None)
    all_articles = []
    seen_titles = set()

    # ==================== Source 1: Google News RSS (Guarantees Balanced Coverage Per Company) ====================
    print("Fetching company-specific articles via Google News RSS (unlimited, balanced coverage)...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    for idx, (company, query_term) in enumerate(company_search_terms.items(), 1):
        try:
            encoded_query = urllib.parse.quote(query_term)
            rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
            req = urllib.request.Request(rss_url, headers=headers)

            with urllib.request.urlopen(req, timeout=10) as resp:
                xml_content = resp.read()
                root = ET.fromstring(xml_content)

                company_articles_count = 0
                for item in root.findall(".//item")[:20]: # Up to 20 top articles per company
                    title = (item.findtext("title") or "").strip()
                    link = (item.findtext("link") or "").strip()
                    pub_date_str = (item.findtext("pubDate") or "").strip()
                    desc_raw = (item.findtext("description") or "").strip()
                    desc_clean = re.sub(r"<[^>]+>", "", desc_raw).strip()

                    source_elem = item.find("source")
                    source_name = source_elem.text.strip() if source_elem is not None and source_elem.text else "Google News"

                    try:
                        pub_date_obj = parsedate_to_datetime(pub_date_str).astimezone(ist).replace(tzinfo=None)
                    except Exception:
                        pub_date_obj = datetime.now(ist).replace(tzinfo=None)

                    if pub_date_obj < cutoff_date:
                        continue

                    content = desc_clean if len(desc_clean) >= 40 else title
                    combined_text = f"{title} {desc_clean}"

                    # Verify that the article is genuinely about this company
                    if not is_company_in_text(company, combined_text):
                        continue

                    # Deduplicate identical syndicated headlines
                    norm_title = normalize_headline(title)
                    dedup_key = (company, norm_title)
                    if dedup_key in seen_titles:
                        continue
                    seen_titles.add(dedup_key)

                    all_articles.append({
                        "Company": company.strip().lower(),
                        "Content": content,
                        "PublicationDate": pub_date_obj,
                        "Source": source_name,
                        "Link": link
                    })
                    company_articles_count += 1

            if idx % 15 == 0 or idx == len(company_search_terms):
                print(f"  Processed RSS for {idx}/{len(company_search_terms)} companies... (Total scraped: {len(all_articles)})")

            time.sleep(0.1) # Brief pause to be respectful
        except Exception as e:
            print(f"  RSS search failed for '{company}': {e}")

    print(f"RSS extraction complete: {len(all_articles)} verified articles across companies.\n")

    # ==================== Source 2: GNews API (Supplementary Booster if API Key Available) ====================
    gnews_api_key = os.getenv("GNEWS_API_KEY")

    if gnews_api_key:
        print("Fetching supplementary articles from GNews API with financial filtering...")
        queries = []
        current_chunk = []
        for company in company_aliases.keys():
            primary_term = company_aliases[company][0]
            current_chunk.append(f'"{primary_term}"')
            if len(" OR ".join(current_chunk)) > 140:
                queries.append("(" + " OR ".join(current_chunk) + ") AND (stock OR shares OR results)")
                current_chunk = []
        if current_chunk:
            queries.append("(" + " OR ".join(current_chunk) + ") AND (stock OR shares OR results)")

        for q in queries[:10]: # Limit query calls to respect GNews quota
            url = "https://gnews.io/api/v4/search"
            params = {
                "q": q,
                "lang": "en",
                "country": "in",
                "max": 50,
                "apikey": gnews_api_key
            }
            try:
                response = requests.get(url, params=params, timeout=10)
                data = response.json()
                if "articles" in data:
                    for entry in data["articles"]:
                        title = entry.get("title", "").strip()
                        link = entry.get("url", "").strip()
                        content = entry.get("content", "").strip()
                        desc = entry.get("description", "").strip()

                        if not content or len(content) < 50:
                            content = desc or title

                        source_name = entry.get("source", {}).get("name", "GNews")
                        published_at = entry.get("publishedAt", "")

                        try:
                            pub_date_obj = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                            pub_date_obj = pub_date_obj.astimezone(ist).replace(tzinfo=None)
                        except Exception:
                            pub_date_obj = datetime.now(ist).replace(tzinfo=None)

                        if pub_date_obj < cutoff_date:
                            continue

                        combined_text = f"{title} {desc} {content}"

                        # Match companies using word boundaries and context
                        for company in company_aliases.keys():
                            if is_company_in_text(company, combined_text):
                                norm_title = normalize_headline(title)
                                dedup_key = (company, norm_title)
                                if dedup_key in seen_titles:
                                    continue
                                seen_titles.add(dedup_key)

                                all_articles.append({
                                    "Company": company.strip().lower(),
                                    "Content": content,
                                    "PublicationDate": pub_date_obj,
                                    "Source": source_name,
                                    "Link": link
                                })
                time.sleep(1)
            except Exception as e:
                print(f"  GNews request failed: {e}")

    # ==================== Database Sync & Cleanup ====================
    if not all_articles:
        print("No new articles matched your companies today.")
    else:
        print(f"\nScraped {len(all_articles)} deduplicated articles across {len(set(a['Company'] for a in all_articles))} companies.")
        print("Syncing to database...")

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

            # 2. Insert new articles
            insert_query = text("""
                INSERT INTO "News" ("Company", "Content", "PublicationDate", "Source", "Link")
                VALUES (:Company, :Content, :PublicationDate, :Source, :Link)
                ON CONFLICT ("Content", "Link") DO NOTHING
            """)

            for article in all_articles:
                conn.execute(insert_query, article)

            # 3. Retention: Delete anything older than 30 days
            conn.execute(text("""
                DELETE FROM "News"
                WHERE "PublicationDate" < NOW() - INTERVAL '30 days'
            """))

        print("Successfully synced to the 'News' table and cleared memory older than 30 days!")

if __name__ == "__main__":
    extract_and_sync_news()