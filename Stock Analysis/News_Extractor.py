import feedparser
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone, time
import time
import os
import re

output_file = r"C:\Users\PC\Documents\News.xlsx"
# --------------------
# COMPANY ALIASES (50 COMPANIES)
# --------------------
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
    "hindustan unilever": ["hindustan unilever"],
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
    "axis bank": ["axis bank"],
    "shree cement": ["shree cement"],
    "tata consumer": ["tata consumer", "tata consumer products"],
    "mahindra": ["mahindra", "mahindra and mahindra"],
    "hal": ["hal", "hindustan aeronautics", "hindustan aeronautics limited"],
    "dlf": ["dlf", "dlf limited"]
}

# --------------------
# RSS FEEDS
# --------------------
rss_feeds = {

    "Google News - Reliance": r'https://news.google.com/rss/search?q="reliance"%20OR%20"reliance%20industries"%20OR%20"ril"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - TCS": r'https://news.google.com/rss/search?q="tcs"%20OR%20"tata%20consultancy%20services"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Infosys": r'https://news.google.com/rss/search?q="infosys"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - HDFC Bank": r'https://news.google.com/rss/search?q="hdfc%20bank"%20OR%20"hdfc"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ICICI Bank": r'https://news.google.com/rss/search?q="icici%20bank"%20OR%20"icici"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Kotak Bank": r'https://news.google.com/rss/search?q="kotak%20bank"%20OR%20"kotak%20mahindra%20bank"%20OR%20"kotak"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - HCL": r'https://news.google.com/rss/search?q="hcl"%20OR%20"hcl%20technologies"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - L&T": r'https://news.google.com/rss/search?q="l&t"%20OR%20"larsen%20and%20toubro"%20OR%20"larsen%20&%20toubro"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ITC": r'https://news.google.com/rss/search?q="itc"%20OR%20"itc%20ltd"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - SBI": r'https://news.google.com/rss/search?q="sbi"%20OR%20"state%20bank%20of%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bharti Airtel": r'https://news.google.com/rss/search?q="bharti%20airtel"%20OR%20"airtel"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Asian Paints": r'https://news.google.com/rss/search?q="asian%20paints"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bajaj Finance": r'https://news.google.com/rss/search?q="bajaj%20finance"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bajaj Finserv": r'https://news.google.com/rss/search?q="bajaj%20finserv"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Hindustan Unilever": r'https://news.google.com/rss/search?q="hindustan%20unilever"%20OR%20"hul"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Maruti": r'https://news.google.com/rss/search?q="maruti"%20OR%20"maruti%20suzuki"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Nestle": r'https://news.google.com/rss/search?q="nestle"%20OR%20"nestle%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - NTPC": r'https://news.google.com/rss/search?q="ntpc"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ONGC": r'https://news.google.com/rss/search?q="ongc"%20OR%20"oil%20and%20natural%20gas%20corporation"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - HDFC Life": r'https://news.google.com/rss/search?q="hdfc%20life"%20OR%20"hdfc%20life%20insurance"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - SBI Life": r'https://news.google.com/rss/search?q="sbi%20life"%20OR%20"sbi%20life%20insurance"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Sun Pharma": r'https://news.google.com/rss/search?q="sun%20pharma"%20OR%20"sun%20pharmaceutical"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Dr Reddy": r'https://news.google.com/rss/search?q="dr%20reddy"%20OR%20"dr%20reddy%27s"%20OR%20"dr%20reddy%27s%20laboratories"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Divis Labs": r'https://news.google.com/rss/search?q="divis%20labs"%20OR%20"divi%27s"%20OR%20"divi%E2%80%99s%20laboratories"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Cipla": r'https://news.google.com/rss/search?q="cipla"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Wipro": r'https://news.google.com/rss/search?q="wipro"%20OR%20"wiproltd"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Tech Mahindra": r'https://news.google.com/rss/search?q="tech%20mahindra"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Tata Steel": r'https://news.google.com/rss/search?q="tata%20steel"%20OR%20"tata"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - JSW Steel": r'https://news.google.com/rss/search?q="jsw%20steel"%20OR%20"jsw"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Ultratech Cement": r'https://news.google.com/rss/search?q="ultratech%20cement"%20OR%20"ultratech"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Grasim": r'https://news.google.com/rss/search?q="grasim"%20OR%20"grasim%20industries"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - IndusInd": r'https://news.google.com/rss/search?q="indusind"%20OR%20"indusind%20bank"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Axis Bank": r'https://news.google.com/rss/search?q="axis%20bank"%20OR%20"axis"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Mahindra": r'https://news.google.com/rss/search?q="mahindra"%20OR%20"m%26m"%20OR%20"mahindra%20and%20mahindra"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Hero MotoCorp": r'https://news.google.com/rss/search?q="hero%20motocorp"%20OR%20"hero"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bajaj Auto": r'https://news.google.com/rss/search?q="bajaj%20auto"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Eicher Motors": r'https://news.google.com/rss/search?q="eicher%20motors"%20OR%20"eicher"%20OR%20"royal%20enfield"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Britannia": r'https://news.google.com/rss/search?q="britannia"%20OR%20"britannia%20industries"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Tata Consumer": r'https://news.google.com/rss/search?q="tata%20consumer"%20OR%20"tata%20consumer%20products"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - UPL": r'https://news.google.com/rss/search?q="upl"%20OR%20"united%20phosphorous"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Apollo Hospitals": r'https://news.google.com/rss/search?q="apollo%20hospitals"%20OR%20"apollo"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Coal India": r'https://news.google.com/rss/search?q="coal%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Hindalco": r'https://news.google.com/rss/search?q="hindalco"%20OR%20"hindalco%20industries"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Shree Cement": r'https://news.google.com/rss/search?q="shree%20cement"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Adani Enterprises": r'https://news.google.com/rss/search?q="adani%20enterprises"%20OR%20"adanient"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Power Grid": r'https://news.google.com/rss/search?q="power%20grid"%20OR%20"power%20grid%20corporation"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - HAL": r'https://news.google.com/rss/search?q="hal"%20OR%20"hindustan%20aeronautics"%20OR%20"hindustan%20aeronautics%20limited"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - DLF": r'https://news.google.com/rss/search?q="dlf"%20OR%20"dlf%20limited"&hl=en-IN&gl=IN&ceid=IN:en',

}

# --------------------
# FETCH AND FILTER
# --------------------
all_articles = []
ist = timezone(timedelta(hours=5,minutes=30))
cutoff_date = (datetime.now(ist) - timedelta(days=7)).replace(tzinfo=None)


for source, url in rss_feeds.items():
    feed = feedparser.parse(url)
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()

        combined_text = title.lower()  # Only use title for company matching

        matched_companies = [
            company for company, aliases in company_aliases.items()
            if any(alias.lower() in combined_text for alias in aliases)
        ]

        if not matched_companies:
            continue

        # Remove redundant/nested company names
        cleaned_companies = []
        for company in matched_companies:
            if not any(company != other and company in other for other in matched_companies):
                cleaned_companies.append(company)
        matched_companies = cleaned_companies

        try:
            pub_date_obj = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            pub_date_obj = pub_date_obj.astimezone(ist).replace(tzinfo=None)


        except:
            pub_date_obj = datetime.now(ist)
        if pub_date_obj < cutoff_date:
            continue

        for company in matched_companies:  # one row per company
            all_articles.append({
                "Company": company.strip().lower(),
                "Content": title,
                "PublicationDate": pub_date_obj,
                "Source": source,
                "Link": link
            })


    time.sleep(1)  # be polite to servers

# --------------------
# CREATE DATAFRAME
# --------------------
if os.path.exists(output_file):
    existing_df =  pd.read_excel(output_file)

    # Ensure PublicationDate exists
    if 'PublicationDate' not in existing_df.columns:
        existing_df['PublicationDate'] = pd.NaT  # empty datetime column

    existing_df['PublicationDate'] = pd.to_datetime(existing_df['PublicationDate'], errors='coerce')
    existing_df = existing_df[existing_df['PublicationDate'] >= cutoff_date]
else:
    existing_df = pd.DataFrame(columns=["Company", "Content", "PublicationDate", "Source", "Link"])

# Create DataFrame from new articles
new_df = pd.DataFrame(all_articles)

# Merge with existing data
combined_df = pd.concat([existing_df, new_df], ignore_index=True)
combined_df.drop_duplicates(subset=["Content", "Link"], inplace=True)
combined_df.sort_values(by="PublicationDate", ascending=False, inplace=True)

# Make PublicationDate timezone-unaware
combined_df['PublicationDate'] = combined_df['PublicationDate'].dt.tz_localize(None)

# Save to Excel
combined_df.to_excel(output_file, index=False)
print(f"✅ Saved {len(combined_df)} filtered articles to '{output_file}'.")
