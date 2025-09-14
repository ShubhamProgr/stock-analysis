import feedparser
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
import os
import urllib

output_file = r"C:\Users\PC\Documents\S&P500_News.xlsx"

# -------------------- Get S&P 500 companies --------------------
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500_table = pd.read_html(sp500_url)[0]  # Read first table

# columns: 'Symbol', 'Security'
sp500_companies = sp500_table[['Symbol', 'Security']]

# Build company aliases: use ticker + company name
company_aliases = {}
for _, row in sp500_companies.iterrows():
    ticker = row['Symbol'].replace('.', '-').lower()
    name = row['Security'].lower()
    company_aliases[name] = [name, ticker]

# -------------------- Generate Google News RSS feeds --------------------
rss_feeds = {}
for company_name, aliases in company_aliases.items():
    # URL-encode each alias to safely include in the URL
    encoded_aliases = [urllib.parse.quote(alias) for alias in aliases]
    query = '%20OR%20'.join(encoded_aliases)
    rss_url = f'https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en'
    rss_feeds[f"Google News - {company_name}"] = rss_url

# -------------------- Fetch and filter --------------------
all_articles = []
ist = timezone(timedelta(hours=5, minutes=30))
cutoff_date = (datetime.now(ist) - timedelta(days=7)).replace(tzinfo=None)

for source, url in rss_feeds.items():
    feed = feedparser.parse(url)
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()

        # Use the company directly from the RSS feed key
        company_from_feed = source.replace("Google News - ", "").strip().lower()

        # Get publication date safely
        try:
            if getattr(entry, 'published_parsed', None):
                pub_date_obj = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                pub_date_obj = pub_date_obj.astimezone(ist).replace(tzinfo=None)
            else:
                pub_date_obj = datetime.now(ist)
        except Exception:
            pub_date_obj = datetime.now(ist)

        all_articles.append({
            "Company": company_from_feed,
            "Content": title,
            "PublicationDate": pub_date_obj,
            "Source": source,
            "Link": link
        })


    time.sleep(1)  # be polite

# -------------------- Create/Update Excel --------------------
if os.path.exists(output_file):
    existing_df = pd.read_excel(output_file)
    existing_df['PublicationDate'] = pd.to_datetime(existing_df.get('PublicationDate', pd.NaT), errors='coerce')
    existing_df = existing_df[existing_df['PublicationDate'] >= cutoff_date]
else:
    existing_df = pd.DataFrame(columns=["Company", "Content", "PublicationDate", "Source", "Link"])

new_df = pd.DataFrame(all_articles)
combined_df = pd.concat([existing_df, new_df], ignore_index=True)
combined_df.drop_duplicates(subset=["Content", "Link"], inplace=True)
combined_df.sort_values(by="PublicationDate", ascending=False, inplace=True)
combined_df['PublicationDate'] = combined_df['PublicationDate'].dt.tz_localize(None)
combined_df.to_excel(output_file, index=False)
print(f"✅ Saved {len(combined_df)} filtered articles to '{output_file}'.")
