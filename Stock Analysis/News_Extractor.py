from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import feedparser
import pandas as pd
import time
import re

load_dotenv()

output_file = os.getenv("NEWS_FILE", r"C:\Users\PC\Documents\News.xlsx")

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
    "Google News - Adani Ports": r'https://news.google.com/rss/search?q="adani%20ports"%20OR%20"adani%20ports%20and%20sez"%20OR%20"adaniports"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ABB": r'https://news.google.com/rss/search?q="abb"%20OR%20"abb%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Adani Green": r'https://news.google.com/rss/search?q="adani%20green"%20OR%20"adani%20green%20energy"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Adani Power": r'https://news.google.com/rss/search?q="adani%20power"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Ambuja Cement": r'https://news.google.com/rss/search?q="ambuja%20cement"%20OR%20"ambuja%20cements"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bajaj Holdings": r'https://news.google.com/rss/search?q="bajaj%20holdings"%20OR%20"bajaj%20holdings%20investment"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bank of Baroda": r'https://news.google.com/rss/search?q="bank%20of%20baroda"%20OR%20"bob"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - BPCL": r'https://news.google.com/rss/search?q="bpcl"%20OR%20"bharat%20petroleum"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bosch": r'https://news.google.com/rss/search?q="bosch"%20OR%20"bosch%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Canara Bank": r'https://news.google.com/rss/search?q="canara%20bank"%20OR%20"canbank"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ACC": r'https://news.google.com/rss/search?q="acc"%20OR%20"acc%20cement"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - DMart": r'https://news.google.com/rss/search?q="dmart"%20OR%20"avenue%20supermarts"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Bandhan Bank": r'https://news.google.com/rss/search?q="bandhan%20bank"%20OR%20"bandhan"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Biocon": r'https://news.google.com/rss/search?q="biocon"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Cholamandalam": r'https://news.google.com/rss/search?q="cholamandalam"%20OR%20"chola%20finance"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Colgate": r'https://news.google.com/rss/search?q="colgate"%20OR%20"colpal"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - GAIL": r'https://news.google.com/rss/search?q="gail"%20OR%20"gail%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Godrej Consumer": r'https://news.google.com/rss/search?q="godrej%20consumer"%20OR%20"godrejcp"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ICICI Lombard": r'https://news.google.com/rss/search?q="icici%20lombard"%20OR%20"icicigi"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - ICICI Prudential": r'https://news.google.com/rss/search?q="icici%20prudential"%20OR%20"icici%20prudential%20life"%20OR%20"icicipru"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Indian Hotels": r'https://news.google.com/rss/search?q="indian%20hotels"%20OR%20"taj%20hotels"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Indus Towers": r'https://news.google.com/rss/search?q="indus%20towers"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Info Edge": r'https://news.google.com/rss/search?q="info%20edge"%20OR%20"naukri"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Indigo": r'https://news.google.com/rss/search?q="indigo"%20OR%20"interglobe%20aviation"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - LIC": r'https://news.google.com/rss/search?q="lic"%20OR%20"life%20insurance%20corporation"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Marico": r'https://news.google.com/rss/search?q="marico"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Mphasis": r'https://news.google.com/rss/search?q="mphasis"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Muthoot Finance": r'https://news.google.com/rss/search?q="muthoot%20finance"%20OR%20"muthoot"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Paytm": r'https://news.google.com/rss/search?q="paytm"%20OR%20"one97"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - PI Industries": r'https://news.google.com/rss/search?q="pi%20industries"%20OR%20"piind"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Pidilite": r'https://news.google.com/rss/search?q="pidilite"%20OR%20"pidilite%20industries"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - SBI Cards": r'https://news.google.com/rss/search?q="sbi%20cards"%20OR%20"sbi%20card"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - SRF": r'https://news.google.com/rss/search?q="srf"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Motherson": r'https://news.google.com/rss/search?q="motherson"%20OR%20"samvardhana%20motherson"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Siemens": r'https://news.google.com/rss/search?q="siemens"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Tata Power": r'https://news.google.com/rss/search?q="tata%20power"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Torrent Pharma": r'https://news.google.com/rss/search?q="torrent%20pharma"%20OR%20"torrent%20pharmaceuticals"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - United Spirits": r'https://news.google.com/rss/search?q="united%20spirits"%20OR%20"mcdowell"%20OR%20"mcdowell-n"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Vedanta": r'https://news.google.com/rss/search?q="vedanta"%20OR%20"vedl"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Zomato": r'https://news.google.com/rss/search?q="zomato"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Petronet": r'https://news.google.com/rss/search?q="petronet"%20OR%20"petronet%20lng"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - PGHH": r'https://news.google.com/rss/search?q="pg%20hh"%20OR%20"procter%20and%20gamble%20hygiene"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Polycab": r'https://news.google.com/rss/search?q="polycab"%20OR%20"polycab%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Havells": r'https://news.google.com/rss/search?q="havells"%20OR%20"havells%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Concor": r'https://news.google.com/rss/search?q="concor"%20OR%20"container%20corporation%20of%20india"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - IRCTC": r'https://news.google.com/rss/search?q="irctc"%20OR%20"indian%20railway%20catering"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Trent": r'https://news.google.com/rss/search?q="trent"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - TVS Motor": r'https://news.google.com/rss/search?q="tvs"%20OR%20"tvs%20motor"&hl=en-IN&gl=IN&ceid=IN:en',
    "Google News - Jubilant Food": r'https://news.google.com/rss/search?q="jubilant%20food"%20OR%20"jubilant%20foodworks"%20OR%20"domino%27s"&hl=en-IN&gl=IN&ceid=IN:en'
}

all_articles = []
ist = timezone(timedelta(hours=5,minutes=30))
cutoff_date = (datetime.now(ist) - timedelta(days=7)).replace(tzinfo=None)

for source, url in rss_feeds.items():
    feed = feedparser.parse(url)
    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        combined_text = title.lower()

        matched_companies = [
            company for company, aliases in company_aliases.items()
            if any(alias.lower() in combined_text for alias in aliases)
        ]

        if not matched_companies:
            continue

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

        for company in matched_companies:
            all_articles.append({
                "Company": company.strip().lower(),
                "Content": title,
                "PublicationDate": pub_date_obj,
                "Source": source,
                "Link": link
            })

    time.sleep(1)

if os.path.exists(output_file):
    existing_df = pd.read_excel(output_file)
    if 'PublicationDate' not in existing_df.columns:
        existing_df['PublicationDate'] = pd.NaT
    existing_df['PublicationDate'] = pd.to_datetime(existing_df['PublicationDate'], errors='coerce')
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
