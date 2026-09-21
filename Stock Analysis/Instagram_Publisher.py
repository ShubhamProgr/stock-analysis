"""
Instagram_Publisher.py
──────────────────────
Automated daily pipeline to:
  1. Fetch the latest Top-10 Gainers & Losers from the `final_analysis` Supabase table.
  2. Fetch AI accuracy scorecard from `prediction_vs_actual`.
  3. Generate a 4-slide carousel using Pillow.
  4. Upload images (or Reel video) to Cloudinary.
  5. Publish the carousel or Reel to the configured Instagram Professional account via
     the Meta Graph API.

Run standalone (Carousel):
    python "stock analysis/Instagram_Publisher.py" --format carousel

Run standalone (Reel):
    python "stock analysis/Instagram_Publisher.py" --format reel

Or use the --preview flag to only save locally (no upload):
    python "stock analysis/Instagram_Publisher.py" --format reel --preview
"""

import os
import sys
import io
import time
import argparse
import textwrap
import requests
import tempfile
import cloudinary
import cloudinary.uploader
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from PIL import Image, ImageDraw, ImageFont, ImageFilter

try:
    # moviepy >= 2.0.0
    from moviepy import ImageSequenceClip
except ImportError:
    try:
        # moviepy < 2.0.0
        from moviepy.editor import ImageSequenceClip
    except ImportError:
        ImageSequenceClip = None

# ─────────────────────────────────────────
# 0. CONFIGURATION
# ─────────────────────────────────────────
load_dotenv(find_dotenv())

# ── Cloudinary ──
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME") or os.getenv("NEXT_PUBLIC_CLOUDINARY_CLOUD_NAME", "").strip('"'),
    api_key=os.getenv("CLOUDINARY_API_KEY") or os.getenv("NEXT_PUBLIC_CLOUDINARY_API_KEY", "").strip('"'),
    api_secret=os.getenv("CLOUDINARY_API_SECRET", "").strip('"'),
)

# ── Instagram / Meta ──
IG_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN", "")
IG_ACCOUNT_ID   = os.getenv("INSTAGRAM_ACCOUNT_ID", "")
IG_API_BASE     = "https://graph.facebook.com/v21.0"

# ── Database ──
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    host     = os.getenv("SUPABASE_DB_HOST")
    port     = os.getenv("SUPABASE_DB_PORT", "5432")
    name     = os.getenv("SUPABASE_DB_NAME", "postgres")
    user     = os.getenv("SUPABASE_DB_USER", "postgres")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    ssl      = os.getenv("SUPABASE_DB_SSLMODE", "require")
    DATABASE_URL = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{host}:{port}/{name}?sslmode={ssl}"
    )

PREVIEW_DIR = os.path.join(os.path.dirname(__file__), "preview_slides")

# ─────────────────────────────────────────
# 1. DESIGN CONSTANTS & TYPOGRAPHY
# ─────────────────────────────────────────
FONTS_DIR       = os.path.join(os.path.dirname(__file__), "fonts")

IMG_W           = 1080
IMG_H           = 1350
PADDING_X       = 80
CARD_WIDTH      = IMG_W - (PADDING_X * 2)

BG_DARK_TOP     = (7, 10, 19)
BG_DARK_MID     = (10, 14, 27)
BG_DARK_BOT     = (6, 8, 16)

CARD_BG         = (15, 23, 42, 225)
CARD_BORDER_GAINER = (22, 101, 52)
CARD_BORDER_LOSER  = (153, 27, 27)

GREEN_PRIMARY   = (52, 211, 153)
GREEN_BG        = (6, 78, 59, 140)

RED_PRIMARY     = (251, 113, 133)
RED_BG          = (136, 19, 55, 140)

TEXT_WHITE      = (255, 255, 255)
TEXT_SUB        = (203, 213, 225)
TEXT_MUTED      = (148, 163, 184)
TEXT_DIM        = (100, 116, 139)
ACCENT_CYAN     = (56, 189, 248)
ACCENT_GOLD     = (250, 204, 21)

_FONTS_CHECKED = False

def _ensure_fonts() -> None:
    global _FONTS_CHECKED
    if _FONTS_CHECKED: return
    _FONTS_CHECKED = True
    os.makedirs(FONTS_DIR, exist_ok=True)
    font_urls = {
        "Inter-Bold.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/inter@latest/latin-700-normal.ttf",
        "Inter-SemiBold.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/inter@latest/latin-600-normal.ttf",
        "Inter-Regular.ttf": "https://cdn.jsdelivr.net/fontsource/fonts/inter@latest/latin-400-normal.ttf",
    }
    for font_name, url in font_urls.items():
        dest = os.path.join(FONTS_DIR, font_name)
        if not os.path.exists(dest):
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    with open(dest, "wb") as f: f.write(r.content)
            except Exception: pass

def _load_font(size: int, weight: str = "regular") -> ImageFont.ImageFont:
    _ensure_fonts()
    weight_map = {
        "bold": ["Inter-Bold.ttf", "Inter-700.ttf", "segoeuib.ttf", "arialbd.ttf"],
        "semibold": ["Inter-SemiBold.ttf", "Inter-600.ttf", "seguisb.ttf", "segoeuib.ttf"],
        "regular": ["Inter-Regular.ttf", "Inter-400.ttf", "segoeui.ttf", "arial.ttf"],
        "medium": ["Inter-SemiBold.ttf", "Inter-500.ttf", "segoeui.ttf", "arial.ttf"],
    }
    candidates = weight_map.get(weight.lower(), weight_map["regular"])
    for name in candidates:
        p = os.path.join(FONTS_DIR, name)
        if os.path.exists(p):
            try: return ImageFont.truetype(p, size)
            except Exception: pass
        win_p = os.path.join(r"C:\Windows\Fonts", name)
        if os.path.exists(win_p):
            try: return ImageFont.truetype(win_p, size)
            except Exception: pass
    return ImageFont.load_default()


# ─────────────────────────────────────────
# 2. DATABASE QUERIES
# ─────────────────────────────────────────
def fetch_predictions() -> tuple[list[dict], list[dict], date]:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        latest_date_row = conn.execute(text('SELECT MAX("Prediction_Date") FROM final_analysis')).fetchone()
        if not latest_date_row or not latest_date_row[0]:
            raise RuntimeError("final_analysis table is empty.")

        prediction_date = latest_date_row[0]
        rows = conn.execute(text("""
            SELECT "Company", "Ticker", "Predicted_Return_Pct", "Sentiment"
            FROM final_analysis
            WHERE "Prediction_Date" = :pd
            ORDER BY "Predicted_Return_Pct" DESC
        """), {"pd": prediction_date}).fetchall()

    all_data = [
        {
            "Company": r[0].title() if r[0] else r[1],
            "Ticker":  r[1],
            "Return":  float(r[2]),
            "Sentiment": r[3] or "Neutral",
        }
        for r in rows
    ]
    return all_data[:5], list(reversed(all_data[-5:])), prediction_date

def fetch_scorecard_data() -> list[dict]:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        latest_date_row = conn.execute(text('SELECT MAX("Date") FROM prediction_vs_actual')).fetchone()
        if not latest_date_row or not latest_date_row[0]:
            return []
            
        latest_date = latest_date_row[0]
        rows = conn.execute(text("""
            SELECT "Company", "Ticker", "Predicted_Closing_Price", "Actual_Closing_Price"
            FROM prediction_vs_actual
            WHERE "Date" = :d
            AND "Predicted_Closing_Price" IS NOT NULL
            AND "Actual_Closing_Price" IS NOT NULL
            AND "Predicted_Closing_Price" > 0
            AND "Actual_Closing_Price" > 0
        """), {"d": latest_date}).fetchall()
        
    scorecard = []
    for r in rows:
        predicted = float(r[2])
        actual = float(r[3])
        error = abs(predicted - actual) / actual * 100
        scorecard.append({
            "Company": r[0],
            "Ticker": r[1].replace(".NS", ""),
            "Predicted": predicted,
            "Actual": actual,
            "ErrorPct": error
        })
    scorecard.sort(key=lambda x: x["ErrorPct"])
    return scorecard[:3]


# ─────────────────────────────────────────
# 3. CAROUSEL IMAGE GENERATION
# ─────────────────────────────────────────
def _create_base_canvas(prediction_date: date, slide_idx: int, total_slides: int):
    base = Image.new("RGBA", (IMG_W, IMG_H), BG_DARK_TOP)
    draw = ImageDraw.Draw(base)

    for y in range(IMG_H):
        t = y / (IMG_H // 2) if y < IMG_H // 2 else (y - IMG_H // 2) / (IMG_H // 2)
        c1, c2 = (BG_DARK_TOP, BG_DARK_MID) if y < IMG_H // 2 else (BG_DARK_MID, BG_DARK_BOT)
        r = int(c1[0] + (c2[0] - c1[0]) * t)
        g = int(c1[1] + (c2[1] - c1[1]) * t)
        b = int(c1[2] + (c2[2] - c1[2]) * t)
        draw.line([(0, y), (IMG_W, y)], fill=(r, g, b, 255))

    glow = Image.new("RGBA", (IMG_W, IMG_H), (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow)
    gd.ellipse([240, -50, 840, 250], fill=(56, 189, 248, 15))
    glow = glow.filter(ImageFilter.GaussianBlur(35))
    base = Image.alpha_composite(base, glow)
    draw = ImageDraw.Draw(base)

    for gx in range(40, IMG_W, 40):
        for gy in range(40, IMG_H, 40):
            draw.point((gx, gy), fill=(255, 255, 255, 12))

    fonts = {
        "brand": _load_font(20, "bold"),
        "title": _load_font(52, "bold"),
        "date": _load_font(18, "semibold"),
        "col_title": _load_font(28, "bold"),
        "name": _load_font(26, "bold"),
        "rank": _load_font(18, "bold"),
        "ticker": _load_font(16, "medium"),
        "pct": _load_font(32, "bold"),
        "footer": _load_font(18, "bold")
    }

    # Header
    brand_txt = "STOCKANALYSIS.ME"
    bw = draw.textlength(brand_txt, font=fonts["brand"])
    bx = PADDING_X
    draw.rounded_rectangle([bx, 44, bx + bw + 50, 82], radius=19, fill=(15, 23, 42, 230), outline=(56, 189, 248, 140), width=1)
    draw.ellipse([bx + 16, 57, bx + 26, 67], fill=GREEN_PRIMARY)
    draw.text((bx + 38, 50), brand_txt, font=fonts["brand"], fill=TEXT_WHITE)

    date_str = prediction_date.strftime("%d %B %Y").upper()
    dw = draw.textlength(date_str, font=fonts["date"])
    dx = IMG_W - PADDING_X - dw - 30
    draw.rounded_rectangle([dx, 44, dx + dw + 30, 82], radius=19, fill=(30, 41, 59, 180), outline=(71, 85, 105, 160), width=1)
    draw.text((dx + 15, 52), date_str, font=fonts["date"], fill=ACCENT_CYAN)

    draw.line([(PADDING_X, 120), (IMG_W - PADDING_X, 120)], fill=(38, 48, 74, 160), width=1)

    # Footer
    FOOTER_Y = 1180
    draw.line([(PADDING_X, FOOTER_Y), (IMG_W - PADDING_X, FOOTER_Y)], fill=(38, 48, 74, 180), width=1)
    fc_y = FOOTER_Y + 25
    draw.rounded_rectangle([PADDING_X, fc_y, IMG_W - PADDING_X, fc_y + 80], radius=14, fill=(15, 23, 42, 210), outline=(51, 65, 85, 140), width=1)
    draw.text((PADDING_X + 24, fc_y + 28), "@StockAnalyticsIN", font=fonts["footer"], fill=ACCENT_CYAN)

    # Carousel Pagination Indicator
    ind_w = 80
    ind_x = (IMG_W - ind_w) / 2
    for i in range(total_slides):
        col = TEXT_WHITE if i == slide_idx else TEXT_DIM
        draw.ellipse([ind_x + (i*25), fc_y + 35, ind_x + (i*25) + 10, fc_y + 45], fill=col)

    if slide_idx == total_slides - 1:
        cta = "Share this analysis"
    else:
        cta = "Swipe for predictions ➔"
    cw = draw.textlength(cta, font=fonts["footer"])
    draw.text((IMG_W - PADDING_X - cw - 30, fc_y + 28), cta, font=fonts["footer"], fill=TEXT_WHITE)

    return base, draw, fonts

def _draw_card_list(draw: ImageDraw.ImageDraw, fonts: dict, data: list[dict], is_gainer: bool):
    CARD_Y = 280
    CARD_H = 140
    GAP = 24

    for i, s in enumerate(data):
        cy = CARD_Y + i * (CARD_H + GAP)
        outline_c = CARD_BORDER_GAINER if is_gainer else CARD_BORDER_LOSER
        primary_c = GREEN_PRIMARY if is_gainer else RED_PRIMARY

        draw.rounded_rectangle([PADDING_X, cy, PADDING_X + CARD_WIDTH, cy + CARD_H], radius=16, fill=CARD_BG, outline=outline_c, width=1)
        draw.rounded_rectangle([PADDING_X, cy + 30, PADDING_X + 4, cy + CARD_H - 30], radius=2, fill=primary_c)

        # Rank
        rx = PADDING_X + 24
        draw.rounded_rectangle([rx, cy + 24, rx + 44, cy + 60], radius=8, fill=(30, 41, 59, 230), outline=(71, 85, 105, 140), width=1)
        draw.text((rx + 12, cy + 30), f"#{i+1}", font=fonts["rank"], fill=TEXT_WHITE if i>0 else ACCENT_GOLD)

        # Name
        cname = s["Company"][:25] + "…" if len(s["Company"]) > 25 else s["Company"]
        draw.text((rx + 60, cy + 28), cname, font=fonts["name"], fill=TEXT_WHITE)

        # Ticker
        tick_y = cy + 74
        t_str = f"NSE: {s['Ticker'].replace('.NS','')}"
        tw = draw.textlength(t_str, font=fonts["ticker"])
        draw.rounded_rectangle([rx + 60, tick_y, rx + 60 + tw + 20, tick_y + 30], radius=6, fill=(30, 41, 59, 140), outline=(51, 65, 85, 180), width=1)
        draw.text((rx + 70, tick_y + 5), t_str, font=fonts["ticker"], fill=TEXT_SUB)

        # Sentiment Badge
        sent = s.get("Sentiment", "NEUTRAL").upper()
        if "POS" in sent or "BULL" in sent: sent_c, sent_bg, sent_txt = GREEN_PRIMARY, GREEN_BG, "BULLISH"
        elif "NEG" in sent or "BEAR" in sent: sent_c, sent_bg, sent_txt = RED_PRIMARY, RED_BG, "BEARISH"
        else: sent_c, sent_bg, sent_txt = TEXT_SUB, (51, 65, 85, 180), "NEUTRAL"
        
        sx = rx + 60 + tw + 32
        sw = draw.textlength(sent_txt, font=fonts["ticker"]) + 30
        draw.rounded_rectangle([sx, tick_y, sx + sw, tick_y + 30], radius=6, fill=sent_bg, outline=sent_c, width=1)
        draw.ellipse([sx + 10, tick_y + 11, sx + 18, tick_y + 19], fill=sent_c)
        draw.text((sx + 26, tick_y + 5), sent_txt, font=fonts["ticker"], fill=sent_c)

        # Return
        ret = s.get("Return", 0.0)
        ret_s = f"+{ret:.2f}%" if ret > 0 else f"{ret:.2f}%"
        rw = draw.textlength(ret_s, font=fonts["pct"])
        px = PADDING_X + CARD_WIDTH - rw - 40
        draw.rounded_rectangle([px, cy + 30, px + rw + 20, cy + 85], radius=12, fill=GREEN_BG if ret>0 else RED_BG, outline=primary_c, width=1)
        draw.text((px + 10, cy + 40), ret_s, font=fonts["pct"], fill=primary_c)

def generate_carousel_images(gainers: list[dict], losers: list[dict], scorecard: list[dict], prediction_date: date) -> list[bytes]:
    slides = []
    total_slides = 4 if scorecard else 3
    
    def to_bytes(img):
        buf = io.BytesIO()
        img.convert("RGB").save(buf, format="JPEG", quality=95, optimize=True)
        return buf.getvalue()

    # SLIDE 0: Hero
    base0, draw0, f0 = _create_base_canvas(prediction_date, 0, total_slides)
    title = "NIFTY 50 AI PREDICTIONS"
    tw = draw0.textlength(title, font=f0["title"])
    draw0.text(((IMG_W - tw)/2, 400), title, font=f0["title"], fill=TEXT_WHITE)
    sub = "Quantitative Forecast & News Sentiment"
    sw = draw0.textlength(sub, font=f0["col_title"])
    draw0.text(((IMG_W - sw)/2, 480), sub, font=f0["col_title"], fill=ACCENT_CYAN)
    
    bw, bh = 500, 250
    bx = (IMG_W - bw) / 2
    draw0.rounded_rectangle([bx, 600, bx + bw, 600 + bh], radius=20, fill=(15, 23, 42, 200), outline=ACCENT_CYAN, width=2)
    t1 = f"{len(gainers)} BULLISH FLAGS"
    t2 = f"{len(losers)} BEARISH FLAGS"
    draw0.text((bx + 110, 650), t1, font=f0["col_title"], fill=GREEN_PRIMARY)
    draw0.text((bx + 110, 720), t2, font=f0["col_title"], fill=RED_PRIMARY)
    slides.append(to_bytes(base0))

    # SLIDE 1: Bullish
    base1, draw1, f1 = _create_base_canvas(prediction_date, 1, total_slides)
    tw1 = draw1.textlength("TOP 5 BULLISH FLAGS", font=f1["title"])
    draw1.text(((IMG_W - tw1)/2, 160), "TOP 5 BULLISH FLAGS", font=f1["title"], fill=GREEN_PRIMARY)
    _draw_card_list(draw1, f1, gainers, True)
    slides.append(to_bytes(base1))

    # SLIDE 2: Bearish
    base2, draw2, f2 = _create_base_canvas(prediction_date, 2, total_slides)
    tw2 = draw2.textlength("TOP 5 BEARISH FLAGS", font=f2["title"])
    draw2.text(((IMG_W - tw2)/2, 160), "TOP 5 BEARISH FLAGS", font=f2["title"], fill=RED_PRIMARY)
    _draw_card_list(draw2, f2, losers, False)
    slides.append(to_bytes(base2))

    # SLIDE 3: Scorecard (Optional)
    if scorecard:
        base3, draw3, f3 = _create_base_canvas(prediction_date, 3, total_slides)
        tw3 = draw3.textlength("AI SCORECARD: TRACK RECORD", font=f3["title"])
        draw3.text(((IMG_W - tw3)/2, 160), "AI SCORECARD: TRACK RECORD", font=f3["title"], fill=ACCENT_GOLD)
        
        CY = 280
        for i, s in enumerate(scorecard):
            cy = CY + i * (160 + 30)
            draw3.rounded_rectangle([PADDING_X, cy, PADDING_X + CARD_WIDTH, cy + 160], radius=16, fill=CARD_BG, outline=ACCENT_GOLD, width=1)
            
            draw3.text((PADDING_X + 30, cy + 30), s["Company"], font=f3["name"], fill=TEXT_WHITE)
            t_str = f"NSE: {s['Ticker']}"
            draw3.text((PADDING_X + 30, cy + 70), t_str, font=f3["ticker"], fill=TEXT_SUB)
            
            pred_txt = f"Predicted: ₹{s['Predicted']:.1f}"
            act_txt = f"Actual: ₹{s['Actual']:.1f}"
            draw3.text((PADDING_X + 450, cy + 30), pred_txt, font=f3["date"], fill=TEXT_MUTED)
            draw3.text((PADDING_X + 450, cy + 70), act_txt, font=f3["date"], fill=TEXT_WHITE)
            
            err = s["ErrorPct"]
            err_txt = f"Error Margin: {err:.1f}%"
            ew = draw3.textlength(err_txt, font=f3["date"])
            ex = PADDING_X + CARD_WIDTH - ew - 40
            draw3.rounded_rectangle([ex-20, cy+30, ex+ew+20, cy+70], radius=8, fill=(30, 41, 59, 200), outline=ACCENT_CYAN, width=1)
            draw3.text((ex, cy + 40), err_txt, font=f3["date"], fill=ACCENT_CYAN)
            
        slides.append(to_bytes(base3))

    return slides


# ─────────────────────────────────────────
# 3.5 REELS VIDEO GENERATION
# ─────────────────────────────────────────
def generate_reel_video(slides_bytes: list[bytes], fps: float = 0.285) -> bytes:
    """
    Stitches static images into an MP4 video using MoviePy.
    fps = 0.285 means roughly 3.5 seconds per slide.
    """
    if not ImageSequenceClip:
        raise RuntimeError("moviepy is not installed. Please install it (pip install moviepy) to generate Reels.")
    
    print("  Generating Reel video from slides...")
    temp_files = []
    
    for i, b in enumerate(slides_bytes):
        fd, path = tempfile.mkstemp(suffix=".jpg")
        with os.fdopen(fd, 'wb') as f:
            f.write(b)
        temp_files.append(path)
        
    clip = ImageSequenceClip(temp_files, fps=fps)
    
    fd, out_path = tempfile.mkstemp(suffix=".mp4")
    os.close(fd)
    
    # Write silent video
    clip.write_videofile(out_path, codec="libx264", audio=False, logger=None)
    
    with open(out_path, "rb") as f:
        video_bytes = f.read()
        
    for p in temp_files:
        try: os.remove(p)
        except Exception: pass
    try: os.remove(out_path)
    except Exception: pass
    
    return video_bytes


# ─────────────────────────────────────────
# 4. CLOUDINARY UPLOAD (CAROUSEL & REELS)
# ─────────────────────────────────────────
def upload_carousel_to_cloudinary(image_bytes_list: list[bytes], base_public_id: str) -> list[str]:
    urls = []
    for i, img_bytes in enumerate(image_bytes_list):
        pid = f"{base_public_id}_slide_{i+1}"
        print(f"  Uploading slide {i+1}...")
        res = cloudinary.uploader.upload(
            img_bytes,
            public_id=pid,
            folder="stock_analytics",
            resource_type="image",
            format="jpg",
            access_mode="public",
            transformation=[{"width": 1080, "height": 1350, "crop": "fill"}, {"quality": "auto:best"}],
        )
        urls.append(res["secure_url"])
    
    print("  Warming up CDN links...")
    time.sleep(3)
    return urls

def upload_video_to_cloudinary(video_bytes: bytes, public_id: str) -> str:
    print("  Uploading video to Cloudinary...")
    res = cloudinary.uploader.upload(
        video_bytes,
        public_id=public_id,
        folder="stock_analytics",
        resource_type="video",
        access_mode="public",
    )
    print("  Warming up CDN link...")
    time.sleep(4)
    return res["secure_url"]


# ─────────────────────────────────────────
# 5. META GRAPH API (CAROUSEL & REELS)
# ─────────────────────────────────────────
def _ig_get(endpoint: str, params: dict) -> dict:
    resp = requests.get(f"{IG_API_BASE}{endpoint}", params=params, timeout=30)
    data = resp.json()
    if "error" in data: raise RuntimeError(f"Meta API error: {data['error']}")
    return data

def _ig_post(endpoint: str, payload: dict) -> dict:
    resp = requests.post(f"{IG_API_BASE}{endpoint}", data=payload, timeout=30)
    data = resp.json()
    if "error" in data: raise RuntimeError(f"Meta API error: {data['error']}")
    return data

def wait_for_media_container(creation_id: str, timeout: int = 120) -> None:
    print(f"    Waiting for container {creation_id}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_data = _ig_get(f"/{creation_id}", {"fields": "status_code", "access_token": IG_ACCESS_TOKEN})
        status_code = status_data.get("status_code", "UNKNOWN")
        if status_code == "FINISHED": return
        if status_code in ["ERROR", "EXPIRED"]: raise RuntimeError(f"Container failed: {status_data}")
        time.sleep(4)
    raise TimeoutError("Container processing timed out.")

def publish_carousel_to_instagram(image_urls: list[str], caption: str) -> str:
    print("  Step 1: Creating item containers...")
    child_ids = []
    for url in image_urls:
        container = _ig_post(f"/{IG_ACCOUNT_ID}/media", {
            "image_url": url,
            "is_carousel_item": "true",
            "access_token": IG_ACCESS_TOKEN
        })
        child_ids.append(container["id"])
        
    for cid in child_ids:
        wait_for_media_container(cid)
        
    print("  Step 2: Creating carousel container...")
    carousel = _ig_post(f"/{IG_ACCOUNT_ID}/media", {
        "media_type": "CAROUSEL",
        "children": ",".join(child_ids),
        "caption": caption,
        "access_token": IG_ACCESS_TOKEN
    })
    
    carousel_id = carousel["id"]
    wait_for_media_container(carousel_id)
    
    print("  Step 3: Publishing...")
    publish = _ig_post(f"/{IG_ACCOUNT_ID}/media_publish", {
        "creation_id": carousel_id,
        "access_token": IG_ACCESS_TOKEN
    })
    return publish["id"]

def publish_reel_to_instagram(video_url: str, caption: str) -> str:
    print("  Step 1: Creating Reel container...")
    container = _ig_post(f"/{IG_ACCOUNT_ID}/media", {
        "media_type": "REELS",
        "video_url": video_url,
        "caption": caption,
        "share_to_feed": "true",
        "access_token": IG_ACCESS_TOKEN
    })
    
    creation_id = container["id"]
    wait_for_media_container(creation_id, timeout=120)
    
    print("  Step 2: Publishing Reel...")
    publish = _ig_post(f"/{IG_ACCOUNT_ID}/media_publish", {
        "creation_id": creation_id,
        "access_token": IG_ACCESS_TOKEN
    })
    return publish["id"]


# ─────────────────────────────────────────
# 6. CAPTION BUILDER
# ─────────────────────────────────────────
def build_caption(gainers: list[dict], losers: list[dict], scorecard: list[dict], prediction_date: date) -> str:
    g_tickers = [s["Ticker"].replace(".NS", "") for s in gainers[:3]]
    l_tickers = [s["Ticker"].replace(".NS", "") for s in losers[:3]]
    
    return textwrap.dedent(f"""
        Will Nifty break out or dump tomorrow? Our AI just flagged extreme momentum divergence 📉📈
        
        Our ensemble quantitative model processed millions of data points across price action, institutional flow, and news sentiment for tomorrow's session.
        
        🟢 Top AI Bullish Flags:
        {', '.join(g_tickers)} are showing heavy positive momentum.
        
        🔴 Top AI Bearish Flags:
        {', '.join(l_tickers)} detected negative sector rotation.
        
        💬 Debate: Do you agree with the model's stance on {l_tickers[0] if l_tickers else 'these tickers'} being bearish? Drop your target below 👇
        
        📌 Save this watchlist before tomorrow’s 9:15 AM opening bell.
        🔗 Link in bio to test the full live screener.
        
        #Nifty50 #NSE #StockMarketIndia #TradingSetup #QuantitativeTrading #Fintech #IndianStocks #AlgoTrading
    """).strip()


# ─────────────────────────────────────────
# 7. MAIN
# ─────────────────────────────────────────
def main(preview_only: bool = False, format_type: str = "carousel"):
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    print(f"\n{'=' * 55}")
    print(f"  Instagram Publisher ({format_type.upper()})  |  {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"{'=' * 55}")

    print("\n[1/5] Fetching data...")
    gainers, losers, prediction_date = fetch_predictions()
    scorecard = fetch_scorecard_data()
    print(f"  Gainers: {len(gainers)}  |  Losers: {len(losers)}  |  Scorecard: {len(scorecard)}")

    print("\n[2/5] Generating slides...")
    slides_bytes = generate_carousel_images(gainers, losers, scorecard, prediction_date)
    print(f"  Generated {len(slides_bytes)} slides.")

    formats_to_run = ["carousel", "reel"] if format_type == "both" else [format_type]

    for fmt in formats_to_run:
        print(f"\n--- Processing {fmt.upper()} ---")
        
        if fmt == "reel":
            print("  Compiling Reel video...")
            reel_bytes = generate_reel_video(slides_bytes)

            if preview_only:
                os.makedirs(PREVIEW_DIR, exist_ok=True)
                p = os.path.join(PREVIEW_DIR, f"preview_reel.mp4")
                with open(p, "wb") as f: f.write(reel_bytes)
                print(f"  [OK] Saved preview reel to {p}")
                continue
                
            print("  Uploading to Cloudinary...")
            base_id = f"daily_reel_{prediction_date.strftime('%Y%m%d')}_{int(time.time())}"
            video_url = upload_video_to_cloudinary(reel_bytes, base_id)

            caption = build_caption(gainers, losers, scorecard, prediction_date)

            print("  Publishing Reel to Instagram...")
            media_id = publish_reel_to_instagram(video_url, caption)
            
            print(f"  [OK] Posted REEL successfully! Media ID: {media_id}")

        else:
            # Carousel logic
            if preview_only:
                os.makedirs(PREVIEW_DIR, exist_ok=True)
                for i, b in enumerate(slides_bytes):
                    p = os.path.join(PREVIEW_DIR, f"slide_{i}.jpg")
                    with open(p, "wb") as f: f.write(b)
                print(f"  [OK] Saved {len(slides_bytes)} preview slides to {PREVIEW_DIR}/")
                continue

            print("  Uploading to Cloudinary...")
            base_id = f"daily_carousel_{prediction_date.strftime('%Y%m%d')}_{int(time.time())}"
            image_urls = upload_carousel_to_cloudinary(slides_bytes, base_id)

            caption = build_caption(gainers, losers, scorecard, prediction_date)

            print("  Publishing Carousel to Instagram...")
            media_id = publish_carousel_to_instagram(image_urls, caption)

            print(f"  [OK] Posted CAROUSEL successfully! Media ID: {media_id}")

    print(f"\n{'=' * 55}")
    print("  Pipeline Completed!")
    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post daily stock carousels or reels to Instagram.")
    parser.add_argument("--preview", action="store_true", help="Generate locally without uploading.")
    parser.add_argument("--format", choices=["carousel", "reel", "both"], default="carousel", help="Format to publish (carousel, reel, or both).")
    args = parser.parse_args()
    main(preview_only=args.preview, format_type=args.format)
