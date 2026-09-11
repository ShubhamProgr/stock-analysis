"""
Instagram_Publisher.py
──────────────────────
Automated daily pipeline to:
  1. Fetch the latest Top-10 Gainers & Losers from the `final_analysis` Supabase table.
  2. Generate a premium 1080x1080 Instagram post image using Pillow.
  3. Upload the image to Cloudinary and retrieve a public URL.
  4. Publish the image to the configured Instagram Professional account via
     the Meta Graph API.
  5. Clean up the Cloudinary asset to preserve storage quota.

Run standalone:
    python "Stock Analysis/Instagram_Publisher.py"

Or use the --preview flag to only save the image locally (no upload):
    python "Stock Analysis/Instagram_Publisher.py" --preview
"""

import os
import sys
import io
import time
import argparse
import textwrap
import requests
import cloudinary
import cloudinary.uploader
from datetime import datetime, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, text
from PIL import Image, ImageDraw, ImageFont

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

# ── Image output path (for --preview) ──
PREVIEW_PATH = os.path.join(os.path.dirname(__file__), "instagram_preview.png")

# ─────────────────────────────────────────
# 1. DESIGN CONSTANTS
# ─────────────────────────────────────────
IMG_SIZE        = 1080                # square canvas
PADDING         = 48
COL_WIDTH       = (IMG_SIZE - PADDING * 3) // 2   # two equal columns

# Colour palette
BG_TOP          = (10,  14,  26)      # near-black navy
BG_BOTTOM       = (18,  22,  42)      # slightly lighter navy
GAINER_PRIMARY  = (0,  230, 118)      # electric green
GAINER_DIM      = (0,  160,  80)
LOSER_PRIMARY   = (255, 69,  88)      # vivid red
LOSER_DIM       = (180,  40,  55)
CARD_BG_GAINER  = (12,  38,  24)      # dark green tint
CARD_BG_LOSER   = (42,  12,  18)      # dark red tint
DIVIDER         = (40,  48,  72)
TEXT_WHITE      = (255, 255, 255)
TEXT_MUTED      = (148, 163, 184)
TEXT_GOLD       = (255, 200,  50)
ACCENT_BLUE     = (99,  179, 237)

# Typography — fall back gracefully if font not found
def _load_font(size: int, bold: bool = False):
    """Try to load a nice system font, fall back to Pillow default."""
    candidates_bold   = ["arialbd.ttf", "DejaVuSans-Bold.ttf", "LiberationSans-Bold.ttf"]
    candidates_normal = ["arial.ttf",   "DejaVuSans.ttf",       "LiberationSans.ttf"]
    candidates = candidates_bold if bold else candidates_normal
    for name in candidates:
        try:
            return ImageFont.truetype(name, size)
        except OSError:
            continue
    return ImageFont.load_default()


# ─────────────────────────────────────────
# 2. DATABASE: fetch top 10 gainers & losers
# ─────────────────────────────────────────
def fetch_predictions() -> tuple[list[dict], list[dict], date]:
    """
    Query the `final_analysis` table for the most-recent prediction date
    and return the top-10 gainers and bottom-10 losers as lists of dicts.

    Each dict has keys: Company, Ticker, Predicted_Return_Pct, Sentiment
    """
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.connect() as conn:
        # Find the latest prediction date that has been inserted
        latest_date_row = conn.execute(text(
            'SELECT MAX("Prediction_Date") FROM final_analysis'
        )).fetchone()
        if not latest_date_row or not latest_date_row[0]:
            raise RuntimeError("final_analysis table is empty — run Final_Analysis.py first.")

        prediction_date: date = latest_date_row[0]
        print(f"  Fetching predictions for: {prediction_date}")

        rows = conn.execute(text("""
            SELECT
                "Company",
                "Ticker",
                "Predicted_Return_Pct",
                "Sentiment"
            FROM final_analysis
            WHERE "Prediction_Date" = :pd
            ORDER BY "Predicted_Return_Pct" DESC
        """), {"pd": prediction_date}).fetchall()

    if not rows:
        raise RuntimeError(f"No predictions found for {prediction_date}.")

    all_data = [
        {
            "Company": r[0].title() if r[0] else r[1],
            "Ticker":  r[1],
            "Return":  float(r[2]),
            "Sentiment": r[3] or "Neutral",
        }
        for r in rows
    ]

    gainers = all_data[:5]
    losers  = list(reversed(all_data[-5:]))    # worst first

    return gainers, losers, prediction_date


# ─────────────────────────────────────────
# 3. IMAGE GENERATION
# ─────────────────────────────────────────
def _gradient_bg(draw: ImageDraw.ImageDraw):
    """Draw a vertical linear gradient background."""
    for y in range(IMG_SIZE):
        t = y / IMG_SIZE
        r = int(BG_TOP[0] + (BG_BOTTOM[0] - BG_TOP[0]) * t)
        g = int(BG_TOP[1] + (BG_BOTTOM[1] - BG_TOP[1]) * t)
        b = int(BG_TOP[2] + (BG_BOTTOM[2] - BG_TOP[2]) * t)
        draw.line([(0, y), (IMG_SIZE, y)], fill=(r, g, b))


def _glow_line(draw: ImageDraw.ImageDraw, x0, y0, x1, y1, colour, width=1):
    """Draw a subtle glowing horizontal/vertical separator line."""
    r, g, b = colour
    for offset, alpha_factor in [(-1, 0.3), (0, 1.0), (1, 0.3)]:
        faded = (int(r * alpha_factor), int(g * alpha_factor), int(b * alpha_factor))
        draw.line([(x0, y0 + offset), (x1, y1 + offset)], fill=faded, width=width)


def _draw_card(draw: ImageDraw.ImageDraw, x, y, w, h, bg_color, border_color):
    """Draw a rounded-corner-style card (approximated with rectangles)."""
    radius = 12
    draw.rounded_rectangle([x, y, x + w, y + h], radius=radius, fill=bg_color, outline=border_color, width=1)


def _short_name(company: str, max_len: int = 16) -> str:
    """Truncate company name to fit card width."""
    return company if len(company) <= max_len else company[:max_len - 1] + "…"


def generate_image(
    gainers: list[dict],
    losers: list[dict],
    prediction_date: date,
) -> bytes:
    """
    Render the Instagram post and return PNG bytes.
    Layout (1080×1080):
      ┌──────────────────────────────────┐
      │           HEADER / BRAND         │  ~175 px
      ├────────────────┬─────────────────┤
      │  TOP 5 GAINERS │  TOP 5 LOSERS   │  ~790 px
      │   (5 cards)    │   (5 cards)     │
      ├────────────────┴─────────────────┤
      │              FOOTER              │  ~115 px
      └──────────────────────────────────┘
    """
    img  = Image.new("RGB", (IMG_SIZE, IMG_SIZE))
    draw = ImageDraw.Draw(img)

    # ── Background ──
    _gradient_bg(draw)

    # ── Subtle dot grid overlay ──
    for gx in range(0, IMG_SIZE, 36):
        for gy in range(0, IMG_SIZE, 36):
            draw.ellipse([gx - 1, gy - 1, gx + 1, gy + 1], fill=(30, 38, 65))

    # ── Fonts ──
    f_brand     = _load_font(26, bold=True)
    f_title_lg  = _load_font(52, bold=True)
    f_date_lg   = _load_font(22, bold=True)       # NEW: big prominent date
    f_col_hdr   = _load_font(24, bold=True)
    f_rank      = _load_font(22, bold=True)
    f_company   = _load_font(20, bold=True)
    f_ticker    = _load_font(15, bold=False)
    f_pct       = _load_font(24, bold=True)
    f_footer    = _load_font(15, bold=False)
    f_hashtag   = _load_font(13, bold=False)

    # ──────────────────── HEADER ────────────────────
    HEADER_H = 180

    # Brand pill (top center)
    brand_text = "StockAnalytics.me"
    bw = draw.textlength(brand_text, font=f_brand)
    bx = (IMG_SIZE - bw) / 2
    draw.rounded_rectangle([bx - 18, 16, bx + bw + 18, 56], radius=22,
                            fill=(20, 32, 68), outline=ACCENT_BLUE, width=2)
    draw.text((bx, 22), brand_text, font=f_brand, fill=ACCENT_BLUE)

    # Main headline
    headline = "Daily Nifty Predictions"
    hw = draw.textlength(headline, font=f_title_lg)
    draw.text(((IMG_SIZE - hw) / 2, 64), headline, font=f_title_lg, fill=TEXT_WHITE)

    # ── Prominent date badge ──
    date_str   = prediction_date.strftime("%d %B %Y").upper()
    day_str    = prediction_date.strftime("%A").upper()
    full_date  = f"{day_str}  |  {date_str}"
    dw         = draw.textlength(full_date, font=f_date_lg)
    dx         = (IMG_SIZE - dw) / 2
    # Glowing pill background for the date
    draw.rounded_rectangle(
        [dx - 22, 124, dx + dw + 22, 166],
        radius=18,
        fill=(30, 20, 60),
        outline=TEXT_GOLD,
        width=2,
    )
    # Left accent stripe inside pill
    draw.rounded_rectangle([dx - 22, 124, dx - 6, 166], radius=10, fill=TEXT_GOLD)
    draw.text((dx, 132), full_date, font=f_date_lg, fill=TEXT_GOLD)

    # Header divider
    _glow_line(draw, PADDING, HEADER_H, IMG_SIZE - PADDING, HEADER_H, DIVIDER, width=1)

    # ──────────────────── COLUMNS ────────────────────
    FOOTER_H    = 115
    COLS_TOP    = HEADER_H + 12
    COLS_BOTTOM = IMG_SIZE - FOOTER_H

    COL_G_X     = PADDING                       # gainers column X
    COL_L_X     = PADDING + COL_WIDTH + PADDING  # losers column X

    # Column header labels
    gh_text = "TOP GAINERS"
    lh_text = "TOP LOSERS"
    CHY     = COLS_TOP + 8

    # Gainer header pill
    ghw = draw.textlength(gh_text, font=f_col_hdr)
    draw.rounded_rectangle([COL_G_X - 2, CHY - 6, COL_G_X + ghw + 20, CHY + 36],
                            radius=10, fill=(10, 50, 28), outline=GAINER_PRIMARY, width=2)
    draw.text((COL_G_X + 10, CHY), gh_text, font=f_col_hdr, fill=GAINER_PRIMARY)

    # Loser header pill
    lhw = draw.textlength(lh_text, font=f_col_hdr)
    draw.rounded_rectangle([COL_L_X - 2, CHY - 6, COL_L_X + lhw + 20, CHY + 36],
                            radius=10, fill=(50, 10, 18), outline=LOSER_PRIMARY, width=2)
    draw.text((COL_L_X + 10, CHY), lh_text, font=f_col_hdr, fill=LOSER_PRIMARY)

    # Vertical divider between columns
    mid_x = PADDING + COL_WIDTH + PADDING // 2
    _glow_line(draw, mid_x, COLS_TOP, mid_x, COLS_BOTTOM, DIVIDER, width=1)

    # ── Cards ── (5 per column, so they can be taller)
    CARD_TOP   = CHY + 50
    CARD_H     = 112
    CARD_GAP   = 8
    RANK_W     = 36

    def draw_card_row(items: list[dict], col_x: int, is_gainer: bool):
        bg_col     = CARD_BG_GAINER if is_gainer else CARD_BG_LOSER
        accent_col = GAINER_PRIMARY if is_gainer else LOSER_PRIMARY
        dim_col    = GAINER_DIM     if is_gainer else LOSER_DIM

        for i, stock in enumerate(items):
            cy = CARD_TOP + i * (CARD_H + CARD_GAP)
            cx = col_x

            # Card background
            _draw_card(draw, cx, cy, COL_WIDTH, CARD_H, bg_col, dim_col)

            # Rank badge — filled circle
            rank_str  = str(i + 1)
            badge_cx  = cx + RANK_W // 2 + 8
            badge_cy  = cy + CARD_H // 2
            badge_r   = 20
            draw.ellipse(
                [badge_cx - badge_r, badge_cy - badge_r,
                 badge_cx + badge_r, badge_cy + badge_r],
                fill=dim_col,
            )
            rw = draw.textlength(rank_str, font=f_rank)
            draw.text((badge_cx - rw / 2, badge_cy - 13), rank_str,
                      font=f_rank, fill=TEXT_GOLD)

            # Separator after rank
            sep_x = cx + RANK_W + 22
            draw.line([(sep_x, cy + 14), (sep_x, cy + CARD_H - 14)],
                      fill=dim_col, width=1)

            # Company name (larger, bold)
            name_x = sep_x + 14
            short  = _short_name(stock["Company"], max_len=18)
            draw.text((name_x, cy + 16), short, font=f_company, fill=TEXT_WHITE)

            # Ticker + Sentiment tag
            tag_str = f"{stock['Ticker'].replace('.NS','')}  |  {stock['Sentiment']}"
            draw.text((name_x, cy + 48), tag_str, font=f_ticker, fill=TEXT_MUTED)

            # Predicted return percentage (right-aligned, big)
            pct_sign  = "+" if stock["Return"] >= 0 else ""
            pct_str   = f"{pct_sign}{stock['Return']:.2f}%"
            pct_w     = draw.textlength(pct_str, font=f_pct)
            pct_x     = cx + COL_WIDTH - pct_w - 12
            pct_color = GAINER_PRIMARY if stock["Return"] >= 0 else LOSER_PRIMARY
            draw.text((pct_x, cy + CARD_H // 2 - 14), pct_str,
                      font=f_pct, fill=pct_color)

    draw_card_row(gainers, COL_G_X, is_gainer=True)
    draw_card_row(losers,  COL_L_X, is_gainer=False)

    # ──────────────────── FOOTER ────────────────────
    footer_y = COLS_BOTTOM + 8
    _glow_line(draw, PADDING, footer_y, IMG_SIZE - PADDING, footer_y, DIVIDER)

    # Disclaimer line
    disclaimer = "AI-generated predictions  |  Not financial advice"
    dw2 = draw.textlength(disclaimer, font=f_footer)
    draw.text(((IMG_SIZE - dw2) / 2, footer_y + 16), disclaimer,
              font=f_footer, fill=TEXT_MUTED)

    # Follow CTA
    cta = "Follow @StockAnalyticsIN for daily pre-market insights"
    ctaw = draw.textlength(cta, font=f_footer)
    draw.text(((IMG_SIZE - ctaw) / 2, footer_y + 42), cta,
              font=f_footer, fill=ACCENT_BLUE)

    # Hashtag line
    hashtags = "#Nifty50  #NSE  #StockMarket  #IndianStocks  #AlgoTrading"
    hhtw = draw.textlength(hashtags, font=f_hashtag)
    draw.text(((IMG_SIZE - hhtw) / 2, footer_y + FOOTER_H - 28),
              hashtags, font=f_hashtag, fill=(55, 75, 110))

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ─────────────────────────────────────────
# 4. CLOUDINARY UPLOAD
# ─────────────────────────────────────────
def upload_to_cloudinary(image_bytes: bytes, public_id: str) -> tuple[str, str]:
    """
    Upload PNG bytes to Cloudinary under stock_analytics/.
    Each day's image is kept permanently for a full post history.
    Returns (secure_url, public_id_with_folder).
    """
    result = cloudinary.uploader.upload(
        image_bytes,
        public_id=public_id,
        folder="stock_analytics",
        resource_type="image",
        format="jpg",          # Instagram requires JPEG
        transformation=[
            {"width": 1080, "height": 1080, "crop": "fill"},
            {"quality": "auto:best"},
        ],
    )
    return result["secure_url"], result["public_id"]


# ─────────────────────────────────────────
# 5. META GRAPH API — INSTAGRAM PUBLISH
# ─────────────────────────────────────────
def _ig_get(endpoint: str, params: dict) -> dict:
    """GET from the Meta Graph API and raise on error."""
    url = f"{IG_API_BASE}{endpoint}"
    resp = requests.get(url, params=params, timeout=30)
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Meta API error: {data['error']}")
    return data


def _ig_post(endpoint: str, payload: dict) -> dict:
    """POST to the Meta Graph API and raise on error."""
    url = f"{IG_API_BASE}{endpoint}"
    resp = requests.post(url, data=payload, timeout=30)
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Meta API error: {data['error']}")
    return data


def wait_for_media_container(creation_id: str, timeout: int = 90, interval: int = 5) -> None:
    """
    Poll the Instagram media container status until it is FINISHED and ready to publish.
    Meta docs: When an image or video container is created, Instagram fetches and processes
    the media asynchronously. Calling /media_publish before status_code is 'FINISHED' results in
    error 9007 / subcode 2207027 ('The media is not ready to be published. Please wait a moment.').
    """
    print(f"  Step 1.5: Waiting for media container {creation_id} to be ready...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_data = _ig_get(
            f"/{creation_id}",
            {
                "fields": "status_code,status",
                "access_token": IG_ACCESS_TOKEN,
            },
        )
        status_code = status_data.get("status_code", "UNKNOWN")
        print(f"    Container status: {status_code}")

        if status_code == "FINISHED":
            return
        elif status_code == "ERROR":
            raise RuntimeError(f"Media container processing failed on Meta's side: {status_data}")
        elif status_code == "EXPIRED":
            raise RuntimeError(f"Media container expired: {status_data}")

        time.sleep(interval)

    raise TimeoutError(f"Media container {creation_id} processing timed out after {timeout} seconds.")


def publish_to_instagram(image_url: str, caption: str) -> str:
    """
    Three-step Instagram publish:
      Step 1   → Create a media container (returns creation_id)
      Step 1.5 → Poll until container status is FINISHED
      Step 2   → Publish the container (returns ig_media_id)
    Returns the final Instagram media ID.
    """
    print("  Step 1: Creating Instagram media container...")
    container = _ig_post(
        f"/{IG_ACCOUNT_ID}/media",
        {
            "image_url":  image_url,
            "caption":    caption,
            "access_token": IG_ACCESS_TOKEN,
        },
    )
    creation_id = container["id"]
    print(f"    Container ID: {creation_id}")

    # Wait for Instagram to asynchronously download and process the Cloudinary image
    wait_for_media_container(creation_id, timeout=90, interval=5)

    print("  Step 2: Publishing media container...")
    max_publish_retries = 3
    for attempt in range(1, max_publish_retries + 1):
        try:
            publish = _ig_post(
                f"/{IG_ACCOUNT_ID}/media_publish",
                {
                    "creation_id":   creation_id,
                    "access_token":  IG_ACCESS_TOKEN,
                },
            )
            media_id = publish["id"]
            print(f"    Published! Instagram Media ID: {media_id}")
            return media_id
        except RuntimeError as e:
            if "2207027" in str(e) and attempt < max_publish_retries:
                print(f"    Media container still propagating (attempt {attempt}/{max_publish_retries}), retrying in 5s...")
                time.sleep(5)
            else:
                raise


# ─────────────────────────────────────────
# 6. CAPTION BUILDER
# ─────────────────────────────────────────
def build_caption(gainers: list[dict], losers: list[dict], prediction_date: date) -> str:
    date_str = prediction_date.strftime("%d %b %Y")
    g_lines  = "\n".join(
        f"  {i+1}. {s['Company'].title()} ({s['Ticker'].replace('.NS','')}) — +{s['Return']:.2f}%"
        for i, s in enumerate(gainers)
    )
    l_lines  = "\n".join(
        f"  {i+1}. {s['Company'].title()} ({s['Ticker'].replace('.NS','')}) — {s['Return']:.2f}%"
        for i, s in enumerate(losers)
    )
    return textwrap.dedent(f"""
        📊 Nifty50 AI Predictions — {date_str}
        For more info go to https://stockanalytics.me

        🟢 Top 5 Predicted Gainers:
        {g_lines}

        🔴 Top 5 Predicted Losers:
        {l_lines}

        The graphics is AI generated.

        #Nifty50 #NSE #StockMarket #IndianStocks #StockPrediction
        #TradingView #Sensex #QuantTrading #AlgoTrading #StockAnalysis
    """).strip()


# ─────────────────────────────────────────
# 7. MAIN
# ─────────────────────────────────────────
def main(preview_only: bool = False):
    IST = ZoneInfo("Asia/Kolkata")
    now = datetime.now(IST)
    print(f"\n{'=' * 55}")
    print(f"  Instagram Publisher  |  {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print(f"{'=' * 55}")

    # ── 1. Fetch data ──
    print("\n[1/5] Fetching predictions from Supabase...")
    gainers, losers, prediction_date = fetch_predictions()
    print(f"  Gainers: {len(gainers)}  |  Losers: {len(losers)}")

    # ── 2. Generate image ──
    print("\n[2/5] Generating Instagram post image...")
    image_bytes = generate_image(gainers, losers, prediction_date)
    print(f"  Image size: {len(image_bytes) / 1024:.1f} KB")

    if preview_only:
        with open(PREVIEW_PATH, "wb") as f:
            f.write(image_bytes)
        print(f"\n[OK] Preview saved locally: {PREVIEW_PATH}")
        print("   Run without --preview to publish to Instagram.\n")
        return

    # ── 3. Upload to Cloudinary ──
    print("\n[3/5] Uploading to Cloudinary...")
    public_id_name = f"daily_{prediction_date.strftime('%Y%m%d')}_{now.strftime('%H%M%S')}"
    image_url, cloudinary_id = upload_to_cloudinary(image_bytes, public_id_name)
    print(f"  Public URL: {image_url}")

    # ── 4. Build caption ──
    caption = build_caption(gainers, losers, prediction_date)

    # ── 4. Publish to Instagram ──
    print("\n[4/4] Publishing to Instagram...")
    media_id = publish_to_instagram(image_url, caption)

    print(f"\n{'=' * 55}")
    print(f"  [OK] Posted successfully!  Media ID: {media_id}")
    print(f"  Image stored at: stock_analytics/{public_id_name}")
    print(f"{'=' * 55}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Post daily stock predictions to Instagram.")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Generate and save the image locally without uploading or posting.",
    )
    args = parser.parse_args()
    main(preview_only=args.preview)
