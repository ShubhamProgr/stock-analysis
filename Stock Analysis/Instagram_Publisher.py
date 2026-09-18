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
from PIL import Image, ImageDraw, ImageFont, ImageFilter

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
# 1. DESIGN CONSTANTS & TYPOGRAPHY
# ─────────────────────────────────────────
FONTS_DIR       = os.path.join(os.path.dirname(__file__), "fonts")

# Canvas Dimensions — Instagram 4:5 Portrait Ratio (Optimal for Feed Visibility)
IMG_W           = 1080
IMG_H           = 1350
PADDING_X       = 48
GAP_COL         = 20
COL_WIDTH       = (IMG_W - (PADDING_X * 2) - GAP_COL) // 2  # 482 px each

# Refined Fintech Palette
BG_DARK_TOP     = (7, 10, 19)
BG_DARK_MID     = (10, 14, 27)
BG_DARK_BOT     = (6, 8, 16)

CARD_BG         = (15, 23, 42, 225)       # Translucent slate
CARD_BORDER_GAINER = (22, 101, 52)        # Emerald border
CARD_BORDER_LOSER  = (153, 27, 27)        # Rose border

GREEN_PRIMARY   = (52, 211, 153)          # Emerald 400
GREEN_BG        = (6, 78, 59, 140)        # Deep emerald pill
GREEN_LIGHT     = (167, 243, 208)

RED_PRIMARY     = (251, 113, 133)         # Rose 400
RED_BG          = (136, 19, 55, 140)      # Deep rose pill
RED_LIGHT       = (254, 205, 211)

TEXT_WHITE      = (255, 255, 255)
TEXT_SUB        = (203, 213, 225)         # Slate 300
TEXT_MUTED      = (148, 163, 184)         # Slate 400
TEXT_DIM        = (100, 116, 139)         # Slate 500
ACCENT_CYAN     = (56, 189, 248)          # Sky 400
ACCENT_BLUE     = (96, 165, 250)          # Blue 400
ACCENT_GOLD     = (250, 204, 21)          # Amber 400

_FONTS_CHECKED = False

def _ensure_fonts() -> None:
    """Download required Inter font files on-demand if missing."""
    global _FONTS_CHECKED
    if _FONTS_CHECKED:
        return
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
                    with open(dest, "wb") as f:
                        f.write(r.content)
            except Exception:
                pass


def _load_font(size: int, weight: str = "regular") -> ImageFont.ImageFont:
    """
    Load bundled modern fonts (Inter / Plus Jakarta Sans) or fallback gracefully.
    Downloads on-demand if missing on headless/cloud environments.
    """
    _ensure_fonts()
    weight_map = {
        "bold": ["Inter-Bold.ttf", "Inter-700.ttf", "PlusJakartaSans-600.ttf", "segoeuib.ttf", "arialbd.ttf"],
        "semibold": ["Inter-SemiBold.ttf", "Inter-600.ttf", "seguisb.ttf", "segoeuib.ttf", "arialbd.ttf"],
        "regular": ["Inter-Regular.ttf", "Inter-400.ttf", "PlusJakartaSans-400.ttf", "segoeui.ttf", "arial.ttf"],
        "medium": ["Inter-SemiBold.ttf", "Inter-500.ttf", "PlusJakartaSans-500.ttf", "segoeui.ttf", "arial.ttf"],
    }
    candidates = weight_map.get(weight.lower(), weight_map["regular"])
    for name in candidates:
        # 1. Check local fonts folder
        p = os.path.join(FONTS_DIR, name)
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
        # 2. Check Windows fonts directory
        win_p = os.path.join(r"C:\Windows\Fonts", name)
        if os.path.exists(win_p):
            try:
                return ImageFont.truetype(win_p, size)
            except Exception:
                pass
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
def _draw_arrow(draw: ImageDraw.ImageDraw, x: int, y: int, size: int, color: tuple, upward: bool = True):
    """Draw a crisp vector directional arrow."""
    h = size
    w = int(size * 1.15)
    if upward:
        points = [(x + w // 2, y), (x, y + h), (x + w, y + h)]
    else:
        points = [(x, y), (x + w, y), (x + w // 2, y + h)]
    draw.polygon(points, fill=color)


def _draw_bookmark_icon(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, color: tuple):
    """Draw a clean bookmark ribbon icon."""
    pts = [(x, y), (x + w, y), (x + w, y + h), (x + w // 2, y + h - 5), (x, y + h)]
    draw.polygon(pts, fill=color)


def generate_image(
    gainers: list[dict],
    losers: list[dict],
    prediction_date: date,
) -> bytes:
    """
    Render a high-conversion 1080x1350 (4:5 portrait) Instagram post and return JPEG bytes.
    Layout:
      ┌──────────────────────────────────────────────┐
      │  HEADER: Brand, Horizon & Date Pills         │  ~250 px
      ├──────────────────────┬───────────────────────┤
      │  TOP 5 GAINERS       │  TOP 5 LOSERS         │  ~910 px
      │  (5 Glass Cards)     │  (5 Glass Cards)      │
      ├──────────────────────┴───────────────────────┤
      │  FOOTER: Handle Pill, Save CTA & Disclaimer  │  ~190 px
      └──────────────────────────────────────────────┘
    """
    # 1. Base Gradient Canvas
    base = Image.new("RGBA", (IMG_W, IMG_H), BG_DARK_TOP)
    draw = ImageDraw.Draw(base)

    # Multi-stop vertical gradient for dark slate depth
    for y in range(IMG_H):
        if y < IMG_H // 2:
            t = y / (IMG_H // 2)
            c1, c2 = BG_DARK_TOP, BG_DARK_MID
        else:
            t = (y - IMG_H // 2) / (IMG_H // 2)
            c1, c2 = BG_DARK_MID, BG_DARK_BOT
        r = int(c1[0] + (c2[0] - c1[0]) * t)
        g = int(c1[1] + (c2[1] - c1[1]) * t)
        b = int(c1[2] + (c2[2] - c1[2]) * t)
        draw.line([(0, y), (IMG_W, y)], fill=(r, g, b, 255))

    # 2. Ambient Lighting Layer (Glow Orbs)
    glow_layer = Image.new("RGBA", (IMG_W, IMG_H), (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_layer)

    # Top Cyan glow behind title
    for rad, alpha in [(300, 14), (200, 22), (100, 30)]:
        glow_draw.ellipse([540 - rad, 110 - rad // 2, 540 + rad, 110 + rad // 2],
                          fill=(56, 189, 248, alpha))

    # Left Emerald glow behind Gainers
    for rad, alpha in [(380, 16), (250, 24), (120, 32)]:
        glow_draw.ellipse([270 - rad, 760 - rad, 270 + rad, 760 + rad],
                          fill=(16, 185, 129, alpha))

    # Right Ruby glow behind Losers
    for rad, alpha in [(380, 16), (250, 24), (120, 32)]:
        glow_draw.ellipse([810 - rad, 760 - rad, 810 + rad, 760 + rad],
                          fill=(244, 63, 94, alpha))

    glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(35))
    base = Image.alpha_composite(base, glow_layer)
    draw = ImageDraw.Draw(base)

    # Subtle tech background dot grid
    for gx in range(40, IMG_W, 40):
        for gy in range(40, IMG_H, 40):
            draw.point((gx, gy), fill=(255, 255, 255, 12))

    # Load Fonts
    f_brand      = _load_font(18, "bold")
    f_badge      = _load_font(13, "bold")
    f_title      = _load_font(48, "bold")
    f_date       = _load_font(15, "semibold")
    f_col_title  = _load_font(21, "bold")
    f_col_sub    = _load_font(12, "bold")
    f_rank       = _load_font(13, "bold")
    f_name       = _load_font(19, "bold")
    f_ticker     = _load_font(12, "medium")
    f_tag        = _load_font(11, "bold")
    f_pct        = _load_font(22, "bold")
    f_pct_lbl    = _load_font(10, "semibold")
    f_footer_cta = _load_font(15, "bold")
    f_footer_sub = _load_font(12, "regular")

    # ──────────────────── HEADER SECTION ────────────────────
    # Top Brand Bar
    brand_text = "STOCKANALYSIS.ME"
    bw = draw.textlength(brand_text, font=f_brand)
    bx = PADDING_X
    draw.rounded_rectangle([bx, 44, bx + bw + 44, 82], radius=19,
                           fill=(15, 23, 42, 230), outline=(56, 189, 248, 140), width=1)
    # Glowing green dot inside brand pill
    draw.ellipse([bx + 14, 59, bx + 22, 67], fill=GREEN_PRIMARY)
    draw.text((bx + 32, 51), brand_text, font=f_brand, fill=TEXT_WHITE)

    # Right Category Badge
    cat_text = "NIFTY 50 • AI QUANT MODEL"
    cw = draw.textlength(cat_text, font=f_badge)
    cx = IMG_W - PADDING_X - cw - 28
    draw.rounded_rectangle([cx, 44, cx + cw + 28, 82], radius=19,
                           fill=(30, 41, 59, 180), outline=(71, 85, 105, 160), width=1)
    draw.text((cx + 14, 54), cat_text, font=f_badge, fill=ACCENT_CYAN)

    # Main Headline
    main_title = "Daily Market Forecast"
    tw = draw.textlength(main_title, font=f_title)
    draw.text(((IMG_W - tw) / 2, 114), main_title, font=f_title, fill=TEXT_WHITE)

    # Date + Forecast Horizon Pill
    date_formatted = prediction_date.strftime("%A, %d %B %Y").upper()
    sub_text = f"FORECAST HORIZON: NEXT TRADING DAY   •   {date_formatted}"
    sw = draw.textlength(sub_text, font=f_date)
    sx = (IMG_W - sw) / 2
    draw.rounded_rectangle([sx - 24, 186, sx + sw + 24, 224], radius=19,
                           fill=(23, 37, 84, 190), outline=(96, 165, 250, 110), width=1)
    # Dot indicator inside date badge
    draw.ellipse([sx - 12, 201, sx - 6, 207], fill=ACCENT_CYAN)
    draw.text((sx + 6, 194), sub_text, font=f_date, fill=ACCENT_CYAN)

    # Header Divider Line with Center Gradient Accent
    draw.line([(PADDING_X, 252), (IMG_W - PADDING_X, 252)], fill=(38, 48, 74, 160), width=1)
    draw.line([(IMG_W // 2 - 130, 252), (IMG_W // 2 + 130, 252)], fill=(56, 189, 248, 220), width=2)

    # ──────────────────── COLUMNS CONFIGURATION ────────────────────
    COL_G_X = PADDING_X
    COL_L_X = PADDING_X + COL_WIDTH + GAP_COL

    CH_Y = 280
    CH_H = 46

    # ── Gainers Header Pill ──
    draw.rounded_rectangle([COL_G_X, CH_Y, COL_G_X + COL_WIDTH, CH_Y + CH_H], radius=12,
                           fill=(6, 78, 59, 140), outline=(16, 185, 129, 160), width=1)
    _draw_arrow(draw, COL_G_X + 18, CH_Y + 16, 14, GREEN_PRIMARY, upward=True)
    draw.text((COL_G_X + 40, CH_Y + 11), "TOP 5 GAINERS", font=f_col_title, fill=GREEN_PRIMARY)
    # Sub-badge right
    gbadge = "BULLISH BIAS"
    gbw = draw.textlength(gbadge, font=f_col_sub)
    draw.rounded_rectangle([COL_G_X + COL_WIDTH - gbw - 24, CH_Y + 11, COL_G_X + COL_WIDTH - 12, CH_Y + CH_H - 11],
                           radius=6, fill=(16, 185, 129, 45))
    draw.text((COL_G_X + COL_WIDTH - gbw - 18, CH_Y + 14), gbadge, font=f_col_sub, fill=GREEN_LIGHT)

    # ── Losers Header Pill ──
    draw.rounded_rectangle([COL_L_X, CH_Y, COL_L_X + COL_WIDTH, CH_Y + CH_H], radius=12,
                           fill=(136, 19, 55, 140), outline=(244, 63, 94, 160), width=1)
    _draw_arrow(draw, COL_L_X + 18, CH_Y + 16, 14, RED_PRIMARY, upward=False)
    draw.text((COL_L_X + 40, CH_Y + 11), "TOP 5 LOSERS", font=f_col_title, fill=RED_PRIMARY)
    # Sub-badge right
    lbadge = "BEARISH BIAS"
    lbw = draw.textlength(lbadge, font=f_col_sub)
    draw.rounded_rectangle([COL_L_X + COL_WIDTH - lbw - 24, CH_Y + 11, COL_L_X + COL_WIDTH - 12, CH_Y + CH_H - 11],
                           radius=6, fill=(244, 63, 94, 45))
    draw.text((COL_L_X + COL_WIDTH - lbw - 18, CH_Y + 14), lbadge, font=f_col_sub, fill=RED_LIGHT)

    # ──────────────────── CARDS (5 rows) ────────────────────
    CARD_START_Y = 346
    CARD_H       = 146
    CARD_GAP     = 14

    def draw_cards(stocks: list[dict], col_x: int, is_gainer: bool):
        card_outline = CARD_BORDER_GAINER if is_gainer else CARD_BORDER_LOSER
        primary_col  = GREEN_PRIMARY if is_gainer else RED_PRIMARY
        badge_bg     = GREEN_BG if is_gainer else RED_BG

        for i, s in enumerate(stocks):
            cy = CARD_START_Y + i * (CARD_H + CARD_GAP)

            # 1. Glass Card Container
            draw.rounded_rectangle([col_x, cy, col_x + COL_WIDTH, cy + CARD_H], radius=14,
                                   fill=(15, 23, 42, 225), outline=card_outline, width=1)

            # Left accent highlight bar
            draw.rounded_rectangle([col_x, cy + 24, col_x + 3, cy + CARD_H - 24], radius=2,
                                   fill=primary_col)

            # 2. Rank Badge
            rank_str = f"#{i+1}"
            rx = col_x + 16
            ry = cy + 18
            rw = 36
            rh = 26
            rank_bg = (30, 41, 59, 230) if i > 0 else ((180, 83, 9, 210) if is_gainer else (153, 27, 27, 210))
            rank_outline = (71, 85, 105, 140) if i > 0 else (ACCENT_GOLD if is_gainer else primary_col)
            draw.rounded_rectangle([rx, ry, rx + rw, ry + rh], radius=6,
                                   fill=rank_bg, outline=rank_outline, width=1)
            rnk_w = draw.textlength(rank_str, font=f_rank)
            draw.text((rx + (rw - rnk_w) / 2, ry + 5), rank_str, font=f_rank,
                      fill=TEXT_WHITE if i > 0 else ACCENT_GOLD)

            # 3. Company Name (Up to 21 chars cleanly)
            cname = s["Company"]
            if len(cname) > 21:
                cname = cname[:20] + "…"
            name_x = rx + rw + 12
            name_y = cy + 19
            draw.text((name_x, name_y), cname, font=f_name, fill=TEXT_WHITE)

            # 4. Ticker Badge (e.g. NSE: RELIANCE)
            ticker_clean = s["Ticker"].replace(".NS", "")
            ticker_txt = f"NSE: {ticker_clean}"
            tick_y = cy + 60
            draw.rounded_rectangle([col_x + 16, tick_y, col_x + 16 + draw.textlength(ticker_txt, font=f_ticker) + 14, tick_y + 24],
                                   radius=5, fill=(30, 41, 59, 140), outline=(51, 65, 85, 180), width=1)
            draw.text((col_x + 23, tick_y + 4), ticker_txt, font=f_ticker, fill=TEXT_SUB)

            # 5. Sentiment Badge
            sent_raw = (s.get("Sentiment") or "NEUTRAL").upper()
            if "POS" in sent_raw or "BULL" in sent_raw:
                sent_label = "BULLISH"
                sent_color = GREEN_PRIMARY
                sent_bg    = (6, 78, 59, 180)
            elif "NEG" in sent_raw or "BEAR" in sent_raw:
                sent_label = "BEARISH"
                sent_color = RED_PRIMARY
                sent_bg    = (136, 19, 55, 180)
            else:
                sent_label = "NEUTRAL"
                sent_color = (203, 213, 225)
                sent_bg    = (51, 65, 85, 180)

            sent_x = col_x + 16
            sent_y = cy + 98
            sent_w = draw.textlength(sent_label, font=f_tag) + 28
            draw.rounded_rectangle([sent_x, sent_y, sent_x + sent_w, sent_y + 24], radius=6,
                                   fill=sent_bg, outline=sent_color, width=1)
            # Dot indicator inside sentiment badge
            draw.ellipse([sent_x + 8, sent_y + 9, sent_x + 14, sent_y + 15], fill=sent_color)
            draw.text((sent_x + 20, sent_y + 4), sent_label, font=f_tag, fill=sent_color)

            # 6. Big Return Pill (Right-Aligned)
            ret_val = s["Return"]
            sign = "+" if ret_val >= 0 else ""
            pct_num_str = f"{sign}{ret_val:.2f}%"

            num_w = draw.textlength(pct_num_str, font=f_pct)
            pw = num_w + 40
            ph = 44
            px = col_x + COL_WIDTH - pw - 16
            py = cy + 24

            draw.rounded_rectangle([px, py, px + pw, py + ph], radius=10,
                                   fill=badge_bg, outline=primary_col, width=1)
            # Vector arrow inside pill
            _draw_arrow(draw, px + 12, py + 16, 13, primary_col, upward=(ret_val >= 0))
            draw.text((px + 30, py + 8), pct_num_str, font=f_pct, fill=primary_col)

            # "PREDICTED 1D" label below pill
            lbl = "PREDICTED 1D"
            lbl_w = draw.textlength(lbl, font=f_pct_lbl)
            draw.text((px + pw - lbl_w - 4, py + ph + 8), lbl, font=f_pct_lbl, fill=TEXT_DIM)

    draw_cards(gainers, COL_G_X, is_gainer=True)
    draw_cards(losers,  COL_L_X, is_gainer=False)

    # ──────────────────── FOOTER SECTION ────────────────────
    FOOTER_Y = 1175
    draw.line([(PADDING_X, FOOTER_Y), (IMG_W - PADDING_X, FOOTER_Y)], fill=(38, 48, 74, 180), width=1)
    draw.line([(IMG_W // 2 - 90, FOOTER_Y), (IMG_W // 2 + 90, FOOTER_Y)], fill=(56, 189, 248, 200), width=2)

    # Footer Action Container
    fc_w = IMG_W - (PADDING_X * 2)
    fc_y = FOOTER_Y + 16
    draw.rounded_rectangle([PADDING_X, fc_y, PADDING_X + fc_w, fc_y + 70], radius=14,
                           fill=(15, 23, 42, 210), outline=(51, 65, 85, 140), width=1)

    # Instagram handle pill (Left)
    handle_txt = " @StockAnalyticsIN "
    hw = draw.textlength(handle_txt, font=f_footer_cta)
    hx = PADDING_X + 16
    hy = fc_y + 16
    draw.rounded_rectangle([hx, hy, hx + hw + 16, hy + 38], radius=8,
                           fill=(30, 41, 59, 240), outline=ACCENT_CYAN, width=1)
    draw.text((hx + 8, hy + 8), handle_txt, font=f_footer_cta, fill=ACCENT_CYAN)

    # Bookmark CTA (Right)
    cta_main = "Save for tomorrow's market session"
    cta_w = draw.textlength(cta_main, font=f_footer_cta)
    cta_rx = PADDING_X + fc_w - cta_w - 38
    draw.text((cta_rx, hy + 8), cta_main, font=f_footer_cta, fill=TEXT_WHITE)
    _draw_bookmark_icon(draw, PADDING_X + fc_w - 30, hy + 9, 14, 18, ACCENT_GOLD)

    # Subtitle disclaimer
    disclaimer = "AI quant predictions generated via ensemble ML • Educational purposes only • Not SEBI registered advice"
    dw = draw.textlength(disclaimer, font=f_footer_sub)
    draw.text(((IMG_W - dw) / 2, fc_y + 94), disclaimer, font=f_footer_sub, fill=TEXT_DIM)

    # Export to JPEG bytes
    buf = io.BytesIO()
    rgb_img = base.convert("RGB")
    rgb_img.save(buf, format="JPEG", quality=95, optimize=True)
    return buf.getvalue()


# ─────────────────────────────────────────
# 4. CLOUDINARY UPLOAD
# ─────────────────────────────────────────
def upload_to_cloudinary(image_bytes: bytes, public_id: str) -> tuple[str, str]:
    """
    Upload JPEG bytes to Cloudinary under stock_analytics/.
    Each day's image is kept permanently for a full post history.
    Returns (secure_url, public_id_with_folder).
    """
    result = cloudinary.uploader.upload(
        image_bytes,
        public_id=public_id,
        folder="stock_analytics",
        resource_type="image",
        format="jpg",          # Instagram requires JPEG
        access_mode="public",
        transformation=[
            {"width": 1080, "height": 1350, "crop": "fill"},
            {"quality": "auto:best"},
        ],
    )
    secure_url = result["secure_url"]

    # Warm up Cloudinary CDN edge so Meta's crawler doesn't hit a 404 / cache miss
    print("  Step 3.5: Warming up Cloudinary CDN URL...")
    for attempt in range(1, 6):
        try:
            r = requests.get(secure_url, timeout=10)
            if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("image/"):
                print(f"    CDN cache ready ({r.headers.get('Content-Type')}, {len(r.content)} bytes)")
                break
        except Exception as e:
            print(f"    CDN check {attempt} failed: {e}")
        time.sleep(2)

    # Brief delay to allow edge replication across Meta's geographic crawl nodes
    time.sleep(3)

    return secure_url, result["public_id"]


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
    max_container_retries = 4
    container = None
    for attempt in range(1, max_container_retries + 1):
        try:
            container = _ig_post(
                f"/{IG_ACCOUNT_ID}/media",
                {
                    "image_url":    image_url,
                    "caption":      caption,
                    "access_token": IG_ACCESS_TOKEN,
                },
            )
            break
        except RuntimeError as e:
            # 2207052 / 9004 = Media download failed / Meta could not fetch URL yet
            if ("2207052" in str(e) or "9004" in str(e)) and attempt < max_container_retries:
                wait_secs = attempt * 5
                print(f"    CDN URL not reached by Meta crawler yet (attempt {attempt}/{max_container_retries}), retrying in {wait_secs}s...")
                time.sleep(wait_secs)
            else:
                raise

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
