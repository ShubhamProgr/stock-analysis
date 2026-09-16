<div align="center">

# Stock Analysis Platform

**A full-stack, AI-powered stock analysis engine for the Indian equity market.**  
Real-time data ingestion · NLP sentiment analysis · XGBoost price prediction · Interactive Next.js dashboard

[![Next.js](https://img.shields.io/badge/Next.js-15-black?style=flat-square&logo=nextdotjs)](https://nextjs.org)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-ML%20Engine-orange?style=flat-square)](https://xgboost.readthedocs.io)
[![FinBERT](https://img.shields.io/badge/FinBERT-NLP%20Sentiment-yellow?style=flat-square&logo=huggingface&logoColor=white)](https://huggingface.co/ProsusAI/finbert)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Supabase-336791?style=flat-square&logo=postgresql&logoColor=white)](https://supabase.com)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

## What It Does

This platform continuously monitors **Nifty 50+ Indian equities**, fuses financial data with real-time news sentiment, and surfaces actionable insights through a modern web dashboard.

| Layer | What happens |
|---|---|
| **Data Ingestion** | `yfinance` fetches OHLCV data (daily + 5-year history) for 50+ NSE-listed tickers |
| **News Extraction** | Scrapers pull financial news; articles are mapped to tickers via NLP entity resolution |
| **Sentiment Analysis** | FinBERT (`transformers`) scores each article; daily aggregates are stored per ticker |
| **Price Prediction** | XGBoost models trained per-ticker on technical features, sentiment scores & macro indicators |
| **Visualization** | Next.js dashboard with candlestick charts, sentiment calendars, prediction overlays, and a stock screener |
| **Publishing** | Automated daily reports published to Instagram via Cloudinary + Graph API |

---

## Architecture

```
+---------------------------------------------------------------+
|                        Docker Compose                         |
|                                                               |
|  +-------------------------+    +-------------------------+   |
|  |      stock_engine       |    |      stock_website      |   |
|  |   (Python / Flask)      |    |   (Next.js / Node.js)   |   |
|  |                         |    |                         |   |
|  |  +-------------------+  |    |  +-------------------+  |   |
|  |  |   APScheduler     |  |    |  |  App Router (API)  |  |   |
|  |  |   (Cron Jobs)     |  |    |  |  /ticker           |  |   |
|  |  +--------+----------+  |    |  |  /sentiment        |  |   |
|  |           |             |    |  |  /predictions      |  |   |
|  |  +--------v----------+  |    |  |  /ohlc             |  |   |
|  |  | Analysis Scripts  |  |    |  +--------+-----------+  |   |
|  |  | * Stock_Data      |  |    |           |             |   |
|  |  | * Sentiment       |  |    |  +--------v-----------+  |   |
|  |  | * Final_Analysis  |  |    |  |  React Components  |  |   |
|  |  | * News_Extractor  |  |    |  |  * Dashboard       |  |   |
|  |  | * Instagram_Pub   |  |    |  |  * Candlestick     |  |   |
|  |  +--------+----------+  |    |  |  * StrategyView    |  |   |
|  +-----------|-------------+    |  |  * ScreenerView    |  |   |
|              |                  |  +--------------------+  |   |
|              |                  +-------------------------+   |
|              |                            |                   |
|              +-------------+--------------+                   |
|                            |                                  |
|                  +---------+---------+                        |
|                  |    PostgreSQL     |                        |
|                  |    (Supabase)     |                        |
|                  +-------------------+                        |
+---------------------------------------------------------------+
```

---

## Features

### Dashboard & Visualization
- **Candlestick Charts** — Interactive OHLC charts with EMA, Bollinger Bands, RSI & MACD overlays
- **Prediction vs. Actual** — Visual comparison of XGBoost forecasts against real closing prices
- **Sentiment Calendar** — Heatmap of daily aggregated news sentiment per ticker
- **Price-Sentiment Chart** — Overlaid price and sentiment time series to reveal correlation
- **Sector Heatmap** — Live sector-level performance view
- **Top Movers** — Real-time gainers/losers board

### AI & ML Engine
- **FinBERT Sentiment** — Financial-domain BERT model classifies news as positive / neutral / negative
- **XGBoost Prediction** — Per-ticker regression models trained with `TimeSeriesSplit` walk-forward validation, evaluated on MAE, RMSE, and R²
- **Peer Comparison** — Automatic peer group detection and relative performance ranking
- **Strategy Backtesting** — Rule-based strategy simulation using predicted signals

### Screener & Fundamentals
- **Stock Screener** — Filter by sector, market cap, momentum, and sentiment score
- **Company Fundamentals** — P/E, EPS, market cap, 52-week range, dividend yield
- **Model Insights** — Feature importances and per-model accuracy metrics
- **Watchlist** — Personalised ticker list persisted in PostgreSQL

### Automated Publishing
- **Daily Reports** — Formatted summary images auto-published to Instagram
- **Cloudinary CDN** — Image assets managed and delivered via Cloudinary

---

## Tech Stack

### Frontend
| Technology | Purpose |
|---|---|
| **Next.js 15** (App Router) | Server + client components, API route handlers |
| **TypeScript** | Type-safe React components |
| **Recharts / custom SVG** | Candlestick & sentiment charts |
| **next-cloudinary** | Optimized image delivery |

### Backend / Engine
| Technology | Purpose |
|---|---|
| **Python 3.11+** | Core analysis runtime |
| **Flask + Gunicorn** | REST API server for the engine |
| **Flask-APScheduler** | Cron-based job scheduling |
| **yfinance** | Market data ingestion (NSE) |
| **XGBoost** | Per-ticker price prediction models |
| **scikit-learn** | Preprocessing, cross-validation, metrics |
| **HuggingFace Transformers + PyTorch** | FinBERT NLP sentiment pipeline |
| **pandas / numpy** | Data manipulation & feature engineering |
| **SQLAlchemy + psycopg2** | ORM and PostgreSQL connectivity |
| **Cloudinary + Pillow** | Image generation and upload |

### Infrastructure
| Technology | Purpose |
|---|---|
| **PostgreSQL (Supabase)** | Primary data store for all market & sentiment data |
| **Docker + Docker Compose** | Containerized, reproducible deployment |
| **dotenv** | Unified environment configuration |

---

## Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) *(recommended)*
- **or** Node.js 20+ and Python 3.11+ for local development

### 1. Clone & Configure

```bash
git clone https://github.com/ShubhamProgr/stock-analysis.git
cd stock-analysis
```

### 2. Environment Variables

Create a `.env` file at the project root:

```env
# Database — Supabase or self-hosted Postgres
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/dbname
# OR use individual Supabase vars:
SUPABASE_DB_HOST=
SUPABASE_DB_PORT=5432
SUPABASE_DB_NAME=postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD=
SUPABASE_DB_SSLMODE=require

# File paths (used inside container)
NEWS_FILE=/app/data/news.csv
SENTIMENT_OUTPUT_FILE=/app/data/sentiment.csv

# Cloudinary (image hosting for reports)
CLOUDINARY_CLOUD_NAME=
CLOUDINARY_API_KEY=
CLOUDINARY_API_SECRET=

# Instagram Graph API (automated publishing)
INSTAGRAM_ACCESS_TOKEN=
INSTAGRAM_ACCOUNT_ID=
```

### 3. Run with Docker *(recommended)*

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Web Dashboard | http://localhost:5000 |
| Engine API | Internal (consumed by website container) |

### 4. Run Locally (Development)

**Python engine:**
```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux
pip install -r requirements.txt
python "stock analysis/Final_Analysis.py"
```

**Next.js website:**
```bash
cd Website
npm install
npm run dev
# -> http://localhost:3000
```

---

## API Reference

All routes live under `/api/` in the Next.js App Router (`Website/src/app/api/`).

| Endpoint | Method | Description |
|---|---|---|
| `/api/ticker/[ticker]` | `GET` | Company metadata and fundamentals |
| `/api/ohlc/[ticker]` | `GET` | OHLCV candlestick data |
| `/api/sentiment/[ticker]` | `GET` | Daily aggregated sentiment scores |
| `/api/predictions/[ticker]` | `GET` | XGBoost price predictions |
| `/api/accuracy/[ticker]` | `GET` | Model accuracy metrics (MAE, RMSE, R²) |
| `/api/peers/[ticker]` | `GET` | Peer comparison data |
| `/api/compare` | `GET` | Multi-ticker relative performance |
| `/api/market` | `GET` | Market-wide overview |
| `/api/movers` | `GET` | Top gainers and losers |
| `/api/screener` | `GET` | Filtered stock screener results |
| `/api/watchlist` | `GET/POST` | User watchlist management |

---

## Project Structure

```
stock-analysis/
├── stock analysis/             # Python ML & data engine
│   ├── Final_Analysis.py       # XGBoost training & prediction pipeline
│   ├── Sentiment_Analyzer.py   # FinBERT NLP sentiment pipeline
│   ├── News_Extractor.py       # Financial news scraper & ticker mapper
│   ├── Stock_Data_Daily.py     # Daily OHLCV ingestion via yfinance
│   ├── Stock_Data_5Y.py        # 5-year historical data ingestion
│   ├── Company_Data.py         # Fundamentals fetcher
│   ├── Instagram_Publisher.py  # Automated Instagram report publisher
│   ├── Actual_vs_Prediction.py # Backtesting accuracy calculator
│   └── Reports.py              # Report generation utilities
│
├── Website/                    # Next.js 15 frontend
│   └── src/
│       ├── app/
│       │   ├── page.tsx        # Application entry point
│       │   ├── layout.tsx      # Root layout
│       │   └── api/            # 11 REST API route handlers
│       ├── components/         # 25 React components
│       │   ├── Dashboard.tsx
│       │   ├── CandlestickChart.tsx
│       │   ├── StrategyView.tsx
│       │   ├── ScreenerView.tsx
│       │   └── ...
│       └── lib/                # Shared utilities & DB client
│
├── data/                       # Local CSV data cache
├── Dockerfile.engine           # Python engine container
├── Dockerfile.website          # Next.js website container
├── docker-compose.yml          # Service orchestration
└── requirements.txt            # Python dependencies
```

---

## ML Model Details

### XGBoost Price Predictor
- **Features:** OHLCV lags, EMA crossovers, RSI, Bollinger Band position, daily sentiment score, volume momentum
- **Validation:** `TimeSeriesSplit` (walk-forward cross-validation) to prevent data leakage
- **Metrics:** MAE, RMSE, R² reported per ticker per training run
- **Fallback:** Automatically falls back to `sklearn.GradientBoostingRegressor` if XGBoost is unavailable

### FinBERT Sentiment Pipeline
- **Model:** `ProsusAI/finbert` via HuggingFace `transformers`
- **Input:** News headlines and article excerpts mapped to NSE tickers via entity matching
- **Output:** Per-article `[positive, neutral, negative]` probability scores aggregated to daily compound scores
- **Storage:** Persisted in Supabase with timestamp + ticker for time-series correlation analysis

---

## Contributing

1. Fork the repository
2. Create a feature branch — `git checkout -b feature/your-feature`
3. Commit your changes — `git commit -m 'feat: add your feature'`
4. Push to the branch — `git push origin feature/your-feature`
5. Open a Pull Request

---

## License

This project is for educational and personal use.

---

<div align="center">
Built for the Indian equity market
</div>
