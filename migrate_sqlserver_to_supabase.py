from __future__ import annotations

import os
from urllib.parse import quote_plus

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import numpy as np
import warnings

load_dotenv()
warnings.filterwarnings("ignore", category=UserWarning)

def get_source_connection_string() -> str:
    server = os.getenv("MSSQL_SERVER")
    database = os.getenv("MSSQL_DATABASE")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")

    if not server or not database:
        raise RuntimeError("Set MSSQL_SERVER and MSSQL_DATABASE in .env")

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )


def get_target_engine() -> Engine:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, pool_pre_ping=True)

    host = os.getenv("SUPABASE_DB_HOST")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    database = os.getenv("SUPABASE_DB_NAME", "postgres")
    user = os.getenv("SUPABASE_DB_USER", "postgres")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    if not host or not password:
        raise RuntimeError("Set DATABASE_URL or SUPABASE_DB_* in .env")

    target_url = (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{database}?sslmode={sslmode}"
    )
    return create_engine(target_url, pool_pre_ping=True)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = df.copy()
    renamed.columns = [str(column).strip().replace(" ", "_") for column in renamed.columns]
    if "52WeekChange" in renamed.columns:
        renamed = renamed.rename(columns={"52WeekChange": "week52change"})
    return renamed

def clean_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    BIGINT_MIN = -9223372036854775808
    BIGINT_MAX = 9223372036854775807

    bigint_cols = [
        "fullTimeEmployees",
        "marketCap",
        "totalRevenue",
        "totalCash",
        "totalDebt",
        "sharesOutstanding",
        "floatShares",
        "Volume",
        "ArticleCount"
    ]

    df = df.replace([np.inf, -np.inf], np.nan)

    for col in bigint_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

            # Print rows that are outside PostgreSQL BIGINT range
            bad = df[
                df[col].notna() &
                ((df[col] > BIGINT_MAX) | (df[col] < BIGINT_MIN))
            ]

            if not bad.empty:
                print(f"\nWARNING: {col} has values outside BIGINT:")
                print(bad[["Ticker", col]] if "Ticker" in bad.columns else bad[[col]])

            # Replace invalid values with NULL
            df.loc[
                (df[col] > BIGINT_MAX) |
                (df[col] < BIGINT_MIN),
                col
            ] = np.nan

            # Convert NaN -> None so PostgreSQL inserts NULL
            df[col] = df[col].astype(object)
            df[col] = df[col].where(pd.notna(df[col]), None)

    return df


def replace_table(source_df: pd.DataFrame, engine: Engine, table_name: str, create_sql: str, insert_sql: str, chunk_size: int = 1000) -> None:
    if source_df.empty:
        print(f"Skipping {table_name}: no rows found")
        return

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(f'DELETE FROM {table_name}'))

        rows = source_df.to_dict(orient="records")
        total = len(rows)
        for start in range(0, total, chunk_size):
            end = min(start + chunk_size, total)
            print(f"{table_name}: {end}/{total}")
            batch = rows[start:end]
            conn.execute(text(insert_sql), batch)

    print(f"Migrated {len(source_df)} rows into {table_name}")


def main() -> None:
    source_conn = pyodbc.connect(get_source_connection_string())
    target_engine = get_target_engine()

    try:
        company_info = clean_numeric_columns(
    normalize_columns(pd.read_sql("SELECT * FROM Company_Info", source_conn))
)

        final_analysis = clean_numeric_columns(
    normalize_columns(pd.read_sql("SELECT * FROM Final_Analysis", source_conn))
)

        prediction_vs_actual = clean_numeric_columns(
    normalize_columns(pd.read_sql("SELECT * FROM Prediction_vs_Actual", source_conn))
)

        sentiments = clean_numeric_columns(
            normalize_columns(pd.read_sql("SELECT * FROM Company_FinBERT_Sentiments", source_conn))
        )

        company_create = """
            CREATE TABLE IF NOT EXISTS company_info (
                \"Ticker\" TEXT PRIMARY KEY,
                \"longName\" TEXT,
                \"sector\" TEXT,
                \"industry\" TEXT,
                \"fullTimeEmployees\" BIGINT,
                \"marketCap\" BIGINT,
                \"totalRevenue\" BIGINT,
                \"grossMargins\" DOUBLE PRECISION,
                \"operatingMargins\" DOUBLE PRECISION,
                \"profitMargins\" DOUBLE PRECISION,
                \"totalCash\" BIGINT,
                \"totalDebt\" BIGINT,
                \"52WeekChange\" DOUBLE PRECISION,
                \"sharesOutstanding\" BIGINT,
                \"floatShares\" BIGINT,
                \"trailingPE\" DOUBLE PRECISION
            )
        """
        company_insert = """
            INSERT INTO company_info (
                \"Ticker\", \"longName\", \"sector\", \"industry\", \"fullTimeEmployees\", \"marketCap\",
                \"totalRevenue\", \"grossMargins\", \"operatingMargins\", \"profitMargins\",
                \"totalCash\", \"totalDebt\", \"52WeekChange\", \"sharesOutstanding\", \"floatShares\", \"trailingPE\"
            ) VALUES (
                :Ticker, :longName, :sector, :industry, :fullTimeEmployees, :marketCap,
                :totalRevenue, :grossMargins, :operatingMargins, :profitMargins,
                :totalCash, :totalDebt, :week52change, :sharesOutstanding, :floatShares, :trailingPE
            )
            ON CONFLICT (\"Ticker\") DO UPDATE SET
                \"longName\" = EXCLUDED.\"longName\",
                \"sector\" = EXCLUDED.\"sector\",
                \"industry\" = EXCLUDED.\"industry\",
                \"fullTimeEmployees\" = EXCLUDED.\"fullTimeEmployees\",
                \"marketCap\" = EXCLUDED.\"marketCap\",
                \"totalRevenue\" = EXCLUDED.\"totalRevenue\",
                \"grossMargins\" = EXCLUDED.\"grossMargins\",
                \"operatingMargins\" = EXCLUDED.\"operatingMargins\",
                \"profitMargins\" = EXCLUDED.\"profitMargins\",
                \"totalCash\" = EXCLUDED.\"totalCash\",
                \"totalDebt\" = EXCLUDED.\"totalDebt\",
                \"52WeekChange\" = EXCLUDED.\"52WeekChange\",
                \"sharesOutstanding\" = EXCLUDED.\"sharesOutstanding\",
                \"floatShares\" = EXCLUDED.\"floatShares\",
                \"trailingPE\" = EXCLUDED.\"trailingPE\"
        """
        replace_table(company_info, target_engine, "company_info", company_create, company_insert)

        final_create = """
            CREATE TABLE IF NOT EXISTS final_analysis (
                \"Company\" TEXT,
                \"Ticker\" TEXT,
                \"Prediction_Date\" DATE,
                \"Predicted_Closing_Price\" DOUBLE PRECISION,
                \"Last_Close\" DOUBLE PRECISION,
                \"Last_Close_Date\" DATE,
                \"MAE\" DOUBLE PRECISION,
                \"MSE\" DOUBLE PRECISION,
                \"RMSE\" DOUBLE PRECISION,
                \"R2_Score\" DOUBLE PRECISION,
                \"Sentiment\" TEXT,
                \"Sentiment_Score\" DOUBLE PRECISION,
                PRIMARY KEY (\"Ticker\", \"Prediction_Date\")
            )
        """
        final_insert = """
            INSERT INTO final_analysis (
                \"Company\", \"Ticker\", \"Prediction_Date\", \"Predicted_Closing_Price\", \"Last_Close\",
                \"Last_Close_Date\", \"MAE\", \"MSE\", \"RMSE\", \"R2_Score\", \"Sentiment\", \"Sentiment_Score\"
            ) VALUES (
                :Company, :Ticker, :Prediction_Date, :Predicted_Closing_Price, :Last_Close,
                :Last_Close_Date, :MAE, :MSE, :RMSE, :R2_Score, :Sentiment, :Sentiment_Score
            )
            ON CONFLICT (\"Ticker\", \"Prediction_Date\") DO UPDATE SET
                \"Company\" = EXCLUDED.\"Company\",
                \"Predicted_Closing_Price\" = EXCLUDED.\"Predicted_Closing_Price\",
                \"Last_Close\" = EXCLUDED.\"Last_Close\",
                \"Last_Close_Date\" = EXCLUDED.\"Last_Close_Date\",
                \"MAE\" = EXCLUDED.\"MAE\",
                \"MSE\" = EXCLUDED.\"MSE\",
                \"RMSE\" = EXCLUDED.\"RMSE\",
                \"R2_Score\" = EXCLUDED.\"R2_Score\",
                \"Sentiment\" = EXCLUDED.\"Sentiment\",
                \"Sentiment_Score\" = EXCLUDED.\"Sentiment_Score\"
        """
        for column in ["Prediction_Date", "Last_Close_Date"]:
            if column in final_analysis.columns:
                final_analysis[column] = pd.to_datetime(final_analysis[column]).dt.date
        replace_table(final_analysis, target_engine, "final_analysis", final_create, final_insert)

        pva_create = """
            CREATE TABLE IF NOT EXISTS prediction_vs_actual (
                \"Company\" TEXT,
                \"Ticker\" TEXT,
                \"Date\" DATE,
                \"Predicted_Closing_Price\" DOUBLE PRECISION,
                \"Actual_Closing_Price\" DOUBLE PRECISION,
                PRIMARY KEY (\"Ticker\", \"Date\")
            )
        """
        pva_insert = """
            INSERT INTO prediction_vs_actual (
                \"Company\", \"Ticker\", \"Date\", \"Predicted_Closing_Price\", \"Actual_Closing_Price\"
            ) VALUES (
                :Company, :Ticker, :Date, :Predicted_Closing_Price, :Actual_Closing_Price
            )
            ON CONFLICT (\"Ticker\", \"Date\") DO UPDATE SET
                \"Company\" = EXCLUDED.\"Company\",
                \"Predicted_Closing_Price\" = EXCLUDED.\"Predicted_Closing_Price\",
                \"Actual_Closing_Price\" = EXCLUDED.\"Actual_Closing_Price\"
        """
        for column in ["Date"]:
            if column in prediction_vs_actual.columns:
                prediction_vs_actual[column] = pd.to_datetime(prediction_vs_actual[column]).dt.date
        replace_table(prediction_vs_actual, target_engine, "prediction_vs_actual", pva_create, pva_insert)

        sentiments_create = """
            CREATE TABLE IF NOT EXISTS company_finbert_sentiments (
                \"Company\" TEXT PRIMARY KEY,
                \"Ticker\" TEXT,
                \"ArticleCount\" INTEGER,
                \"Paragraph\" TEXT,
                \"Sentiment\" TEXT,
                \"Score\" DOUBLE PRECISION
            )
        """
        sentiments_insert = """
            INSERT INTO company_finbert_sentiments (
                \"Company\", \"Ticker\", \"ArticleCount\", \"Paragraph\", \"Sentiment\", \"Score\"
            ) VALUES (
                :Company, :Ticker, :ArticleCount, :Paragraph, :Sentiment, :Score
            )
            ON CONFLICT (\"Company\") DO UPDATE SET
                \"Ticker\" = EXCLUDED.\"Ticker\",
                \"ArticleCount\" = EXCLUDED.\"ArticleCount\",
                \"Paragraph\" = EXCLUDED.\"Paragraph\",
                \"Sentiment\" = EXCLUDED.\"Sentiment\",
                \"Score\" = EXCLUDED.\"Score\"
        """
        replace_table(sentiments, target_engine, "company_finbert_sentiments", sentiments_create, sentiments_insert)

    finally:
        source_conn.close()


if __name__ == "__main__":
    main()
