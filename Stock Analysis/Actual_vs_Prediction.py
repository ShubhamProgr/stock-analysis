from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text

load_dotenv()


def build_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    supabase_host = os.getenv("SUPABASE_DB_HOST")
    supabase_port = os.getenv("SUPABASE_DB_PORT", "5432")
    supabase_name = os.getenv("SUPABASE_DB_NAME", "postgres")
    supabase_user = os.getenv("SUPABASE_DB_USER", "postgres")
    supabase_password = os.getenv("SUPABASE_DB_PASSWORD")
    supabase_sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    if not all([supabase_host, supabase_password]):
        raise RuntimeError("Set DATABASE_URL or SUPABASE_DB_* environment variables")

    return (
        f"postgresql+psycopg2://{quote_plus(supabase_user)}:{quote_plus(supabase_password)}"
        f"@{supabase_host}:{supabase_port}/{supabase_name}?sslmode={supabase_sslmode}"
    )


CREATE_TABLE_QUERY = text("""
CREATE TABLE IF NOT EXISTS prediction_vs_actual (
    "Company" TEXT,
    "Ticker" TEXT,
    "Date" DATE,
    "Predicted_Closing_Price" DOUBLE PRECISION,
    "Actual_Closing_Price" DOUBLE PRECISION,
    PRIMARY KEY ("Ticker", "Date")
)
""")


INSERT_QUERY = text("""
INSERT INTO prediction_vs_actual (
    "Company",
    "Ticker",
    "Date",
    "Predicted_Closing_Price",
    "Actual_Closing_Price"
)
SELECT
    fa."Company",
    fa."Ticker",
    fa."Prediction_Date",
    fa."Predicted_Closing_Price",
    sd."Close"
FROM final_analysis fa
JOIN stock_data sd
    ON fa."Ticker" = sd."Ticker"
    AND fa."Prediction_Date" = sd."Date"
ON CONFLICT ("Ticker", "Date") DO NOTHING
""")


def main() -> None:
    engine = create_engine(build_database_url(), pool_pre_ping=True)

    try:
        with engine.begin() as conn:
            conn.execute(CREATE_TABLE_QUERY)
            print("Table 'Prediction_vs_Actual' checked/created.")

            conn.execute(INSERT_QUERY)
            print("Prediction_vs_Actual updated without duplicates.")
    except Exception as exc:
        print("Failed to update Prediction_vs_Actual:", exc)
        raise
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
