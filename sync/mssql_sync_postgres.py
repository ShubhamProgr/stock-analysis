import os
import pyodbc
import pandas as pd
import warnings
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

# ===============================
# MSSQL CONNECTION
# ===============================
mssql_conn = pyodbc.connect(
    f"DRIVER={{{os.getenv('MSSQL_DRIVER')}}};"
    f"SERVER={os.getenv('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE')};"
    f"UID={os.getenv('MSSQL_USERNAME')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')};"
)

# ===============================
# POSTGRES CONNECTION (Render)
# ===============================
pg_engine = create_engine(
    os.getenv("DATABASE_URL"),
    pool_pre_ping=True,
    connect_args={"options": "-csearch_path=app"}
)

BATCH_SIZE = 2000
SCHEMA = "app"

# ===============================
# TABLE DEFINITIONS
# ===============================
SNAPSHOT_TABLES = {
    "Company_FinBERT_Sentiments": ["Company"],
    "Company_Info": ["Ticker"],
    "Final_Analysis": ["Company", "Ticker", "Prediction_Date"],
    "Prediction_vs_Actual": ["Company", "Date"]
}

STOCK_TABLE = {
    "name": "StockData",
    "symbol_col": "Ticker",
    "date_col": "Date"
}

# ===============================
# SNAPSHOT UPSERT
# ===============================
def upsert_snapshot(df, table, keys):
    cols = df.columns.tolist()

    insert_cols = ", ".join(f'"{c}"' for c in cols)
    value_cols = ", ".join(f":{c}" for c in cols)
    conflict_cols = ", ".join(f'"{k}"' for k in keys)

    update_stmt = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in cols if c not in keys
    )

    sql = f"""
    INSERT INTO {SCHEMA}."{table}" ({insert_cols})
    VALUES ({value_cols})
    ON CONFLICT ({conflict_cols})
    DO UPDATE SET {update_stmt};
    """

    with pg_engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

# ===============================
# SNAPSHOT CLEANUP
# ===============================
def delete_removed_snapshot_rows(table, keys, df_mssql):
    temp = f"tmp_{table.lower()}"

    df_mssql[keys].drop_duplicates().to_sql(
        temp,
        pg_engine,
        schema=SCHEMA,
        index=False,
        if_exists="replace"
    )

    join_cond = " AND ".join(
        [f'pg."{k}" = tmp."{k}"' for k in keys]
    )

    sql = f"""
    DELETE FROM {SCHEMA}."{table}" pg
    WHERE NOT EXISTS (
        SELECT 1 FROM {SCHEMA}."{temp}" tmp
        WHERE {join_cond}
    );
    DROP TABLE {SCHEMA}."{temp}";
    """

    with pg_engine.begin() as conn:
        conn.execute(text(sql))

# ===============================
# SNAPSHOT SYNC
# ===============================
def sync_snapshot_table(table, keys):
    print(f"\n Syncing {table}")

    df = pd.read_sql(f"SELECT * FROM {table}", mssql_conn)

    if df.empty:
        print(" No data found")
        return

    upsert_snapshot(df, table, keys)
    delete_removed_snapshot_rows(table, keys, df)

    print(f" Synced {len(df)} rows")

# ===============================
# STOCKDATA INCREMENTAL SYNC
# ===============================
def get_last_dates_pg():
    sql = f"""
    SELECT "Ticker", MAX("Date") AS last_date
    FROM {SCHEMA}."StockData"
    GROUP BY "Ticker";
    """
    try:
        return pd.read_sql(sql, pg_engine)
    except Exception:
        return pd.DataFrame(columns=["Ticker", "last_date"])

def fetch_incremental_stockdata(last_dates):
    table = STOCK_TABLE["name"]

    if last_dates.empty:
        print(" First sync → pulling full StockData")
        return pd.read_sql(f"SELECT * FROM {table}", mssql_conn)

    dfs = []

    for _, row in last_dates.iterrows():
        query = f"""
        SELECT *
        FROM {table}
        WHERE Ticker = ?
          AND Date > ?
        """
        df = pd.read_sql(query, mssql_conn, params=[row["Ticker"], row["last_date"]])
        if not df.empty:
            dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def sync_stockdata():
    print("\n Syncing StockData (incremental only)")

    last_dates = get_last_dates_pg()
    df_new = fetch_incremental_stockdata(last_dates)

    if df_new.empty:
        print(" No new stock data to sync")
        return

    df_new.to_sql(
        "StockData",
        pg_engine,
        schema=SCHEMA,
        index=False,
        if_exists="append",
        method="multi",
        chunksize=BATCH_SIZE
    )

    print(f" Inserted {len(df_new)} new rows")

# ===============================
# RUN SYNC
# ===============================
sync_stockdata()

for table, keys in SNAPSHOT_TABLES.items():
    sync_snapshot_table(table, keys)

print("\n All tables synced successfully")
