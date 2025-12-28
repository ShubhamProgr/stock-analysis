import os
import pyodbc
import pandas as pd
import warnings
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

warnings.filterwarnings("ignore")

# =====================
# LOAD ENV
# =====================

load_dotenv()

# =====================
# CONNECTIONS
# =====================

mssql_conn = pyodbc.connect(
    f"DRIVER={{{os.getenv('MSSQL_DRIVER')}}};"
    f"SERVER={os.getenv('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE')};"
    f"UID={os.getenv('MSSQL_USERNAME')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')};"
)

pg_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}",
    pool_pre_ping=True
)

BATCH_SIZE = 2000

# =====================
# TABLE CONFIG
# =====================

SNAPSHOT_TABLES = {
    "Company_FinBERT_Sentiments": ["Company"],
    "Company_Info": ["Ticker"],
    "Final_Analysis": ["Company", "Prediction_Date"],
    "Prediction_vs_Actual": ["Company", "Date"]
}

STOCK_TABLE = {
    "name": "StockData",
    "symbol_col": "symbol",
    "date_col": "trade_date"
}

# =====================
# SNAPSHOT UPSERT
# =====================

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
    INSERT INTO "{table}" ({insert_cols})
    VALUES ({value_cols})
    ON CONFLICT ({conflict_cols})
    DO UPDATE SET {update_stmt};
    """

    data = df.to_dict(orient="records")

    with pg_engine.begin() as conn:
        conn.execute(text(sql), data)

# =====================
# SNAPSHOT DELETE SYNC
# =====================

def delete_removed_snapshot_rows(table, keys, df_mssql):
    temp = f"tmp_{table.lower()}"

    df_mssql[keys].drop_duplicates().to_sql(
        temp,
        pg_engine,
        index=False,
        if_exists="replace"
    )

    join_cond = " AND ".join(
        [f'pg."{k}" = tmp."{k}"' for k in keys]
    )

    sql = f"""
    DELETE FROM "{table}" pg
    WHERE NOT EXISTS (
        SELECT 1 FROM "{temp}" tmp
        WHERE {join_cond}
    );
    DROP TABLE "{temp}";
    """

    with pg_engine.begin() as conn:
        conn.execute(text(sql))

# =====================
# SNAPSHOT TABLE SYNC
# =====================

def sync_snapshot_table(table, keys):
    print(f"\n Syncing {table}")

    df = pd.read_sql(f"SELECT * FROM {table}", mssql_conn)

    if df.empty:
        print(" No data found")
        return

    upsert_snapshot(df, table, keys)
    delete_removed_snapshot_rows(table, keys, df)

    print(f" Synced {len(df)} rows")

# =====================
# STOCKDATA INCREMENTAL
# =====================

def get_last_dates_pg():
    sql = f"""
    SELECT {STOCK_TABLE['symbol_col']},
           MAX({STOCK_TABLE['date_col']}) AS last_date
    FROM "{STOCK_TABLE['name']}"
    GROUP BY {STOCK_TABLE['symbol_col']};
    """
    try:
        return pd.read_sql(sql, pg_engine)
    except Exception:
        return pd.DataFrame(
            columns=[STOCK_TABLE["symbol_col"], "last_date"]
        )

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
        WHERE {STOCK_TABLE['symbol_col']} = ?
          AND {STOCK_TABLE['date_col']} > ?
        """
        df = pd.read_sql(
            query,
            mssql_conn,
            params=[row[0], row[1]]
        )
        if not df.empty:
            dfs.append(df)

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    return pd.DataFrame()

def sync_stockdata():
    print("\n Syncing StockData (incremental only)")

    last_dates = get_last_dates_pg()
    df_new = fetch_incremental_stockdata(last_dates)

    if df_new.empty:
        print(" No new stock data to sync")
        return

    df_new.to_sql(
        STOCK_TABLE["name"],
        pg_engine,
        index=False,
        if_exists="append",
        method="multi",
        chunksize=BATCH_SIZE
    )

    print(f" Inserted {len(df_new)} new rows")

# =====================
# RUN SYNC
# =====================

sync_stockdata()

for table, keys in SNAPSHOT_TABLES.items():
    sync_snapshot_table(table, keys)

print("\n All tables synced successfully")
