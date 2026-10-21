"""Run a two-table, database-backed paper portfolio simulation."""

import os
from urllib.parse import quote_plus

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

INITIAL_CASH = float(os.getenv("PAPER_INITIAL_CASH", "1000"))
TARGET_ALLOCATIONS = tuple(
    float(value.strip())
    for value in os.getenv("PAPER_ALLOCATIONS", "500,200,100,100,100").split(",")
)


def build_database_url() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    host = os.getenv("SUPABASE_DB_HOST")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    if not host or not password:
        raise RuntimeError("Set DATABASE_URL or SUPABASE_DB_HOST and SUPABASE_DB_PASSWORD.")
    user = os.getenv("SUPABASE_DB_USER", "postgres")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    name = os.getenv("SUPABASE_DB_NAME", "postgres")
    sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{name}?sslmode={sslmode}"
    )


CREATE_HOLDINGS = text('''
    CREATE TABLE IF NOT EXISTS paper_simulation_holdings (
        id BIGSERIAL UNIQUE,
        investment_date DATE NOT NULL,
        ticker TEXT NOT NULL,
        amount DOUBLE PRECISION NOT NULL,
        shares DOUBLE PRECISION NOT NULL,
        entry_price DOUBLE PRECISION NOT NULL,
        market_value DOUBLE PRECISION NOT NULL,
        next_day DATE,
        result DOUBLE PRECISION,
        profit_loss DOUBLE PRECISION,
        PRIMARY KEY (investment_date, ticker)
    )
''')

CREATE_TOTALS = text('''
    CREATE TABLE IF NOT EXISTS paper_simulation_daily_totals (
        trading_date DATE PRIMARY KEY,
        simulation_day INTEGER NOT NULL UNIQUE,
        total_value DOUBLE PRECISION NOT NULL,
        profit_loss DOUBLE PRECISION NOT NULL
    )
''')

COPY_OLD_INVESTMENTS = text('''
    INSERT INTO paper_simulation_holdings
        (id, investment_date, ticker, amount, shares, entry_price, market_value,
         next_day, result, profit_loss)
    SELECT DISTINCT ON ("Invest_On", "Ticker")
           id, "Invest_On", "Ticker", "Amount", "Shares",
           CASE WHEN "Shares" > 0 THEN "Amount" / "Shares" ELSE 0 END,
           COALESCE("Result", "Amount"), "Next_Day", "Result", "Profit_Loss"
    FROM paper_investment_records
    ORDER BY "Invest_On", "Ticker", id DESC
    ON CONFLICT (investment_date, ticker) DO NOTHING
''')

RESET_HOLDING_ID_SEQUENCE = text('''
    SELECT setval(
        pg_get_serial_sequence('paper_simulation_holdings', 'id'),
        GREATEST(COALESCE((SELECT MAX(id) FROM paper_simulation_holdings), 1), 1),
        EXISTS (SELECT 1 FROM paper_simulation_holdings)
    )
''')

COPY_OLD_POSITIONS = text('''
    INSERT INTO paper_simulation_holdings
        (investment_date, ticker, amount, shares, entry_price, market_value,
         next_day, result, profit_loss)
    SELECT COALESCE(i."Invest_On", a.last_processed_date, p.opened_for),
           p.ticker, p.cost_basis, p.shares, p.entry_price, p.market_value,
           p.opened_for, i."Result", i."Profit_Loss"
    FROM paper_portfolio_positions p
    JOIN paper_portfolio_account a ON a.account_id = p.account_id
    LEFT JOIN paper_investment_records i ON i.id = p.investment_id
    WHERE p.account_id = 'default'
    ON CONFLICT (investment_date, ticker) DO UPDATE SET
        amount = EXCLUDED.amount,
        shares = EXCLUDED.shares,
        entry_price = EXCLUDED.entry_price,
        market_value = EXCLUDED.market_value,
        next_day = EXCLUDED.next_day,
        result = EXCLUDED.result,
        profit_loss = EXCLUDED.profit_loss
''')

COPY_OLD_TOTALS = text('''
    INSERT INTO paper_simulation_daily_totals
        (trading_date, simulation_day, total_value, profit_loss)
    SELECT trading_date, simulation_day, total_value, day_pnl
    FROM paper_portfolio_daily_results
    WHERE account_id = 'default'
    ON CONFLICT (trading_date) DO UPDATE SET
        simulation_day = EXCLUDED.simulation_day,
        total_value = EXCLUDED.total_value,
        profit_loss = EXCLUDED.profit_loss
''')

INSERT_DAY_ZERO = text('''
    INSERT INTO paper_simulation_daily_totals
        (trading_date, simulation_day, total_value, profit_loss)
    SELECT MIN(investment_date), 0,
           :initial_cash, 0
    FROM paper_simulation_holdings
    HAVING COUNT(*) > 0
    ON CONFLICT (trading_date) DO UPDATE SET
        total_value = EXCLUDED.total_value,
        profit_loss = 0
    WHERE paper_simulation_daily_totals.simulation_day = 0
''')

LATEST_COMPLETED_DATE = text('''
    SELECT MAX(fa."Prediction_Date")
    FROM final_analysis fa
    JOIN stock_data sd
      ON sd."Ticker" = fa."Ticker" AND sd."Date" = fa."Prediction_Date"
    WHERE fa."Predicted_Return_Pct" IS NOT NULL AND sd."Close" > 0
''')

UPCOMING_PREDICTIONS = text('''
    SELECT fa."Prediction_Date" AS prediction_date,
           fa."Ticker" AS ticker,
           fa."Predicted_Return_Pct" AS predicted_return_pct,
           fa."Last_Close_Date" AS entry_date,
           fa."Last_Close" AS entry_price
    FROM final_analysis fa
    WHERE fa."Prediction_Date" = (
        SELECT MIN("Prediction_Date") FROM final_analysis
        WHERE (:after_date IS NULL OR "Prediction_Date" > :after_date)
    )
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND fa."Last_Close_Date" IS NOT NULL
      AND fa."Last_Close" > 0
    ORDER BY fa."Predicted_Return_Pct" DESC NULLS LAST
''')

GET_LATEST_TOTAL = text('''
    SELECT trading_date, simulation_day, total_value
    FROM paper_simulation_daily_totals
    ORDER BY trading_date DESC LIMIT 1
''')

GET_LATEST_HOLDING_DATE = text('''
    SELECT MAX(investment_date)
    FROM paper_simulation_holdings
    WHERE result IS NULL
''')

GET_HOLDINGS = text('''
    SELECT ticker, amount, shares, entry_price, market_value, next_day
    FROM paper_simulation_holdings
    WHERE investment_date = :investment_date
    ORDER BY ticker
''')

NEXT_ACTUAL_DATE = text('''
    SELECT sd."Date"
    FROM stock_data sd
    JOIN paper_simulation_holdings h ON h.ticker = sd."Ticker"
    WHERE h.investment_date = :investment_date
      AND h.result IS NULL
      AND sd."Date" >= h.next_day
      AND sd."Close" > 0
    GROUP BY sd."Date"
    HAVING COUNT(DISTINCT h.ticker) = (
        SELECT COUNT(*) FROM paper_simulation_holdings
        WHERE investment_date = :investment_date AND result IS NULL
    )
    ORDER BY sd."Date"
    LIMIT 1
''')

UPDATE_HOLDING_RESULT = text('''
    UPDATE paper_simulation_holdings
    SET result = :result,
        profit_loss = :result - amount
    WHERE investment_date = :investment_date AND ticker = :ticker AND result IS NULL
''')

UPSERT_HOLDING = text('''
    INSERT INTO paper_simulation_holdings
        (investment_date, ticker, amount, shares, entry_price, market_value,
         next_day, result, profit_loss)
    VALUES (:investment_date, :ticker, :amount, :shares, :entry_price,
            :market_value, :next_day, NULL, NULL)
    ON CONFLICT (investment_date, ticker) DO UPDATE SET
        amount = EXCLUDED.amount,
        shares = EXCLUDED.shares,
        entry_price = EXCLUDED.entry_price,
        market_value = EXCLUDED.market_value,
        next_day = EXCLUDED.next_day,
        result = NULL,
        profit_loss = NULL
''')

UPSERT_TOTAL = text('''
    INSERT INTO paper_simulation_daily_totals
        (trading_date, simulation_day, total_value, profit_loss)
    VALUES (:trading_date, :simulation_day, :total_value, :profit_loss)
    ON CONFLICT (trading_date) DO UPDATE SET
        simulation_day = EXCLUDED.simulation_day,
        total_value = EXCLUDED.total_value,
        profit_loss = EXCLUDED.profit_loss
''')


def unique_top_five(rows):
    picks, seen = [], set()
    for row in rows:
        if row["ticker"] in seen:
            continue
        seen.add(row["ticker"])
        picks.append(dict(row))
        if len(picks) == 5:
            break
    return picks


def migrate_to_two_tables(connection):
    """Copy existing paper data to two tables, then remove obsolete state tables."""
    connection.execute(CREATE_HOLDINGS)
    connection.execute(text('''
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'paper_simulation_holdings'
                  AND column_name = 'holding_date'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'paper_simulation_holdings'
                  AND column_name = 'investment_date'
            ) THEN
                ALTER TABLE paper_simulation_holdings
                    RENAME COLUMN holding_date TO investment_date;
            END IF;
        END $$
    '''))
    connection.execute(CREATE_TOTALS)
    connection.execute(text('''
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'paper_simulation_daily_totals'
                  AND column_name = 'day_pnl'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'paper_simulation_daily_totals'
                  AND column_name = 'profit_loss'
            ) THEN
                ALTER TABLE paper_simulation_daily_totals RENAME COLUMN day_pnl TO profit_loss;
            ELSIF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'paper_simulation_daily_totals'
                  AND column_name = 'day_pnl'
            ) THEN
                UPDATE paper_simulation_daily_totals SET profit_loss = day_pnl;
                ALTER TABLE paper_simulation_daily_totals DROP COLUMN day_pnl;
            END IF;
        END $$
    '''))
    # Remove legacy required columns before inserting into the simplified
    # schema; otherwise their NOT NULL constraints reject the new rows.
    connection.execute(text(
        "ALTER TABLE paper_simulation_daily_totals DROP COLUMN IF EXISTS invested_value"
    ))
    connection.execute(text(
        "ALTER TABLE paper_simulation_daily_totals DROP COLUMN IF EXISTS cash"
    ))
    if connection.execute(text("SELECT to_regclass('paper_investment_records')")).scalar():
        connection.execute(COPY_OLD_INVESTMENTS)
        connection.execute(RESET_HOLDING_ID_SEQUENCE)
    if connection.execute(text("SELECT to_regclass('paper_portfolio_positions')")).scalar():
        connection.execute(COPY_OLD_POSITIONS)
    if connection.execute(text("SELECT to_regclass('paper_portfolio_daily_results')")).scalar():
        connection.execute(COPY_OLD_TOTALS)
    connection.execute(INSERT_DAY_ZERO, {"initial_cash": INITIAL_CASH})
    # The useful ledger and results are now in the two new tables.
    connection.execute(text("DROP TABLE IF EXISTS paper_portfolio_positions CASCADE"))
    connection.execute(text("DROP TABLE IF EXISTS paper_investment_records CASCADE"))
    connection.execute(text("DROP TABLE IF EXISTS paper_portfolio_daily_results CASCADE"))
    connection.execute(text("DROP TABLE IF EXISTS paper_portfolio_account CASCADE"))


def run_simulation(engine):
    if len(TARGET_ALLOCATIONS) != 5 or abs(sum(TARGET_ALLOCATIONS) - INITIAL_CASH) > 0.001:
        raise ValueError("PAPER_ALLOCATIONS must contain five positive amounts totaling PAPER_INITIAL_CASH.")

    with engine.begin() as connection:
        migrate_to_two_tables(connection)
        latest_total = connection.execute(GET_LATEST_TOTAL).mappings().first()

        # Bootstrap only when no holding snapshot exists. The session's five
        # rows use the close carried by the prediction for the upcoming date.
        snapshot_date = connection.execute(text(
            "SELECT MAX(investment_date) FROM paper_simulation_holdings"
        )).scalar()
        if snapshot_date is None:
            latest_completed = connection.execute(LATEST_COMPLETED_DATE).scalar()
            picks = unique_top_five(connection.execute(
                UPCOMING_PREDICTIONS, {"after_date": latest_completed}
            ).mappings().all())
            if len(picks) != 5:
                print("No upcoming session has five usable predictions; no holdings created.")
                return
            invest_on = picks[0]["entry_date"]
            next_day = picks[0]["prediction_date"]
            invested = 0.0
            print(f"Opening five holdings for {next_day} using {invest_on} closes:")
            for rank, row in enumerate(picks):
                if row["entry_date"] != invest_on:
                    raise RuntimeError("The five picks do not share the same entry close date.")
                amount = TARGET_ALLOCATIONS[rank]
                price = float(row["entry_price"])
                shares = amount / price
                connection.execute(UPSERT_HOLDING, {
                    "investment_date": invest_on, "ticker": row["ticker"], "amount": amount,
                    "shares": shares, "entry_price": price, "market_value": amount,
                    "next_day": next_day,
                })
                invested += amount
                print(f"  {row['ticker']}: INR {amount:.2f} at INR {price:.2f}")
            connection.execute(UPSERT_TOTAL, {
                "trading_date": invest_on, "simulation_day": 0,
                "total_value": INITIAL_CASH, "profit_loss": 0,
            })
            latest_total = connection.execute(GET_LATEST_TOTAL).mappings().one()

        # Process any available close for the current five-row snapshot.
        while True:
            snapshot_date = connection.execute(GET_LATEST_HOLDING_DATE).scalar()
            if snapshot_date is None:
                # A prior run may have completed the last snapshot before the
                # next predictions were available. Start the next session from
                # the latest account value instead of requiring a pending row.
                latest_total = connection.execute(GET_LATEST_TOTAL).mappings().first()
                if latest_total is None:
                    print("No pending holdings or prior simulation total is available.")
                    break

                picks = unique_top_five(connection.execute(
                    UPCOMING_PREDICTIONS,
                    {"after_date": latest_total["trading_date"]},
                ).mappings().all())
                if len(picks) != 5:
                    print("No new prediction session has five usable picks.")
                    break

                invest_on = picks[0]["entry_date"]
                next_day = picks[0]["prediction_date"]
                if any(row["entry_date"] != invest_on for row in picks):
                    print("The next five picks do not share one entry close date.")
                    break
                if invest_on < latest_total["trading_date"]:
                    print(f"Skipping stale picks with entry date {invest_on}.")
                    break

                available_cash = float(latest_total["total_value"])
                if available_cash <= 0:
                    print("Portfolio value is zero; no new holdings were opened.")
                    break

                # If the next available entry close is later than the last
                # recorded day, the account stayed in cash in between.
                if invest_on > latest_total["trading_date"]:
                    day_number = int(latest_total["simulation_day"]) + 1
                    connection.execute(UPSERT_TOTAL, {
                        "trading_date": invest_on,
                        "simulation_day": day_number,
                        "total_value": available_cash,
                        "profit_loss": 0,
                    })

                scale = min(1.0, available_cash / sum(TARGET_ALLOCATIONS))
                print(f"Opening five holdings for {next_day} using {invest_on} closes:")
                for rank, row in enumerate(picks):
                    price = float(row["entry_price"])
                    amount = TARGET_ALLOCATIONS[rank] * scale
                    connection.execute(UPSERT_HOLDING, {
                        "investment_date": invest_on, "ticker": row["ticker"],
                        "amount": amount, "shares": amount / price,
                        "entry_price": price, "market_value": amount,
                        "next_day": next_day,
                    })
                    print(f"  {row['ticker']}: INR {amount:.2f} at INR {price:.2f}")
                continue
            holdings = [dict(row) for row in connection.execute(
                GET_HOLDINGS, {"investment_date": snapshot_date}
            ).mappings().all()]
            if len(holdings) != 5:
                print(f"Pending snapshot {snapshot_date} has {len(holdings)} rows; expected five.")
                break
            next_day = holdings[0]["next_day"]
            if next_day is None:
                print(f"No next trading date is assigned to holdings from {snapshot_date}.")
                break
            actual_date = connection.execute(NEXT_ACTUAL_DATE, {
                "investment_date": snapshot_date,
            }).scalar()
            if actual_date is None:
                print(f"Waiting for closing prices on or after {next_day}; no result recorded yet.")
                break

            close_rows = connection.execute(text('''
                SELECT "Ticker" AS ticker, "Close" AS close_price
                FROM stock_data
                WHERE "Date" = :trading_date AND "Close" > 0
            '''), {"trading_date": actual_date}).mappings().all()
            closes = {row["ticker"]: float(row["close_price"]) for row in close_rows}
            if any(row["ticker"] not in closes for row in holdings):
                print(f"Waiting: one or more held tickers lack a close for {actual_date}.")
                break

            old_total = connection.execute(GET_LATEST_TOTAL).mappings().one()
            # Cash is not stored in the daily totals table. Reconstruct it from
            # the latest total and the values in this pending holdings snapshot.
            cash = max(
                0.0,
                float(old_total["total_value"])
                - sum(float(row["market_value"]) for row in holdings),
            )
            positions = {}
            for row in holdings:
                value = row["shares"] * closes[row["ticker"]]
                row["market_value"] = value
                row["result"] = value
                row["profit_loss"] = value - row["amount"]
                positions[row["ticker"]] = row
                connection.execute(UPDATE_HOLDING_RESULT, {
                    "investment_date": snapshot_date, "ticker": row["ticker"], "result": value,
                })

            marked_total = cash + sum(p["market_value"] for p in positions.values())
            day_number = int(old_total["simulation_day"]) + 1
            print(f"\nSimulation day {day_number} result for {actual_date}:")
            for row in holdings:
                print(
                    f"  {row['ticker']}: INR {row['market_value']:.2f} "
                    f"(P/L INR {row['profit_loss']:+.2f})"
                )
            print(f"  Total value: INR {marked_total:.2f} (P/L INR {marked_total-float(old_total['total_value']):+.2f})")

            picks = unique_top_five(connection.execute(
                UPCOMING_PREDICTIONS, {"after_date": actual_date}
            ).mappings().all())
            next_prediction_date = picks[0]["prediction_date"] if len(picks) == 5 else None
            can_rebalance = (
                len(picks) == 5
                and all(row["entry_date"] == actual_date for row in picks)
            )
            if can_rebalance:
                selected = {row["ticker"] for row in picks}
                for ticker in list(positions):
                    if ticker not in selected:
                        cash += positions.pop(ticker)["market_value"]
                for rank, pick in enumerate(picks):
                    ticker = pick["ticker"]
                    if ticker in positions:
                        continue
                    amount = min(TARGET_ALLOCATIONS[rank], cash)
                    if amount <= 0:
                        continue
                    price = float(pick["entry_price"])
                    shares = amount / price
                    positions[ticker] = {
                        "ticker": ticker, "amount": amount, "shares": shares,
                        "entry_price": price, "market_value": amount,
                    }
                    cash -= amount
            else:
                print("No complete same-close next allocation is available; keeping current holdings.")

            next_day = next_prediction_date if can_rebalance else None
            if next_day is not None:
                for p in positions.values():
                    connection.execute(UPSERT_HOLDING, {
                        "investment_date": actual_date, "ticker": p["ticker"],
                        "amount": p["amount"], "shares": p["shares"],
                        "entry_price": p["entry_price"], "market_value": p["market_value"],
                        "next_day": next_day,
                    })

            invested_value = sum(p["market_value"] for p in positions.values())
            total_value = invested_value + cash
            connection.execute(UPSERT_TOTAL, {
                "trading_date": actual_date, "simulation_day": day_number,
                "total_value": total_value,
                "profit_loss": total_value - float(old_total["total_value"]),
            })
            latest_total = connection.execute(GET_LATEST_TOTAL).mappings().one()
            if next_day is None:
                break

        latest_total = connection.execute(GET_LATEST_TOTAL).mappings().first()
        if latest_total:
            print(
                f"\nDay {latest_total['simulation_day']} ({latest_total['trading_date']}): "
                f"INR {float(latest_total['total_value']):.2f}"
            )


def main():
    engine = create_engine(build_database_url(), pool_pre_ping=True)
    try:
        run_simulation(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
