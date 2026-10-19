"""Persist and report a database-backed paper portfolio.

Predictions come from ``final_analysis`` and actual closes from ``stock_data``.
The paper account is stored in dedicated ``paper_portfolio_*`` tables. No broker
connection or real order is made.
"""

import os
from collections import defaultdict
from datetime import datetime
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

INITIAL_CASH = float(os.getenv("PAPER_INITIAL_CASH", "1000"))
TARGET_ALLOCATIONS = tuple(
    float(value.strip())
    for value in os.getenv("PAPER_ALLOCATIONS", "500,200,100,100,100").split(",")
)
ACCOUNT_ID = "default"


def build_database_url() -> str:
    """Use the same DATABASE_URL/SUPABASE_DB_* settings as project scripts."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    host = os.getenv("SUPABASE_DB_HOST")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    if not host or not password:
        raise RuntimeError(
            "Database settings not found. Set DATABASE_URL or SUPABASE_DB_HOST "
            "and SUPABASE_DB_PASSWORD in the project .env file."
        )

    user = os.getenv("SUPABASE_DB_USER", "postgres")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    name = os.getenv("SUPABASE_DB_NAME", "postgres")
    sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{name}?sslmode={sslmode}"
    )


CREATE_TABLES = (
    text('''
        CREATE TABLE IF NOT EXISTS paper_portfolio_account (
            account_id TEXT PRIMARY KEY,
            initial_cash DOUBLE PRECISION NOT NULL,
            cash DOUBLE PRECISION NOT NULL,
            last_processed_date DATE,
            pending_prediction_date DATE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    '''),
    text('''
        CREATE TABLE IF NOT EXISTS paper_investment_records (
            id BIGSERIAL PRIMARY KEY,
            "Invest_On" DATE NOT NULL,
            "Ticker" TEXT NOT NULL,
            "Amount" DOUBLE PRECISION NOT NULL,
            "Next_Day" DATE NOT NULL,
            "Result" DOUBLE PRECISION,
            "Profit_Loss" DOUBLE PRECISION,
            "Shares" DOUBLE PRECISION NOT NULL
        )
    '''),
    text('''
        CREATE TABLE IF NOT EXISTS paper_portfolio_positions (
            account_id TEXT NOT NULL REFERENCES paper_portfolio_account(account_id),
            ticker TEXT NOT NULL,
            shares DOUBLE PRECISION NOT NULL,
            cost_basis DOUBLE PRECISION NOT NULL,
            market_value DOUBLE PRECISION NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            opened_for DATE NOT NULL,
            investment_id BIGINT REFERENCES paper_investment_records(id),
            PRIMARY KEY (account_id, ticker)
        )
    '''),
    text('''
        CREATE TABLE IF NOT EXISTS paper_portfolio_daily_results (
            account_id TEXT NOT NULL REFERENCES paper_portfolio_account(account_id),
            trading_date DATE NOT NULL,
            simulation_day INTEGER NOT NULL,
            invested_value DOUBLE PRECISION NOT NULL,
            cash DOUBLE PRECISION NOT NULL,
            total_value DOUBLE PRECISION NOT NULL,
            day_pnl DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (account_id, trading_date)
        )
    '''),
    text('''
        ALTER TABLE paper_portfolio_daily_results
        ADD COLUMN IF NOT EXISTS simulation_day INTEGER
    '''),
    text('''
        WITH numbered AS (
            SELECT account_id, trading_date,
                   ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY trading_date) AS day_number
            FROM paper_portfolio_daily_results
        )
        UPDATE paper_portfolio_daily_results AS daily
        SET simulation_day = numbered.day_number
        FROM numbered
        WHERE daily.account_id = numbered.account_id
          AND daily.trading_date = numbered.trading_date
          AND daily.simulation_day IS NULL
    '''),
    text('''
        ALTER TABLE paper_portfolio_daily_results
        ALTER COLUMN simulation_day SET NOT NULL
    '''),
    text('''
        CREATE UNIQUE INDEX IF NOT EXISTS paper_portfolio_daily_simulation_day_idx
        ON paper_portfolio_daily_results (account_id, simulation_day)
    '''),
    text('''
        ALTER TABLE paper_portfolio_positions
        ADD COLUMN IF NOT EXISTS investment_id BIGINT
        REFERENCES paper_investment_records(id)
    '''),
)


LATEST_COMPLETED_DATE_QUERY = text('''
    SELECT MAX(fa."Prediction_Date")
    FROM final_analysis AS fa
    JOIN stock_data AS sd
      ON sd."Ticker" = fa."Ticker"
     AND sd."Date" = fa."Prediction_Date"
    WHERE fa."Prediction_Date" IS NOT NULL
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND sd."Close" IS NOT NULL
      AND sd."Close" > 0
''')

PREVIOUS_COMPLETED_DATE_QUERY = text('''
    SELECT MAX(fa."Prediction_Date")
    FROM final_analysis AS fa
    JOIN stock_data AS sd
      ON sd."Ticker" = fa."Ticker"
     AND sd."Date" = fa."Prediction_Date"
    WHERE fa."Prediction_Date" < :before_date
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND sd."Close" IS NOT NULL
      AND sd."Close" > 0
''')

MISSED_SESSION_DATE_QUERY = text('''
    SELECT fa."Prediction_Date"
    FROM final_analysis AS fa
    JOIN stock_data AS sd
      ON sd."Ticker" = fa."Ticker"
     AND sd."Date" = fa."Prediction_Date"
    WHERE fa."Prediction_Date" <= :latest_completed
      AND fa."Prediction_Date" >= :today - INTERVAL '3 days'
      AND fa."Last_Close_Date" IS NOT NULL
      AND fa."Last_Close_Date" < fa."Prediction_Date"
      AND fa."Prediction_Date" - fa."Last_Close_Date" > 1
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND fa."Last_Close" IS NOT NULL
      AND fa."Last_Close" > 0
      AND sd."Close" IS NOT NULL
      AND sd."Close" > 0
    GROUP BY fa."Prediction_Date"
    HAVING COUNT(DISTINCT fa."Ticker") >= 5
    ORDER BY fa."Prediction_Date" DESC
    LIMIT 1
''')

COMPLETED_PREDICTIONS_QUERY = text('''
    SELECT fa."Prediction_Date" AS prediction_date,
           fa."Ticker" AS ticker,
           fa."Predicted_Return_Pct" AS predicted_return_pct,
           fa."Last_Close_Date" AS entry_date,
           fa."Last_Close" AS entry_price
    FROM final_analysis AS fa
    JOIN stock_data AS sd
      ON sd."Ticker" = fa."Ticker"
     AND sd."Date" = fa."Prediction_Date"
    WHERE (:after_date IS NULL OR fa."Prediction_Date" > :after_date)
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND fa."Last_Close_Date" IS NOT NULL
      AND fa."Last_Close" IS NOT NULL
      AND fa."Last_Close" > 0
      AND sd."Close" IS NOT NULL
      AND sd."Close" > 0
    ORDER BY fa."Prediction_Date" ASC,
             fa."Predicted_Return_Pct" DESC NULLS LAST
''')

COMPLETED_CLOSES_QUERY = text('''
    SELECT sd."Date" AS trading_date,
           sd."Ticker" AS ticker,
           sd."Close" AS close_price
    FROM stock_data AS sd
    WHERE sd."Date" IN (
        SELECT DISTINCT fa."Prediction_Date"
        FROM final_analysis AS fa
        JOIN stock_data AS matching_day
          ON matching_day."Ticker" = fa."Ticker"
         AND matching_day."Date" = fa."Prediction_Date"
        WHERE (:after_date IS NULL OR fa."Prediction_Date" > :after_date)
    )
      AND sd."Close" IS NOT NULL
      AND sd."Close" > 0
''')

UPCOMING_PREDICTIONS_QUERY = text('''
    SELECT fa."Prediction_Date" AS prediction_date,
           fa."Ticker" AS ticker,
           fa."Predicted_Return_Pct" AS predicted_return_pct,
           fa."Last_Close_Date" AS entry_date,
           fa."Last_Close" AS entry_price
    FROM final_analysis AS fa
    WHERE fa."Prediction_Date" = (
        SELECT MIN("Prediction_Date")
        FROM final_analysis
        WHERE (:after_date IS NULL OR "Prediction_Date" > :after_date)
    )
      AND fa."Predicted_Return_Pct" IS NOT NULL
      AND fa."Last_Close_Date" IS NOT NULL
      AND fa."Last_Close" IS NOT NULL
      AND fa."Last_Close" > 0
    ORDER BY fa."Predicted_Return_Pct" DESC NULLS LAST
''')

INSERT_ACCOUNT = text('''
    INSERT INTO paper_portfolio_account
        (account_id, initial_cash, cash, last_processed_date)
    VALUES (:account_id, :initial_cash, :cash, :last_processed_date)
    ON CONFLICT (account_id) DO NOTHING
''')

GET_ACCOUNT = text('''
    SELECT account_id, initial_cash, cash, last_processed_date, pending_prediction_date
    FROM paper_portfolio_account
    WHERE account_id = :account_id
    FOR UPDATE
''')

GET_POSITIONS = text('''
    SELECT ticker, shares, cost_basis, market_value, entry_price, opened_for, investment_id
    FROM paper_portfolio_positions
    WHERE account_id = :account_id
    ORDER BY ticker
''')

UPSERT_POSITION = text('''
    INSERT INTO paper_portfolio_positions
        (account_id, ticker, shares, cost_basis, market_value, entry_price, opened_for, investment_id)
    VALUES (:account_id, :ticker, :shares, :cost_basis, :market_value, :entry_price, :opened_for, :investment_id)
    ON CONFLICT (account_id, ticker) DO UPDATE SET
        shares = EXCLUDED.shares,
        cost_basis = EXCLUDED.cost_basis,
        market_value = EXCLUDED.market_value,
        entry_price = EXCLUDED.entry_price,
        opened_for = EXCLUDED.opened_for,
        investment_id = EXCLUDED.investment_id
''')

INSERT_INVESTMENT = text('''
    INSERT INTO paper_investment_records
        ("Invest_On", "Ticker", "Amount", "Next_Day", "Result", "Profit_Loss", "Shares")
    VALUES (:invest_on, :ticker, :amount, :next_day, NULL, NULL, :shares)
    RETURNING id
''')

UPDATE_INVESTMENT_RESULT = text('''
    UPDATE paper_investment_records
    SET "Result" = :result,
        "Profit_Loss" = :result - "Amount"
    WHERE id = :investment_id
      AND "Next_Day" = :next_day
      AND "Result" IS NULL
''')

DELETE_POSITION = text('''
    DELETE FROM paper_portfolio_positions
    WHERE account_id = :account_id AND ticker = :ticker
''')

UPDATE_ACCOUNT = text('''
    UPDATE paper_portfolio_account
    SET cash = :cash,
        last_processed_date = :last_processed_date,
        pending_prediction_date = :pending_prediction_date,
        updated_at = NOW()
    WHERE account_id = :account_id
''')

UPSERT_DAILY_RESULT = text('''
    INSERT INTO paper_portfolio_daily_results
        (account_id, trading_date, simulation_day, invested_value, cash, total_value, day_pnl)
    VALUES (:account_id, :trading_date, :simulation_day, :invested_value, :cash, :total_value, :day_pnl)
    ON CONFLICT (account_id, trading_date) DO UPDATE SET
        simulation_day = EXCLUDED.simulation_day,
        invested_value = EXCLUDED.invested_value,
        cash = EXCLUDED.cash,
        total_value = EXCLUDED.total_value,
        day_pnl = EXCLUDED.day_pnl
''')

MAX_SIMULATION_DAY_QUERY = text('''
    SELECT COALESCE(MAX(simulation_day), 0)
    FROM paper_portfolio_daily_results
    WHERE account_id = :account_id
''')


def top_five(rows):
    result = []
    seen = set()
    for row in rows:
        if row["ticker"] in seen:
            continue
        seen.add(row["ticker"])
        result.append(row)
        if len(result) == len(TARGET_ALLOCATIONS):
            break
    return result


def buy_new_winners(connection, positions, cash, winners, execution_date, pending_date):
    """Retain overlaps and use sale proceeds to buy newly selected tickers."""
    winner_tickers = {row["ticker"] for row in winners}

    for ticker in list(positions):
        if ticker in winner_tickers:
            continue
        position = positions.pop(ticker)
        cash += position["market_value"]
        connection.execute(DELETE_POSITION, {"account_id": ACCOUNT_ID, "ticker": ticker})
        print(
            f"  Sold {ticker}: INR {position['market_value']:.2f} "
            f"(P/L INR {position['market_value'] - position['cost_basis']:+.2f})"
        )

    for rank, row in enumerate(winners):
        ticker = row["ticker"]
        if ticker in positions:
            print(f"  Keep {ticker}: INR {positions[ticker]['market_value']:.2f} invested")
            continue

        price = float(row["entry_price"])
        amount = min(TARGET_ALLOCATIONS[rank], cash)
        if amount <= 0:
            continue
        if row["entry_date"] != execution_date:
            raise RuntimeError(
                f"{ticker}'s prediction for {pending_date} uses a stale close "
                f"from {row['entry_date']}; refusing to simulate a mismatched entry."
            )

        shares = amount / price
        investment_id = connection.execute(INSERT_INVESTMENT, {
            "invest_on": execution_date,
            "ticker": ticker,
            "amount": amount,
            "next_day": pending_date,
            "shares": shares,
        }).scalar_one()
        position = {
            "ticker": ticker,
            "shares": shares,
            "cost_basis": amount,
            "market_value": amount,
            "entry_price": price,
            "opened_for": pending_date,
            "investment_id": investment_id,
        }
        positions[ticker] = position
        cash -= amount
        connection.execute(UPSERT_POSITION, {"account_id": ACCOUNT_ID, **position})
        print(f"  Bought {ticker}: INR {amount:.2f} at {row['entry_date']} close INR {price:.2f}")

    return cash


def run_simulation(engine) -> None:
    if len(TARGET_ALLOCATIONS) != 5 or any(amount <= 0 for amount in TARGET_ALLOCATIONS):
        raise ValueError("PAPER_ALLOCATIONS must contain five positive comma-separated amounts.")
    if abs(sum(TARGET_ALLOCATIONS) - INITIAL_CASH) > 0.001:
        raise ValueError(f"Target allocations must total the initial amount (INR {INITIAL_CASH:.2f}).")

    with engine.begin() as connection:
        for statement in CREATE_TABLES:
            connection.execute(statement)

        latest_completed = connection.execute(LATEST_COMPLETED_DATE_QUERY).scalar()
        account_row = connection.execute(GET_ACCOUNT, {"account_id": ACCOUNT_ID}).mappings().first()
        if account_row is None:
            # Recover a recent missed session (typically Monday after a missed
            # Sunday run) from retained predictions using the prior session close.
            # The short date window prevents an ordinary Sunday bootstrap from
            # replaying an older Monday prediction.
            today_in_market_timezone = datetime.now(ZoneInfo("Asia/Kolkata")).date()
            start_date = latest_completed
            if latest_completed is not None:
                missed_session_date = connection.execute(
                    MISSED_SESSION_DATE_QUERY,
                    {"latest_completed": latest_completed, "today": today_in_market_timezone},
                ).scalar()
            else:
                missed_session_date = None
            if missed_session_date is not None:
                prior_completed = connection.execute(
                    PREVIOUS_COMPLETED_DATE_QUERY,
                    {"before_date": missed_session_date},
                ).scalar()
                missed_session_rows = connection.execute(
                    UPCOMING_PREDICTIONS_QUERY,
                    {"after_date": prior_completed},
                ).mappings().all()
                missed_session = top_five(missed_session_rows)
                if (
                    len(missed_session) == len(TARGET_ALLOCATIONS)
                    and missed_session[0]["prediction_date"] == missed_session_date
                ):
                    start_date = prior_completed
                    print(
                        f"Recovering missed paper entry for {missed_session_date} "
                        "from the stored prior-session closes."
                    )

            connection.execute(INSERT_ACCOUNT, {
                "account_id": ACCOUNT_ID,
                "initial_cash": INITIAL_CASH,
                "cash": INITIAL_CASH,
                "last_processed_date": start_date,
            })
            account_row = connection.execute(GET_ACCOUNT, {"account_id": ACCOUNT_ID}).mappings().one()
            print(f"Started a new paper portfolio with INR {INITIAL_CASH:.2f}.")

        initial_cash = float(account_row["initial_cash"])
        cash = float(account_row["cash"])
        last_processed_date = account_row["last_processed_date"]
        positions = {
            row["ticker"]: dict(row)
            for row in connection.execute(GET_POSITIONS, {"account_id": ACCOUNT_ID}).mappings()
        }

        # On first start (Sunday evening, for example), invest the account at
        # the latest close carried by the upcoming prediction rows.
        if account_row["pending_prediction_date"] is None:
            upcoming_rows = connection.execute(
                UPCOMING_PREDICTIONS_QUERY,
                {"after_date": last_processed_date},
            ).mappings().all()
            upcoming = top_five(upcoming_rows)
            if len(upcoming) == len(TARGET_ALLOCATIONS):
                prediction_date = upcoming[0]["prediction_date"]
                execution_date = upcoming[0]["entry_date"]
                print(f"\nPaper entry plan for {prediction_date} using the previous session's close:")
                cash = buy_new_winners(
                    connection, positions, cash, upcoming, execution_date, prediction_date
                )
                account_row = dict(account_row)
                account_row["pending_prediction_date"] = prediction_date
                connection.execute(UPDATE_ACCOUNT, {
                    "account_id": ACCOUNT_ID,
                    "cash": cash,
                    "last_processed_date": last_processed_date,
                    "pending_prediction_date": prediction_date,
                })
            else:
                print("No upcoming date has five usable predictions yet; initial cash remains uninvested.")

        completed_rows = connection.execute(
            COMPLETED_PREDICTIONS_QUERY,
            {"after_date": last_processed_date},
        ).mappings().all()
        predictions_by_date = defaultdict(list)
        for row in completed_rows:
            predictions_by_date[row["prediction_date"]].append(row)

        close_rows = connection.execute(
            COMPLETED_CLOSES_QUERY,
            {"after_date": last_processed_date},
        ).mappings().all()
        closes_by_date = defaultdict(dict)
        for row in close_rows:
            closes_by_date[row["trading_date"]][row["ticker"]] = float(row["close_price"])

        processed_dates = sorted(predictions_by_date)
        next_simulation_day = int(connection.execute(
            MAX_SIMULATION_DAY_QUERY, {"account_id": ACCOUNT_ID}
        ).scalar()) + 1
        for trading_date in processed_dates:
            winners = top_five(predictions_by_date[trading_date])
            if len(winners) != len(TARGET_ALLOCATIONS):
                continue
            if account_row["pending_prediction_date"] != trading_date:
                print(
                    f"\nWarning: portfolio was set for {account_row['pending_prediction_date']}, "
                    f"but database has results for {trading_date}; using its actual closes."
                )

            closes = closes_by_date[trading_date]
            value_before_day = cash + sum(position["market_value"] for position in positions.values())
            simulation_day = next_simulation_day
            print(
                f"\nSimulation day {simulation_day} results for {trading_date} "
                "(previous close to today's close):"
            )
            for ticker, position in positions.items():
                if ticker not in closes:
                    raise RuntimeError(f"Missing {ticker} closing price for {trading_date}.")
                position["market_value"] = position["shares"] * closes[ticker]
                if position.get("investment_id") is not None:
                    connection.execute(UPDATE_INVESTMENT_RESULT, {
                        "result": position["market_value"],
                        "investment_id": position["investment_id"],
                        "next_day": trading_date,
                    })
                connection.execute(UPSERT_POSITION, {"account_id": ACCOUNT_ID, **position})
                print(
                    f"  {ticker}: INR {position['market_value']:.2f} "
                    f"(P/L INR {position['market_value'] - position['cost_basis']:+.2f})"
                )

            invested = sum(position["market_value"] for position in positions.values())
            total = invested + cash
            day_pnl = total - value_before_day
            print(
                f"  End of day: invested INR {invested:.2f}, cash INR {cash:.2f}, "
                f"total INR {total:.2f}, day P/L INR {day_pnl:+.2f}, "
                f"overall P/L INR {total - initial_cash:+.2f}"
            )
            connection.execute(UPSERT_DAILY_RESULT, {
                "account_id": ACCOUNT_ID,
                "trading_date": trading_date,
                "simulation_day": simulation_day,
                "invested_value": invested,
                "cash": cash,
                "total_value": total,
                "day_pnl": day_pnl,
            })

            last_processed_date = trading_date
            next_simulation_day += 1
            next_rows = connection.execute(
                UPCOMING_PREDICTIONS_QUERY,
                {"after_date": trading_date},
            ).mappings().all()
            next_winners = top_five(next_rows)
            next_prediction_date = next_winners[0]["prediction_date"] if len(next_winners) == 5 else None
            if next_prediction_date is not None:
                print(f"  Rebalance at {trading_date} close for predictions dated {next_prediction_date}:")
                cash = buy_new_winners(
                    connection, positions, cash, next_winners, trading_date, next_prediction_date
                )
                # New positions are entered at today's close; unchanged holdings
                # remain in place and retain their original cost basis.
                for position in positions.values():
                    connection.execute(UPSERT_POSITION, {"account_id": ACCOUNT_ID, **position})

            connection.execute(UPDATE_ACCOUNT, {
                "account_id": ACCOUNT_ID,
                "cash": cash,
                "last_processed_date": last_processed_date,
                "pending_prediction_date": next_prediction_date,
            })
            account_row = dict(account_row)
            account_row["last_processed_date"] = last_processed_date
            account_row["pending_prediction_date"] = next_prediction_date

        invested = sum(position["market_value"] for position in positions.values())
        print("\nCurrent paper account")
        print(f"  Invested: INR {invested:.2f}")
        print(f"  Cash: INR {cash:.2f}")
        print(f"  Total: INR {invested + cash:.2f}")
        print(f"  Overall P/L: INR {invested + cash - initial_cash:+.2f}")
        print(f"  Pending prediction date: {account_row['pending_prediction_date']}")


def main() -> None:
    engine = create_engine(build_database_url(), pool_pre_ping=True)
    try:
        run_simulation(engine)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
