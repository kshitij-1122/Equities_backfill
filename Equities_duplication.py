import os
import sqlite3
import random
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from bbg_dl import download

# --- CONFIG ---
START_DATE = datetime(2022, 6, 18)
END_DATE = datetime.now() - timedelta(days=1)
SQLITE_PATH = "equity_backfill.sqlite"
TABLE_NAME = "bloomberg_equity_history"


# --- DATABASE CONNECTIONS ---
def connect_back_office_applications():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        url = "postgresql://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications"
    else:
        url = "postgresql://postgres:6wjiKOxXuWJ4CrJ9@backoffice.postgres.storage.mosaic.hartreepartners.com:5432/applications"
    return create_engine(url, echo=False)


def connect_market_data():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        url = "postgresql://postgres:p0stgresisforttda@ttda.postgres.storage.dev.mosaic.hartreepartners.com:5435/postgres"
    else:
        url = "postgresql://postgres:p0stgrespr0d4ttda@ttda.postgres.storage.mosaic.hartreepartners.com:5432/postgres"
    return create_engine(url, echo=False)


# --- GET TICKERS FROM DB AND HISTORY COMBINED ---
def get_combined_ticker_universe():
    # 1. Get yesterday's tickers from applications DB
    engine = connect_back_office_applications()
    valuation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
        SELECT DISTINCT bloomberg_ticker
        FROM position.aggregated_valuations
        WHERE valuation_date = '{valuation_date}'
          AND etrm = 'enfusion'
          AND bloomberg_ticker IS NOT NULL
    """
    df_new = pd.read_sql(query, engine).dropna().drop_duplicates()
    df_new.columns = ["ticker"]

    # 2. Get existing tickers from SQLite history table
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        df_existing = pd.read_sql(f"SELECT DISTINCT TICKER FROM {TABLE_NAME}", conn)
    except Exception:
        df_existing = pd.DataFrame(columns=["TICKER"])
    conn.close()

    df_all = pd.concat([df_new, df_existing.rename(columns={"TICKER": "ticker"})], ignore_index=True).drop_duplicates()
    return df_all["ticker"].tolist()


# --- Existing market data from warehouse ---
def get_existing_date_ranges():
    engine = connect_market_data()
    query = """
        SELECT identifier, MIN(date) AS min_date, MAX(date) AS max_date
        FROM raw.bbg_values
        GROUP BY identifier
    """
    df = pd.read_sql(query, engine, parse_dates=["min_date", "max_date"])
    return df.set_index("identifier").to_dict(orient="index")


# --- BACKFILL PROCESS ---
def backfill_missing_data():
    tickers = get_combined_ticker_universe()
    existing_ranges = get_existing_date_ranges()

    tickers_to_backfill = []

    for ticker in tickers:
        if ticker not in existing_ranges:
            tickers_to_backfill.append((ticker, START_DATE, END_DATE))
        else:
            min_date = existing_ranges[ticker]["min_date"].date()
            max_date = existing_ranges[ticker]["max_date"].date()

            if min_date > START_DATE.date():
                tickers_to_backfill.append((ticker, START_DATE, min_date - timedelta(days=1)))
            if max_date < END_DATE.date():
                tickers_to_backfill.append((ticker, max_date + timedelta(days=1), END_DATE))

    if not tickers_to_backfill:
        print("✅ All tickers are fully backfilled in raw.bbg_values.")
        return

    request_id = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
    universe = [
        {
            "@type": "Identifier",
            "identifierType": "TICKER",
            "identifierValue": ticker
        }
        for ticker, _, _ in tickers_to_backfill
    ]

    global_start = min(pd.to_datetime(start) for _, start, _ in tickers_to_backfill)
    global_end = max(pd.to_datetime(end) for _, _, end in tickers_to_backfill)

    req = {
        "@type": "HistoryRequest",
        "name": request_id,
        "description": f"Backfill for {len(set(t[0] for t in tickers_to_backfill))} tickers",
        "universe": {
            "@type": "Universe",
            "contains": universe
        },
        "trigger": {"@type": "SubmitTrigger"},
        "formatting": {"@type": "MediaType", "outputMediaType": "text/csv"},
        "runtimeOptions": {
            "@type": "HistoryRuntimeOptions",
            "dateRange": {
                "@type": "IntervalDateRange",
                "startDate": global_start.strftime("%Y-%m-%d"),
                "endDate": global_end.strftime("%Y-%m-%d")
            }
        },
        "fieldList": {
            "@type": "HistoryFieldList",
            "contains": [
                {"mnemonic": "PX_LAST"},
                {"mnemonic": "PX_SETTLE"},
                {"mnemonic": "PX_VOLUME"},
                {"mnemonic": "SECURITY_TYP"},
                {"mnemonic": "TICKER"},
            ]
        }
    }

    df = download(req)
    if df is not None and not df.empty:
        conn = sqlite3.connect(SQLITE_PATH)
        df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
        conn.close()
        print(f"✅ Backfilled {len(df)} rows for {len(set(t[0] for t in tickers_to_backfill))} tickers.")
    else:
        print("⚠️ No data returned from Bloomberg.")


# --- MAIN ---
if __name__ == "__main__":
    backfill_missing_data()
