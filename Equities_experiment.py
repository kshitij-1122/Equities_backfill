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
    url = "postgresql://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications"
    return create_engine(url, echo=False)

def connect_market_data():
    url = "postgresql://postgres:p0stgresisforttda@ttda.postgres.storage.dev.mosaic.hartreepartners.com:5435/postgres"
    return create_engine(url, echo=False)

# --- STEP 1: Get identifiers from backoffice (positions) ---
def get_current_identifiers():
    engine = connect_back_office_applications()
    valuation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
        SELECT DISTINCT bloomberg_ticker AS identifier
        FROM position.aggregated_valuations
        WHERE valuation_date = '{valuation_date}'
          AND etrm = 'enfusion'
          AND bloomberg_ticker IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    return df['identifier'].dropna().unique().tolist()

# --- STEP 2: Append and persist identifier universe ---
def update_identifier_universe(current_identifiers):
    conn = sqlite3.connect(SQLITE_PATH)
    try:
        df_existing = pd.read_sql("SELECT DISTINCT identifier FROM identifier_universe", conn)
    except Exception:
        df_existing = pd.DataFrame(columns=["identifier"])

    df_new = pd.DataFrame(current_identifiers, columns=["identifier"])
    df_all = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates()
    df_all.to_sql("identifier_universe", conn, if_exists="replace", index=False)
    conn.close()
    return df_all["identifier"].tolist()

# --- STEP 3: Get existing identifier-date pairs from Postgres ---
def get_existing_identifier_dates(tracked_identifiers):
    if not tracked_identifiers:
        return pd.DataFrame(columns=["identifier", "date"])

    formatted = "', '".join([f"'{id}'" for id in tracked_identifiers])
    sql_list = f"({formatted})"

    query = f"""
        SELECT identifier, date
        FROM raw.bbg_values
        WHERE identifier IN {sql_list}
          AND date >= '2022-06-18' AND date <= CURRENT_DATE - INTERVAL '1 day'
    """

    engine = connect_market_data()
    df = pd.read_sql(query, engine, parse_dates=["date"])
    return df

# --- STEP 4: Identify which identifiers need backfill ---
def identify_gaps(tracked_identifiers, df_existing):
    full_dates = set(pd.date_range(start=START_DATE, end=END_DATE).date)
    to_backfill = []

    for identifier in tracked_identifiers:
        existing_dates = df_existing[df_existing['identifier'] == identifier]['date'].dt.date.unique()
        if set(existing_dates) != full_dates:
            to_backfill.append(identifier)

    return to_backfill

# --- STEP 5: Build and send Bloomberg request ---
def request_bloomberg_data(identifiers_to_backfill):
    if not identifiers_to_backfill:
        print("✅ All identifiers have complete date coverage.")
        return None

    request_id = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
    universe = [
        {"@type": "Identifier", "identifierType": "TICKER", "identifierValue": i}
        for i in identifiers_to_backfill
    ]

    req = {
        "@type": "HistoryRequest",
        "name": request_id,
        "description": f"Backfill for {len(universe)} identifiers",
        "universe": {"@type": "Universe", "contains": universe},
        "trigger": {"@type": "SubmitTrigger"},
        "formatting": {"@type": "MediaType", "outputMediaType": "text/csv"},
        "runtimeOptions": {
            "@type": "HistoryRuntimeOptions",
            "dateRange": {
                "@type": "IntervalDateRange",
                "startDate": START_DATE.strftime("%Y-%m-%d"),
                "endDate": END_DATE.strftime("%Y-%m-%d")
            }
        },
        "fieldList": {
            "@type": "HistoryFieldList",
            "contains": [
                {"mnemonic": "PX_LAST"},
                {"mnemonic": "PX_SETTLE"},
                {"mnemonic": "PX_VOLUME"},
                {"mnemonic": "SECURITY_TYP"},
                {"mnemonic": "IDENTIFIER"}
            ]
        }
    }

    df = download(req)
    return df

# --- STEP 6: Save data to SQLite safely ---
def save_to_sqlite(df):
    if df is None or df.empty:
        print("⚠️ No data returned from Bloomberg.")
        return

    df.columns = [col.upper() for col in df.columns]
    df = df.dropna(subset=["IDENTIFIER", "DATE"])
    df["IDENTIFIER"] = df["IDENTIFIER"].astype(str).str.strip()
    df["DATE"] = pd.to_datetime(df["DATE"])
    df = df[df["IDENTIFIER"] != ""]

    conn = sqlite3.connect(SQLITE_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
    conn.close()

    print(f"✅ Appended {len(df)} new rows to {TABLE_NAME}.")

# --- MAIN ORCHESTRATION ---
def backfill_missing_data():
    current_ids = get_current_identifiers()
    all_ids = update_identifier_universe(current_ids)
    existing_data = get_existing_identifier_dates(all_ids)
    identifiers_to_backfill = identify_gaps(all_ids, existing_data)
    df = request_bloomberg_data(identifiers_to_backfill)
    save_to_sqlite(df)

if __name__ == "__main__":
    backfill_missing_data()