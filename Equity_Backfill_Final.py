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


# --- GET IDENTIFIERS FROM POSITIONS DB ---
def get_current_identifiers():
    engine = connect_back_office_applications()
    valuation_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    query = f"""
        SELECT DISTINCT LOWER(bloomberg_ticker) AS identifier
        FROM position.aggregated_valuations
        WHERE valuation_date = '{valuation_date}'
          AND etrm = 'enfusion'
          AND bloomberg_ticker IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    return df['identifier'].dropna().unique().tolist()


# --- GET min/max DATES FROM POSTGRES PER IDENTIFIER ---
def get_identifier_date_ranges():
    engine = connect_market_data()
    query = """
        SELECT LOWER(identifier) AS identifier,
               MIN(date) AS min_date,
               MAX(date) AS max_date
        FROM raw.bbg_values
        WHERE date >= '2022-06-18'
        GROUP BY LOWER(identifier)
    """
    df = pd.read_sql(query, engine, parse_dates=["min_date", "max_date"])
    df['identifier'] = df['identifier'].str.lower()
    return df.set_index("identifier").to_dict(orient="index")


# --- BACKFILL PROCESS ---
def backfill_missing_data():
    identifiers = get_current_identifiers()
    existing_ranges = get_identifier_date_ranges()

    full_date_set = set(pd.date_range(start=START_DATE, end=END_DATE).date)
    identifiers_to_backfill = []

    for identifier in identifiers:
        if identifier not in existing_ranges:
            identifiers_to_backfill.append(identifier)
        else:
            min_date = existing_ranges[identifier]["min_date"].date()
            max_date = existing_ranges[identifier]["max_date"].date()

            existing_range = set(pd.date_range(min_date, max_date).date)
            if full_date_set - existing_range:
                identifiers_to_backfill.append(identifier)

    if not identifiers_to_backfill:
        print("✅ All identifiers have complete date coverage.")
        return

    request_id = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
    universe = [
        {
            "@type": "Identifier",
            "identifierType": "TICKER",
            "identifierValue": identifier.upper()
        }
        for identifier in identifiers_to_backfill
    ]

    req = {
        "@type": "HistoryRequest",
        "name": request_id,
        "description": f"Backfill for {len(identifiers_to_backfill)} identifiers with missing dates",
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

    if df is not None and not df.empty:
        df.columns = [col.upper() for col in df.columns]
        df = df.dropna(subset=["IDENTIFIER", "DATE"])
        df["IDENTIFIER"] = df["IDENTIFIER"].astype(str).str.strip().str.lower()
        df["DATE"] = pd.to_datetime(df["DATE"])
        df = df[df["IDENTIFIER"] != ""]

        conn = sqlite3.connect(SQLITE_PATH)

        try:
            existing_keys = pd.read_sql(f"SELECT IDENTIFIER, DATE FROM {TABLE_NAME}", conn)
            existing_keys["IDENTIFIER"] = existing_keys["IDENTIFIER"].astype(str).str.strip().str.lower()
            existing_keys["DATE"] = pd.to_datetime(existing_keys["DATE"])
            existing_keys_set = set(zip(existing_keys["IDENTIFIER"], existing_keys["DATE"]))
        except Exception:
            existing_keys_set = set()

        df_filtered = df[~df.set_index(["IDENTIFIER", "DATE"]).index.isin(existing_keys_set)]

        if not df_filtered.empty:
            df_filtered.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
            print(f"✅ Appended {len(df_filtered)} new rows to {TABLE_NAME}.")
        else:
            print("✅ No new rows to append — all data already exists.")

        conn.close()
    else:
        print("⚠️ No data returned from Bloomberg.")


# --- MAIN ---
if __name__ == "__main__":
    backfill_missing_data()
