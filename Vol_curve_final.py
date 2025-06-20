import os
import random
import logging
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from bbg_dl import download

logger = logging.getLogger(__name__)

def connect_back_office_applications():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        return create_engine(
            "postgresql://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications"
        )
    else:
        return create_engine(
            "postgresql://postgres:6wjiKOxXuWJ4CrJ9@backoffice.postgres.storage.mosaic.hartreepartners.com:5432/applications"
        )

def connect_market_data():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        return create_engine(
            "postgresql://postgres:p0stgresisforttda@ttda.postgres.storage.dev.mosaic.hartreepartners.com:5435/postgres"
        )
    else:
        return create_engine(
            "postgresql://postgres:p0stgrespr0d4ttda@ttda.postgres.storage.mosaic.hartreepartners.com:5432/postgres"
        )

def connect_crate_db():
    return create_engine("crate://ttda.storage.mosaic.hartreepartners.com:4200", echo=True)

def get_combined_ticker_list():
    """
    Combines current tickers from DB with historical tickers from 'all_tickers.csv'.
    Uses valuation_date = today - 1.
    Saves the updated list back to CSV.
    """
    valuation_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    engine = connect_back_office_applications()
    query = f"""
        SELECT DISTINCT bloomberg_ticker
        FROM position.aggregated_valuations av 
        WHERE valuation_date = '{valuation_date}' 
        AND etrm = 'enfusion' 
        AND bloomberg_ticker IS NOT NULL
    """
    df_db = pd.read_sql(query, engine)
    db_tickers = df_db['bloomberg_ticker'].dropna().unique().tolist()

    ticker_file = "all_tickers.csv"
    if os.path.exists(ticker_file):
        df_saved = pd.read_csv(ticker_file)
        saved_tickers = df_saved['bloomberg_ticker'].dropna().unique().tolist()
    else:
        saved_tickers = []

    combined = sorted(set(db_tickers + saved_tickers))
    pd.DataFrame({'bloomberg_ticker': combined}).to_csv(ticker_file, index=False)

    return combined

def extract_vol_surfaces_to_sqlite():
    """
    Extracts EOD implied vol surface data from Bloomberg and saves to a persistent SQLite DB.
    The surface data is exploded to one row per (expiry, delta, strike, vol), and appended.
    """
    tickers = get_combined_ticker_list()
    request_name = "".join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
    db_path = "vol_curves.sqlite"  # ✅ Persistent SQLite file
    table_name = "vol_curves"

    universe = {
        "@type": "Universe",
        "contains": [
            {
                "@type": "Identifier",
                "identifierType": "TICKER",
                "identifierValue": ticker,
                "fieldOverrides": [
                    {"@type": "FieldOverride", "mnemonic": "IVOL_SURFACE_AXIS_TYPE", "override": "Mixed/Pct"},
                    {"@type": "FieldOverride", "mnemonic": "TIME_ZONE_OVERRIDE", "override": "22"}
                ]
            }
            for ticker in tickers
        ]
    }

    req = {
        "@type": "DataRequest",
        "name": request_name,
        "description": "EOD Implied Volatility Surfaces",
        "universe": universe,
        "trigger": {"@type": "SubmitTrigger"},
        "formatting": {"@type": "MediaType", "outputMediaType": "text/csv"},
        "fieldList": {
            "@type": "DataFieldList",
            "contains": [
                {"mnemonic": "IVOL_SURFACE_AXIS_TYPE"},
                {"mnemonic": "EOD_IMPLIED_VOLATILITY_SURFACE"},
                {"mnemonic": "PX_LAST_EOD"},
                {"mnemonic": "NAME"},
                {"mnemonic": "SECURITY_TYP"},
                {"mnemonic": "TICKER"},
                {"mnemonic": "LAST_UPDATE_DATE_EOD"},
                {"mnemonic": "PARSEKYABLE_DES"}
            ]
        }
    }

    df = download(req)
    if df is not None and not df.empty:
        cols_to_explode = [
            "EOD_IMPLIED_VOLATILITY_SURFACE.BC_TENOR",
            "EOD_IMPLIED_VOLATILITY_SURFACE.BC_EXP_DATE",
            "EOD_IMPLIED_VOLATILITY_SURFACE.MONEY_DELTA",
            "EOD_IMPLIED_VOLATILITY_SURFACE.OPT_STRIKE_PX",
            "EOD_IMPLIED_VOLATILITY_SURFACE.IVOL"
        ]

        def safe_split(cell):
            if pd.isna(cell):
                return []
            return str(cell).split('|')

        for col in cols_to_explode:
            df[col] = df[col].apply(safe_split)

        df = df.explode(cols_to_explode).reset_index(drop=True)

        # Convert types
        df["EOD_IMPLIED_VOLATILITY_SURFACE.BC_EXP_DATE"] = pd.to_datetime(
            df["EOD_IMPLIED_VOLATILITY_SURFACE.BC_EXP_DATE"], errors="coerce"
        ).dt.date
        df["EOD_IMPLIED_VOLATILITY_SURFACE.MONEY_DELTA"] = pd.to_numeric(
            df["EOD_IMPLIED_VOLATILITY_SURFACE.MONEY_DELTA"], errors="coerce"
        )
        df["EOD_IMPLIED_VOLATILITY_SURFACE.OPT_STRIKE_PX"] = pd.to_numeric(
            df["EOD_IMPLIED_VOLATILITY_SURFACE.OPT_STRIKE_PX"], errors="coerce"
        )
        df["EOD_IMPLIED_VOLATILITY_SURFACE.IVOL"] = pd.to_numeric(
            df["EOD_IMPLIED_VOLATILITY_SURFACE.IVOL"], errors="coerce"
        )

        # Append to persistent SQLite DB
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.close()

        print(f"✅ Appended vol surfaces for {len(tickers)} tickers to {db_path} (table: {table_name})")
    else:
        print("⚠️ No vol surface data returned.")

if __name__ == "__main__":
    extract_vol_surfaces_to_sqlite()
