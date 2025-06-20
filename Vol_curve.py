import random
from bbg_dl import download
from sqlalchemy import create_engine
import logging
import os
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def connect_back_office_applictions():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        return create_engine(
            "postgres://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications"
        )
    else:
        return create_engine(
            "postgresql://postgres:6wjiKOxXuWJ4CrJ9@backoffice.postgres.storage.mosaic.hartreepartners.com:5432/applications"
        )


def connect_market_data():
    env = os.getenv("MOSAIC_ENV", "DEV")
    if env == "DEV":
        conn = create_engine(
            "postgresql://postgres:p0stgresisforttda@ttda.postgres.storage.dev.mosaic.hartreepartners.com:5435/postgres"
        )
    else:
        conn = create_engine(
            "postgresql://postgres:p0stgrespr0d4ttda@ttda.postgres.storage.mosaic.hartreepartners.com:5432/postgres",
        )
    return conn


def connect_crate_db():
    conn = create_engine(
        "crate://ttda.storage.mosaic.hartreepartners.com:4200", echo=True
    )
    return conn

def get_tickers_from_db():
    engine = create_engine("postgresql+psycopg2://postgres:YvXSdf2XifnpTZF5@backoffice.postgres.storage.dev.mosaic.hartreepartners.com:5435/applications")
    query = """
        select distinct bloomberg_ticker
        from position.aggregated_valuations av 
        where valuation_date = '06-18-2025' 
        and etrm = 'enfusion' 
        and bloomberg_ticker notnull
    """
    df = pd.read_sql(query, engine)
    return df['bloomberg_ticker'].dropna().tolist()


def vol_request(tickers):
    request_name = "".join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
    return {
        "@type": "DataRequest",
        "name": request_name,
        "description": "Example request",
        "universe": {
            "@type": "Universe",
            "contains": [
                {
                    "@type": "Identifier",
                    "identifierType": "TICKER",
                    "identifierValue": ticker,
                    "fieldOverrides": [
                        {
                            "@type": "FieldOverride",
                            "mnemonic": "IVOL_SURFACE_AXIS_TYPE",
                            "override": "Mixed/Pct",
                        }
                    ],
                }
                for ticker in tickers
            ],
        },
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
                {"mnemonic": "PARSEKYABLE_DES"},
            ],
        },
    }


if __name__ == "__main__":
    # Get tickers from DB
    tickers = get_tickers_from_db()
    print(f"Fetched {len(tickers)} tickers from DB.")
    
    # Get vol curves for all tickers in batches (to avoid too large requests)
    batch_size = 10  # adjust as needed
    all_results = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"Processing batch {i//batch_size+1}: {batch}")
        req_v = vol_request(batch)
        df_v = download(req_v)
        if isinstance(df_v, pd.DataFrame) and not df_v.empty:
            df_v['batch_tickers'] = ','.join(batch)
            all_results.append(df_v)
        else:
            print(f"No data returned for batch: {batch}")
    
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        # Store results in CSV file
        csv_path = 'vol_curves.csv'
        final_df.to_csv(csv_path, index=False)
        print(f"Stored {len(final_df)} rows in {csv_path}")
        print(final_df.head())
    else:
        print("No results to store.")
