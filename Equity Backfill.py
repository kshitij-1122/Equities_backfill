import random
from bbg_dl import download
from sqlalchemy import create_engine
import logging
import os
import pandas as pd
import sqlite3
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
    return df['bloomberg_ticker'].dropna().unique().tolist()


def save_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()


def backfill_equity_history(db_path, table_name="bloomberg_equity_history"):
    """
    For each ticker in the database, backfill historical data from the last available date in SQLite (or from start_date if new) to today.
    Do not remove data for tickers that are no longer in the DB.
    Only pull missing data to avoid gaps and minimize unnecessary Bloomberg requests.
    """
    import random
    start_date = datetime(2022, 6, 18)
    end_date = datetime.now()
    tickers = get_tickers_from_db()

    # Use the original request_name logic and do not change it again
    request_name = "".join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))

    # Use a new SQLite file for this run
    run_db_path = f"equity_backfill_{request_name}.sqlite"

    # Connect to SQLite and read existing data (from the new file)
    conn = sqlite3.connect(run_db_path)
    try:
        df_existing = pd.read_sql(f"SELECT TICKER, DATE FROM {table_name}", conn)
    except Exception:
        df_existing = pd.DataFrame(columns=["TICKER", "DATE"])
    
    # Ensure DATE column is datetime
    if not df_existing.empty:
        df_existing["DATE"] = pd.to_datetime(df_existing["DATE"])
    
    new_data = []
    for ticker in tickers:
        # Find last date for this ticker
        if not df_existing.empty and ticker in df_existing["TICKER"].values:
            last_date = df_existing[df_existing["TICKER"] == ticker]["DATE"].max()
            # If last_date is yesterday, only pull for today
            if last_date.date() >= (end_date.date() - timedelta(days=1)):
                fetch_start = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                fetch_start = last_date + timedelta(days=1)
        else:
            fetch_start = start_date
        if fetch_start > end_date:
            continue  # Already up to date
        req = {
            "@type": "HistoryRequest",
            "name": request_name,
            "description": f"Equity history for {ticker}",
            "universe": {
                "@type": "Universe",
                "contains": [
                    {
                        "@type": "Identifier",
                        "identifierType": "TICKER",
                        "identifierValue": ticker,
                    }
                ],
            },
            "trigger": {"@type": "SubmitTrigger"},
            "formatting": {"@type": "MediaType", "outputMediaType": "text/csv"},
            "runtimeOptions": {
                "@type": "HistoryRuntimeOptions",
                "dateRange": {
                    "@type": "IntervalDateRange",
                    "startDate": fetch_start.strftime("%Y-%m-%d"),
                    "endDate": end_date.strftime("%Y-%m-%d"),
                },
            },
            "fieldList": {
                "@type": "HistoryFieldList",
                "contains": [
                    {"mnemonic": "PX_LAST"},
                    {"mnemonic": "PX_SETTLE"},
                    {"mnemonic": "PX_VOLUME"},
                    {"mnemonic": "SECURITY_TYP"},
                    {"mnemonic": "TICKER"},
                ],
            },
        }
        print(f"Request body for {ticker}: {req}")  # Debug: print the request body
        df = download(req)
        if df is not None and not df.empty:
            new_data.append(df)
    if new_data:
        df_new = pd.concat(new_data, ignore_index=True)
        # Append to SQLite (do not replace)
        df_new.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Appended {len(df_new)} new rows to {table_name} in {run_db_path}")
    else:
        print(f"No new data to backfill in {run_db_path}.")
    conn.close()


if __name__ == "__main__":
    # Diagnostic: print columns in bloomberg_equity_history table
    conn = sqlite3.connect("equity_backfill.sqlite")
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(bloomberg_equity_history)")
    columns = cursor.fetchall()
    print("bloomberg_equity_history columns:")
    for col in columns:
        print(col)
    conn.close()

    # Main block now only uses backfill_equity_history for all EOD data
    backfill_equity_history("equity_backfill.sqlite", "bloomberg_equity_history")

  
  
  # def equity_corporate_actions_request(tickers, start_date, end_date):
#     request_name = "".join(random.choices("abcdefghijklmnopqrstuvwxyz123456789", k=12))
#     return {
#         "@type": "HistoryRequest",
#         "name": request_name,
#         "description": "Equity Corporate Actions request",
#         "universe": {
#             "@type": "Universe",
#             "contains": [
#                 {
#                     "@type": "Identifier",
#                     "identifierType": "TICKER",
#                     "identifierValue": ticker,
#                 }
#                 for ticker in tickers
#             ],
#         },
#         "trigger": {"@type": "SubmitTrigger"},
#         "formatting": {"@type": "MediaType", "outputMediaType": "text/csv"},
#         "runtimeOptions": {
#             "@type": "HistoryRuntimeOptions",
#             "dateRange": {
#                 "@type": "IntervalDateRange",
#                 "startDate": start_date.strftime("%Y-%m-%d"),
#                 "endDate": end_date.strftime("%Y-%m-%d"),
#             },
#         },
#         "fieldList": {
#             "@type": "HistoryFieldList",
#             "contains": [
#                 {"mnemonic": "EQY_SPLIT_RATIO"},
#                 {"mnemonic": "EQY_SPLIT_DT"},
#             ],
#         },
#     }

  
  
    # # Corporate actions request
    # req_corp = equity_corporate_actions_request(tickers,start_date, end_date)
    # df_corp = download(req_corp)
    # print(df_corp)
    # save_to_sqlite(df_corp, "equity_backfill.sqlite", "bloomberg_equity_corp_actions")


    # # Loop for daily corporate actions
    # corp_actions_all = []
    # current_date = start_date
    # while current_date <= end_date:
    #     req_corp = equity_corporate_actions_request(tickers, current_date, current_date)
    #     df_corp = download(req_corp)
    #     if df_corp is not None and not df_corp.empty:
    #         # Only keep rows where at least one of the corporate action columns is not null
    #         ca_cols = [
    #             'DVD_HIST_ALL',
    #             'EQY_SPLIT_RATIO',
    #             'EQY_SPLIT_DT',
    #             'EARN_ANN_DT_TIME_HIST_WITH_EPS'
    #         ]
    #         # Some columns may have subfields, so filter on any column that starts with these names
    #         ca_present = df_corp[[col for col in df_corp.columns if any(col.startswith(ca) for ca in ca_cols)]]
    #         mask = ca_present.notnull().any(axis=1)
    #         df_corp_filtered = df_corp[mask].copy()
    #         if not df_corp_filtered.empty:
    #             df_corp_filtered['as_of_date'] = current_date.strftime('%Y-%m-%d')
    #             corp_actions_all.append(df_corp_filtered)
    #     current_date += timedelta(days=1)
    # if corp_actions_all:
    #     df_corp_all = pd.concat(corp_actions_all, ignore_index=True)
    #     print(df_corp_all)
    #     save_to_sqlite(df_corp_all, "equity_backfill.sqlite", "bloomberg_equity_corp_actions_daily")
    # else:
    #     print("No corporate actions data found for any day in the range.")