import requests
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()
APP_ID = os.getenv("OPENEXCHANGE_APP_ID")
CURRENCY_API_URL = os.getenv("CURRENCY_API_URL")
FX_RATES_API_URL = os.getenv("FX_RATES_API_URL")
DUCKDB_FILE = os.path.join("data", "fx_data_bronze.duckdb")  # Unified DuckDB file

def fetch_exchange_rates():
    url = f"{FX_RATES_API_URL}?app_id={APP_ID}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        print("FX rates fetched")
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error fetching exchange rates:", e)
        return None

def fetch_currency_metadata():
    url = f"{CURRENCY_API_URL}?app_id={APP_ID}"
    try:
        response = requests.get(url)
        print("Response status code:", response.status_code)
        response.raise_for_status()
        data = response.json()
        print("Currency metadata fetched")
        return data
    except requests.exceptions.RequestException as e:
        print("Error fetching currency metadata:", e)
        return None

def clean_and_store_fx_rates(fx_data, currency_metadata):
    timestamp = fx_data.get("timestamp")
    rates = fx_data.get("rates", {})
    metadata = currency_metadata

    # Convert to DataFrame
    df_rates = pd.DataFrame(rates.items(), columns=["currency", "rate"])
    df_rates["timestamp"] = pd.to_datetime(timestamp, unit='s')

    # Deduplicate currency-rate combos (to avoid duplicates from API)
    df_rates = df_rates.drop_duplicates(subset=["currency", "rate"])

    df_currency = pd.DataFrame(list(metadata.items()), columns=["currency", "name"])

    # Join FX rate data with currency name
    df_merged = pd.merge(df_rates, df_currency, on="currency", how="left")

    ts_str = df_merged["timestamp"].iloc[0].isoformat()

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("""
        CREATE TABLE IF NOT EXISTS exchange_rates (
            currency TEXT,
            rate DOUBLE,
            timestamp TIMESTAMP,
            name TEXT
        )
    """)

    # Deduplication check
    existing = con.execute(f"""
        SELECT COUNT(*) FROM exchange_rates WHERE timestamp = '{ts_str}'
    """).fetchone()[0]

    if existing == 0:
        con.execute("INSERT INTO exchange_rates SELECT * FROM df_merged")
        print(f"New FX rates inserted for timestamp {ts_str}")
    else:
        print(f"Timestamp {ts_str} already exists. Skipping insert.")

    con.close()
    return df_merged

if __name__ == "__main__":
    fx_data = fetch_exchange_rates()
    currency_data = fetch_currency_metadata()

    if fx_data and currency_data:
        result_df = clean_and_store_fx_rates(fx_data, currency_data)
        print("Final Combined Data:")
        print(result_df)
