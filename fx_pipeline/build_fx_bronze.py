import requests
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

# Load variables from .env into environment
load_dotenv()
# Use them
APP_ID = os.getenv("OPENEXCHANGE_APP_ID")
CURRENCY_API_URL = os.getenv("CURRENCY_API_URL")
FX_RATES_API_URL = os.getenv("FX_RATES_API_URL")
DUCKDB_FILE = os.path.join("data", "fx_data_bronze.duckdb")   # Persistent DuckDB file path

def fetch_exchange_rates():
    url = f"{FX_RATES_API_URL}?app_id={APP_ID}"
    print("Starting the request...")  # Debugging line
    try:
        response = requests.get(url)
        print("Response status code:", response.status_code)  # Debugging line
        response.raise_for_status()  # Raises HTTPError if status is 4xx or 5xx
        data = response.json()
        print(data)  # Debugging line
        return data
    except requests.exceptions.RequestException as e:
        print("Error fetching exchange rates:", e)
        return None
    
def clean_and_store_fx_rates(data):
    """Extract and store only timestamp and rates into DuckDB (only if new timestamp)."""
    timestamp = data.get("timestamp")
    rates = data.get("rates", {})
    
    # Convert to DataFrame
    df = pd.DataFrame(rates.items(), columns=["currency", "rate"])
    df["timestamp"] = pd.to_datetime(timestamp, unit='s')  # Convert UNIX timestamp to datetime
    ts_str = df["timestamp"].iloc[0].isoformat()  # e.g., '2025-04-15T23:40:16'

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_FILE)
    
    # Create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS exchange_rates (
            currency TEXT,
            rate DOUBLE,
            timestamp TIMESTAMP
        )
    """)

    # Check if this timestamp already exists
    existing = con.execute(f"""
        SELECT COUNT(*) FROM exchange_rates WHERE timestamp = '{ts_str}'
    """).fetchone()[0]

    if existing == 0:
        con.execute("INSERT INTO exchange_rates SELECT * FROM df")
        print(f"✅ New data inserted for timestamp {ts_str}")
    else:
        print(f"⏩ Timestamp {ts_str} already exists. Skipping insert.")

    con.close()
    return df




if __name__ == "__main__":
    exchange_data = fetch_exchange_rates()
    if exchange_data:
        cleaned_data = clean_and_store_fx_rates(exchange_data)
        print("Cleaned Data:")
        print(cleaned_data)


