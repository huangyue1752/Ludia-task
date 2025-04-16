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
DUCKDB_FILE = os.path.join("data", "fx_data_currency.duckdb") # Persistent DuckDB file path

def fetch_currency():
    url = f"{CURRENCY_API_URL}?app_id={APP_ID}"
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
    
def store_currency_table(data):
    """Overwrite the DuckDB currencies table with new data."""
    df = pd.DataFrame(data.items(), columns=["currency", "name"])
    
    con = duckdb.connect(DUCKDB_FILE)

    # Overwrite the table every time
    con.execute("CREATE OR REPLACE TABLE currencies AS SELECT * FROM df")
    
    con.close()
    print("✅ 'currencies' table overwritten in fx_data_currency.duckdb.")
    return df

if __name__ == "__main__":
    currency_data = fetch_currency()
    if currency_data:
        df = store_currency_table(currency_data)
        print(df)  # Optional: preview