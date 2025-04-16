import duckdb
import os

BRONZE_DB = os.path.join("data", "fx_data_bronze.duckdb")
CURRENCY_DB = os.path.join("data", "fx_data_currency.duckdb")
SILVER_DB = os.path.join("data", "fx_data_silver.duckdb")

def build_fx_silver():
    # Connect to silver DB (we will overwrite the table here)
    con = duckdb.connect(SILVER_DB)

    # Attach bronze and currency DBs
    con.execute(f"ATTACH DATABASE '{BRONZE_DB}' AS bronze")
    con.execute(f"ATTACH DATABASE '{CURRENCY_DB}' AS currency")

    # Get the latest timestamp from the bronze table
    latest_ts = con.execute("""
        SELECT MAX(timestamp) FROM bronze.exchange_rates
    """).fetchone()[0]

    print(f"🕒 Latest timestamp: {latest_ts}")

    # Join exchange rates with currency names
    con.execute("""
    CREATE OR REPLACE TABLE exchange_rates_silver AS
    SELECT 
        bronze.exchange_rates.currency,
        currency.currencies.name AS currency_name,
        round(bronze.exchange_rates.rate,2) as rate,
        bronze.exchange_rates.timestamp,
    FROM bronze.exchange_rates
    LEFT JOIN currency.currencies
    ON bronze.exchange_rates.currency = currency.currencies.currency
    WHERE bronze.exchange_rates.timestamp = ?
""", [latest_ts])

    # Preview result
    result = con.execute("SELECT * FROM exchange_rates_silver").fetchdf()
    print("✅ Silver table created. Sample:")
    print(result)

    con.close()

if __name__ == "__main__":
    build_fx_silver()
