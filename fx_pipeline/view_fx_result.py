import duckdb
import pandas as pd
import os

BRONZE_DB = os.path.join("data", "fx_data_bronze.duckdb")
SILVER_DB = os.path.join("data", "fx_data_silver.duckdb")
DIM_DB = os.path.join("data", "fx_data_currency.duckdb")

def view_dimention_data():
    con = duckdb.connect(DIM_DB)

    # Show available tables
    print("📋 Tables in fx_data_currency.duckdb:")
    tables = con.execute("SHOW TABLES").fetchdf()
    print(tables)

    # Preview full exchange rate data
    df = con.execute("""
        SELECT * FROM currencies
    """).fetchdf()

    con.close()

    print("\n💱 currency:")
    print(df)
    print(f"\n📊 Total records: {len(df)}")

def view_bronze_data():
    con = duckdb.connect(BRONZE_DB)

    # Show available tables
    print("📋 Tables in fx_data_bronze.duckdb:")
    tables = con.execute("SHOW TABLES").fetchdf()
    print(tables)

    # Preview full exchange rate data
    df = con.execute("""
        SELECT * FROM exchange_rates
        ORDER BY timestamp DESC, currency
    """).fetchdf()

    con.close()

    print("\n💱 Exchange Rate Records (most recent first):")
    print(df)
    print(f"\n📊 Total records: {len(df)}")

def view_silver_data():
    con = duckdb.connect(SILVER_DB)

    # Show available tables
    print("📋 Tables in fx_data_silver.duckdb:")
    tables = con.execute("SHOW TABLES").fetchdf()
    print(tables)

    # Preview full exchange rate data
    df = con.execute("""
        SELECT * FROM exchange_rates_silver
        ORDER BY currency
    """).fetchdf()

    con.close()

    print("\n💱 Exchange Rate Records (most recent):")
    print(df)
    print(f"\n📊 Total records: {len(df)}")

if __name__ == "__main__":
    view_dimention_data()
    view_bronze_data()
    view_silver_data()
