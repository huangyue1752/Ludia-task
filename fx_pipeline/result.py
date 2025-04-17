import duckdb
import os

DUCKDB_FILE = os.path.join("data", "fx_data_bronze.duckdb")

def preview_exchange_rates():
    con = duckdb.connect(DUCKDB_FILE)
    
    print("Previewing the latest FX data...\n")

    df = con.execute("""
        SELECT * 
        FROM exchange_rates
        ORDER BY timestamp DESC
    """).fetchdf()

    print(df)
    con.close()

if __name__ == "__main__":
    preview_exchange_rates()
