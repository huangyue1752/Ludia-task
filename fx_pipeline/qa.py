import duckdb
import pandas as pd
import sys
import os

SILVER_DB = os.path.join("data", "fx_data_bronze.duckdb")

def run_quality_checks():
    con = duckdb.connect(SILVER_DB)
    issues = []

    print("🧪 Running quality checks on exchange_rates...\n")

    # 1. Total row count
    total = con.execute("SELECT COUNT(*) FROM exchange_rates").fetchone()[0]
    print(f"✅ Total rows: {total}")

    # 2. Null values check
    nulls = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE currency IS NULL) AS null_currency,
            COUNT(*) FILTER (WHERE rate IS NULL) AS null_rate,
            COUNT(*) FILTER (WHERE timestamp IS NULL) AS null_timestamp,
            COUNT(*) FILTER (WHERE name IS NULL) AS null_currency_name
        FROM exchange_rates
    """).fetchone()

    print("\n🚨 Null values:")
    print(f"currency: {nulls[0]}, rate: {nulls[1]}, timestamp: {nulls[2]}, name: {nulls[3]}")
    if any(val > 0 for val in nulls):
        issues.append("❌ Null values found in one or more columns.")

    # 3. Duplicate check (by currency + timestamp)
    duplicate_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT currency, timestamp, COUNT(*) AS cnt
            FROM exchange_rates
            GROUP BY currency, timestamp
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    print(f"\n🔍 Duplicate rows (currency + timestamp): {duplicate_count}")
    if duplicate_count > 0:
        issues.append("❌ Duplicate rows detected.")

    # 4. Missing currency names
    missing_names = con.execute("""
        SELECT COUNT(*) FROM exchange_rates WHERE name IS NULL
    """).fetchone()[0]
    print(f"\n❓ Missing currency names: {missing_names}")
    if missing_names > 0:
        issues.append("❌ Missing currency names found.")

    # 5. Negative rate check
    negative_rates = con.execute("""
        SELECT COUNT(*) FROM exchange_rates WHERE rate < 0
    """).fetchone()[0]
    print(f"\n⚠️ Negative exchange rates found: {negative_rates}")
    if negative_rates > 0:
        issues.append("❌ Negative FX rates detected.")

    con.close()

    # Raise error if any issues were found
    if issues:
        print("\n❌ QUALITY CHECK FAILED:")
        for issue in issues:
            print(issue)
        sys.exit(1)  # Exit with error for CI/CD to catch
    else:
        print("\n✅ ALL QUALITY CHECKS PASSED.")

if __name__ == "__main__":
    run_quality_checks()
