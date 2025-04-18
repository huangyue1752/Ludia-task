import duckdb
import sys
import os
from datetime import datetime, timedelta

DUCKDB_FILE = os.path.join("data", "fx_data_bronze.duckdb")

def run_quality_checks():
    con = duckdb.connect(DUCKDB_FILE)
    issues = []

    print("Running quality checks on exchange_rates...\n")

    # 1. Total row count
    total = con.execute("SELECT COUNT(*) FROM exchange_rates").fetchone()[0]
    print(f"Total rows: {total}")

    # 2. Null values check
    nulls = con.execute("""
        SELECT 
            COUNT(*) FILTER (WHERE currency IS NULL) AS null_currency,
            COUNT(*) FILTER (WHERE rate IS NULL) AS null_rate,
            COUNT(*) FILTER (WHERE timestamp IS NULL) AS null_timestamp,
            COUNT(*) FILTER (WHERE name IS NULL) AS null_currency_name
        FROM exchange_rates
    """).fetchone()

    print("\nNull values:")
    print(f"currency: {nulls[0]}, rate: {nulls[1]}, timestamp: {nulls[2]}, name: {nulls[3]}")
    if any(val > 0 for val in nulls):
        issues.append("Null values found in one or more columns.")

    # 3. Duplicate check (by currency + timestamp)
    duplicate_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT currency, timestamp, COUNT(*) AS cnt
            FROM exchange_rates
            GROUP BY currency, timestamp
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    print(f"\nDuplicate rows (currency + timestamp): {duplicate_count}")
    if duplicate_count > 0:
        issues.append("Duplicate rows detected.")

    # 4. Missing currency names
    missing_names = con.execute("""
        SELECT COUNT(*) FROM exchange_rates WHERE name IS NULL
    """).fetchone()[0]
    print(f"\nMissing currency names: {missing_names}")
    if missing_names > 0:
        issues.append("Missing currency names found.")

    # 5. Negative rate check
    negative_rates = con.execute("""
        SELECT COUNT(*) FROM exchange_rates WHERE rate < 0
    """).fetchone()[0]
    print(f"\nNegative exchange rates found: {negative_rates}")
    if negative_rates > 0:
        issues.append("Negative FX rates detected.")

    # 6. Date continuity check
    latest_dates = con.execute("""
        SELECT DISTINCT CAST(timestamp AS DATE) as ts
        FROM exchange_rates
        ORDER BY ts DESC
        LIMIT 2
    """).fetchall()

    if len(latest_dates) < 2:
        issues.append("Not enough distinct dates to perform date continuity check.")
    else:
        latest_date = latest_dates[0][0]
        second_latest_date = latest_dates[1][0]
        today = datetime.today().date()

        print(f"\nLatest date in DB: {latest_date}")
        print(f"Second latest date in DB: {second_latest_date}")
        print(f"Today's date: {today}")

        if latest_date != today:
            issues.append(f"Latest data is not from today. Found: {latest_date}")
        if (latest_date - second_latest_date).days != 1:
            issues.append("Latest and second latest timestamps are not consecutive.")

    con.close()

    # Raise error if any issues were found
    if issues:
        print("\nQUALITY CHECK FAILED:")
        for issue in issues:
            print(f"- {issue}")
        sys.exit(1)
    else:
        print("\nALL QUALITY CHECKS PASSED.")

if __name__ == "__main__":
    run_quality_checks()
