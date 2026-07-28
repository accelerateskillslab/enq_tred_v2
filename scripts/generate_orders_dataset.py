import csv
import random
import argparse
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)

ROW_COUNT = 500
### Set DUMP_FORMAT TO EITHER "csv" OR "parquet" BASED ON YOUR PREFERENCE
DUMP_FORMAT = "csv"
OUTPUT_DIR = Path("data")

CITIES = ["Kolkata", "Mumbai", "Bangalore", "Delhi", "Hyderabad", "Pune", "Chennai"]
CATEGORIES = ["Electronics", "Fashion", "Grocery", "Home", "Beauty", "Sports"]
ORDER_STATUS = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_STATUS = ["PAID", "PENDING", "FAILED", "REFUNDED"]


def random_date(start_date: datetime, days: int) -> datetime:
    return start_date + timedelta(days=random.randint(0, days))


def generate_orders(row_count: int):
    rows = []
    start_date = datetime(2026, 1, 1)

    for i in range(1, row_count + 1):
        order_id = 100000 + i
        customer_id = f"C{random.randint(1, 300):04d}"
        order_date = random_date(start_date, 60)

        order_status = random.choice(["PLACED", "SHIPPED", "DELIVERED"])
        payment_status = random.choice(["PAID", "PENDING"])

        rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "product_category": random.choice(CATEGORIES),
            "city": random.choice(CITIES),
            "order_amount": round(random.uniform(300, 75000), 2),
            "order_status": order_status,
            "payment_status": payment_status,
            "updated_at": (order_date + timedelta(hours=random.randint(1, 48))).strftime("%Y-%m-%d %H:%M:%S")
        })

    return rows


def write_csv(path: Path, rows):
    if not rows:
        return

    with open(path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def write_parquet(path: Path, rows):
    if not rows:
        return

    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("Parquet output requires pandas and pyarrow. Install them with: pip install pandas pyarrow") from exc

    try:
        pd.DataFrame(rows).to_parquet(path, index=False)
    except ImportError as exc:
        raise RuntimeError("Parquet output requires pandas and pyarrow. Install them with: pip install pandas pyarrow") from exc


def write_dataset(output_dir: Path, file_name: str, rows, dump_format: str):
    if dump_format == "csv":
        write_csv(output_dir / f"{file_name}.csv", rows)
        return

    if dump_format == "parquet":
        write_parquet(output_dir / f"{file_name}.parquet", rows)
        return

    raise ValueError("dump_format must be either 'csv' or 'parquet'")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data for Iceberg hands-on labs.")
    parser.add_argument("--output-dir", default=OUTPUT_DIR, type=Path, help="Location where data files should be created.")
    parser.add_argument("--dump-format", choices=["csv", "parquet"], default=DUMP_FORMAT, help="Output file format.")
    parser.add_argument("--row-count", type=int, default=ROW_COUNT, help="Number of order rows.")

    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    dump_format = args.dump_format.lower()

    orders = generate_orders(args.row_count)
    write_dataset(output_dir, "orders", orders, dump_format)

    print(f"{dump_format.upper()} file generated at: {output_dir.resolve() / f'orders.{dump_format}'}")


if __name__ == "__main__":
    main()
