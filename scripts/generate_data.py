"""
generate_data.py
----------------
Generates synthetic customers, products and orders with Faker, then
deliberately injects data quality problems for Spark to clean.

Output (data/raw/):
    customers.csv        - CSV
    products.json        - JSON Lines (one JSON object per line)
    orders.csv           - CSV
    generation_summary.json - how many of each problem were injected

Run inside Docker (this is what the Airflow task does):
    python /opt/airflow/scripts/generate_data.py

Settings (environment variables):
    NUM_RECORDS   clean records per entity (default 100000; use 1000 for quick tests)
    DATA_DIR      base data folder (default /opt/airflow/data)

The 7 data quality issues from Section 2.1.2 of the brief:
    1. Duplicate records      - exact copies, and near-copies of the same ID
    2. Null values            - missing emails, product names, order amounts, IDs
    3. Inconsistent formats   - "new york" / "New York" / "NEW YORK"
    4. Invalid values         - negative amounts, discount > total, future dates
    5. Orphan records         - orders pointing to customers that don't exist
    6. Whitespace issues      - leading/trailing spaces in text fields
    7. Date format mix        - "2024-01-15", "01/15/2024", "Jan 15, 2024"
Plus a few corrupt (unreadable) lines in every file, for Spark's
PERMISSIVE / DROPMALFORMED read modes.
"""

import csv
import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
NUM_RECORDS = int(os.environ.get("NUM_RECORDS", "100000"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw"
SEED = 42

# How often each problem is injected (fraction of NUM_RECORDS)
RATES = {
    "exact_duplicates": 0.02,
    "near_duplicates": 0.03,
    "null_fields": 0.03,
    "null_primary_keys": 0.005,
    "whitespace": 0.05,
    "negative_amount": 0.01,
    "discount_gt_total": 0.01,
    "orphan_orders": 0.02,
    "future_order_dates": 0.005,
}
CORRUPT_LINES_PER_FILE = 10

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

TODAY = datetime(2026, 11, 1)
CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books",
              "Sports", "Toys", "Beauty", "Grocery"]
CUSTOMER_TYPES = ["Regular", "Premium", "VIP"]
ORDER_STATUSES = ["Pending", "Shipped", "Delivered"]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]

# A fixed pool of cities, so city-level analytics have meaningful groups
CITIES = sorted({fake.city() for _ in range(400)})[:200]
BRANDS = sorted({fake.last_name() for _ in range(300)})[:150]


# ---------------------------------------------------------------------------
# Date formatting - issue 7 (mixed formats)
# ---------------------------------------------------------------------------
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y"]                       # 2024-01-15 | 01/15/2024 | Jan 15, 2024
DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%b %d, %Y %H:%M:%S"]


def pick_format():
    """Each record gets one date style: ISO 60% of the time, the other two 20% each."""
    return random.choices([0, 1, 2], weights=[60, 20, 20])[0]


def random_date(start_days_ago, end_days_ago=0):
    return TODAY - timedelta(days=random.randint(end_days_ago, start_days_ago),
                             seconds=random.randint(0, 86399))


# ---------------------------------------------------------------------------
# Clean base records
# ---------------------------------------------------------------------------
def make_customers(n):
    rows = []
    for i in range(1, n + 1):
        first, last = fake.first_name(), fake.last_name()
        rows.append({
            "customer_id": i,
            "_fmt": pick_format(),
            "first_name": first,
            "last_name": last,
            # name + id guarantees every email is unique
            "email": f"{first}.{last}{i}@{fake.free_email_domain()}".lower(),
            "phone": fake.numerify("###-###-####"),
            "city": random.choice(CITIES),
            "registration_date": random_date(5 * 365).date(),
            "customer_type": random.choices(CUSTOMER_TYPES, weights=[70, 20, 10])[0],
            "last_updated": random_date(365, 30),
        })
    return rows


def make_products(n):
    rows = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        rows.append({
            "product_id": i,
            "_fmt": pick_format(),
            "product_name": f"{fake.color_name()} {fake.word().capitalize()} {category.split()[0]}",
            "category": category,
            "brand": random.choice(BRANDS),
            "unit_price": round(random.uniform(1.99, 499.99), 2),
            "stock_quantity": random.randint(0, 500),
            "is_active": random.random() < 0.9,
            "last_updated": random_date(365, 30),
        })
    return rows


def make_orders(n, n_customers, products):
    rows = []
    for i in range(1, n + 1):
        product = products[random.randrange(len(products))]
        quantity = random.randint(1, 5)
        total = round(product["unit_price"] * quantity, 2)
        rows.append({
            "order_id": i,
            "_fmt": pick_format(),
            "customer_id": random.randint(1, n_customers),   # always a real customer
            "product_id": product["product_id"],
            "quantity": quantity,
            "order_date": random_date(2 * 365),
            "order_status": random.choice(ORDER_STATUSES),
            "payment_method": random.choice(PAYMENT_METHODS),
            "total_amount": total,
            "discount_amount": round(total * random.choice([0, 0, 0.05, 0.10, 0.15]), 2),
            "shipping_cost": random.choice([0, 4.99, 9.99, 14.99]),
            "last_updated": random_date(30),
        })
    return rows


# ---------------------------------------------------------------------------
# Problem injection
# ---------------------------------------------------------------------------
def pick(rows, rate):
    """Random sample of rows (as indices) of size rate * NUM_RECORDS."""
    k = max(1, int(NUM_RECORDS * rate))
    return random.sample(range(len(rows)), min(k, len(rows)))


def vary_case(text):
    return random.choice([text.lower(), text.upper(), text.title()])


def add_spaces(text):
    return random.choice([" ", "  ", "\t"]) + text + random.choice(["", " ", "   "])


def inject_common(rows, pk, text_fields, nullable_fields, counts):
    """Duplicates, nulls, null primary keys, whitespace - issues 1, 2, 6."""
    # Issue 2: null values in non-key fields
    for i in pick(rows, RATES["null_fields"]):
        rows[i][random.choice(nullable_fields)] = None
    counts["null_fields"] = int(NUM_RECORDS * RATES["null_fields"])

    # Issue 6: leading/trailing whitespace
    for i in pick(rows, RATES["whitespace"]):
        field = random.choice(text_fields)
        if rows[i][field]:
            rows[i][field] = add_spaces(str(rows[i][field]))
    counts["whitespace"] = int(NUM_RECORDS * RATES["whitespace"])

    extra = []
    # Issue 1a: exact duplicates (identical row twice)
    for i in pick(rows, RATES["exact_duplicates"]):
        extra.append(dict(rows[i]))
    counts["exact_duplicates"] = len(extra)

    # Issue 1b: near-duplicates - same ID, slightly different data, newer timestamp
    near = 0
    for i in pick(rows, RATES["near_duplicates"]):
        dup = dict(rows[i])
        field = random.choice(text_fields)
        if dup[field]:
            dup[field] = random.choice([vary_case(str(dup[field]).strip()),
                                        add_spaces(str(dup[field]).strip())])
        dup["last_updated"] = dup["last_updated"] + timedelta(days=random.randint(1, 20))
        extra.append(dup)
        near += 1
    counts["near_duplicates"] = near

    # Issue 2: rows with a missing primary key (unusable, must be dropped)
    junk = 0
    for i in pick(rows, RATES["null_primary_keys"]):
        bad = dict(rows[i])
        bad[pk] = None
        extra.append(bad)
        junk += 1
    counts["null_primary_keys"] = junk

    rows.extend(extra)


def inject_customer_issues(rows):
    counts = {}
    # Issue 3: inconsistent case in city names and customer type
    for r in rows:
        if random.random() < 0.3:
            r["city"] = vary_case(r["city"])
        if random.random() < 0.1:
            r["customer_type"] = vary_case(r["customer_type"])
        if random.random() < 0.1:
            r["email"] = r["email"].upper()
    counts["mixed_case_rows"] = "~30% of cities, ~10% of types and emails"
    inject_common(rows, "customer_id",
                  text_fields=["first_name", "last_name", "email", "city", "phone"],
                  nullable_fields=["email", "first_name", "last_name", "city", "phone", "customer_type"],
                  counts=counts)
    return counts


def inject_product_issues(rows):
    counts = {}
    for r in rows:
        if random.random() < 0.1:
            r["category"] = vary_case(r["category"])
    inject_common(rows, "product_id",
                  text_fields=["product_name", "category", "brand"],
                  nullable_fields=["product_name", "brand", "stock_quantity", "category"],
                  counts=counts)
    return counts


def inject_order_issues(rows, n_customers):
    counts = {}
    # Issue 4: negative total_amount
    for i in pick(rows, RATES["negative_amount"]):
        rows[i]["total_amount"] = -abs(rows[i]["total_amount"])
    counts["negative_amount"] = int(NUM_RECORDS * RATES["negative_amount"])

    # Issue 4: discount larger than the total
    for i in pick(rows, RATES["discount_gt_total"]):
        rows[i]["discount_amount"] = round(abs(rows[i]["total_amount"]) * random.uniform(1.1, 2.0), 2)
    counts["discount_gt_total"] = int(NUM_RECORDS * RATES["discount_gt_total"])

    # Issue 4: impossible dates (in the future)
    for i in pick(rows, RATES["future_order_dates"]):
        rows[i]["order_date"] = TODAY + timedelta(days=random.randint(30, 3000))
    counts["future_order_dates"] = int(NUM_RECORDS * RATES["future_order_dates"])

    # Issue 5: orphan orders - customer IDs that don't exist
    for i in pick(rows, RATES["orphan_orders"]):
        rows[i]["customer_id"] = random.randint(n_customers + 1, n_customers * 2)
    counts["orphan_orders"] = int(NUM_RECORDS * RATES["orphan_orders"])

    # Inconsistent case in order status
    for r in rows:
        if random.random() < 0.1:
            r["order_status"] = vary_case(r["order_status"])

    inject_common(rows, "order_id",
                  text_fields=["order_status", "payment_method"],
                  nullable_fields=["total_amount", "payment_method", "order_status", "shipping_cost"],
                  counts=counts)
    return counts


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------
def to_text(row):
    """Format dates/timestamps as strings, in the record's own date style.
    Copies of a record share its style, so exact duplicates stay identical."""
    style = row.get("_fmt", 0)
    out = {}
    for key, value in row.items():
        if key == "_fmt":
            continue
        if isinstance(value, datetime):
            out[key] = value.strftime(DATETIME_FORMATS[style])
        elif hasattr(value, "strftime"):
            out[key] = value.strftime(DATE_FORMATS[style])
        else:
            out[key] = value
    return out


def write_csv(rows, path):
    random.shuffle(rows)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[k for k in rows[0] if k != "_fmt"])
        writer.writeheader()
        for r in rows:
            writer.writerow(to_text(r))
        # Corrupt lines: wrong number of fields / garbage
        for _ in range(CORRUPT_LINES_PER_FILE):
            f.write(random.choice(["###CORRUPTED RECORD###\n",
                                   "abc,def\n",
                                   "12345,\"unterminated quote,xyz\n"]))


def write_jsonl(rows, path):
    random.shuffle(rows)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(to_text(r)) + "\n")
        # Corrupt lines: broken JSON
        for _ in range(CORRUPT_LINES_PER_FILE):
            f.write(random.choice(['{"product_id": 999999, "product_name": "broken\n',
                                   "not json at all\n",
                                   '{"product_id": }\n']))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    start = time.time()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Generating {NUM_RECORDS:,} records per entity into {RAW_DIR}")

    customers = make_customers(NUM_RECORDS)
    products = make_products(NUM_RECORDS)
    orders = make_orders(NUM_RECORDS, NUM_RECORDS, products)
    print(f"  clean base records created ({time.time() - start:.1f}s)")

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "num_records": NUM_RECORDS,
        "seed": SEED,
        "customers": inject_customer_issues(customers),
        "products": inject_product_issues(products),
        "orders": inject_order_issues(orders, NUM_RECORDS),
    }

    write_csv(customers, RAW_DIR / "customers.csv")
    write_jsonl(products, RAW_DIR / "products.json")
    write_csv(orders, RAW_DIR / "orders.csv")

    for entity, rows in [("customers", customers), ("products", products), ("orders", orders)]:
        summary[entity]["raw_rows_written"] = len(rows)
        summary[entity]["corrupt_lines"] = CORRUPT_LINES_PER_FILE
        print(f"  {entity}: {len(rows):,} rows + {CORRUPT_LINES_PER_FILE} corrupt lines")

    with open(RAW_DIR / "generation_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"Done in {time.time() - start:.1f}s. Injected issues: {RAW_DIR / 'generation_summary.json'}")


if __name__ == "__main__":
    main()
