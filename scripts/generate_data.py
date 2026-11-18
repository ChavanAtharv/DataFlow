import argparse
import random
from pathlib import Path
import pandas as pd

FIRST_NAMES = [
    "Aarav", "Neha", "Rohan", "Priya", "Aditya", "Ananya", "Vikram", "Sneha",
    "Rahul", "Pooja", "Amit", "Kavita", "Suresh", "Meera", "Deepak", "Tanvi",
    "Arjun", "Isha", "Manish", "Divya", "Karan", "Rhea", "Nikhil", "Shreya"
]

LAST_NAMES = [
    "Sharma", "Patil", "Deshmukh", "Joshi", "Kulkarni", "Verma", "Chavan",
    "Gupta", "Mehta", "Bose", "Nair", "Reddy", "Iyer", "Rao", "Singh"
]

CITIES = [
    "Pune", "Mumbai", "Nashik", "Delhi", "Bengaluru",
    "Hyderabad", "Chennai", "Kolkata", "Ahmedabad", "Jaipur"
]

DOMAINS = ["example.com", "mail.com", "datalake.org", "test.net"]


def generate_customers(
    count: int = 100,
    dirty_ratio: float = 0.05,
    output_path: str = "data/raw/customers.csv",
):
    """
    Generate synthetic customer records with optional realistic dirty/corrupt data
    to exercise the DataFlow ingestion, quality checks, and quarantine engine.
    """
    records = []

    dirty_count = int(count * dirty_ratio)
    clean_count = count - dirty_count

    print(f"Generating {count} customer records ({clean_count} clean, {dirty_count} dirty)...")

    # 1. Clean records
    for i in range(1, clean_count + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{random.choice(DOMAINS)}"
        age = random.randint(18, 65)
        city = random.choice(CITIES)

        # Introduce realistic casing and whitespace quirks
        if random.random() < 0.2:
            name = f"  {name}  "
        if random.random() < 0.2:
            email = email.upper()
        if random.random() < 0.2:
            city = f" {city} "

        records.append({
            "customer_id": i,
            "name": name,
            "email": email,
            "age": age,
            "city": city,
        })

    # 2. Corrupt / dirty records (for quarantine engine)
    for j in range(dirty_count):
        corruption_type = random.choice(["null_field", "duplicate_id", "invalid_age"])
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)

        if corruption_type == "null_field":
            null_target = random.choice(["name", "email", "city"])
            rec = {
                "customer_id": clean_count + j + 1,
                "name": None if null_target == "name" else f"{first} {last}",
                "email": None if null_target == "email" else f"{first.lower()}@{random.choice(DOMAINS)}",
                "age": random.randint(18, 60),
                "city": None if null_target == "city" else random.choice(CITIES),
            }
        elif corruption_type == "duplicate_id":
            # Re-use an existing customer_id from clean records
            dup_id = random.randint(1, max(1, clean_count))
            rec = {
                "customer_id": dup_id,
                "name": f"{first} {last} (Duplicate)",
                "email": f"dup_{first.lower()}@{random.choice(DOMAINS)}",
                "age": random.randint(18, 60),
                "city": random.choice(CITIES),
            }
        else:  # invalid_age
            bad_age = random.choice([-15, -1, 135, 180, 250])
            rec = {
                "customer_id": clean_count + j + 1,
                "name": f"{first} {last}",
                "email": f"{first.lower()}@{random.choice(DOMAINS)}",
                "age": bad_age,
                "city": random.choice(CITIES),
            }

        records.append(rec)

    # Shuffle rows so dirty records are interspersed
    random.shuffle(records)

    df = pd.DataFrame(records)
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False)

    print(f"Successfully generated {len(df)} records to: {out_file}")
    return df


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic customer dataset for DataFlow"
    )
    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=100,
        help="Total number of records to generate (default: 100)",
    )
    parser.add_argument(
        "--dirty-ratio",
        "-d",
        type=float,
        default=0.08,
        help="Proportion of dirty records (nulls, duplicates, invalid ages) to inject (default: 0.08)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="data/raw/customers.csv",
        help="Output CSV path (default: data/raw/customers.csv)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    generate_customers(
        count=args.count,
        dirty_ratio=args.dirty_ratio,
        output_path=args.output,
    )

