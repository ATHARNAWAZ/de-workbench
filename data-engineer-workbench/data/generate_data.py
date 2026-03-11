"""
Sample data generator for the Senior Data Engineer Workbench.
Run this script to regenerate all sample datasets.
"""
import csv
import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

def generate_orders(n=1000, output="orders.csv"):
    statuses = ["completed", "shipped", "processing", "cancelled", "refunded"]
    channels = ["web", "mobile", "marketplace", "wholesale"]
    regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]

    with open(output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "product_id", "order_date", "category",
                         "quantity", "unit_price", "discount", "revenue", "status", "channel", "region"])
        for i in range(n):
            qty = random.randint(1, 10)
            price = round(random.uniform(9.99, 499.99), 2)
            disc = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3]), 2)
            rev = round(qty * price * (1 - disc), 2)
            writer.writerow([
                f"ORD-{i+1:06d}",
                f"CUST-{random.randint(1, 200):05d}",
                f"PROD-{random.randint(1, 100):04d}",
                (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                random.choice(categories),
                qty, price, disc, rev,
                random.choice(statuses),
                random.choice(channels),
                random.choice(regions),
            ])
    print(f"Generated {n} orders to {output}")


def generate_user_events(n=500, output="user_events.json"):
    event_types = ["page_view", "product_view", "add_to_cart", "checkout_start",
                   "purchase", "search", "review_submit", "wishlist_add"]
    pages = ["/", "/products", "/cart", "/checkout", "/orders", "/search", "/profile"]
    devices = ["desktop", "mobile", "tablet"]

    events = []
    for i in range(n):
        event_type = random.choice(event_types)
        ts = datetime.now() - timedelta(hours=random.randint(0, 168))
        events.append({
            "event_id": fake.uuid4(),
            "user_id": f"USR-{random.randint(1, 500):05d}",
            "session_id": fake.uuid4()[:8],
            "event_type": event_type,
            "timestamp": ts.isoformat(),
            "page": random.choice(pages),
            "device": random.choice(devices),
            "properties": {
                "product_id": f"PROD-{random.randint(1, 100):04d}" if event_type in ["product_view", "add_to_cart", "purchase"] else None,
                "search_query": fake.word() if event_type == "search" else None,
                "revenue": round(random.uniform(9.99, 499.99), 2) if event_type == "purchase" else None,
            }
        })

    with open(output, "w") as f:
        json.dump(events, f, indent=2, default=str)
    print(f"Generated {n} events to {output}")


def generate_sensor_readings(n=500, output="sensor_readings.csv"):
    locations = ["Factory-A", "Factory-B", "Warehouse-1", "Warehouse-2", "Office-HQ"]
    sensor_ids = [f"SENSOR-{i:03d}" for i in range(1, 21)]

    with open(output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_id", "location", "timestamp", "temperature_c",
                         "humidity_pct", "pressure_hpa", "battery_pct", "alert"])
        for i in range(n):
            sensor = random.choice(sensor_ids)
            temp = round(random.gauss(22, 5), 2)
            humid = round(random.uniform(30, 80), 1)
            pressure = round(random.gauss(1013, 10), 1)
            battery = round(random.uniform(20, 100), 1)
            alert = temp > 35 or temp < 5 or humid > 75 or battery < 25
            writer.writerow([
                sensor,
                random.choice(locations),
                (datetime.now() - timedelta(minutes=random.randint(0, 10080))).isoformat(),
                temp, humid, pressure, battery,
                "CRITICAL" if alert else "OK",
            ])
    print(f"Generated {n} sensor readings to {output}")


if __name__ == "__main__":
    generate_orders(1000)
    generate_user_events(500)
    generate_sensor_readings(500)
    print("All sample data generated successfully!")
