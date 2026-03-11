import sqlite3
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)


def get_db(db_path="workbench.db"):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_database(db_path="workbench.db"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            country TEXT,
            segment TEXT,
            created_at TEXT,
            is_active INTEGER
        );

        CREATE TABLE IF NOT EXISTS dim_product (
            product_id TEXT PRIMARY KEY,
            product_name TEXT,
            category TEXT,
            subcategory TEXT,
            brand TEXT,
            unit_price REAL,
            cost REAL,
            sku TEXT
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id TEXT PRIMARY KEY,
            full_date TEXT,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name TEXT,
            week INTEGER,
            day_of_week TEXT,
            is_weekend INTEGER
        );

        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            product_id TEXT,
            date_id TEXT,
            quantity INTEGER,
            unit_price REAL,
            discount REAL,
            revenue REAL,
            status TEXT,
            channel TEXT,
            region TEXT
        );

        CREATE TABLE IF NOT EXISTS bronze_raw_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_json TEXT,
            source TEXT,
            ingested_at TEXT,
            batch_id TEXT
        );

        CREATE TABLE IF NOT EXISTS silver_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            product_id TEXT,
            order_date TEXT,
            quantity INTEGER,
            unit_price REAL,
            revenue REAL,
            status TEXT,
            validated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS gold_daily_revenue (
            date TEXT,
            category TEXT,
            region TEXT,
            total_revenue REAL,
            order_count INTEGER,
            avg_order_value REAL,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            pipeline_name TEXT,
            status TEXT,
            started_at TEXT,
            completed_at TEXT,
            duration_seconds REAL,
            rows_processed INTEGER,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id TEXT PRIMARY KEY,
            rule_name TEXT,
            metric TEXT,
            operator TEXT,
            threshold REAL,
            severity TEXT,
            enabled INTEGER,
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS audit_log (
            log_id TEXT PRIMARY KEY,
            user_email TEXT,
            action TEXT,
            resource TEXT,
            resource_type TEXT,
            ip_address TEXT,
            timestamp TEXT
        );
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dim_customer")
    if cur.fetchone()[0] > 0:
        conn.close()
        return

    segments = ["Enterprise", "SMB", "Consumer", "Government"]
    countries = ["United States", "United Kingdom", "Germany", "France", "Canada", "Australia"]
    cities = ["New York", "London", "Berlin", "Paris", "Toronto", "Sydney", "Chicago", "Los Angeles"]

    customers = []
    for i in range(500):
        cid = f"CUST-{i+1:05d}"
        customers.append((
            cid,
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.phone_number()[:20],
            random.choice(cities),
            random.choice(countries),
            random.choice(segments),
            (datetime.now() - timedelta(days=random.randint(30, 1000))).isoformat(),
            random.choice([1, 1, 1, 0])
        ))
    cur.executemany("INSERT OR IGNORE INTO dim_customer VALUES (?,?,?,?,?,?,?,?,?,?)", customers)

    categories = [
        ("Electronics", "Laptops"), ("Electronics", "Phones"), ("Electronics", "Tablets"),
        ("Clothing", "Men's"), ("Clothing", "Women's"), ("Clothing", "Kids"),
        ("Home & Garden", "Furniture"), ("Home & Garden", "Kitchen"), ("Home & Garden", "Tools"),
        ("Sports", "Fitness"), ("Sports", "Outdoor"), ("Books", "Technical"),
    ]
    brands = ["TechPro", "StyleCo", "HomeBase", "ActiveGear", "KnowledgePlus"]
    products = []
    for i in range(200):
        pid = f"PROD-{i+1:04d}"
        cat, subcat = random.choice(categories)
        price = round(random.uniform(9.99, 999.99), 2)
        products.append((
            pid,
            fake.catch_phrase()[:50],
            cat,
            subcat,
            random.choice(brands),
            price,
            round(price * random.uniform(0.4, 0.7), 2),
            f"SKU-{fake.bothify('??####')}"
        ))
    cur.executemany("INSERT OR IGNORE INTO dim_product VALUES (?,?,?,?,?,?,?,?)", products)

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    dow = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    dates = []
    base = datetime(2023, 1, 1)
    for i in range(730):
        d = base + timedelta(days=i)
        dates.append((
            d.strftime("%Y%m%d"),
            d.strftime("%Y-%m-%d"),
            d.year,
            (d.month - 1) // 3 + 1,
            d.month,
            month_names[d.month - 1],
            d.isocalendar()[1],
            dow[d.weekday()],
            1 if d.weekday() >= 5 else 0
        ))
    cur.executemany("INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?,?,?)", dates)

    statuses = ["completed", "completed", "completed", "shipped", "processing", "cancelled", "refunded"]
    channels = ["web", "mobile", "marketplace", "wholesale"]
    regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
    orders = []
    for i in range(5000):
        oid = f"ORD-{i+1:06d}"
        cid = f"CUST-{random.randint(1, 500):05d}"
        pid = f"PROD-{random.randint(1, 200):04d}"
        d = base + timedelta(days=random.randint(0, 729))
        qty = random.randint(1, 10)
        price = round(random.uniform(9.99, 499.99), 2)
        disc = round(random.uniform(0, 0.3), 2)
        rev = round(qty * price * (1 - disc), 2)
        orders.append((
            oid, cid, pid, d.strftime("%Y%m%d"), qty, price, disc, rev,
            random.choice(statuses), random.choice(channels), random.choice(regions)
        ))
    cur.executemany("INSERT OR IGNORE INTO fact_orders VALUES (?,?,?,?,?,?,?,?,?,?,?)", orders)

    pipeline_names = ["ingest_orders", "transform_customers", "build_gold_layer", "sync_catalog", "run_quality_checks"]
    statuses_p = ["success", "success", "success", "success", "failed", "running"]
    runs = []
    for i in range(100):
        rid = f"RUN-{i+1:04d}"
        pname = random.choice(pipeline_names)
        st = datetime.now() - timedelta(hours=random.randint(1, 168))
        dur = round(random.uniform(10, 450), 1)
        status = random.choice(statuses_p)
        runs.append((
            rid, pname, status,
            st.isoformat(),
            (st + timedelta(seconds=dur)).isoformat() if status != "running" else None,
            dur if status != "running" else None,
            random.randint(1000, 100000),
            "Connection timeout after 30s" if status == "failed" else None
        ))
    cur.executemany("INSERT OR IGNORE INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)", runs)

    rules = [
        ("RULE-001", "Pipeline Failure Alert", "pipeline_failure_rate", ">", 0.1, "critical", 1),
        ("RULE-002", "High Null Rate", "null_rate", ">", 0.05, "warning", 1),
        ("RULE-003", "Low Data Volume", "row_count", "<", 1000, "warning", 1),
        ("RULE-004", "Long Pipeline Duration", "duration_seconds", ">", 600, "info", 1),
        ("RULE-005", "Freshness SLA Breach", "hours_since_update", ">", 6, "critical", 1),
    ]
    for r in rules:
        cur.execute(
            "INSERT OR IGNORE INTO alert_rules VALUES (?,?,?,?,?,?,?,?)",
            r + (datetime.now().isoformat(),)
        )

    actions = ["SELECT", "SELECT", "SELECT", "INSERT", "UPDATE", "DELETE", "EXPORT", "DOWNLOAD"]
    resources = ["fact_orders", "dim_customer", "gold_daily_revenue", "silver_orders", "bronze_raw_orders"]
    users = ["alice@corp.com", "bob@corp.com", "charlie@corp.com", "diana@corp.com", "eve@corp.com"]
    for i in range(200):
        cur.execute(
            "INSERT OR IGNORE INTO audit_log VALUES (?,?,?,?,?,?,?)",
            (
                f"LOG-{i+1:04d}",
                random.choice(users),
                random.choice(actions),
                random.choice(resources),
                "table",
                fake.ipv4(),
                (datetime.now() - timedelta(hours=random.randint(0, 720))).isoformat()
            )
        )

    for i in range(50):
        d = base + timedelta(days=random.randint(0, 729))
        cat = random.choice([c[0] for c in categories])
        reg = random.choice(regions)
        rev = round(random.uniform(5000, 50000), 2)
        cnt = random.randint(50, 500)
        cur.execute(
            "INSERT OR IGNORE INTO gold_daily_revenue VALUES (?,?,?,?,?,?,?)",
            (d.strftime("%Y-%m-%d"), cat, reg, rev, cnt, round(rev / cnt, 2), datetime.now().isoformat())
        )

    conn.commit()
    conn.close()
