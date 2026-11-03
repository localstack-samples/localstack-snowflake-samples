"""A deterministic, synthetic store-by-SKU inventory data set.

Six stores, 40 SKUs, one stock row per store and SKU, about 120 purchase orders
and a 35-day stock ledger. Everything comes from one seeded ``random.Random``,
so every run (and every test session) loads byte-identical data, and tests can
compute expected values from these rows instead of reading them back.

Two traits matter for the demo and are planted on purpose:

* ``BACKORDERED`` stock rows have ``ALLOCATED > ON_HAND`` (the store sold into
  backorder). The bad migration ``V1.3.0`` turns exactly these rows negative.
* Some legacy shelf addresses are lower case or carry stray spaces
  (``' a07-b12'``), so the ``V1.2.0`` backfill has real normalising to do.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from snowflake.connector import SnowflakeConnection

from release import settings as cfg
from release.deploy import deploy, staged
from release.settings import Settings

SEED = 20261024
LEDGER_START = datetime(2026, 9, 1, 8, 0, 0)
LEDGER_DAYS = 35
BACKORDERED = 9

STORES = [
    ("S01", "Portland Pearl District", "WEST", date(2014, 3, 1)),
    ("S02", "Seattle Capitol Hill", "WEST", date(2016, 5, 14)),
    ("S03", "Denver Union Station", "CENTRAL", date(2017, 9, 2)),
    ("S04", "Chicago Wicker Park", "CENTRAL", date(2015, 11, 20)),
    ("S05", "Brooklyn Williamsburg", "EAST", date(2018, 4, 7)),
    ("S06", "Boston Back Bay", "EAST", date(2019, 8, 30)),
]

CATALOG = {
    "PANTRY": ["Arborio Rice 1kg", "Rolled Oats 750g", "Basmati Rice 2kg", "Red Lentils 500g",
               "Penne Rigate 500g", "Chickpeas 400g", "Coconut Milk 400ml", "Tomato Passata 700g"],
    "BEVERAGE": ["Cold Brew Coffee 1l", "Sparkling Water 12x", "Green Tea 40ct", "Orange Juice 1.5l",
                 "Oat Milk 1l", "Kombucha Ginger 330ml", "Espresso Beans 1kg", "Mineral Water 6x"],
    "HOUSEHOLD": ["Dish Soap 750ml", "Laundry Pods 32ct", "Paper Towels 6x", "Trash Bags 40ct",
                  "Sponges 4x", "Glass Cleaner 500ml", "Toilet Paper 12x", "Aluminium Foil 30m"],
    "PERSONAL": ["Toothpaste 100ml", "Shampoo 400ml", "Body Wash 500ml", "Sunscreen SPF50",
                 "Hand Soap 300ml", "Deodorant 50ml", "Cotton Pads 80ct", "Lip Balm 4g"],
    "SNACKS": ["Sea Salt Crisps 150g", "Dark Chocolate 100g", "Trail Mix 400g", "Rice Cakes 130g",
               "Salted Almonds 200g", "Granola Bars 6x", "Popcorn 90g", "Pretzel Twists 250g"],
}  # fmt: skip


@dataclass(frozen=True)
class Dataset:
    stores: list[tuple]
    products: list[tuple]
    stock_levels: list[dict]
    purchase_orders: list[dict]
    stock_movements: list[dict]

    def counts(self) -> dict[str, int]:
        return {
            "STORES": len(self.stores),
            "PRODUCTS": len(self.products),
            "STOCK_LEVELS": len(self.stock_levels),
            "PURCHASE_ORDERS": len(self.purchase_orders),
            "STOCK_MOVEMENTS": len(self.stock_movements),
        }


def _location(rng: random.Random) -> str | None:
    roll = rng.random()
    if roll < 0.05:
        return None
    address = f"A{rng.randint(1, 14):02d}-B{rng.randint(1, 20):02d}"
    if roll < 0.10:
        return address.lower()
    if roll < 0.14:
        return f" {address} "
    return address


def generate() -> Dataset:
    rng = random.Random(SEED)
    products = []
    for category_no, (category, names) in enumerate(CATALOG.items()):
        for i, name in enumerate(names):
            sku = f"SKU-{10001 + category_no * 100 + i}"
            products.append((sku, name, category, round(rng.uniform(0.8, 24.0), 2)))

    pairs = [(store[0], product[0]) for store in STORES for product in products]
    backordered = set(rng.sample(pairs, BACKORDERED))
    stock_levels = []
    for store_id, sku in pairs:
        if (store_id, sku) in backordered:
            on_hand = rng.randint(0, 3)
            allocated = on_hand + rng.randint(1, 6)
        else:
            on_hand = rng.randint(4, 120)
            allocated = rng.randint(0, min(12, on_hand))
        stock_levels.append(
            {
                "STORE_ID": store_id,
                "SKU": sku,
                "ON_HAND": on_hand,
                "ALLOCATED": allocated,
                "LOCATION": _location(rng),
                "UPDATED_AT": LEDGER_START + timedelta(days=LEDGER_DAYS, minutes=rng.randint(0, 600)),
            }
        )

    purchase_orders = []
    movements = []
    for n in range(1, 121):
        store_id, sku = rng.choice(pairs)
        ordered_at = LEDGER_START + timedelta(days=rng.randint(0, LEDGER_DAYS - 8), minutes=rng.randint(0, 600))
        status = rng.choices(["RECEIVED", "OPEN", "CANCELLED"], weights=[6, 3, 1])[0]
        po = {
            "PO_ID": f"PO-{n:06d}",
            "STORE_ID": store_id,
            "SKU": sku,
            "ORDERED_QTY": rng.choice([12, 24, 36, 48, 60]),
            "STATUS": status,
            "ORDERED_AT": ordered_at,
            "EXPECTED_ON": (ordered_at + timedelta(days=rng.randint(4, 7))).date(),
        }
        purchase_orders.append(po)
        if status == "RECEIVED":
            received_at = ordered_at + timedelta(days=rng.randint(3, 6))
            movements.append((store_id, sku, "RECEIPT", po["ORDERED_QTY"], po["PO_ID"], received_at))

    for _ in range(1400):
        store_id, sku = rng.choice(pairs)
        moved_at = LEDGER_START + timedelta(days=rng.randint(0, LEDGER_DAYS - 1), minutes=rng.randint(0, 780))
        movements.append((store_id, sku, "SALE", -rng.randint(1, 6), None, moved_at))
    for _ in range(60):
        store_id, sku = rng.choice(pairs)
        moved_at = LEDGER_START + timedelta(days=rng.randint(0, LEDGER_DAYS - 1), minutes=rng.randint(0, 780))
        movements.append((store_id, sku, "ADJUSTMENT", rng.choice([-4, -3, -2, -1, 1, 2]), None, moved_at))

    movements.sort(key=lambda m: (m[5], m[0], m[1]))
    stock_movements = [
        dict(zip(("STORE_ID", "SKU", "MOVEMENT_TYPE", "QUANTITY", "PO_ID", "MOVED_AT"), m, strict=True))
        | {"MOVEMENT_ID": i}
        for i, m in enumerate(movements, start=1)
    ]
    return Dataset(STORES, products, stock_levels, purchase_orders, stock_movements)


BASELINE_SCRIPTS = ("migrations/V1.0.0__baseline.sql", "migrations/A__grants.sql")


def seed(settings: Settings, data: Dataset | None = None) -> Dataset:
    """(Re)create ``settings.database`` as production stood before this change set.

    Drops and recreates the database, bootstraps the live schema with the
    baseline migration only (plus the always-run grants), and loads the data
    set. The release under test (V1.1.0, V1.2.0, the new views) is left for
    ``release.py release``.
    """
    data = data or generate()
    with cfg.connect(settings) as conn:
        cur = conn.cursor()
        cur.execute(f"CREATE OR REPLACE DATABASE {settings.database}")
        cur.execute(f"CREATE SCHEMA {settings.database}.{settings.schema}")
        cur.execute(f"DROP SCHEMA IF EXISTS {settings.database}.PUBLIC")
    with staged(*BASELINE_SCRIPTS) as root:
        deploy(settings, settings.schema, root_folder=root)
    with cfg.connect(settings) as conn:
        load(conn, f"{settings.database}.{settings.schema}", data)
    return data


def _insert(conn: SnowflakeConnection, table: str, columns: list[str], rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(columns))
    conn.cursor().executemany(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})", rows)


def load(conn: SnowflakeConnection, schema: str, data: Dataset) -> None:
    """Insert the data set into the (empty) baseline tables of ``schema`` (``DB.SCHEMA``)."""
    _insert(conn, f"{schema}.STORES", ["STORE_ID", "STORE_NAME", "REGION", "OPENED_ON"], data.stores)
    _insert(conn, f"{schema}.PRODUCTS", ["SKU", "PRODUCT_NAME", "CATEGORY", "UNIT_COST"], data.products)
    for table, rows in (
        ("STOCK_LEVELS", data.stock_levels),
        ("PURCHASE_ORDERS", data.purchase_orders),
        ("STOCK_MOVEMENTS", data.stock_movements),
    ):
        columns = list(rows[0])
        _insert(conn, f"{schema}.{table}", columns, [tuple(r[c] for c in columns) for r in rows])
