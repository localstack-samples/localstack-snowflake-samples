"""Seeded synthetic data for Tenantboard: merchants, customers, orders and line items.

Everything is a pure function of (seed, merchant count), so the test suite can
regenerate the exact dataset it loaded and compute expected values from it
independently of the API and of Snowflake.

    uv run python -m scripts.seed               # load into TENANTBOARD (idempotent)
    uv run python -m scripts.seed --merchants 10
"""

from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from app import provisioning, tenancy
from app.db import connect_admin
from app.settings import get_settings

SEED = 42
MERCHANTS = 50
# Order timestamps fall in [WINDOW_START, WINDOW_START + WINDOW_DAYS).
WINDOW_START = datetime(2026, 9, 1)
WINDOW_DAYS = 30
# Volumes per merchant, by plan: (customers, orders). Small enough that the
# whole suite, one template plus a clone per worker, runs in about a minute.
VOLUME = {"starter": ((4, 8), (5, 9)), "growth": ((8, 14), (10, 16)), "enterprise": ((14, 22), (18, 28))}
STATUSES = (("completed", 82), ("refunded", 6), ("cancelled", 12))
CHANNELS = (("web", 55), ("mobile", 30), ("marketplace", 15))
PLANS = ("starter", "growth", "enterprise")
REGIONS = {"us-east": "USD", "eu-west": "EUR", "uk-south": "GBP"}

# The first merchants are fixed (slug, name, plan) so demos and tests can name them.
FIXED_MERCHANTS = (
    ("acme", "Acme Corporation", "enterprise"),
    ("globex", "Globex Corporation", "growth"),
    ("initech", "Initech", "starter"),
)
_ADJECTIVES = (
    "Amber",
    "Birch",
    "Blue",
    "Cedar",
    "Copper",
    "Coral",
    "Crimson",
    "Delta",
    "Ember",
    "Fable",
    "Granite",
    "Harbor",
    "Indigo",
    "Juniper",
    "Lumen",
    "Maple",
    "Meadow",
    "North",
    "Oak",
    "Onyx",
)
_NOUNS = (
    "Goods",
    "Supply",
    "Outfitters",
    "Market",
    "Works",
    "Trading",
    "Provisions",
    "Mercantile",
    "Collective",
    "Studio",
    "Depot",
    "Atelier",
)
_FIRST = (
    "ada",
    "ben",
    "chloe",
    "dev",
    "elena",
    "farid",
    "grace",
    "hiro",
    "ines",
    "jonas",
    "kai",
    "lena",
    "mateo",
    "nora",
    "omar",
    "priya",
    "quinn",
    "rosa",
    "sam",
    "tara",
    "uma",
    "viktor",
    "wen",
    "yara",
)
_LAST = (
    "adams",
    "baker",
    "chen",
    "diaz",
    "evans",
    "fischer",
    "garcia",
    "haddad",
    "ito",
    "jensen",
    "khan",
    "larsen",
    "moreau",
    "nakamura",
    "okafor",
    "patel",
    "rossi",
    "silva",
    "tanaka",
    "weber",
)
_COUNTRIES = ("US", "CA", "GB", "DE", "FR", "NL", "ES", "IT", "SE", "JP")


@dataclass(frozen=True)
class Dataset:
    merchants: pd.DataFrame
    customers: pd.DataFrame
    orders: pd.DataFrame
    line_items: pd.DataFrame

    @property
    def tenant_ids(self) -> list[str]:
        return self.merchants["TENANT_ID"].tolist()

    def frames(self) -> dict[str, pd.DataFrame]:
        return {
            "MERCHANTS": self.merchants,
            "CUSTOMERS": self.customers,
            "ORDERS": self.orders,
            "LINE_ITEMS": self.line_items,
        }


def tenant_id_for(slug: str) -> str:
    return slug.upper().replace("-", "_")


def _weighted(rng: random.Random, choices: tuple[tuple[str, int], ...]) -> str:
    return rng.choices([c for c, _ in choices], weights=[w for _, w in choices])[0]


def _merchants(rng: random.Random, count: int) -> list[tuple[str, str, str]]:
    merchants = list(FIXED_MERCHANTS[:count])
    combos = [(a, n) for a in _ADJECTIVES for n in _NOUNS]
    rng.shuffle(combos)
    for adjective, noun in combos[: max(0, count - len(merchants))]:
        merchants.append((f"{adjective}-{noun}".lower(), f"{adjective} {noun}", rng.choice(PLANS)))
    return merchants


def generate(seed: int = SEED, merchants: int = MERCHANTS) -> Dataset:
    if not 3 <= merchants <= len(_ADJECTIVES) * len(_NOUNS):
        raise ValueError("merchants must be between 3 and 240")
    rng = random.Random(seed)
    merchant_rows, customer_rows, order_rows, line_rows = [], [], [], []
    for index, (slug, name, plan) in enumerate(_merchants(rng, merchants)):
        tenant_id = tenancy.validate_tenant_id(tenant_id_for(slug))
        region = rng.choice(list(REGIONS))
        currency = REGIONS[region]
        created_at = datetime(2025, 1, 6) + timedelta(days=index * 5)
        merchant_rows.append((tenant_id, slug, name, plan, region, currency, created_at))

        (min_customers, max_customers), (min_orders, max_orders) = VOLUME[plan]
        customer_count = rng.randint(min_customers, max_customers)
        for customer_id in range(1, customer_count + 1):
            first, last = rng.choice(_FIRST), rng.choice(_LAST)
            customer_rows.append(
                (
                    tenant_id,
                    customer_id,
                    f"{first.title()} {last.title()}",
                    f"{first}.{last}.{customer_id}@{slug}-buyers.example",
                    rng.choice(_COUNTRIES),
                    created_at + timedelta(days=rng.randrange(180)),
                )
            )

        for number in range(1, rng.randint(min_orders, max_orders) + 1):
            order_id = f"ORD-{number:06d}"
            ordered_at = WINDOW_START + timedelta(seconds=rng.randrange(WINDOW_DAYS * 86_400))
            total_cents = 0
            for line_no in range(1, rng.randint(1, 3) + 1):
                quantity = rng.randint(1, 5)
                unit_cents = rng.randrange(299, 19_999)
                total_cents += quantity * unit_cents
                line_rows.append(
                    (tenant_id, order_id, line_no, f"SKU-{rng.randrange(1, 400):04d}", quantity, unit_cents / 100)
                )
            order_rows.append(
                (
                    tenant_id,
                    order_id,
                    rng.randint(1, customer_count),
                    ordered_at,
                    _weighted(rng, STATUSES),
                    _weighted(rng, CHANNELS),
                    currency,
                    total_cents / 100,
                )
            )

    return Dataset(
        merchants=pd.DataFrame(
            merchant_rows, columns=["TENANT_ID", "SLUG", "NAME", "PLAN", "REGION", "CURRENCY", "CREATED_AT"]
        ),
        customers=pd.DataFrame(
            customer_rows, columns=["TENANT_ID", "CUSTOMER_ID", "NAME", "EMAIL", "COUNTRY", "CREATED_AT"]
        ),
        orders=pd.DataFrame(
            order_rows,
            columns=[
                "TENANT_ID",
                "ORDER_ID",
                "CUSTOMER_ID",
                "ORDERED_AT",
                "STATUS",
                "CHANNEL",
                "CURRENCY",
                "TOTAL_AMOUNT",
            ],
        ),
        line_items=pd.DataFrame(
            line_rows, columns=["TENANT_ID", "ORDER_ID", "LINE_NO", "SKU", "QUANTITY", "UNIT_PRICE"]
        ),
    )


def seed_database(database: str, dataset: Dataset) -> tuple[int, dict[str, int]]:
    """Provision every tenant's roles and replace the tables' contents with `dataset`."""
    settings = get_settings()
    with connect_admin(settings) as conn:
        provisioned = provisioning.provision_tenants(conn, settings, dataset.tenant_ids)
        return provisioned, provisioning.load_tables(conn, database, dataset.frames())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--merchants", type=int, default=MERCHANTS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()
    settings = get_settings()
    started = time.monotonic()
    dataset = generate(args.seed, args.merchants)
    provisioned, loaded = seed_database(settings.tb_database, dataset)
    summary = ", ".join(f"{table} {rows}" for table, rows in loaded.items())
    print(f"Seeded {settings.tb_database}: {summary} ({time.monotonic() - started:.1f}s)")
    print(
        f"Tenant roles TENANT_<ID> and TENANT_<ID>_ADMIN: {len(dataset.tenant_ids)} merchants, "
        f"{provisioned} newly provisioned, granted to {settings.tb_app_user}"
    )


if __name__ == "__main__":
    main()
