"""Shared test helpers and the independent oracle.

Expected values are computed here, in plain pandas and Decimal arithmetic over
the seeded dataset, without touching the API or Snowflake.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pandas as pd
import snowflake.connector

from scripts.seed import Dataset


def dict_rows(
    conn: snowflake.connector.SnowflakeConnection, sql: str, params: list[Any] | None = None
) -> list[dict[str, Any]]:
    with conn.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())


def money(value: float) -> Decimal:
    """Amounts are generated as cents / 100; str() of such a float is its exact 2-decimal form."""
    return Decimal(str(value)).quantize(Decimal("0.01"))


def orders_of(ds: Dataset, tenant_id: str) -> pd.DataFrame:
    return ds.orders[ds.orders["TENANT_ID"] == tenant_id]


def completed_of(ds: Dataset, tenant_id: str) -> pd.DataFrame:
    orders = orders_of(ds, tenant_id)
    return orders[orders["STATUS"] == "completed"]


def expected_summary(ds: Dataset, tenant_id: str) -> dict[str, Any]:
    orders, completed = orders_of(ds, tenant_id), completed_of(ds, tenant_id)
    revenue = sum((money(v) for v in completed["TOTAL_AMOUNT"]), Decimal("0.00"))
    return {
        "orders": len(orders),
        "completed_orders": len(completed),
        "customers": orders["CUSTOMER_ID"].nunique(),
        "revenue": revenue,
        "average_order_value": (revenue / len(completed)).quantize(Decimal("0.01")),
    }


def expected_daily_revenue(ds: Dataset, tenant_id: str) -> dict[date, tuple[int, Decimal]]:
    out: dict[date, tuple[int, Decimal]] = {}
    for _, row in completed_of(ds, tenant_id).iterrows():
        day = row["ORDERED_AT"].date()
        count, total = out.get(day, (0, Decimal("0.00")))
        out[day] = (count + 1, total + money(row["TOTAL_AMOUNT"]))
    return out


def expected_top_customers(ds: Dataset, tenant_id: str, limit: int) -> list[tuple[int, int, Decimal]]:
    """(customer_id, orders, revenue), highest revenue first, ties by customer id."""
    totals: dict[int, tuple[int, Decimal]] = {}
    for _, row in completed_of(ds, tenant_id).iterrows():
        count, total = totals.get(int(row["CUSTOMER_ID"]), (0, Decimal("0.00")))
        totals[int(row["CUSTOMER_ID"])] = (count + 1, total + money(row["TOTAL_AMOUNT"]))
    ranked = sorted(totals.items(), key=lambda item: (-item[1][1], item[0]))
    return [(cid, count, total) for cid, (count, total) in ranked[:limit]]


def email_of(ds: Dataset, tenant_id: str, customer_id: int) -> str:
    customers = ds.customers
    match = customers[(customers["TENANT_ID"] == tenant_id) & (customers["CUSTOMER_ID"] == customer_id)]
    return str(match["EMAIL"].iloc[0])
