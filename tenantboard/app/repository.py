"""Every SQL statement the API runs.

Layer 1 of tenant isolation lives here: each tenant query binds TENANT_ID as
a qmark parameter (never formatted into the SQL text). Layer 2 is the
TENANT_ISOLATION row access policy, evaluated against the session's role. Each
layer is sufficient on its own; tests/test_isolation.py proves both.
"""

from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import Any

import pandas as pd
from snowflake.connector.aio import SnowflakeConnection, write_pandas

from app import db
from app.models import DailyRevenue, Merchant, Order, OrderPage, RevenueSeries, Summary, TenantRole, TopCustomer

COMPLETED = "completed"


def _lower_keys(row: dict[str, Any]) -> dict[str, Any]:
    return {k.lower(): v for k, v in row.items()}


class PlatformRepository:
    """Tenant directory, read under PLATFORM_ADMIN (the only cross-tenant query in the API)."""

    def __init__(self, conn: SnowflakeConnection, tag: str) -> None:
        self._conn, self._tag = conn, tag

    async def list_merchants(self) -> list[Merchant]:
        rows = await db.fetch_dicts(
            self._conn,
            "SELECT TENANT_ID, SLUG, NAME, PLAN, REGION, CURRENCY FROM MERCHANTS ORDER BY NAME",
            tag=self._tag,
        )
        return [Merchant(**_lower_keys(row)) for row in rows]

    async def get_merchant(self, slug: str) -> Merchant | None:
        rows = await db.fetch_dicts(
            self._conn,
            "SELECT TENANT_ID, SLUG, NAME, PLAN, REGION, CURRENCY FROM MERCHANTS WHERE SLUG = ?",
            [slug],
            tag=self._tag,
        )
        return Merchant(**_lower_keys(rows[0])) if rows else None


class TenantRepository:
    """Queries for one tenant, on a session that runs as one of that tenant's roles."""

    def __init__(
        self, conn: SnowflakeConnection, merchant: Merchant, role: TenantRole, snowflake_role: str, tag: str
    ) -> None:
        self._conn = conn
        self.merchant = merchant
        self.role = role
        self.snowflake_role = snowflake_role
        self._tag = tag

    @property
    def tenant_id(self) -> str:
        return self.merchant.tenant_id

    async def summary(self) -> Summary:
        rows = await db.fetch_dicts(
            self._conn,
            """
            SELECT COUNT(*)                                            AS ORDERS,
                   COUNT_IF(STATUS = ?)                                AS COMPLETED_ORDERS,
                   COUNT(DISTINCT CUSTOMER_ID)                         AS CUSTOMERS,
                   COALESCE(SUM(IFF(STATUS = ?, TOTAL_AMOUNT, 0)), 0)  AS REVENUE,
                   MIN(ORDERED_AT)                                     AS FIRST_ORDER_AT,
                   MAX(ORDERED_AT)                                     AS LAST_ORDER_AT
              FROM ORDERS
             WHERE TENANT_ID = ?
            """,
            [COMPLETED, COMPLETED, self.tenant_id],
            tag=self._tag,
        )
        row = rows[0]
        revenue = Decimal(row["REVENUE"])
        completed = int(row["COMPLETED_ORDERS"])
        return Summary(
            tenant_id=self.tenant_id,
            role=self.role,
            snowflake_role=self.snowflake_role,
            orders=int(row["ORDERS"]),
            completed_orders=completed,
            customers=int(row["CUSTOMERS"]),
            revenue=revenue,
            average_order_value=(revenue / completed).quantize(Decimal("0.01")) if completed else Decimal("0.00"),
            currency=self.merchant.currency,
            first_order_at=row["FIRST_ORDER_AT"],
            last_order_at=row["LAST_ORDER_AT"],
        )

    async def revenue_by_day(self, start: date | None, end: date | None) -> RevenueSeries:
        """Daily completed revenue, gap-filled and smoothed in pandas.

        The aggregate comes back as Arrow batches (fetch_pandas_all), so the
        series lands in a DataFrame without per-row Python conversion.
        """
        frame = await db.fetch_frame(
            self._conn,
            """
            SELECT TO_DATE(ORDERED_AT) AS DAY, COUNT(*) AS ORDERS, SUM(TOTAL_AMOUNT) AS REVENUE
              FROM ORDERS
             WHERE TENANT_ID = ?
               AND STATUS = ?
               AND TO_DATE(ORDERED_AT) BETWEEN COALESCE(?::DATE, '0001-01-01'::DATE) AND COALESCE(?::DATE, '9999-12-31'::DATE)
             GROUP BY 1
             ORDER BY 1
            """,
            [self.tenant_id, COMPLETED, start.isoformat() if start else None, end.isoformat() if end else None],
            tag=self._tag,
        )
        if frame.empty and (start is None or end is None):
            return RevenueSeries(tenant_id=self.tenant_id, currency=self.merchant.currency, days=[])
        frame["DAY"] = pd.to_datetime(frame["DAY"])
        index = pd.date_range(start or frame["DAY"].min(), end or frame["DAY"].max(), freq="D", name="DAY")
        series = (
            frame.set_index("DAY")[["ORDERS", "REVENUE"]]
            .astype({"ORDERS": "int64", "REVENUE": "float64"})
            .reindex(index, fill_value=0)
        )
        series["AVG7"] = series["REVENUE"].rolling(7, min_periods=1).mean()
        days = [
            DailyRevenue(
                day=day.date(),
                orders=int(r.ORDERS),
                revenue=round(float(r.REVENUE), 2),
                revenue_7d_avg=round(float(r.AVG7), 2),
            )
            for day, r in series.iterrows()
        ]
        return RevenueSeries(tenant_id=self.tenant_id, currency=self.merchant.currency, days=days)

    async def top_customers(self, limit: int) -> list[TopCustomer]:
        rows = await db.fetch_dicts(
            self._conn,
            """
            SELECT c.CUSTOMER_ID, c.NAME, c.EMAIL, COUNT(*) AS ORDERS, SUM(o.TOTAL_AMOUNT) AS REVENUE
              FROM ORDERS o
              JOIN CUSTOMERS c ON c.TENANT_ID = o.TENANT_ID AND c.CUSTOMER_ID = o.CUSTOMER_ID
             WHERE o.TENANT_ID = ?
               AND o.STATUS = ?
             GROUP BY c.CUSTOMER_ID, c.NAME, c.EMAIL
             ORDER BY REVENUE DESC, c.CUSTOMER_ID
             LIMIT ?
            """,
            [self.tenant_id, COMPLETED, limit],
            tag=self._tag,
        )
        return [
            TopCustomer(
                customer_id=r["CUSTOMER_ID"], name=r["NAME"], email=r["EMAIL"], orders=r["ORDERS"], revenue=r["REVENUE"]
            )
            for r in rows
        ]

    async def list_orders(self, page: int, page_size: int) -> OrderPage:
        """One page of orders, newest first, with the total from the same scan (COUNT(*) OVER ())."""
        rows = await db.fetch_dicts(
            self._conn,
            """
            SELECT o.ORDER_ID, o.TENANT_ID, o.ORDERED_AT, o.STATUS, o.CHANNEL, o.CURRENCY, o.TOTAL_AMOUNT,
                   o.CUSTOMER_ID, c.NAME AS CUSTOMER_NAME, c.EMAIL AS CUSTOMER_EMAIL,
                   COUNT(*) OVER () AS TOTAL
              FROM ORDERS o
              LEFT JOIN CUSTOMERS c ON c.TENANT_ID = o.TENANT_ID AND c.CUSTOMER_ID = o.CUSTOMER_ID
             WHERE o.TENANT_ID = ?
             ORDER BY o.ORDERED_AT DESC, o.ORDER_ID DESC
             LIMIT ? OFFSET ?
            """,
            [self.tenant_id, page_size, (page - 1) * page_size],
            tag=self._tag,
        )
        if rows:
            total = int(rows[0]["TOTAL"])
        else:  # past the last page: the window had no rows to count
            counted = await db.fetch_dicts(
                self._conn, "SELECT COUNT(*) AS N FROM ORDERS WHERE TENANT_ID = ?", [self.tenant_id], tag=self._tag
            )
            total = int(counted[0]["N"])
        items = [Order(**_lower_keys({k: v for k, v in row.items() if k != "TOTAL"})) for row in rows]
        return OrderPage(items=items, page=page, page_size=page_size, total=total)

    async def upload_orders(self, frame: pd.DataFrame) -> tuple[int, int]:
        """Stage the rows with write_pandas into a temporary table, then MERGE them into ORDERS.

        The MERGE is one statement, so it applies all rows or none. Its ON
        clause binds the tenant: an upload can only ever match this tenant's
        orders, even though every tenant has an ORD-000001.
        """
        staging = f"ORDERS_UPLOAD_{uuid.uuid4().hex.upper()}"
        result = await write_pandas(
            self._conn,
            frame,
            staging,
            auto_create_table=True,
            table_type="temp",
            use_logical_type=True,
            quote_identifiers=True,
        )
        if not result.success:
            raise RuntimeError(f"write_pandas staging failed: {result.copy_results}")
        try:
            row = await db.execute(
                self._conn,
                """
                MERGE INTO ORDERS t
                USING (
                    SELECT ? AS TENANT_ID, "ORDER_ID", "CUSTOMER_ID", "ORDERED_AT", "STATUS", "CHANNEL", "CURRENCY", "TOTAL_AMOUNT"
                      FROM IDENTIFIER(?)
                ) s
                   ON t.TENANT_ID = ? AND t.TENANT_ID = s.TENANT_ID AND t.ORDER_ID = s.ORDER_ID
                 WHEN MATCHED THEN UPDATE SET
                      CUSTOMER_ID = s.CUSTOMER_ID, ORDERED_AT = s.ORDERED_AT, STATUS = s.STATUS,
                      CHANNEL = s.CHANNEL, CURRENCY = s.CURRENCY, TOTAL_AMOUNT = s.TOTAL_AMOUNT
                 WHEN NOT MATCHED THEN INSERT
                      (TENANT_ID, ORDER_ID, CUSTOMER_ID, ORDERED_AT, STATUS, CHANNEL, CURRENCY, TOTAL_AMOUNT)
                      VALUES (s.TENANT_ID, s.ORDER_ID, s.CUSTOMER_ID, s.ORDERED_AT, s.STATUS, s.CHANNEL, s.CURRENCY, s.TOTAL_AMOUNT)
                """,
                [self.tenant_id, staging, self.tenant_id],
                tag=self._tag,
            )
        finally:
            await db.execute(self._conn, "DROP TABLE IF EXISTS IDENTIFIER(?)", [staging], tag=self._tag)
        inserted, updated = (int(row[0]), int(row[1])) if row else (0, 0)
        return inserted, updated
