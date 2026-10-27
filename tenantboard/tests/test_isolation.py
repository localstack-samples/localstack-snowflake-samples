"""Tenant isolation and masking, proven below the API.

The headline leak test bypasses the app-layer filter entirely: it runs a raw
`SELECT * FROM ORDERS` (no WHERE clause) as one tenant's role and must get
that tenant's rows and nothing else. Only the row access policy can make that
true. Removing the policy from init/03_tables.sf.sql makes it fail;
test_detaching_the_policy_exposes_every_tenant checks the same sensitivity on
every run.
"""

from __future__ import annotations

import asyncio
from collections import Counter

import pytest

from app import provisioning
from app.db import connect_admin, connect_sync, open_role_session
from app.models import Merchant, TenantRole
from app.repository import TenantRepository
from app.settings import Settings
from app.tenancy import PLATFORM_ROLE, admin_role, viewer_role
from tests.conftest import TEMPLATE_DB
from tests.helpers import dict_rows, email_of, expected_summary, orders_of

TENANT_TABLES = ("MERCHANTS", "CUSTOMERS", "ORDERS", "LINE_ITEMS")


def _expected_rows_per_tenant(dataset, table: str) -> Counter:
    frame = {
        "MERCHANTS": dataset.merchants,
        "CUSTOMERS": dataset.customers,
        "ORDERS": dataset.orders,
        "LINE_ITEMS": dataset.line_items,
    }[table]
    return Counter(frame["TENANT_ID"])


@pytest.mark.parametrize("tenant", ["ACME", "GLOBEX"])
def test_leak_raw_select_star_from_orders_returns_only_the_tenants_rows(connect_as, dataset, tenant):
    """Headline: no WHERE clause, no app code. The row access policy alone isolates the tenant."""
    got = dict_rows(connect_as(viewer_role(tenant)), "SELECT * FROM ORDERS")

    assert {row["TENANT_ID"] for row in got} == {tenant}
    assert sorted(row["ORDER_ID"] for row in got) == sorted(orders_of(dataset, tenant)["ORDER_ID"])


@pytest.mark.parametrize("table", TENANT_TABLES)
def test_every_tenant_table_is_filtered_to_the_sessions_tenant(connect_as, dataset, table):
    got = dict_rows(
        connect_as(viewer_role("GLOBEX")), f"SELECT TENANT_ID, COUNT(*) AS N FROM {table} GROUP BY TENANT_ID"
    )

    assert {row["TENANT_ID"]: row["N"] for row in got} == {
        "GLOBEX": _expected_rows_per_tenant(dataset, table)["GLOBEX"]
    }


def test_tenant_admin_role_inherits_the_same_isolation(connect_as, dataset):
    got = dict_rows(connect_as(admin_role("ACME")), "SELECT TENANT_ID, COUNT(*) AS N FROM ORDERS GROUP BY TENANT_ID")

    assert {row["TENANT_ID"]: row["N"] for row in got} == {"ACME": len(orders_of(dataset, "ACME"))}


def test_platform_admin_sees_every_tenant(connect_as, dataset):
    got = dict_rows(connect_as(PLATFORM_ROLE), "SELECT TENANT_ID, COUNT(*) AS N FROM ORDERS GROUP BY TENANT_ID")

    assert {row["TENANT_ID"]: row["N"] for row in got} == dict(_expected_rows_per_tenant(dataset, "ORDERS"))


@pytest.mark.parametrize(
    ("role", "clear"),
    [("viewer", False), ("admin", True), ("platform", False)],
)
def test_customer_email_is_clear_only_for_the_tenants_admin(connect_as, dataset, role, clear):
    session_role = {"viewer": viewer_role("ACME"), "admin": admin_role("ACME"), "platform": PLATFORM_ROLE}[role]
    got = dict_rows(connect_as(session_role), "SELECT CUSTOMER_ID, EMAIL FROM CUSTOMERS WHERE TENANT_ID = ?", ["ACME"])

    assert len(got) == int((dataset.customers["TENANT_ID"] == "ACME").sum())
    expected = [email_of(dataset, "ACME", r["CUSTOMER_ID"]) if clear else "***MASKED***" for r in got]
    assert [r["EMAIL"] for r in got] == expected


def test_app_layer_filter_alone_isolates_the_tenant(settings: Settings, dataset):
    """Layer 1 on its own: the repository under PLATFORM_ADMIN (which the policy lets see everything)
    still returns only ACME's orders, because every query binds TENANT_ID."""
    merchant = Merchant(
        tenant_id="ACME", slug="acme", name="Acme Corporation", plan="enterprise", region="uk-south", currency="GBP"
    )

    async def run():
        conn = await open_role_session(settings, PLATFORM_ROLE)
        try:
            repo = TenantRepository(conn, merchant, TenantRole.viewer, PLATFORM_ROLE, tag="tenantboard:test")
            return await repo.list_orders(page=1, page_size=200), await repo.summary()
        finally:
            await conn.close()

    page, summary = asyncio.run(run())

    assert {order.tenant_id for order in page.items} == {"ACME"}
    assert page.total == len(orders_of(dataset, "ACME"))
    assert summary.revenue == expected_summary(dataset, "ACME")["revenue"]


def test_detaching_the_policy_exposes_every_tenant(settings: Settings, dataset, worker_db):
    """The control for the leak test: on a scratch clone, drop the policy and the same raw SELECT leaks."""
    scratch = f"{worker_db}_LEAK"
    with connect_admin(settings) as admin:
        provisioning.clone_database(admin, TEMPLATE_DB, scratch)
        try:
            policy = f"{scratch}.APP.TENANT_ISOLATION"
            with connect_sync(
                settings, user=settings.tb_app_user, role=viewer_role("ACME"), database=scratch, USE_CACHED_RESULT=False
            ) as tenant:
                before = {r["TENANT_ID"] for r in dict_rows(tenant, "SELECT DISTINCT TENANT_ID FROM ORDERS")}
                provisioning.query(admin, f"ALTER TABLE {scratch}.APP.ORDERS DROP ROW ACCESS POLICY {policy}")
                after = {r["TENANT_ID"] for r in dict_rows(tenant, "SELECT DISTINCT TENANT_ID FROM ORDERS")}
        finally:
            provisioning.drop_database(admin, scratch)

    assert before == {"ACME"}
    assert after == set(dataset.tenant_ids)
