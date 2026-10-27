"""The HTTP API: values against the independent oracle, tenant switching, masking, errors."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from app import provisioning
from tests.helpers import (
    dict_rows,
    email_of,
    expected_daily_revenue,
    expected_summary,
    expected_top_customers,
    orders_of,
)

ADMIN = {"X-Tenant-Role": "admin"}
VIEWER = {"X-Tenant-Role": "viewer"}
PROBLEM_JSON = "application/problem+json"


def test_health_reports_the_snowflake_version(client):
    response = client.get("/api/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["snowflake_version"][0].isdigit()


def test_tenant_directory_lists_every_seeded_merchant(client, dataset):
    response = client.get("/api/tenants")

    assert response.status_code == 200
    merchants = response.json()
    assert [m["slug"] for m in merchants] == dataset.merchants.sort_values("NAME")["SLUG"].tolist()
    assert {"acme", "globex"} <= {m["slug"] for m in merchants}


@pytest.mark.parametrize("slug", ["acme", "globex"])
def test_summary_matches_the_seeded_orders(client, dataset, slug):
    tenant = slug.upper()
    response = client.get(f"/api/tenants/{slug}/summary")

    assert response.status_code == 200
    body = response.json()
    expected = expected_summary(dataset, tenant)
    assert body["tenant_id"] == tenant
    assert body["snowflake_role"] == f"TENANT_{tenant}"
    assert (body["orders"], body["completed_orders"], body["customers"]) == (
        expected["orders"],
        expected["completed_orders"],
        expected["customers"],
    )
    assert Decimal(body["revenue"]) == expected["revenue"]
    assert Decimal(body["average_order_value"]) == expected["average_order_value"]


def test_switching_tenant_changes_totals_and_never_shows_the_other_tenants_orders(client, dataset):
    """Through the API the dashboard uses."""
    acme = client.get("/api/tenants/acme/orders", params={"page_size": 200}).json()
    globex = client.get("/api/tenants/globex/orders", params={"page_size": 200}).json()

    assert {o["tenant_id"] for o in acme["items"]} == {"ACME"}
    assert {o["tenant_id"] for o in globex["items"]} == {"GLOBEX"}
    assert (acme["total"], globex["total"]) == (len(orders_of(dataset, "ACME")), len(orders_of(dataset, "GLOBEX")))
    assert acme["total"] != globex["total"]


def test_revenue_by_day_is_gap_filled_and_matches_the_seeded_orders(client, dataset):
    response = client.get("/api/tenants/acme/revenue")

    assert response.status_code == 200
    days = response.json()["days"]
    expected = expected_daily_revenue(dataset, "ACME")
    first, last = min(expected), max(expected)
    # One point per calendar day from the first to the last completed order, zeros included.
    assert [d["day"] for d in days] == [(first + timedelta(n)).isoformat() for n in range((last - first).days + 1)]
    for point in days:
        count, revenue = expected.get(date.fromisoformat(point["day"]), (0, Decimal("0.00")))
        assert (point["orders"], Decimal(str(point["revenue"]))) == (count, revenue), point["day"]
    # Trailing 7-day mean, computed independently for the last day.
    window = [expected.get(last - timedelta(n), (0, Decimal(0)))[1] for n in range(7)]
    assert abs(Decimal(str(days[-1]["revenue_7d_avg"])) - sum(window) / 7) <= Decimal("0.005")


def test_revenue_window_includes_days_without_orders(client, dataset):
    expected = expected_daily_revenue(dataset, "GLOBEX")
    quiet = next(d for d in (date(2026, 9, 1) + timedelta(n) for n in range(30)) if d not in expected)

    response = client.get("/api/tenants/globex/revenue", params={"start": quiet.isoformat(), "end": quiet.isoformat()})

    assert response.json()["days"] == [{"day": quiet.isoformat(), "orders": 0, "revenue": 0.0, "revenue_7d_avg": 0.0}]


def test_top_customers_match_the_seeded_orders_with_email_masked_for_viewers(client, dataset):
    response = client.get("/api/tenants/acme/top-customers", params={"limit": 5})

    assert response.status_code == 200
    got = [(c["customer_id"], c["orders"], Decimal(c["revenue"]), c["email"]) for c in response.json()]
    assert got == [(cid, n, revenue, "***MASKED***") for cid, n, revenue in expected_top_customers(dataset, "ACME", 5)]


def test_viewer_sees_masked_email_and_admin_sees_it_clear(client, dataset):
    """The same orders page under the tenant's two roles."""
    viewer = client.get("/api/tenants/acme/orders", params={"page_size": 10}, headers=VIEWER).json()["items"]
    admin = client.get("/api/tenants/acme/orders", params={"page_size": 10}, headers=ADMIN).json()["items"]

    assert [o["order_id"] for o in viewer] == [o["order_id"] for o in admin]
    assert {o["customer_email"] for o in viewer} == {"***MASKED***"}
    assert [o["customer_email"] for o in admin] == [email_of(dataset, "ACME", o["customer_id"]) for o in admin]


def test_orders_pages_walk_every_order_once_newest_first(client, dataset):
    expected = orders_of(dataset, "ACME").sort_values(["ORDERED_AT", "ORDER_ID"], ascending=False)["ORDER_ID"].tolist()
    seen: list[str] = []
    for page in range(1, len(expected) // 10 + 2):
        body = client.get("/api/tenants/acme/orders", params={"page": page, "page_size": 10}).json()
        assert (body["page"], body["page_size"], body["total"]) == (page, 10, len(expected))
        seen += [o["order_id"] for o in body["items"]]

    assert seen == expected


def test_queries_are_tagged_per_endpoint(client, admin, settings):
    client.get("/api/tenants/globex/top-customers")

    tags = dict_rows(
        admin,
        "SELECT QUERY_TAG, USER_NAME FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 1000)) "
        "WHERE QUERY_TAG = ?",
        ["tenantboard:/api/tenants/{slug}/top-customers"],
    )
    assert tags, "no query carried the endpoint's QUERY_TAG"
    assert {t["USER_NAME"] for t in tags} == {settings.tb_app_user}


def test_unknown_tenant_is_a_404_problem(client):
    response = client.get("/api/tenants/no-such-merchant/summary")

    assert response.status_code == 404
    assert response.headers["content-type"] == PROBLEM_JSON
    body = response.json()
    assert (body["title"], body["status"], body["tenant"]) == ("Unknown tenant", 404, "no-such-merchant")
    assert body["type"].endswith("tenant-not-found")


@pytest.mark.parametrize(
    ("path", "headers", "field"),
    [
        ("/api/tenants/Not_A_Slug/summary", {}, "slug"),
        ("/api/tenants/acme/summary", {"X-Tenant-Role": "superuser"}, "X-Tenant-Role"),
        ("/api/tenants/acme/orders?page_size=1000", {}, "page_size"),
    ],
)
def test_invalid_request_is_a_422_problem(client, path, headers, field):
    response = client.get(path, headers=headers)

    assert response.status_code == 422
    assert response.headers["content-type"] == PROBLEM_JSON
    assert field in {str(e["loc"][-1]) for e in response.json()["errors"]}


def test_revenue_rejects_a_window_that_ends_before_it_starts(client):
    response = client.get("/api/tenants/acme/revenue", params={"start": "2026-09-10", "end": "2026-09-01"})

    assert response.status_code == 422
    assert response.json()["detail"] == "start must not be after end."


def test_tenant_without_provisioned_roles_surfaces_snowflake_390189(client, admin, dataset, settings):
    """Error path: a merchant row exists but onboarding never created its roles.

    Snowflake refuses the session (390189); the API reports it as a 403 problem
    carrying the Snowflake error code instead of a bare 500.
    """
    ghost = dataset.merchants.iloc[[0]].assign(TENANT_ID="GHOST", SLUG="ghost", NAME="Ghost Merchant")
    provisioning.load_tables(
        admin, settings.tb_database, {"MERCHANTS": pd.concat([dataset.merchants, ghost], ignore_index=True)}
    )
    try:
        response = client.get("/api/tenants/ghost/summary")
    finally:
        provisioning.load_tables(admin, settings.tb_database, {"MERCHANTS": dataset.merchants})

    assert response.status_code == 403
    body = response.json()
    assert body["title"] == "Tenant role is not available to the service"
    assert body["snowflake"]["error_code"] == "390189"
    assert "TENANT_GHOST" in body["detail"]
