"""CSV upload of offline orders: write_pandas into a temporary table, then one MERGE.

All uploads go to INITECH, which no other test file reads, with order ids
unique per test, so tests stay independent within a worker's database.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.tenancy import admin_role, viewer_role
from tests.helpers import dict_rows, money, orders_of

ADMIN = {"X-Tenant-Role": "admin"}
HEADER = "order_id,customer_id,ordered_at,status,total_amount\n"
URL = "/api/tenants/initech/orders/upload"


def upload(client, body: str, headers=ADMIN):
    return client.post(URL, files={"file": ("offline.csv", (HEADER + body).encode(), "text/csv")}, headers=headers)


def order(connect_as, tenant: str, order_id: str) -> dict | None:
    found = dict_rows(
        connect_as(admin_role(tenant)), "SELECT * FROM ORDERS WHERE TENANT_ID = ? AND ORDER_ID = ?", [tenant, order_id]
    )
    return found[0] if found else None


def test_upload_inserts_new_and_updates_existing_orders_of_this_tenant_only(client, connect_as, dataset):
    """ORD-000001 exists for every merchant; the upload may only touch INITECH's."""
    acme_before = order(connect_as, "ACME", "ORD-000001")

    response = upload(
        client, "ORD-910001,2,2026-09-30T12:00:00,completed,99.95\nORD-000001,1,2026-09-02T08:30:00,refunded,10.00\n"
    )

    assert response.status_code == 200
    assert response.json() == {"tenant_id": "INITECH", "rows": 2, "inserted": 1, "updated": 1}
    new, updated = order(connect_as, "INITECH", "ORD-910001"), order(connect_as, "INITECH", "ORD-000001")
    assert (new["CHANNEL"], new["TOTAL_AMOUNT"], new["CURRENCY"]) == ("offline", Decimal("99.95"), "EUR")
    assert (updated["STATUS"], updated["TOTAL_AMOUNT"]) == ("refunded", Decimal("10.00"))
    assert order(connect_as, "ACME", "ORD-000001") == acme_before
    seeded = orders_of(dataset, "ACME").set_index("ORDER_ID").loc["ORD-000001"]
    assert acme_before["TOTAL_AMOUNT"] == money(seeded["TOTAL_AMOUNT"])


def test_uploading_the_same_file_twice_updates_and_inserts_nothing_new(client, connect_as):
    body = "ORD-911001,3,2026-09-15T10:00:00,completed,25.00\nORD-911002,3,2026-09-15T11:00:00,cancelled,5.50\n"

    first, second = upload(client, body), upload(client, body)

    assert (first.json()["inserted"], first.json()["updated"]) == (2, 0)
    assert (second.json()["inserted"], second.json()["updated"]) == (0, 2)
    count = dict_rows(
        connect_as(viewer_role("INITECH")), "SELECT COUNT(*) AS N FROM ORDERS WHERE ORDER_ID LIKE 'ORD-9110%'"
    )
    assert count == [{"N": 2}]


def test_viewer_role_cannot_upload(client, connect_as):
    response = upload(client, "ORD-912001,1,2026-09-15T10:00:00,completed,1.00\n", headers={"X-Tenant-Role": "viewer"})

    assert response.status_code == 403
    assert response.json()["title"] == "Admin role required"
    assert order(connect_as, "INITECH", "ORD-912001") is None


def test_null_in_a_required_column_rejects_the_whole_upload_with_snowflake_100072(client, connect_as):
    """Error path: the table's NOT NULL constraint, not the API, rejects the row, and the
    single MERGE statement leaves the valid row unapplied too."""
    response = upload(
        client, "ORD-913001,1,2026-09-15T10:00:00,completed,12.00\nORD-913002,,2026-09-15T11:00:00,completed,8.00\n"
    )

    assert response.status_code == 422
    body = response.json()
    assert body["snowflake"]["error_code"] == "100072"
    assert body["snowflake"]["query_id"]
    assert order(connect_as, "INITECH", "ORD-913001") is None
    assert order(connect_as, "INITECH", "ORD-913002") is None


@pytest.mark.parametrize(
    ("csv", "expected"),
    [
        (
            "order_id,customer\nORD-1,2\n",
            {"missing": ["customer_id", "ordered_at", "status", "total_amount"], "unexpected": ["customer"]},
        ),
        (HEADER, None),
        (
            HEADER + "ORD-914001,x,yesterday,shipped,-1\n",
            {"columns": {"customer_id", "ordered_at", "status", "total_amount"}},
        ),
        (
            HEADER + "ORD-914002,1,2026-09-15,completed,1\nORD-914002,1,2026-09-15,completed,1\n",
            {"columns": {"order_id"}},
        ),
    ],
    ids=["wrong-header", "no-rows", "bad-values", "duplicate-ids"],
)
def test_malformed_csv_is_a_422_problem_and_writes_nothing(client, connect_as, csv, expected):
    response = client.post(URL, files={"file": ("offline.csv", csv.encode(), "text/csv")}, headers=ADMIN)

    assert response.status_code == 422
    body = response.json()
    assert body["title"] == "Invalid upload"
    if expected and "missing" in expected:
        assert (body["missing"], body["unexpected"]) == (expected["missing"], expected["unexpected"])
    if expected and "columns" in expected:
        assert {e["column"] for e in body["errors"]} == expected["columns"]
        assert {e["line"] for e in body["errors"]} <= {2, 3}
    count = dict_rows(
        connect_as(viewer_role("INITECH")), "SELECT COUNT(*) AS N FROM ORDERS WHERE ORDER_ID LIKE 'ORD-914%'"
    )
    assert count == [{"N": 0}]
