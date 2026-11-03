"""`make demo`: the leak test, narrated.

Runs a raw `SELECT * FROM ORDERS` (no WHERE clause, no app code) under
tenant roles, shows customer email masking per role, then detaches the row
access policy and runs the same SELECT again. Everything happens in TB_DEMO, a
clone of TENANTBOARD, which is dropped at the end.
"""

from __future__ import annotations

import sys
import time
from collections import Counter

from app import provisioning
from app.db import connect_admin, connect_sync
from app.settings import Settings, get_settings
from app.tenancy import PLATFORM_ROLE, admin_role, viewer_role
from scripts import seed

DEMO_DB = "TB_DEMO"
RAW_SELECT = "SELECT * FROM ORDERS"


def tenants_seen(settings: Settings, role: str) -> Counter:
    with connect_sync(
        settings, user=settings.tb_app_user, role=role, database=DEMO_DB, USE_CACHED_RESULT=False
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(RAW_SELECT)
            column = [d.name for d in cur.description].index("TENANT_ID")
            return Counter(row[column] for row in cur.fetchall())


def describe(seen: Counter) -> str:
    tenants = ", ".join(sorted(seen)[:3]) + (", ..." if len(seen) > 3 else "")
    return f"{sum(seen.values()):4d} rows from {len(seen):2d} tenant(s): {tenants}"


def emails(settings: Settings, role: str) -> list[str]:
    with connect_sync(settings, user=settings.tb_app_user, role=role, database=DEMO_DB) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EMAIL FROM CUSTOMERS ORDER BY CUSTOMER_ID LIMIT 2")
            return [row[0] for row in cur.fetchall()]


def main() -> int:
    settings = get_settings()
    started = time.monotonic()
    with connect_admin(settings) as admin:
        with connect_sync(
            settings, user=settings.tb_app_user, role=PLATFORM_ROLE, database=settings.tb_database
        ) as platform:
            merchants = provisioning.query(platform, "SELECT COUNT(*) FROM MERCHANTS")[0][0]
        if merchants == 0:
            print(f"{settings.tb_database} is empty; seeding it first (make seed)")
            seed.seed_database(settings.tb_database, seed.generate())

        print(f"[1/5] CREATE DATABASE {DEMO_DB} CLONE {settings.tb_database}")
        provisioning.clone_database(admin, settings.tb_database, DEMO_DB)
        try:
            policy = f"{DEMO_DB}.APP.TENANT_ISOLATION"
            print(f"\n[2/5] {RAW_SELECT}   -- no WHERE clause, run under each role")
            for role in (viewer_role("ACME"), viewer_role("GLOBEX"), admin_role("ACME"), PLATFORM_ROLE):
                print(f"      as {role:<18} {describe(tenants_seen(settings, role))}")

            print("\n[3/5] SELECT EMAIL FROM CUSTOMERS   -- masking policy EMAIL_MASK")
            for role in (viewer_role("ACME"), admin_role("ACME")):
                print(f"      as {role:<18} {', '.join(emails(settings, role))}")

            print("\n[4/5] ALTER TABLE ORDERS DROP ROW ACCESS POLICY TENANT_ISOLATION   -- simulate a missing policy")
            provisioning.query(admin, f"ALTER TABLE {DEMO_DB}.APP.ORDERS DROP ROW ACCESS POLICY {policy}")
            leaked = tenants_seen(settings, viewer_role("ACME"))
            print(f"      as {viewer_role('ACME'):<18} {describe(leaked)}   <-- LEAK: every tenant's orders")

            print("\n[5/5] ALTER TABLE ORDERS ADD ROW ACCESS POLICY TENANT_ISOLATION ON (TENANT_ID)")
            provisioning.query(admin, f"ALTER TABLE {DEMO_DB}.APP.ORDERS ADD ROW ACCESS POLICY {policy} ON (TENANT_ID)")
            restored = tenants_seen(settings, viewer_role("ACME"))
            print(f"      as {viewer_role('ACME'):<18} {describe(restored)}")
        finally:
            provisioning.drop_database(admin, DEMO_DB)

    isolated = set(restored) == {"ACME"} and len(leaked) > 1
    print(
        f"\n{'Isolation holds' if isolated else 'UNEXPECTED RESULT'}: the policy, not the app, decides which rows a tenant session can read."
        f" ({time.monotonic() - started:.1f}s, {DEMO_DB} dropped)"
    )
    return 0 if isolated else 1


if __name__ == "__main__":
    sys.exit(main())
