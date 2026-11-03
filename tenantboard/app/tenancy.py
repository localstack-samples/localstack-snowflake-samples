"""Tenant ids and the Snowflake role names derived from them.

The row access policy matches 'TENANT_' || TENANT_ID against the session's
roles, so the role name *is* the tenant's identity. That makes the naming rule
a security invariant: a tenant called ADMIN or READER, or one whose id ends in
_ADMIN, would collide with another role (TENANT_ACME_ADMIN would see the rows
of a tenant ACME_ADMIN), so such ids are refused at provisioning time.
"""

from __future__ import annotations

import re

TENANT_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*$")
RESERVED_TENANT_IDS = frozenset({"ADMIN", "READER"})
MAX_TENANT_ID_LENGTH = 48
PLATFORM_ROLE = "PLATFORM_ADMIN"


def validate_tenant_id(tenant_id: str) -> str:
    if (
        len(tenant_id) > MAX_TENANT_ID_LENGTH
        or not TENANT_ID_PATTERN.fullmatch(tenant_id)
        or tenant_id in RESERVED_TENANT_IDS
        or tenant_id.endswith("_ADMIN")
    ):
        raise ValueError(f"invalid tenant id {tenant_id!r}")
    return tenant_id


def viewer_role(tenant_id: str) -> str:
    """TENANT_<ID>: sees the tenant's rows, customer email masked."""
    return f"TENANT_{validate_tenant_id(tenant_id)}"


def admin_role(tenant_id: str) -> str:
    """TENANT_<ID>_ADMIN: inherits TENANT_<ID> and TENANT_ADMIN, so email is clear."""
    return f"TENANT_{validate_tenant_id(tenant_id)}_ADMIN"
