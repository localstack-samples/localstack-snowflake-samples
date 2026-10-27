"""Parsing and validating a CSV of offline orders.

The API validates shape and types here and leaves required-ness to the table:
ORDERS' NOT NULL constraints are the single source of truth, and a violation
comes back from Snowflake as 100072, reported as 422 with the Snowflake code.
"""

from __future__ import annotations

import io

import pandas as pd

from app.errors import ProblemError

REQUIRED_COLUMNS = ("order_id", "customer_id", "ordered_at", "status", "total_amount")
OPTIONAL_COLUMNS = ("currency",)
STATUSES = frozenset({"completed", "refunded", "cancelled"})
MAX_ROWS = 5_000
ORDER_ID_PATTERN = r"^[A-Z0-9][A-Z0-9-]{0,31}$"


def _invalid(detail: str, **extensions: object) -> ProblemError:
    return ProblemError(422, "invalid-upload", "Invalid upload", detail, **extensions)


def parse_orders_csv(payload: bytes, default_currency: str) -> pd.DataFrame:
    """Return the upload as a frame with ORDERS' column names (TENANT_ID is added by the MERGE)."""
    try:
        raw = pd.read_csv(io.BytesIO(payload), dtype=str, keep_default_na=False, na_values=[""])
    except (pd.errors.ParserError, pd.errors.EmptyDataError, UnicodeDecodeError) as exc:
        raise _invalid(f"Not a readable CSV file: {exc}") from exc
    raw.columns = [str(c).strip().lower() for c in raw.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    unexpected = [c for c in raw.columns if c not in REQUIRED_COLUMNS + OPTIONAL_COLUMNS]
    if missing or unexpected:
        raise _invalid("CSV header does not match the offline order format.", missing=missing, unexpected=unexpected)
    if raw.empty:
        raise _invalid("CSV has a header but no rows.")
    if len(raw) > MAX_ROWS:
        raise _invalid(f"At most {MAX_ROWS} rows per upload, got {len(raw)}.")

    problems: list[dict[str, object]] = []

    def check(column: str, bad: pd.Series, message: str) -> None:
        # CSV line numbers: header is line 1.
        problems.extend({"line": int(i) + 2, "column": column, "msg": message} for i in raw.index[bad])

    order_id = raw["order_id"].str.strip().str.upper()
    check(
        "order_id",
        order_id.notna() & ~order_id.str.fullmatch(ORDER_ID_PATTERN, na=False),
        "expected letters, digits and dashes",
    )
    check("order_id", order_id.notna() & order_id.duplicated(keep=False), "duplicate order_id in this file")

    customer_id = pd.to_numeric(raw["customer_id"], errors="coerce")
    check(
        "customer_id",
        raw["customer_id"].notna() & (customer_id.isna() | (customer_id % 1 != 0) | (customer_id < 1)),
        "expected a positive integer",
    )

    ordered_at = pd.to_datetime(raw["ordered_at"], errors="coerce", format="ISO8601")
    check("ordered_at", raw["ordered_at"].notna() & ordered_at.isna(), "expected an ISO 8601 timestamp")

    status = raw["status"].str.strip().str.lower()
    check("status", status.notna() & ~status.isin(STATUSES), f"expected one of {sorted(STATUSES)}")

    amount = pd.to_numeric(raw["total_amount"], errors="coerce")
    check(
        "total_amount",
        raw["total_amount"].notna() & (amount.isna() | (amount < 0) | (amount.round(2) != amount)),
        "expected a non-negative amount with at most 2 decimals",
    )

    currency = (
        raw["currency"].str.strip().str.upper()
        if "currency" in raw.columns
        else pd.Series(default_currency, index=raw.index)
    )
    check("currency", currency.notna() & ~currency.str.fullmatch(r"^[A-Z]{3}$", na=False), "expected an ISO 4217 code")

    if problems:
        raise _invalid(f"{len(problems)} invalid value(s).", errors=problems[:50])

    if ordered_at.dt.tz is not None:
        ordered_at = ordered_at.dt.tz_convert("UTC").dt.tz_localize(None)
    return pd.DataFrame(
        {
            "ORDER_ID": order_id,
            "CUSTOMER_ID": customer_id.astype("Int64"),
            "ORDERED_AT": ordered_at,
            "STATUS": status,
            "CHANNEL": "offline",
            "CURRENCY": currency.fillna(default_currency),
            "TOTAL_AMOUNT": amount,
        }
    )
