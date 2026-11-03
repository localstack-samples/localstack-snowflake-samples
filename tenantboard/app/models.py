"""Request and response models of the public API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class TenantRole(StrEnum):
    """Which of the tenant's two Snowflake roles a request runs under."""

    viewer = "viewer"  # TENANT_<ID>: customer email masked
    admin = "admin"  # TENANT_<ID>_ADMIN: email clear, may upload


class _Model(BaseModel):
    model_config = ConfigDict(frozen=True)


class Merchant(_Model):
    tenant_id: str
    slug: str
    name: str
    plan: str
    region: str
    currency: str


class Summary(_Model):
    tenant_id: str
    role: TenantRole
    snowflake_role: str = Field(description="Role the request's Snowflake session ran under")
    orders: int
    completed_orders: int
    customers: int
    revenue: Decimal = Field(description="Sum of TOTAL_AMOUNT over completed orders")
    average_order_value: Decimal
    currency: str
    first_order_at: datetime | None
    last_order_at: datetime | None


class DailyRevenue(_Model):
    day: date
    orders: int
    revenue: float
    revenue_7d_avg: float = Field(description="Trailing 7-day mean of daily revenue, days without orders count as 0")


class RevenueSeries(_Model):
    tenant_id: str
    currency: str
    days: list[DailyRevenue]


class TopCustomer(_Model):
    customer_id: int
    name: str
    email: str = Field(description="***MASKED*** unless the request runs as the tenant's admin role")
    orders: int
    revenue: Decimal


class Order(_Model):
    order_id: str
    tenant_id: str
    ordered_at: datetime
    status: str
    channel: str
    currency: str
    total_amount: Decimal
    customer_id: int
    customer_name: str | None
    customer_email: str | None


class OrderPage(_Model):
    items: list[Order]
    page: int
    page_size: int
    total: int


class UploadResult(_Model):
    tenant_id: str
    rows: int
    inserted: int
    updated: int
