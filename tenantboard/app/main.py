"""Tenantboard API: customer-facing analytics for the merchants of a B2B commerce platform.

    uv run uvicorn app.main:app --port 8000

Every tenant request runs on a Snowflake session whose role is the tenant's
own (TENANT_<ID> for viewers, TENANT_<ID>_ADMIN for admins), so Snowflake's
row access and masking policies apply to it. The caller's tenant and role come
from the URL and the X-Tenant-Role header here; in production they come from
the verified claims of the caller's token, never from the request body.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from typing import Annotated

from fastapi import Depends, FastAPI, File, Header, Path, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from app import db, errors, tenancy
from app.errors import ProblemError, tenant_not_found
from app.models import Merchant, OrderPage, RevenueSeries, Summary, TenantRole, TopCustomer, UploadResult
from app.repository import PlatformRepository, TenantRepository
from app.settings import Settings, get_settings
from app.uploads import parse_orders_csv

log = logging.getLogger(__name__)

SLUG_PATTERN = r"^[a-z0-9][a-z0-9-]{0,47}$"


@dataclass(frozen=True)
class AppState:
    settings: Settings
    sessions: db.RoleSessionCache


def get_state(request: Request) -> AppState:
    return request.app.state.tenantboard


def endpoint_tag(request: Request) -> str:
    """QUERY_TAG for the route (its path template, so tags group by endpoint, not by tenant)."""
    route = request.scope.get("route")
    return db.query_tag(getattr(route, "path", request.url.path))


StateDep = Annotated[AppState, Depends(get_state)]
TagDep = Annotated[str, Depends(endpoint_tag)]


async def platform_repository(state: StateDep, tag: TagDep) -> AsyncIterator[PlatformRepository]:
    async with state.sessions.lease(tenancy.PLATFORM_ROLE) as conn:
        yield PlatformRepository(conn, tag)


PlatformDep = Annotated[PlatformRepository, Depends(platform_repository)]


async def tenant_repository(
    slug: Annotated[str, Path(pattern=SLUG_PATTERN, description="Merchant slug, e.g. acme")],
    state: StateDep,
    tag: TagDep,
    platform: PlatformDep,
    role: Annotated[TenantRole, Header(alias="X-Tenant-Role")] = TenantRole.viewer,
) -> AsyncIterator[TenantRepository]:
    merchant = await platform.get_merchant(slug)
    if merchant is None:
        raise tenant_not_found(slug)
    snowflake_role = (tenancy.admin_role if role is TenantRole.admin else tenancy.viewer_role)(merchant.tenant_id)
    async with state.sessions.lease(snowflake_role) as conn:
        yield TenantRepository(conn, merchant, role, snowflake_role, tag)


TenantDep = Annotated[TenantRepository, Depends(tenant_repository)]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        sessions = db.RoleSessionCache(
            lambda role: db.open_role_session(settings, role), max_size=settings.tb_max_role_connections
        )
        app.state.tenantboard = AppState(settings=settings, sessions=sessions)
        try:
            yield
        finally:
            await sessions.aclose()

    app = FastAPI(
        title="Tenantboard API",
        version="0.1.0",
        summary="Multi-tenant analytics whose isolation and masking are enforced by Snowflake policies",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.tb_cors_origins,
        allow_methods=["GET", "POST"],
        allow_headers=["X-Tenant-Role"],
    )
    errors.install(app)

    @app.get("/api/health", tags=["platform"])
    async def health(state: StateDep, tag: TagDep) -> dict[str, str]:
        async with state.sessions.lease(tenancy.PLATFORM_ROLE) as conn:
            row = await db.execute(conn, "SELECT CURRENT_VERSION()", tag=tag)
        return {
            "status": "ok",
            "snowflake_version": str(row[0]) if row else "unknown",
            "database": settings.tb_database,
        }

    @app.get("/api/tenants", tags=["platform"])
    async def list_tenants(platform: PlatformDep) -> list[Merchant]:
        """The tenant directory (the dashboard's tenant picker)."""
        return await platform.list_merchants()

    @app.get("/api/tenants/{slug}/summary", tags=["analytics"])
    async def summary(repo: TenantDep) -> Summary:
        return await repo.summary()

    @app.get("/api/tenants/{slug}/revenue", tags=["analytics"])
    async def revenue(
        repo: TenantDep,
        start: Annotated[date | None, Query(description="First day (inclusive); default: first order")] = None,
        end: Annotated[date | None, Query(description="Last day (inclusive); default: last order")] = None,
    ) -> RevenueSeries:
        if start and end and start > end:
            raise ProblemError(422, "invalid-request", "Invalid request", "start must not be after end.")
        return await repo.revenue_by_day(start, end)

    @app.get("/api/tenants/{slug}/top-customers", tags=["analytics"])
    async def top_customers(repo: TenantDep, limit: Annotated[int, Query(ge=1, le=50)] = 5) -> list[TopCustomer]:
        return await repo.top_customers(limit)

    @app.get("/api/tenants/{slug}/orders", tags=["orders"])
    async def list_orders(
        repo: TenantDep,
        page: Annotated[int, Query(ge=1, le=10_000)] = 1,
        page_size: Annotated[int, Query(ge=1, le=200)] = 25,
    ) -> OrderPage:
        return await repo.list_orders(page, page_size)

    @app.post("/api/tenants/{slug}/orders/upload", tags=["orders"], status_code=200)
    async def upload_orders(
        repo: TenantDep, file: Annotated[UploadFile, File(description="CSV of offline orders")]
    ) -> UploadResult:
        """Insert or update offline orders from a CSV (admin role only)."""
        if repo.role is not TenantRole.admin:
            raise ProblemError(
                403, "admin-role-required", "Admin role required", "Uploading orders requires X-Tenant-Role: admin."
            )
        payload = await file.read(settings.tb_max_upload_bytes + 1)
        if len(payload) > settings.tb_max_upload_bytes:
            raise ProblemError(
                413, "upload-too-large", "Upload too large", f"At most {settings.tb_max_upload_bytes} bytes."
            )
        frame = parse_orders_csv(payload, repo.merchant.currency)
        inserted, updated = await repo.upload_orders(frame)
        log.info(
            "tenant %s uploaded %d orders (%d inserted, %d updated)", repo.tenant_id, len(frame), inserted, updated
        )
        return UploadResult(tenant_id=repo.tenant_id, rows=len(frame), inserted=inserted, updated=updated)

    return app


app = create_app()
