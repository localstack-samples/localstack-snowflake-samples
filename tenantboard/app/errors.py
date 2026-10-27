"""RFC 9457 problem details for every error the API returns.

Snowflake errors keep their identity on the wire: the response carries the
Snowflake error code, SQLSTATE and query id, so a client (or an on-call
engineer) can find the failing statement in QUERY_HISTORY.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from snowflake.connector.errors import Error as SnowflakeError
from starlette.exceptions import HTTPException as StarletteHTTPException

log = logging.getLogger(__name__)

PROBLEM_JSON = "application/problem+json"
PROBLEM_BASE = "urn:tenantboard:problem"

# Snowflake error code -> (HTTP status, problem type slug, title)
SNOWFLAKE_ERRORS: dict[int, tuple[int, str, str]] = {
    # Role in the connect string does not exist or is not granted to the user.
    390189: (403, "tenant-role-unavailable", "Tenant role is not available to the service"),
    # NULL in a NOT NULL column during DML.
    100072: (422, "constraint-violation", "Uploaded rows violate a table constraint"),
    # MERGE matched one target row to several source rows.
    100090: (409, "duplicate-merge-source", "Several uploaded rows target the same order"),
    # Object does not exist or not authorized / insufficient privileges.
    2003: (403, "not-authorized", "Object does not exist or not authorized"),
    3001: (403, "not-authorized", "Insufficient privileges"),
}


class ProblemError(Exception):
    """An error the API reports as problem details."""

    def __init__(self, status: int, slug: str, title: str, detail: str, **extensions: Any) -> None:
        super().__init__(detail)
        self.status, self.slug, self.title, self.detail, self.extensions = status, slug, title, detail, extensions


def tenant_not_found(slug: str) -> ProblemError:
    return ProblemError(404, "tenant-not-found", "Unknown tenant", f"No merchant with slug {slug!r}.", tenant=slug)


def problem_response(
    request: Request, status: int, slug: str, title: str, detail: str, **extensions: Any
) -> JSONResponse:
    body = {
        "type": f"{PROBLEM_BASE}:{slug}",
        "title": title,
        "status": status,
        "detail": detail,
        "instance": request.url.path,
        **extensions,
    }
    return JSONResponse(body, status_code=status, media_type=PROBLEM_JSON)


def snowflake_problem(exc: SnowflakeError) -> ProblemError:
    errno = exc.errno or 0
    status, slug, title = SNOWFLAKE_ERRORS.get(errno, (502, "snowflake-error", "Snowflake rejected the statement"))
    # The connector's message starts with "<code> (<sqlstate>): "; keep the text.
    message = (exc.raw_msg or exc.msg or str(exc)).strip()
    return ProblemError(
        status,
        slug,
        title,
        message,
        snowflake={"error_code": f"{errno:06d}", "sql_state": exc.sqlstate, "query_id": exc.sfqid},
    )


def install(app: FastAPI) -> None:
    @app.exception_handler(ProblemError)
    async def _problem(request: Request, exc: ProblemError) -> JSONResponse:
        return problem_response(request, exc.status, exc.slug, exc.title, exc.detail, **exc.extensions)

    @app.exception_handler(SnowflakeError)
    async def _snowflake(request: Request, exc: SnowflakeError) -> JSONResponse:
        problem = snowflake_problem(exc)
        log.warning("Snowflake error %s on %s: %s", exc.errno, request.url.path, problem.detail)
        return problem_response(
            request, problem.status, problem.slug, problem.title, problem.detail, **problem.extensions
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [{"loc": list(e["loc"]), "msg": e["msg"], "type": e["type"]} for e in exc.errors()]
        return problem_response(
            request, 422, "invalid-request", "Invalid request", "The request did not validate.", errors=errors
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        title = HTTPStatus(exc.status_code).phrase
        return problem_response(request, exc.status_code, title.lower().replace(" ", "-"), title, str(exc.detail))
