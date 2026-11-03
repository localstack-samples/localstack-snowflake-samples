"""Connection and app settings, read from the environment in one place.

TEST_TARGET picks the defaults: EMULATOR (the local Snowflake-Next container)
or SNOWFLAKE_CLOUD (the SNOWFLAKE_* variables). Nothing else in the sample
branches on the target.

Two identities are configured:

* the *admin* identity (SNOWFLAKE_USER / SNOWFLAKE_ROLE) deploys schema,
  provisions tenants and seeds data; the API never uses it;
* the *app* identity (TB_APP_USER) is the API's service user. It never runs
  with an administrative role: each request runs under the tenant's own role.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

SAMPLE_ROOT = Path(__file__).resolve().parent.parent
EMULATOR_HOST = "snowflake.localhost.localstack.cloud"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    test_target: Literal["EMULATOR", "SNOWFLAKE_CLOUD"] = "EMULATOR"

    snowflake_account: str | None = None
    snowflake_host: str | None = None
    snowflake_port: int = 4566
    snowflake_user: str = "SAMPLE_SVC"
    snowflake_role: str | None = None
    snowflake_warehouse: str = "TB_WH"
    snowflake_private_key_path: Path = SAMPLE_ROOT / ".secrets" / "rsa_key.p8"

    tb_app_user: str = "TENANTBOARD_SVC"
    tb_database: str = "TENANTBOARD"
    tb_schema: str = "APP"
    # Upper bound on open per-role sessions; least recently used ones are closed.
    tb_max_role_connections: int = Field(default=16, ge=1, le=256)
    tb_cors_origins: list[str] = ["http://localhost:5173", "http://127.0.0.1:5173"]
    tb_max_upload_bytes: int = 1_000_000

    @field_validator("test_target", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @field_validator("snowflake_private_key_path")
    @classmethod
    def _absolute(cls, value: Path) -> Path:
        return value if value.is_absolute() else SAMPLE_ROOT / value

    @model_validator(mode="after")
    def _cloud_requirements(self) -> Settings:
        if self.test_target == "SNOWFLAKE_CLOUD":
            missing = [name for name in ("snowflake_account", "snowflake_role") if not getattr(self, name)]
            if missing:
                names = ", ".join(name.upper() for name in missing)
                raise ValueError(f"{names} must be set when TEST_TARGET=SNOWFLAKE_CLOUD")
        return self

    @property
    def is_emulator(self) -> bool:
        return self.test_target == "EMULATOR"

    @property
    def account(self) -> str:
        if self.is_emulator:
            # Init hooks run in the emulator's SNOWFLAKE account; accounts are isolated.
            return "snowflake"
        assert self.snowflake_account is not None  # checked by _cloud_requirements
        return self.snowflake_account

    @property
    def host(self) -> str:
        if self.is_emulator:
            return EMULATOR_HOST
        return self.snowflake_host or f"{self.account}.snowflakecomputing.com"

    @property
    def port(self) -> int:
        return self.snowflake_port if self.is_emulator else 443

    @property
    def admin_role(self) -> str:
        return self.snowflake_role or "ACCOUNTADMIN"


@lru_cache
def get_settings() -> Settings:
    return Settings()
