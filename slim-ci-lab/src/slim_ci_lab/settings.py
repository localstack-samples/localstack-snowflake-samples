"""Connection settings, read from the environment in one place.

TEST_TARGET picks the connection:

* EMULATOR (default): the Snowflake-Next emulator on
  snowflake.localhost.localstack.cloud:${SNOWFLAKE_PORT}. Cloud SNOWFLAKE_*
  variables are deliberately ignored, so a stray real-account variable in
  your shell can never send emulator runs to a billable account.
* SNOWFLAKE_CLOUD: a real account from SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
  SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_ROLE and SNOWFLAKE_WAREHOUSE.

Both use key-pair (SNOWFLAKE_JWT) auth, the configuration a real account's
service users need; `make up` generates a throwaway key pair for local runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import serialization

SAMPLE_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = SAMPLE_ROOT / "dbt"
CACHE_DIR = SAMPLE_ROOT / ".cache"

EMULATOR_HOST = "snowflake.localhost.localstack.cloud"
EMULATOR_ACCOUNT = "snowflake"
DEFAULT_USER = "SAMPLE_SVC"
DEFAULT_ROLE = "TRANSFORMER"
DEFAULT_WAREHOUSE = "TRANSFORMING"
DEFAULT_DATABASE = "SLIM_CI"
QUERY_TAG = "slim-ci-lab"


def test_target() -> str:
    value = os.getenv("TEST_TARGET", "EMULATOR").strip().upper()
    if value not in {"EMULATOR", "SNOWFLAKE_CLOUD"}:
        raise ValueError(f"TEST_TARGET must be EMULATOR or SNOWFLAKE_CLOUD, got {value!r}")
    return value


def is_emulator() -> bool:
    return test_target() == "EMULATOR"


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set when TEST_TARGET=SNOWFLAKE_CLOUD")
    return value


def _key_path() -> Path:
    raw = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
    if not raw:
        if not is_emulator():
            _required("SNOWFLAKE_PRIVATE_KEY_PATH")
        raw = ".secrets/rsa_key.p8"
    path = Path(raw).expanduser()
    return path if path.is_absolute() else SAMPLE_ROOT / path


@dataclass(frozen=True)
class Settings:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    private_key_path: Path
    host: str | None = None
    port: int | None = None

    def connect_kwargs(self, **overrides: Any) -> dict[str, Any]:
        """Keyword arguments for snowflake.connector.connect()."""
        key = serialization.load_pem_private_key(self.private_key_path.read_bytes(), password=None)
        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "role": self.role,
            "warehouse": self.warehouse,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key": key,
            "session_parameters": {"QUERY_TAG": QUERY_TAG},
        }
        if self.host:
            kwargs.update(host=self.host, port=self.port, protocol="https")
        kwargs.update(overrides)
        return kwargs


def load(database: str | None = None) -> Settings:
    database = database or os.getenv("DBT_DATABASE", DEFAULT_DATABASE)
    if is_emulator():
        return Settings(
            account=EMULATOR_ACCOUNT,
            user=DEFAULT_USER,
            role=DEFAULT_ROLE,
            warehouse=DEFAULT_WAREHOUSE,
            database=database,
            private_key_path=_key_path(),
            host=EMULATOR_HOST,
            port=int(os.getenv("SNOWFLAKE_PORT", "4566")),
        )
    return Settings(
        account=_required("SNOWFLAKE_ACCOUNT"),
        user=_required("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE", DEFAULT_ROLE),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
        database=database,
        private_key_path=_key_path(),
    )


def connect(database: str | None = None, **overrides: Any):
    """Open a connector session for the configured target."""
    import snowflake.connector

    return snowflake.connector.connect(**load(database).connect_kwargs(**overrides))
