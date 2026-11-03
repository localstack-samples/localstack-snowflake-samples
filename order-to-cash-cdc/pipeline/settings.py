"""Connection settings for the emulator and for real Snowflake, read in one place.

TEST_TARGET picks the defaults: EMULATOR (the local Snowflake-Next container)
or SNOWFLAKE_CLOUD (the SNOWFLAKE_* variables). Nothing else in the sample
branches on the target.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SAMPLE_ROOT = Path(__file__).resolve().parent.parent
EMULATOR_HOST = "snowflake.localhost.localstack.cloud"


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _required(name: str) -> str:
    value = _env(name)
    if value is None:
        raise RuntimeError(f"{name} must be set when TEST_TARGET=SNOWFLAKE_CLOUD")
    return value


@dataclass(frozen=True)
class Settings:
    target: str
    account: str
    host: str
    port: int
    protocol: str
    user: str
    private_key_path: Path
    role: str
    warehouse: str
    database: str
    # What the Kafka Connect container uses to reach Snowflake. On the
    # emulator this is the compose network alias on port 4566, not the host
    # port mapping.
    connector_url: str
    connector_role: str
    kafka_bootstrap: str
    connect_url: str

    @property
    def is_emulator(self) -> bool:
        return self.target == "EMULATOR"


def load() -> Settings:
    target = (_env("TEST_TARGET", "EMULATOR") or "EMULATOR").upper()
    if target not in ("EMULATOR", "SNOWFLAKE_CLOUD"):
        raise RuntimeError(f"TEST_TARGET must be EMULATOR or SNOWFLAKE_CLOUD, got {target!r}")

    key_path = Path(_env("SNOWFLAKE_PRIVATE_KEY_PATH", str(SAMPLE_ROOT / ".secrets" / "rsa_key.p8")))
    if not key_path.is_absolute():
        key_path = SAMPLE_ROOT / key_path
    common = {
        "user": _env("SNOWFLAKE_USER", "SAMPLE_SVC"),
        "private_key_path": key_path,
        "warehouse": _env("SNOWFLAKE_WAREHOUSE", "O2C_WH"),
        "database": _env("O2C_DATABASE", "O2C"),
        "connector_role": _env("O2C_CONNECTOR_ROLE", "O2C_INGEST"),
        "kafka_bootstrap": f"localhost:{_env('KAFKA_PORT', '19092')}",
        "connect_url": f"http://127.0.0.1:{_env('CONNECT_PORT', '18083')}",
    }

    if target == "EMULATOR":
        return Settings(
            target=target,
            account=_env("SNOWFLAKE_ACCOUNT", "snowflake"),
            host=EMULATOR_HOST,
            port=int(_env("SNOWFLAKE_PORT", "4566")),
            protocol="https",
            role=_env("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            connector_url=f"https://{EMULATOR_HOST}:4566",
            **common,
        )

    account = _required("SNOWFLAKE_ACCOUNT")
    host = _env("SNOWFLAKE_HOST", f"{account}.snowflakecomputing.com")
    return Settings(
        target=target,
        account=account,
        host=host,
        port=443,
        protocol="https",
        role=_required("SNOWFLAKE_ROLE"),
        connector_url=f"https://{host}:443",
        **common,
    )
