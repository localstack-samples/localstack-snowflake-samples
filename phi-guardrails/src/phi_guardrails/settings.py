"""Connection settings, read from the environment in one place.

``TEST_TARGET`` picks the target:

* ``EMULATOR`` (default): the Snowflake-Next emulator on
  ``snowflake.localhost.localstack.cloud:${SNOWFLAKE_PORT:-4566}``. Only the
  port is configurable, so a stray cloud variable in the shell can never point
  an emulator run at a real account.
* ``SNOWFLAKE_CLOUD``: a real account, from the ``SNOWFLAKE_*`` variables.

Every connection uses key-pair (``SNOWFLAKE_JWT``) auth on both targets, so
the config that passes locally is the config that runs against a real account.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

import snowflake.connector
from snowflake.connector import SnowflakeConnection

SAMPLE_ROOT = Path(__file__).resolve().parents[2]
EMULATOR_HOST = "snowflake.localhost.localstack.cloud"
QUERY_TAG = "phi-guardrails"

# The connector's login telemetry probes for cloud platforms (it builds a boto3
# client to look for AWS credentials, among others), which costs seconds per
# process and tells Snowflake nothing useful about a test run. Opt out unless
# the caller decided otherwise.
PLATFORM_DETECTION_ENV = "SNOWFLAKE_DISABLE_PLATFORM_DETECTION"
os.environ.setdefault(PLATFORM_DETECTION_ENV, "true")


@dataclass(frozen=True)
class Settings:
    target: str
    account: str
    user: str
    private_key_path: Path
    role: str
    warehouse: str
    database: str
    host: str | None = None
    port: int | None = None
    protocol: str | None = None

    @property
    def is_emulator(self) -> bool:
        return self.target == "EMULATOR"

    def with_database(self, database: str) -> Settings:
        return replace(self, database=database)

    def connect_kwargs(self, *, user: str | None = None, role: str | None = None) -> dict:
        kwargs: dict = {
            "account": self.account,
            "user": user or self.user,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": str(self.private_key_path),
            "role": role or self.role,
            "warehouse": self.warehouse,
            "session_parameters": {"QUERY_TAG": QUERY_TAG},
            "login_timeout": 60,
        }
        if self.is_emulator:
            # TLS is still verified; the OCSP revocation lookup is turned off
            # because the endpoint is local (127.0.0.1).
            kwargs.update(host=self.host, port=self.port, protocol=self.protocol, disable_ocsp_checks=True)
        return kwargs


def _env(name: str, default: str) -> str:
    """An environment variable, treating an empty value (``VAR=`` in .env) as unset."""
    return os.getenv(name) or default


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} must be set when TEST_TARGET=SNOWFLAKE_CLOUD")
    return value


def _key_path() -> Path:
    path = Path(_env("SNOWFLAKE_PRIVATE_KEY_PATH", ".secrets/rsa_key.p8"))
    return path if path.is_absolute() else SAMPLE_ROOT / path


def load() -> Settings:
    target = _env("TEST_TARGET", "EMULATOR").upper()
    database = _env("PHI_DATABASE", "PHI").upper()
    if target == "EMULATOR":
        return Settings(
            target=target,
            account="snowflake",
            user="SAMPLE_SVC",
            private_key_path=_key_path(),
            role="ACCOUNTADMIN",
            warehouse="PHI_WH",
            database=database,
            host=EMULATOR_HOST,
            port=int(_env("SNOWFLAKE_PORT", "4566")),
            protocol="https",
        )
    if target == "SNOWFLAKE_CLOUD":
        return Settings(
            target=target,
            account=_required("SNOWFLAKE_ACCOUNT"),
            user=_required("SNOWFLAKE_USER"),
            private_key_path=_key_path(),
            role=_env("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=_required("SNOWFLAKE_WAREHOUSE"),
            database=database,
        )
    raise RuntimeError(f"TEST_TARGET must be EMULATOR or SNOWFLAKE_CLOUD, got {target!r}")


def connect(settings: Settings, *, user: str | None = None, role: str | None = None) -> SnowflakeConnection:
    """Open a session as ``user`` with primary role ``role`` (defaults: the deploy identity)."""
    return snowflake.connector.connect(**settings.connect_kwargs(user=user, role=role))


def public_key_body(settings: Settings) -> str:
    """The deploy key's public half, in the form ``ALTER USER ... SET RSA_PUBLIC_KEY`` expects."""
    pub = settings.private_key_path.with_suffix(".pub")
    lines = pub.read_text(encoding="ascii").splitlines()
    return "".join(line for line in lines if not line.startswith("-----"))
