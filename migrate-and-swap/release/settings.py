"""Connection settings, read from the environment in one place.

``TEST_TARGET`` picks the target:

* ``EMULATOR`` (default): the Snowflake-Next emulator on
  ``snowflake.localhost.localstack.cloud:${SNOWFLAKE_PORT:-4566}``. Only the
  port is configurable, so a stray cloud variable in the shell can never point
  an emulator run at a real account.
* ``SNOWFLAKE_CLOUD``: a real account, from the ``SNOWFLAKE_*`` variables.

Every connection uses key-pair (``SNOWFLAKE_JWT``) auth. Using the same shape
everywhere means the config that passes locally is the config that runs
against a real account.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector import SnowflakeConnection

SAMPLE_ROOT = Path(__file__).resolve().parents[1]
EMULATOR_HOST = "snowflake.localhost.localstack.cloud"
QUERY_TAG = "migrate-and-swap"

# The connector's login telemetry probes for cloud platforms (it builds a boto3
# client to look for AWS credentials, among others), which costs seconds per
# process and tells Snowflake nothing useful about a release. Opt out unless
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
    schema: str
    host: str | None = None
    port: int | None = None
    protocol: str | None = None

    @property
    def is_emulator(self) -> bool:
        return self.target == "EMULATOR"

    @property
    def green_schema(self) -> str:
        """The candidate schema a release migrates; after a swap it holds the previous version."""
        return f"{self.schema}_GREEN"

    def with_database(self, database: str) -> Settings:
        return replace(self, database=database.upper())

    def private_key_der(self) -> bytes:
        """The private key as DER bytes.

        The connector's ``private_key_file`` option refuses key files that are
        group- or world-writable, which is every file on some filesystems
        (a WSL ``/mnt/c`` checkout, for one). Loading the key here works on any
        filesystem and keeps the key out of the process environment.
        """
        key = serialization.load_pem_private_key(self.private_key_path.read_bytes(), password=None)
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def connect_kwargs(self) -> dict:
        kwargs: dict = {
            "account": self.account,
            "user": self.user,
            "authenticator": "SNOWFLAKE_JWT",
            "private_key": self.private_key_der(),
            "role": self.role,
            "warehouse": self.warehouse,
            "session_parameters": {"QUERY_TAG": QUERY_TAG},
            "login_timeout": 60,
        }
        if self.is_emulator:
            # TLS is still verified. The OCSP revocation lookup is skipped for
            # the local certificate of a 127.0.0.1 endpoint.
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
    database = _env("INVENTORY_DATABASE", "RETAIL").upper()
    schema = _env("INVENTORY_SCHEMA", "INVENTORY").upper()
    if target == "EMULATOR":
        return Settings(
            target=target,
            account="snowflake",
            user="SAMPLE_SVC",
            private_key_path=_key_path(),
            role="ACCOUNTADMIN",
            warehouse="RELEASE_WH",
            database=database,
            schema=schema,
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
            schema=schema,
        )
    raise RuntimeError(f"TEST_TARGET must be EMULATOR or SNOWFLAKE_CLOUD, got {target!r}")


def connect(settings: Settings) -> SnowflakeConnection:
    """Open a session as the release identity. Callers use fully qualified names."""
    return snowflake.connector.connect(**settings.connect_kwargs())
