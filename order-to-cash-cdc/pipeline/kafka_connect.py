"""Kafka topics and the Snowflake sink connector (Kafka Connect REST API).

CLI (used by `make up`):
    python -m pipeline.kafka_connect setup     # create orders.cdc, register the sink
    python -m pipeline.kafka_connect status
"""

from __future__ import annotations

import copy
import json
import sys
import time

import requests
from confluent_kafka.admin import AdminClient, NewTopic

from pipeline.settings import SAMPLE_ROOT, Settings, load

SINK_TEMPLATE = SAMPLE_ROOT / "connect" / "snowflake-sink.json"
TOPIC_PARTITIONS = 3


def sink_config(settings: Settings, topic: str, database: str, table: str = "ORDERS_CDC") -> dict:
    """The committed connector config, pointed at `topic` -> `database`.RAW.`table`."""
    config = copy.deepcopy(json.loads(SINK_TEMPLATE.read_text(encoding="utf-8"))["config"])
    config.update({
        "topics": topic,
        "snowflake.topic2table.map": f"{topic}:{table}",
        "snowflake.database.name": database,
        "snowflake.url.name": settings.connector_url,
        "snowflake.user.name": settings.user,
        "snowflake.role.name": settings.connector_role,
    })
    return config


def default_connector_name() -> str:
    return json.loads(SINK_TEMPLATE.read_text(encoding="utf-8"))["name"]


class Topics:
    def __init__(self, settings: Settings) -> None:
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})

    def create(self, name: str, partitions: int = TOPIC_PARTITIONS) -> None:
        futures = self.admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
        for future in futures.values():
            try:
                future.result(timeout=30)
            except Exception as exc:  # noqa: BLE001 - KafkaException carries the error code
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise

    def delete(self, name: str) -> None:
        for future in self.admin.delete_topics([name], operation_timeout=30).values():
            try:
                future.result(timeout=30)
            except Exception as exc:  # noqa: BLE001
                if "UNKNOWN_TOPIC_OR_PART" not in str(exc):
                    raise


class ConnectClient:
    def __init__(self, settings: Settings) -> None:
        self.url = settings.connect_url

    def wait_ready(self, timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            try:
                plugins = requests.get(f"{self.url}/connector-plugins", timeout=5).json()
                if any(p["class"].endswith("SnowflakeSinkConnector") for p in plugins):
                    return
                last = f"plugins: {[p['class'] for p in plugins]}"
            except (requests.RequestException, ValueError) as exc:
                last = str(exc)
            time.sleep(2)
        raise TimeoutError(f"Kafka Connect at {self.url} not ready after {timeout}s ({last}); "
                           "check 'docker compose logs connect'")

    def put(self, name: str, config: dict) -> None:
        resp = requests.put(f"{self.url}/connectors/{name}/config", json=config, timeout=30)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"registering connector {name} failed: {resp.status_code} {resp.text}")

    def status(self, name: str) -> dict | None:
        resp = requests.get(f"{self.url}/connectors/{name}/status", timeout=10)
        return resp.json() if resp.status_code == 200 else None

    def wait_running(self, name: str, timeout: float = 120) -> None:
        deadline = time.monotonic() + timeout
        last: dict | None = None
        while time.monotonic() < deadline:
            last = self.status(name)
            if last:
                states = [last["connector"]["state"]] + [t["state"] for t in last.get("tasks", [])]
                if "FAILED" in states:
                    trace = next((t.get("trace", "") for t in last.get("tasks", []) if t["state"] == "FAILED"), "")
                    raise RuntimeError(f"connector {name} FAILED: {trace[:2000]}")
                if last.get("tasks") and all(s == "RUNNING" for s in states):
                    return
            time.sleep(1)
        raise TimeoutError(f"connector {name} not RUNNING after {timeout}s: {last}")

    def delete(self, name: str) -> None:
        resp = requests.delete(f"{self.url}/connectors/{name}", timeout=30)
        if resp.status_code not in (204, 404):
            raise RuntimeError(f"deleting connector {name} failed: {resp.status_code} {resp.text}")


def setup(settings: Settings) -> None:
    """Create the orders.cdc topic and register the sink for O2C.RAW.ORDERS_CDC."""
    from snowflake.connector.errors import ProgrammingError

    from pipeline import snowflake_db

    # Init hooks run after the emulator starts answering; wait for the landing table.
    with snowflake_db.connect(settings) as conn:
        deadline = time.monotonic() + 120
        while True:
            try:
                snowflake_db.query(conn, f"SELECT COUNT(*) FROM {settings.database}.RAW.ORDERS_CDC")
                break
            except ProgrammingError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(2)
    connect = ConnectClient(settings)
    connect.wait_ready()
    config = sink_config(settings, "orders.cdc", settings.database)
    Topics(settings).create(config["topics"])
    name = default_connector_name()
    connect.put(name, config)
    connect.wait_running(name)
    print(f"Connector {name} RUNNING: {config['topics']} -> {settings.database}.RAW.ORDERS_CDC")


def main() -> None:
    settings = load()
    command = sys.argv[1] if len(sys.argv) > 1 else "setup"
    if command == "setup":
        setup(settings)
    elif command == "status":
        print(json.dumps(ConnectClient(settings).status(default_connector_name()), indent=2))
    else:
        sys.exit(f"unknown command {command!r} (setup | status)")


if __name__ == "__main__":
    main()
