"""``make seed``: (re)create the inventory database as production stood before this change set.

Drops and recreates ``$INVENTORY_DATABASE`` (default ``RETAIL``), bootstraps
the live schema at version 1.0.0 with schemachange, and loads the
deterministic data set from ``release.dataset``. Re-running it gives the same
state, byte for byte.
"""

from __future__ import annotations

from release import dataset
from release import settings as cfg


def main() -> None:
    settings = cfg.load()
    data = dataset.seed(settings)
    counts = ", ".join(f"{table} {n}" for table, n in data.counts().items())
    print(f"Seeded {settings.database}.{settings.schema} at version 1.0.0: {counts}")


if __name__ == "__main__":
    main()
