"""Pinned NYC TLC trip data: download, verify, downsample.

The full January 2024 yellow-taxi file has about 3 million rows. CI only needs
enough rows to exercise every model, so the loader keeps a deterministic
1-in-N extract: sort by every column (a total order, so ties are identical
rows), then take every Nth row. The same input always yields the same extract,
on any machine, so test expectations computed from it are stable.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from slim_ci_lab.settings import CACHE_DIR


@dataclass(frozen=True)
class TlcFile:
    url: str
    sha256: str

    @property
    def name(self) -> str:
        return self.url.rsplit("/", 1)[-1]


YELLOW_2024_01 = TlcFile(
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    sha256="c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510",
)
SAMPLE_EVERY = 300


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(spec: TlcFile = YELLOW_2024_01, cache_dir: Path = CACHE_DIR / "tlc") -> Path:
    """Return the cached file, downloading it first if needed. Verifies SHA-256."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / spec.name
    if not target.exists():
        partial = target.with_suffix(".part")
        print(f"Downloading {spec.url}")
        with requests.get(spec.url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with partial.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    fh.write(chunk)
        partial.rename(target)
    actual = _sha256(target)
    if actual != spec.sha256:
        target.unlink()
        raise RuntimeError(
            f"SHA-256 mismatch for {spec.name}: expected {spec.sha256}, got {actual}. "
            "The cached copy was removed; re-run to download it again."
        )
    return target


def downsample(source: Path, every: int = SAMPLE_EVERY) -> Path:
    """Write (once) and return the deterministic 1-in-`every` extract of `source`."""
    target = source.with_name(f"{source.stem}.sample-1in{every}.parquet")
    if target.exists():
        return target
    table = pq.read_table(source)
    order = pc.sort_indices(table, sort_keys=[(name, "ascending") for name in table.column_names])
    picked = pc.take(order, pa.array(range(0, len(order), every)))
    sample = table.take(picked)
    partial = target.with_suffix(".part")
    pq.write_table(sample, partial)
    partial.rename(target)
    return target


def sample_file() -> Path:
    """The downsampled extract the pipeline loads (downloading the month if needed)."""
    return downsample(download())
