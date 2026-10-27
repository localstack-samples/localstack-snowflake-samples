"""Run dbt as a subprocess against a (possibly patched) copy of the project.

Patched copies stand in for a pull request branch: the demo and the tests
apply a patch from demos/ to a temporary copy, never to dbt/ itself.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from slim_ci_lab.settings import DBT_PROJECT_DIR, SAMPLE_ROOT, is_emulator

DBT = Path(sys.executable).with_name("dbt")


def targets() -> tuple[str, str]:
    """(production target, pull request target) for the configured TEST_TARGET."""
    return ("prod", "pr") if is_emulator() else ("cloud", "cloud")


def copy_project(dest: Path, source: Path = DBT_PROJECT_DIR) -> Path:
    """Copy the dbt project to `dest` (without build output) and return it."""
    shutil.copytree(source, dest, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
    return dest


def apply_patch(project_dir: Path, patch_file: Path) -> None:
    subprocess.run(
        ["patch", "--batch", "--forward", "--silent", "-p1", "-d", str(project_dir), "-i", str(patch_file.resolve())],
        check=True,
    )


def dbt_env(**overrides: str) -> dict[str, str]:
    env = dict(os.environ)
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR))
    key = env.get("SNOWFLAKE_PRIVATE_KEY_PATH") or ".secrets/rsa_key.p8"
    key_path = Path(key).expanduser()
    env["SNOWFLAKE_PRIVATE_KEY_PATH"] = str(key_path if key_path.is_absolute() else SAMPLE_ROOT / key_path)
    env.update(overrides)
    return env


def run_dbt(
    args: Sequence[str],
    project_dir: Path = DBT_PROJECT_DIR,
    env: Mapping[str, str] | None = None,
    echo: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run `dbt <args>` in `project_dir`. Output is captured (and echoed if asked)."""
    cmd = [str(DBT), "--no-use-colors", *args, "--project-dir", str(project_dir)]
    proc = subprocess.run(
        cmd,
        cwd=SAMPLE_ROOT,
        env=dict(env) if env is not None else dbt_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if echo:
        sys.stdout.write(proc.stdout)
        sys.stderr.write(proc.stderr)
    return proc
