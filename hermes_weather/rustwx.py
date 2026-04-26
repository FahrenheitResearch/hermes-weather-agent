"""rustwx subprocess layer — binary discovery + safe execution.

Each rustwx-cli proof binary (`derived_batch`, `direct_batch`,
`hrrr_ecape_ratio_display`, `cross_section_proof`, `forecast_now`, etc.) is
its own standalone executable. This module finds them, sets up the right
environment, invokes them with timeouts, and harvests PNG outputs plus JSON
manifests from `--out-dir`.

The main `rustwx-cli` binary (registry/fetch) is treated as optional — if
absent, model and recipe catalogs fall back to a hard-coded mirror.
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"
EXE = ".exe" if IS_WINDOWS else ""

# Proof binaries we wrap as MCP tools. Order is informational — discovery
# walks all of them and reports which exist.
KNOWN_BINARIES = [
    # registry / discovery
    "rustwx-cli",                  # model & source registry, idx-byte-range fetch
    "product_catalog",             # live product inventory (109+ products)
    # multi-model rendering
    "direct_batch",                # direct GRIB recipes (any registered model)
    "derived_batch",               # derived thermodynamic recipes (any model)
    # HRRR-specialised rendering
    "hrrr_direct_batch",           # HRRR-only direct (superset of direct_batch)
    "hrrr_derived_batch",          # HRRR-only derived (full ECAPE recipe set)
    "hrrr_windowed_batch",         # QPF / UH time-window products
    # heavy multi-product panels
    "heavy_panel_hour",            # severe + ECAPE families from one heavy load
    "hrrr_severe_proof",           # severe-weather proof gallery
    # ECAPE first-class binaries
    "hrrr_ecape_ratio_display",    # MLECAPE + ECAPE/CAPE ratio display
    "hrrr_ecape_grid_research",    # full-grid ECAPE research over a swath
    "hrrr_ecape_profile_probe",    # point ECAPE diagnostics
    # vertical / orchestration
    "cross_section_proof",         # vertical cross sections
    "forecast_now",                # multi-model multi-hour orchestrator
    "production_runner",           # operational scheduler
    # dataset / region helpers
    "hrrr_dataset_export",         # ML-friendly HRRR dataset export
    "hrrr_us_region_hours",        # parallel multi-region multi-hour runs
    "hrrr_region_city_gallery",    # city-centred crops for a region
    "non_ecape_hour",              # all-model non-ECAPE hour pass (legacy fallback)
]

# Common install locations searched when HERMES_RUSTWX_BIN_DIR is unset.
DEFAULT_SEARCH_PATHS = [
    Path.home() / "rustwx" / "target" / "release",
    Path.home() / "rustwx" / "target" / "debug",
    Path.home() / ".cargo" / "bin",
    Path("/usr/local/bin"),
    Path("/opt/rustwx/bin"),
]

@dataclass
class RustwxEnv:
    """Resolved binary paths + runtime env for invoking rustwx tools."""
    bin_dir: Path | None
    binaries: dict[str, Path] = field(default_factory=dict)
    netcdf_dir: Path | None = None
    cache_dir: Path = field(default_factory=lambda: Path.cwd() / "cache" / "rustwx")
    out_root: Path = field(default_factory=lambda: Path.cwd() / "outputs")

    def has(self, name: str) -> bool:
        return name in self.binaries

    def require(self, name: str) -> Path:
        if name not in self.binaries:
            raise RustwxBinaryMissing(name, self.bin_dir)
        return self.binaries[name]

    def subprocess_env(self) -> dict[str, str]:
        return os.environ.copy()


class RustwxBinaryMissing(RuntimeError):
    def __init__(self, name: str, bin_dir: Path | None):
        self.binary_name = name
        self.bin_dir = bin_dir
        msg = (
            f"rustwx binary '{name}' not found"
            + (f" in {bin_dir}" if bin_dir else "")
            + ". Set HERMES_RUSTWX_BIN_DIR to your rustwx target/release directory, "
            f"or build the binary with: cargo build --release --bin {name}"
        )
        super().__init__(msg)


def _find_netcdf_dir() -> Path | None:
    # Current rustwx uses netcrust for WRF NetCDF4 reads, so the agent does not
    # need to patch PATH with a native netCDF runtime.
    return None


def _candidate_dirs() -> list[Path]:
    """Search order: env var → common locations → PATH."""
    out: list[Path] = []
    env_dir = os.environ.get("HERMES_RUSTWX_BIN_DIR")
    if env_dir:
        out.append(Path(env_dir))
    out.extend(DEFAULT_SEARCH_PATHS)
    return out


def discover() -> RustwxEnv:
    """Locate rustwx binaries. Always succeeds; check `.binaries`."""
    binaries: dict[str, Path] = {}
    bin_dir: Path | None = None

    for d in _candidate_dirs():
        if not d.exists():
            continue
        found_any = False
        for name in KNOWN_BINARIES:
            p = d / f"{name}{EXE}"
            if p.exists() and name not in binaries:
                binaries[name] = p
                found_any = True
        if found_any and bin_dir is None:
            bin_dir = d

    # Fall back to PATH for anything still unresolved
    for name in KNOWN_BINARIES:
        if name in binaries:
            continue
        path_hit = shutil.which(name)
        if path_hit:
            binaries[name] = Path(path_hit)

    cache_root = Path(os.environ.get("HERMES_CACHE_DIR", Path.cwd() / "cache" / "rustwx"))
    out_root = Path(os.environ.get("HERMES_OUT_DIR", Path.cwd() / "outputs"))

    return RustwxEnv(
        bin_dir=bin_dir,
        binaries=binaries,
        netcdf_dir=_find_netcdf_dir(),
        cache_dir=cache_root,
        out_root=out_root,
    )


@dataclass
class RunResult:
    binary: str
    args: list[str]
    returncode: int
    seconds: float
    stdout: str
    stderr: str
    out_dir: Path | None
    pngs: list[Path]
    manifests: list[Path]

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def to_payload(self) -> dict:
        return {
            "ok": self.ok,
            "binary": self.binary,
            "returncode": self.returncode,
            "seconds": round(self.seconds, 3),
            "out_dir": str(self.out_dir) if self.out_dir else None,
            "pngs": [str(p) for p in self.pngs],
            "manifests": [str(p) for p in self.manifests],
            # Tail of stderr is more useful than full output for diagnosis
            "stderr_tail": self.stderr.splitlines()[-12:] if self.stderr else [],
        }


def run(
    env: RustwxEnv,
    binary: str,
    args: list[str],
    *,
    out_dir: Path | None = None,
    timeout: int = 600,
    cwd: Path | None = None,
) -> RunResult:
    """Invoke a rustwx binary, returning result + harvested artifacts.

    `out_dir`, when provided, is appended as `--out-dir <out_dir>` and used
    for PNG/manifest discovery on completion.
    """
    exe = env.require(binary)
    cmd: list[str] = [str(exe), *args]
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd.extend(["--out-dir", str(out_dir.resolve())])

    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env.subprocess_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    elapsed = time.time() - started

    pngs: list[Path] = []
    manifests: list[Path] = []
    if out_dir is not None and out_dir.exists():
        pngs = sorted(out_dir.rglob("*.png"))
        manifests = sorted(out_dir.rglob("*manifest*.json"))

    return RunResult(
        binary=binary,
        args=cmd,
        returncode=proc.returncode,
        seconds=elapsed,
        stdout=proc.stdout,
        stderr=proc.stderr,
        out_dir=out_dir,
        pngs=pngs,
        manifests=manifests,
    )


def run_json(
    env: RustwxEnv,
    binary: str,
    args: list[str],
    *,
    timeout: int = 60,
) -> dict | list:
    """Invoke a binary that emits JSON to stdout (rustwx-cli list/show/etc).

    Raises RuntimeError if the binary returns non-zero or stdout is not JSON.
    """
    exe = env.require(binary)
    proc = subprocess.run(
        [str(exe), *args],
        env=env.subprocess_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{binary} {args} failed (rc={proc.returncode}): {proc.stderr.strip()[:400]}"
        )
    out = proc.stdout.strip()
    if not out:
        return {}
    try:
        return json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{binary} stdout was not JSON: {exc}\n--- stdout ---\n{out[:400]}") from exc


def parse_run(run_str: str) -> tuple[str, int]:
    """Convert the agent-facing run string to (date_yyyymmdd, cycle_int).

    Accepts:
      "YYYY-MM-DD/HHz"   e.g. "2026-04-25/12z"
      "YYYYMMDD/HH"      e.g. "20260425/12"
      "YYYYMMDDHH"       e.g. "2026042512"
    """
    s = run_str.strip().lower().rstrip("z")
    if "/" in s:
        date_part, cycle_part = s.split("/", 1)
    elif len(s) == 10 and s.isdigit():
        date_part, cycle_part = s[:8], s[8:]
    else:
        raise ValueError(f"Bad run string: {run_str!r} (expected YYYY-MM-DD/HHz)")
    date = date_part.replace("-", "")
    if len(date) != 8 or not date.isdigit():
        raise ValueError(f"Bad date in run string: {run_str!r}")
    try:
        cycle = int(cycle_part)
    except ValueError as exc:
        raise ValueError(f"Bad cycle in run string: {run_str!r}") from exc
    if not 0 <= cycle <= 23:
        raise ValueError(f"Cycle must be 0-23, got {cycle}")
    return date, cycle


def resolve_latest_run(model: str = "hrrr") -> tuple[str, int]:
    """Resolve 'latest' by probing NOMADS directory listings.

    Works without rustwx-cli; uses the same logic as the prior agent.
    """
    import re
    from datetime import datetime, timedelta, timezone

    import requests

    bases = {
        "hrrr": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod",
    }
    if model not in bases:
        # For now we only auto-resolve HRRR — other models can fall back to wallclock
        now = datetime.now(timezone.utc) - timedelta(hours=4)
        return now.strftime("%Y%m%d"), now.hour

    base = bases[model]
    now = datetime.now(timezone.utc)
    pattern = re.compile(r"hrrr\.t(\d{2})z\.wrf(?:sfc|prs|nat)f\d{2}\.grib2")

    for day_offset in range(2):
        date = now - __import__("datetime").timedelta(days=day_offset)
        date_str = date.strftime("%Y%m%d")
        try:
            r = requests.get(f"{base}/hrrr.{date_str}/conus/", timeout=15)
        except Exception:
            continue
        if not r.ok:
            continue
        hours = sorted(set(pattern.findall(r.text)))
        if hours:
            return date_str, int(hours[-1])

    fb = now - __import__("datetime").timedelta(hours=3)
    return fb.strftime("%Y%m%d"), fb.hour
