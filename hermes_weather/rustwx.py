"""rustwx integration layer.

The plugin's primary path is the rustwx Python API (agent-v1 contract):

    pip install rustwx>=0.4.4
    import rustwx
    rustwx.agent_capabilities_json()
    rustwx.list_domains_json()
    rustwx.render_maps_json(request_json)

That covers all map rendering — direct, light derived, heavy ECAPE, and
HRRR windowed products — with no Rust toolchain on the user's machine.

A handful of specialty paths (sounding, cross sections, single-point
ECAPE profile probe, full-grid ECAPE research swath, radar export) aren't in the
formal agent-v1 contract yet. Those tools fall back to optional rustwx
proof binaries discovered on disk via HERMES_RUSTWX_BIN_DIR / PATH; if
the binaries aren't built, the corresponding MCP tools degrade with a
clear error rather than crashing the server.
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"
EXE = ".exe" if IS_WINDOWS else ""

# Optional proof binaries used only when an MCP tool's path isn't covered
# by the agent-v1 contract yet. None of these are required for the bulk
# of the plugin (maps, ECAPE, windowed, heavy panels).
OPTIONAL_BINARIES = [
    "sounding_plot",                 # native skew-T renderer
    "hrrr_pressure_volume_store",    # HRRR pressure VolumeStore builder
    "volume_store_cross_section_render",  # fast VolumeStore cross-section renderer
    "radar_export",                  # native Rust NEXRAD Level-II renderer
    "hrrr_ecape_profile_probe",      # single-point ECAPE diagnostics
    "hrrr_ecape_grid_research",      # swath-scale ECAPE statistics
    "hrrr_ecape_ratio_display",      # legacy ratio panel (render_maps_json now covers single recipes)
    "rustwx-cli",                    # legacy registry CLI
]

DEFAULT_SEARCH_PATHS = [
    Path.home() / "rustwx" / "target" / "release",
    Path.home() / "rustwx" / "target" / "debug",
    Path.home() / ".cargo" / "bin",
    Path("/usr/local/bin"),
    Path("/opt/rustwx/bin"),
]


@dataclass
class RustwxEnv:
    """Resolved Python module + optional binary paths + cache/output roots.

    `module_available` and `module_version` describe the rustwx PyPI
    package state — that's the primary path for maps. `binaries` lists
    any optional proof binaries on disk for the specialty paths.
    """
    module_available: bool
    module_version: str | None
    capabilities: dict | None
    binaries: dict[str, Path] = field(default_factory=dict)
    bin_dir: Path | None = None
    cache_dir: Path = field(default_factory=lambda: Path.cwd() / "cache" / "rustwx")
    out_root: Path = field(default_factory=lambda: Path.cwd() / "outputs")

    def has_binary(self, name: str) -> bool:
        return name in self.binaries

    def require_binary(self, name: str) -> Path:
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
            + ". This is an optional binary used by specialty tools (sounding,"
              " cross sections, VolumeStore cross sections, ECAPE profile probe,"
              " ECAPE grid research)."
              " Build it with: cargo build --release --bin "
            + name
            + ", or set HERMES_RUSTWX_BIN_DIR to your rustwx target/release dir."
        )
        super().__init__(msg)


# ── Module discovery ────────────────────────────────────────────────────


def _probe_module() -> tuple[bool, str | None, dict | None]:
    """Try `import rustwx` and pull capabilities. Returns (available, version, caps)."""
    try:
        import rustwx  # noqa: F401
    except Exception:
        return False, None, None
    version: str | None = None
    caps: dict | None = None
    try:
        # rustwx exposes version via package metadata; fall back to capabilities
        try:
            from importlib.metadata import version as _v
            version = _v("rustwx")
        except Exception:
            version = None
        caps_json = rustwx.agent_capabilities_json()
        caps = json.loads(caps_json)
        if version is None:
            version = caps.get("version") or caps.get("package")
    except Exception:
        pass
    return True, version, caps


def _candidate_dirs() -> list[Path]:
    """Search order for optional binaries: env var → common locations."""
    out: list[Path] = []
    env_dir = os.environ.get("HERMES_RUSTWX_BIN_DIR")
    if env_dir:
        out.append(Path(env_dir))
    out.extend(DEFAULT_SEARCH_PATHS)
    return out


def discover() -> RustwxEnv:
    """Locate rustwx Python module + any optional binaries.

    Always succeeds — check `.module_available` to decide whether agent-v1
    paths are usable, and `.has_binary(name)` for specialty paths.
    """
    module_available, module_version, capabilities = _probe_module()

    binaries: dict[str, Path] = {}
    bin_dir: Path | None = None

    for d in _candidate_dirs():
        if not d.exists():
            continue
        found_any = False
        for name in OPTIONAL_BINARIES:
            p = d / f"{name}{EXE}"
            if p.exists() and name not in binaries:
                binaries[name] = p
                found_any = True
        if found_any and bin_dir is None:
            bin_dir = d

    for name in OPTIONAL_BINARIES:
        if name in binaries:
            continue
        path_hit = shutil.which(name)
        if path_hit:
            binaries[name] = Path(path_hit)

    cache_root = Path(os.environ.get(
        "HERMES_CACHE_DIR", Path.cwd() / "cache" / "rustwx"
    ))
    out_root = Path(os.environ.get(
        "HERMES_OUT_DIR", Path.cwd() / "outputs"
    ))

    return RustwxEnv(
        module_available=module_available,
        module_version=module_version,
        capabilities=capabilities,
        binaries=binaries,
        bin_dir=bin_dir,
        cache_dir=cache_root,
        out_root=out_root,
    )


# ── Agent-v1 wrappers ──────────────────────────────────────────────────


def render_maps(env: RustwxEnv, request: dict) -> dict:
    """Call rustwx.render_maps_json with a Python dict request.

    Returns the parsed JSON response. Raises RuntimeError if the rustwx
    module isn't installed or the call fails.
    """
    if not env.module_available:
        raise RuntimeError(
            "rustwx Python module not installed. Install with: "
            "pip install 'rustwx>=0.4'"
        )
    import rustwx
    payload = json.dumps(request, default=str)
    result_json = rustwx.render_maps_json(payload)
    return json.loads(result_json)


def list_domains(env: RustwxEnv, *, kind: str | None = None,
                 limit: int | None = None) -> dict:
    """Call rustwx.list_domains_json with optional filters."""
    if not env.module_available:
        return {"count": 0, "domains": [], "error": "rustwx module not installed"}
    import rustwx
    return json.loads(rustwx.list_domains_json(kind=kind, limit=limit))


def render_sounding_column(env: RustwxEnv, column: dict, output_path: str | Path) -> dict:
    """Call rustwx.render_sounding_column_json. `column` is a pre-extracted
    profile column dict; `output_path` is where the PNG lands.
    """
    if not env.module_available:
        raise RuntimeError("rustwx Python module not installed")
    import rustwx
    payload = json.dumps(column, default=str)
    result = rustwx.render_sounding_column_json(payload, str(output_path))
    return json.loads(result) if result else {"output_path": str(output_path)}


# ── Optional binary subprocess (specialty paths) ───────────────────────


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
    """Subprocess an optional rustwx binary. Used only for specialty paths
    not yet covered by the agent-v1 contract.
    """
    exe = env.require_binary(binary)
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
        binary=binary, args=cmd, returncode=proc.returncode, seconds=elapsed,
        stdout=proc.stdout, stderr=proc.stderr,
        out_dir=out_dir, pngs=pngs, manifests=manifests,
    )


# ── Run-string helpers ─────────────────────────────────────────────────


def parse_run(run_str: str) -> tuple[str, int]:
    """Convert 'YYYY-MM-DD/HHz' / 'YYYYMMDD/HH' / 'YYYYMMDDHH' to (date, cycle)."""
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


def available_forecast_hours(
    model: str = "hrrr",
    date_yyyymmdd: str | None = None,
    cycle_utc: int | None = None,
    *,
    product: str = "sfc",
    source: str = "nomads",
) -> list[int]:
    """Return advertised forecast hours for a model/product/cycle.

    This mirrors the CA Fire runner's policy: resolve against live availability
    instead of assuming that the newest cycle already has every requested hour.
    """
    if date_yyyymmdd is None or cycle_utc is None:
        raise ValueError("date_yyyymmdd and cycle_utc are required")

    # NOMADS directory listings are very fast and avoid expensive per-cycle
    # resolver work for the common HRRR path.
    if model.lower() == "hrrr" and source.lower() == "nomads":
        try:
            return _available_hrrr_nomads_hours(date_yyyymmdd, int(cycle_utc), product)
        except Exception:
            pass

    try:
        import rustwx

        if hasattr(rustwx, "available_forecast_hours_json"):
            payload = json.loads(
                rustwx.available_forecast_hours_json(
                    model,
                    date_yyyymmdd,
                    int(cycle_utc),
                    product,
                    source,
                )
            )
            if isinstance(payload, list):
                return sorted({int(hour) for hour in payload})
            return sorted({int(hour) for hour in payload.get("forecast_hours", [])})
    except Exception:
        pass

    # Lightweight HRRR fallback for environments where the wheel is older than
    # the availability API. It is intentionally conservative and only knows the
    # public NOMADS filename patterns.
    if model.lower() != "hrrr":
        return []

    return _available_hrrr_nomads_hours(date_yyyymmdd, int(cycle_utc), product)


def _available_hrrr_nomads_hours(date_yyyymmdd: str, cycle_utc: int, product: str) -> list[int]:
    import re
    import requests

    product_key = {
        "prs": "wrfprs",
        "pressure": "wrfprs",
        "nat": "wrfnat",
        "native": "wrfnat",
        "sfc": "wrfsfc",
        "surface": "wrfsfc",
    }.get(product.lower(), "wrfsfc")
    url = f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{date_yyyymmdd}/conus/"
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    pattern = re.compile(rf"hrrr\.t{int(cycle_utc):02d}z\.{product_key}f(\d{{2}})\.grib2")
    return sorted({int(hour) for hour in pattern.findall(response.text)})


def resolve_latest_run(model: str = "hrrr", source: str = "nomads") -> tuple[str, int]:
    """Resolve 'latest' by probing NOMADS HRRR directory.

    rustwx provides latest_run_json() too — when stable for all models
    we'll route through that instead.
    """
    import re
    import requests

    model_key = model.lower()
    if model_key == "gfs":
        now = datetime.now(timezone.utc)
        seen: set[tuple[str, int]] = set()
        for hour_offset in range(0, 96):
            candidate = now - timedelta(hours=hour_offset)
            cycle = (candidate.hour // 6) * 6
            date = candidate.strftime("%Y%m%d")
            key = (date, cycle)
            if key in seen:
                continue
            seen.add(key)
            url = (
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
                f"gfs.{date}/{cycle:02d}/atmos/gfs.t{cycle:02d}z.pgrb2.0p25.f000"
            )
            try:
                if requests.head(url, timeout=10).ok:
                    return date, cycle
            except Exception:
                continue
        return _scheduled_latest_cycle(model_key)

    bases = {"hrrr": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"}
    if model_key not in bases:
        return _scheduled_latest_cycle(model_key)

    base = bases[model_key]
    now = datetime.now(timezone.utc)
    pattern = re.compile(r"hrrr\.t(\d{2})z\.wrf(?:sfc|prs|nat)f\d{2}\.grib2")

    for day_offset in range(2):
        date = now - timedelta(days=day_offset)
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

    fb = now - timedelta(hours=3)
    return fb.strftime("%Y%m%d"), fb.hour


def _scheduled_latest_cycle(model: str) -> tuple[str, int]:
    """Fallback resolver that snaps non-HRRR models to valid cycle hours."""
    model_key = model.lower()
    interval_hours = {
        "gfs": 6,
        "ecmwf-open-data": 12,
        "ecmwf": 12,
        "ecmwf_ifs": 12,
        "rrfs-a": 1,
        "rrfs_a": 1,
        "wrf-gdex": 1,
        "wrf_gdex": 1,
    }.get(model_key, 6)
    latency_hours = {
        "gfs": 5,
        "ecmwf-open-data": 9,
        "ecmwf": 9,
        "ecmwf_ifs": 9,
        "rrfs-a": 2,
        "rrfs_a": 2,
        "wrf-gdex": 0,
        "wrf_gdex": 0,
    }.get(model_key, 4)
    available = datetime.now(timezone.utc) - timedelta(hours=latency_hours)
    cycle = (available.hour // interval_hours) * interval_hours
    return available.strftime("%Y%m%d"), cycle


def resolve_latest_run_for_hours(
    model: str = "hrrr",
    *,
    source: str = "nomads",
    forecast_hours: list[int] | tuple[int, ...] | None = None,
    product: str = "sfc",
    synoptic_only: bool = False,
    lookback_hours: int = 96,
) -> tuple[str, int]:
    """Resolve the newest cycle that actually advertises the requested hours.

    For HRRR requests beyond f018, this automatically searches only the
    extended 00/06/12/18 UTC cycles, matching the production CA Fire runner.
    """
    wanted = sorted({int(hour) for hour in (forecast_hours or [0])})
    if any(hour < 0 for hour in wanted):
        raise ValueError(f"forecast_hours must be non-negative, got {wanted}")
    if model.lower() != "hrrr":
        return resolve_latest_run(model, source)

    extended_cycles = {0, 6, 12, 18}
    require_synoptic = synoptic_only or (max(wanted) > 18)
    now = datetime.now(timezone.utc)
    errors: list[str] = []
    saw_availability = False
    seen: set[tuple[str, int]] = set()

    for hour_offset in range(0, lookback_hours + 1):
        cycle_time = now - timedelta(hours=hour_offset)
        cycle = cycle_time.hour
        if require_synoptic and cycle not in extended_cycles:
            continue
        key = (cycle_time.strftime("%Y%m%d"), cycle)
        if key in seen:
            continue
        seen.add(key)
        date, cycle = key
        try:
            available = set(
                available_forecast_hours(
                    model,
                    date,
                    cycle,
                    product=product,
                    source=source,
                )
            )
            saw_availability = True
        except Exception as exc:
            errors.append(f"{date} {cycle:02d}Z {product}: {exc}")
            continue
        if all(hour in available for hour in wanted):
            return date, cycle

    if not saw_availability:
        # Preserve the old behavior if availability listing is unreachable.
        return resolve_latest_run(model, source)

    raise RuntimeError(
        "no latest run found with requested forecast hours "
        f"{wanted} for {model}/{product}/{source}; checked {lookback_hours}h"
        + (f"; errors: {'; '.join(errors[:4])}" if errors else "")
    )
