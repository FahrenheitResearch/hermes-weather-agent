"""rustwx integration layer.

The plugin's primary path is the rustwx Python API (agent-v1 contract):

    pip install rustwx>=0.4
    import rustwx
    rustwx.agent_capabilities_json()
    rustwx.list_domains_json()
    rustwx.render_maps_json(request_json)

That covers all map rendering — direct, light derived, heavy ECAPE, and
HRRR windowed products — with no Rust toolchain on the user's machine.

A handful of specialty paths (sounding, cross sections, single-point
ECAPE profile probe, full-grid ECAPE research swath) aren't in the
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
from pathlib import Path
from typing import Any

IS_WINDOWS = platform.system() == "Windows"
EXE = ".exe" if IS_WINDOWS else ""

# Optional proof binaries used only when an MCP tool's path isn't covered
# by the agent-v1 contract yet. None of these are required for the bulk
# of the plugin (maps, ECAPE, windowed, heavy panels).
OPTIONAL_BINARIES = [
    "sounding_plot",                 # native skew-T renderer
    "cross_section_proof",           # vertical cross sections
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
              " cross sections, ECAPE profile probe, ECAPE grid research)."
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


def resolve_latest_run(model: str = "hrrr") -> tuple[str, int]:
    """Resolve 'latest' by probing NOMADS HRRR directory.

    rustwx provides latest_run_json() too — when stable for all models
    we'll route through that instead.
    """
    import re
    from datetime import datetime, timedelta, timezone
    import requests

    bases = {"hrrr": "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"}
    if model not in bases:
        now = datetime.now(timezone.utc) - timedelta(hours=4)
        return now.strftime("%Y%m%d"), now.hour

    base = bases[model]
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
