"""wx_fetch — selective GRIB2 fetch via rustwx-cli (idx-first by default).

Idx selective fetch downloads only the byte-ranges for the requested
variable patterns (e.g. "TMP:2 m above ground", "UGRD:10 m above ground").
This is dramatically faster than pulling whole HRRR/GFS files when you only
need a few fields. Set `full=True` to skip the idx and download the whole
GRIB2 (useful when you'll be running rustwx batch tools on it).

Requires the `rustwx-cli` binary. If you only have the proof binaries,
prefer wx_render_recipe / wx_ecape_* — those fetch internally.
"""
from __future__ import annotations

from pathlib import Path

import json
import subprocess

from ..rustwx import RustwxEnv, parse_run, resolve_latest_run


def _run_json(env: RustwxEnv, binary: str, args: list[str], *, timeout: int = 60):
    exe = env.require_binary(binary)
    proc = subprocess.run(
        [str(exe), *args], env=env.subprocess_env(),
        capture_output=True, text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{binary} {args} failed (rc={proc.returncode}): {proc.stderr.strip()[:400]}"
        )
    out = proc.stdout.strip()
    if not out:
        return {}
    return json.loads(out)


def fetch(
    env: RustwxEnv,
    *,
    model: str = "hrrr",
    run: str = "latest",
    forecast_hour: int = 0,
    product: str = "default",
    variables: list[str] | None = None,
    source: str | None = None,
    output: str | None = None,
    cache_dir: str | None = None,
    full: bool = False,
) -> dict:
    if not env.has_binary("rustwx-cli"):
        return {
            "ok": False,
            "error": (
                "rustwx-cli binary not found. Build it with:\n"
                "  cargo build --release --bin rustwx-cli\n"
                "then point HERMES_RUSTWX_BIN_DIR at the target/release dir, "
                "or use wx_render_recipe / wx_ecape_* (those fetch internally)."
            ),
        }

    if run == "latest":
        date, cycle = resolve_latest_run(model)
    else:
        date, cycle = parse_run(run)

    args = [
        "fetch", model, date, str(cycle), str(forecast_hour),
        product if product else "default",
    ]
    if source:
        args.extend(["--source", source])
    if not full and variables:
        for v in variables:
            args.extend(["--var", v])
    if output:
        args.extend(["--output", str(Path(output).resolve())])
    cdir = cache_dir or str(env.cache_dir.resolve())
    args.extend(["--cache-dir", cdir])

    try:
        result = _run_json(env, "rustwx-cli", args, timeout=600)
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "model": model,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "product": product,
        "source": result.get("source"),
        "url": result.get("url"),
        "output": result.get("output"),
        "bytes": result.get("bytes"),
        "cache_hit": result.get("cache_hit"),
        "cache_path": result.get("cache_path"),
        "selective": bool(variables) and not full,
    }


def latest(env: RustwxEnv, *, model: str = "hrrr") -> dict:
    """Return the latest available run for a model. Uses rustwx-cli if
    present, otherwise probes NOMADS directly."""
    if env.has_binary("rustwx-cli"):
        try:
            from datetime import datetime, timezone
            today = datetime.now(timezone.utc).strftime("%Y%m%d")
            result = _run_json(env, "rustwx-cli", ["latest", model, today])
            return {"ok": True, "model": model, "latest": result, "source": "rustwx-cli"}
        except Exception:
            pass
    date, cycle = resolve_latest_run(model)
    return {
        "ok": True,
        "model": model,
        "date": date,
        "cycle": cycle,
        "run": f"{date[:4]}-{date[4:6]}-{date[6:]}/{cycle:02d}z",
        "source": "nomads-probe",
    }
