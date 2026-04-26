"""Heavy severe + ECAPE panel tool.

Wraps `heavy_panel_hour`, which generates severe and ECAPE map families
together from one shared heavy thermodynamic load. Use this when you
intentionally want the multi-product severe panel (e.g. an end-of-day
case-summary plate) — for individual products call `wx_render_recipe` or
`wx_ecape` instead, which is cheaper.
"""
from __future__ import annotations

from pathlib import Path

from ..geo import resolve_region
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run


def severe_panel(
    env: RustwxEnv,
    *,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    region: str | None = None,
    location: str | dict | tuple | None = None,
    source: str = "aws",
    surface_product: str | None = None,
    pressure_product: str | None = None,
    allow_large_heavy_domain: bool = False,
    out_dir: str | None = None,
    timeout: int = 1200,
) -> dict:
    """Generate the severe + ECAPE panel for a region."""
    binary = "heavy_panel_hour"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}

    if run_str == "latest":
        date, cycle = resolve_latest_run(model)
    else:
        date, cycle = parse_run(run_str)

    chosen_region = resolve_region(region, location, allow_extended=True)
    out_root = Path(out_dir) if out_dir else (
        env.out_root / "severe_panel" /
        f"{model}_{date}_{cycle:02d}z_f{forecast_hour:03d}_{chosen_region}"
    )

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        "--region", chosen_region,
        "--cache-dir", str(env.cache_dir.resolve()),
    ]
    if surface_product:
        args.extend(["--surface-product", surface_product])
    if pressure_product:
        args.extend(["--pressure-product", pressure_product])
    if allow_large_heavy_domain:
        args.append("--allow-large-heavy-domain")

    result = run(env, binary, args, out_dir=out_root, timeout=timeout)
    return {
        "ok": result.ok,
        "model": model,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "region": chosen_region,
        "out_dir": str(out_root),
        "pngs": [str(p) for p in result.pngs],
        "manifests": [str(p) for p in result.manifests],
        "png_count": len(result.pngs),
        "elapsed_s": round(result.seconds, 2),
        "stderr_tail": result.stderr.splitlines()[-8:] if result.stderr else [],
    }
