"""Cross-section tool — vertical slices through model fields.

Wraps `cross_section_proof`, which renders rustwx-cross-section output as
PNG. Accepts either a route preset (amarillo-chicago, kansas-city-chicago,
san-francisco-tahoe, etc.) or explicit start/end lat-lon.

Products: temperature, relative-humidity, specific-humidity, theta-e,
wind-speed, wet-bulb, vapor-pressure-deficit, dewpoint-depression,
moisture-transport, fire-weather.
"""
from __future__ import annotations

from pathlib import Path

from ..geo import resolve_location
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run
from .catalog import CROSS_SECTION_PRODUCTS, CROSS_SECTION_ROUTES


def cross_section(
    env: RustwxEnv,
    *,
    product: str = "temperature",
    route: str | None = None,
    start: str | dict | tuple | None = None,
    end: str | dict | tuple | None = None,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    source: str = "aws",
    palette: str | None = None,
    sample_count: int = 181,
    no_wind_overlay: bool = False,
    out_dir: str | None = None,
    timeout: int = 600,
) -> dict:
    """Render a vertical cross section. Supply either `route` OR (start, end).

    `start`/`end` accept any form resolve_location() understands (city
    name, lat/lon string, dict, tuple).
    """
    binary = "cross_section_proof"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}

    if product not in CROSS_SECTION_PRODUCTS:
        return {"ok": False, "error": f"unknown product {product!r}. "
                                       f"Choose from: {CROSS_SECTION_PRODUCTS}"}

    if route is not None and route not in CROSS_SECTION_ROUTES:
        return {"ok": False, "error": f"unknown route {route!r}. "
                                       f"Choose from: {CROSS_SECTION_ROUTES}"}

    custom_pts = (start is not None and end is not None)
    if not route and not custom_pts:
        route = "amarillo-chicago"  # binary default

    if model != "hrrr":
        # cross_section_proof supports any model; default source is nomads.
        pass

    date, cycle = (resolve_latest_run(model) if run_str == "latest" else parse_run(run_str))
    out_root = Path(out_dir) if out_dir else (
        env.out_root / "cross_section" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{product}_{route or 'custom'}"
    )

    args = [
        "--model", model,
        "--product", product,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        "--sample-count", str(sample_count),
        "--cache-dir", str(env.cache_dir.resolve()),
    ]
    if route:
        args.extend(["--route", route])
    if custom_pts:
        sll = resolve_location(start)
        ell = resolve_location(end)
        if sll is None or ell is None:
            return {"ok": False, "error": f"could not resolve start/end: {start}, {end}"}
        args.extend([
            f"--start-lat={sll[0]:.6f}",
            f"--start-lon={sll[1]:.6f}",
            f"--end-lat={ell[0]:.6f}",
            f"--end-lon={ell[1]:.6f}",
        ])
    if palette:
        args.extend(["--palette", palette])
    if no_wind_overlay:
        args.append("--no-wind-overlay")

    result = run(env, binary, args, out_dir=out_root, timeout=timeout)
    return {
        "ok": result.ok,
        "model": model,
        "product": product,
        "route": route,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "out_dir": str(out_root),
        "pngs": [str(p) for p in result.pngs],
        "png_count": len(result.pngs),
        "elapsed_s": round(result.seconds, 2),
        "stderr_tail": result.stderr.splitlines()[-8:] if result.stderr else [],
    }
