"""Windowed-product tool — QPF and 2-5 km UH time-window products.

Wraps `hrrr_windowed_batch`. Catalog slugs (qpf_1h, qpf_6h, qpf_12h,
qpf_24h, qpf_total, uh_2to5km_1h_max, uh_2to5km_3h_max, uh_2to5km_run_max)
are translated to the binary's CLI flags via WINDOWED_CLI_SLUG.

The windowed binary writes one PNG per product at the requested forecast
hour. For QPF totals, set forecast_hour to the last hour of the window.
"""
from __future__ import annotations

from pathlib import Path

from ..geo import resolve_region
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run
from .catalog import FALLBACK_WINDOWED, WINDOWED_CLI_SLUG


def windowed(
    env: RustwxEnv,
    *,
    products: list[str],
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 6,
    region: str | None = None,
    location: str | dict | tuple | None = None,
    source: str = "aws",
    out_dir: str | None = None,
    timeout: int = 600,
) -> dict:
    """Render one or more windowed products as PNGs."""
    binary = "hrrr_windowed_batch"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}
    if not products:
        return {"ok": False, "error": "products list is empty"}

    cli_slugs: list[str] = []
    unknown: list[str] = []
    for p in products:
        if p in WINDOWED_CLI_SLUG:
            cli_slugs.append(WINDOWED_CLI_SLUG[p])
        elif p in WINDOWED_CLI_SLUG.values():
            cli_slugs.append(p)
        else:
            unknown.append(p)
    if unknown:
        return {
            "ok": False,
            "error": f"unknown windowed products: {unknown}",
            "supported": list(FALLBACK_WINDOWED.keys()),
        }

    if run_str == "latest":
        date, cycle = resolve_latest_run(model)
    else:
        date, cycle = parse_run(run_str)

    chosen_region = resolve_region(region, location, allow_extended=True)
    out_root = Path(out_dir) if out_dir else (
        env.out_root / "windowed" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{chosen_region}"
    )

    args = [
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        "--region", chosen_region,
        "--cache-dir", str(env.cache_dir.resolve()),
        "--product", *cli_slugs,
    ]
    result = run(env, binary, args, out_dir=out_root, timeout=timeout)
    return {
        "ok": result.ok,
        "model": model,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "region": chosen_region,
        "products": products,
        "out_dir": str(out_root),
        "pngs": [str(p) for p in result.pngs],
        "manifests": [str(p) for p in result.manifests],
        "png_count": len(result.pngs),
        "elapsed_s": round(result.seconds, 2),
        "stderr_tail": result.stderr.splitlines()[-8:] if result.stderr else [],
    }
