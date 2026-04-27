"""Cross-section tool — optional binary path until rustwx ships a Python API.

`rustwx.normalize_cross_section_request_json` exists but is a request
normaliser, not a renderer. Until rustwx exposes a one-shot fetch+render
Python API, this tool subprocess-calls the optional `cross_section_proof`
binary.
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
    start=None,
    end=None,
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
    binary = "cross_section_proof"
    if not env.has_binary(binary):
        return {
            "ok": False,
            "error": (
                f"{binary} binary not built. The cross-section renderer "
                "isn't in the agent-v1 contract yet. Build with: "
                f"cargo build --release --bin {binary}"
            ),
        }
    if product not in CROSS_SECTION_PRODUCTS:
        return {"ok": False, "error": f"unknown product {product!r}",
                "available": CROSS_SECTION_PRODUCTS}
    if route is not None and route not in CROSS_SECTION_ROUTES:
        return {"ok": False, "error": f"unknown route {route!r}",
                "available": CROSS_SECTION_ROUTES}

    custom_pts = (start is not None and end is not None)
    if not route and not custom_pts:
        route = "amarillo-chicago"

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
        "model": model, "product": product, "route": route,
        "date": date, "cycle": cycle, "forecast_hour": forecast_hour,
        "out_dir": str(out_root),
        "pngs": [str(p) for p in result.pngs],
        "png_count": len(result.pngs),
        "elapsed_s": round(result.seconds, 2),
        "stderr_tail": result.stderr.splitlines()[-8:] if result.stderr else [],
    }
