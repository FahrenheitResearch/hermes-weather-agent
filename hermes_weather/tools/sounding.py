"""Sounding tool — optional binary path until rustwx exposes a fetch+render Python API.

`rustwx.render_sounding_column_json` exists in the rustwx wheel but only
renders an *already-extracted* column; it doesn't fetch HRRR or extract
the column itself. Until rustwx ships a column-fetch Python API, this
tool uses the optional `sounding_plot` binary which does fetch + extract
+ render in one shot.
"""
from __future__ import annotations

import json
from pathlib import Path

from ..geo import resolve_location
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run


def sounding(
    env: RustwxEnv,
    *,
    location,
    run_str: str = "latest",
    forecast_hour: int = 1,
    model: str = "hrrr",
    source: str = "aws",
    sample_method: str = "nearest",
    box_radius_km: float | None = None,
    box_radius_deg: float | None = None,
    crop_radius_deg: float | None = None,
    out_dir: str | None = None,
    timeout: int = 120,
) -> dict:
    """Render a SHARPpy-style skew-T sounding via the optional sounding_plot binary."""
    binary = "sounding_plot"
    if not env.has_binary(binary):
        return {
            "ok": False,
            "error": (
                f"{binary} binary not built. The native rustwx skew-T renderer "
                "isn't in the agent-v1 contract yet. Build with: "
                f"cargo build --release --bin {binary}, then set "
                "HERMES_RUSTWX_BIN_DIR."
            ),
        }

    latlon = resolve_location(location)
    if latlon is None:
        return {"ok": False, "error": f"could not resolve location {location!r}"}
    lat, lon = latlon

    date, cycle = (resolve_latest_run(model) if run_str == "latest" else parse_run(run_str))
    out = Path(out_dir) if out_dir else (
        env.out_root / "sounding" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}"
    )
    out.mkdir(parents=True, exist_ok=True)
    png_path = out / "skewt.png"
    manifest_path = out / "sounding_manifest.json"

    station_id = (
        str(location)
        if isinstance(location, str) and location.strip()
        else f"{lat:.2f},{lon:.2f}"
    )

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        f"--lat={lat:.6f}",
        f"--lon={lon:.6f}",
        "--sample-method", sample_method,
        "--station-id", station_id,
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(png_path.resolve()),
        "--manifest", str(manifest_path.resolve()),
    ]
    if crop_radius_deg is not None:
        args.extend(["--crop-radius-deg", str(crop_radius_deg)])
    if box_radius_km is not None:
        args.extend(["--box-radius-km", str(box_radius_km)])
    if box_radius_deg is not None:
        args.extend(["--box-radius-deg", str(box_radius_deg)])

    result = run(env, binary, args, out_dir=out, timeout=timeout)
    if not result.ok:
        return {
            "ok": False,
            "error": result.stderr.strip()[-400:] or f"{binary} rc={result.returncode}",
        }

    payload: dict = {}
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            payload = {"manifest_error": str(exc)}

    return {
        "ok": True,
        "lat": lat, "lon": lon,
        "date": date, "cycle": cycle,
        "forecast_hour": forecast_hour, "model": model,
        "out_dir": str(out),
        "manifest": str(manifest_path),
        "png": str(png_path),
        "renderer": "rustwx-sounding native Rust",
        "request": payload.get("request"),
        "sampled_point": payload.get("sampled_point"),
        "profile": payload.get("profile"),
        "timing": payload.get("timing"),
    }
