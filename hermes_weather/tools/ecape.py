"""ECAPE tools: point profile probe, full-grid swath, and ratio display.

Current RustWX ECAPE workflows use:

  * `hrrr_ecape_profile_probe` for point profiles.
  * `hrrr_ecape_grid_research` for full-grid swath statistics.
  * `hrrr_ecape_ratio_display` for ECAPE/CAPE ratio maps with magnitude masks.

Full-grid jobs are heavy enough to run in the background for large domains.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from .. import jobs
from ..geo import Bbox, bbox_from_center, resolve_location, resolve_region
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run, run_json


def _resolve_run(run_str: str, model: str = "hrrr") -> tuple[str, int]:
    if run_str == "latest":
        return resolve_latest_run(model)
    return parse_run(run_str)


def profile(
    env: RustwxEnv,
    *,
    location: str | dict | tuple,
    run_str: str = "latest",
    forecast_hour: int = 1,
    model: str = "hrrr",
    source: str = "aws",
    crop_radius_deg: float = 1.0,
    include_input_column: bool = False,
    out_dir: str | None = None,
    timeout: int = 90,
) -> dict:
    """Per-profile ECAPE diagnostic at a (lat, lon).

    Computes ECAPE/CAPE/CIN/LFC/EL for SB/ML/MU × entraining/non-entraining ×
    pseudoadiabatic/irreversible. Sub-second per profile.
    """
    binary = "hrrr_ecape_profile_probe"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}

    latlon = resolve_location(location)
    if latlon is None:
        return {"ok": False, "error": f"could not resolve location {location!r}"}
    lat, lon = latlon

    date, cycle = _resolve_run(run_str, model)
    out = Path(out_dir) if out_dir else (
        env.out_root / "ecape_profile" / f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}"
    )
    out.mkdir(parents=True, exist_ok=True)
    output_json = out / "profile.json"

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        f"--lat={lat:.6f}",
        f"--lon={lon:.6f}",
        "--crop-radius-deg", str(crop_radius_deg),
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(output_json.resolve()),
    ]
    if include_input_column:
        args.append("--include-input-column")

    started = time.time()
    result = run(env, binary, args, timeout=timeout)
    elapsed = time.time() - started

    payload: dict = {
        "ok": result.ok,
        "lat": lat,
        "lon": lon,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "model": model,
        "elapsed_s": round(elapsed, 3),
        "output_json": str(output_json),
        "binary_seconds": round(result.seconds, 3),
    }
    if not result.ok:
        payload["error"] = result.stderr.strip()[-500:] or result.stdout.strip()[-500:]
        return payload

    if output_json.exists():
        try:
            data = json.loads(output_json.read_text(encoding="utf-8"))
            # Surface the core diagnostics so the agent doesn't have to re-read the file
            payload["diagnostics"] = data
        except Exception as exc:
            payload["json_parse_error"] = str(exc)
    return payload


def grid(
    env: RustwxEnv,
    *,
    bbox: dict | None = None,
    location: str | dict | tuple | None = None,
    radius_km: float = 400.0,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 1,
    source: str = "aws",
    domain_slug: str = "custom_swath",
    background: bool = True,
    timeout: int = 1800,
) -> dict:
    """Full-grid ECAPE statistics for a lat/lon swath.

    Runs `hrrr_ecape_grid_research` over the requested bbox. Full swaths are
    background jobs by default.
    Defaults to a background job — poll with wx_job_status.
    """
    binary = "hrrr_ecape_grid_research"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}

    if bbox is not None:
        try:
            bb = Bbox(
                west=float(bbox["west"]),
                east=float(bbox["east"]),
                south=float(bbox["south"]),
                north=float(bbox["north"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return {"ok": False, "error": f"bad bbox: {exc}"}
    else:
        latlon = resolve_location(location)
        if latlon is None:
            return {"ok": False, "error": "supply either bbox or location"}
        bb = bbox_from_center(*latlon, radius_km=radius_km)

    date, cycle = _resolve_run(run_str, model)
    out_dir = env.out_root / "ecape_grid" / f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{domain_slug}"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_json = out_dir / f"{domain_slug}.json"

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        *bb.to_args(),
        "--domain-slug", domain_slug,
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(output_json.resolve()),
    ]

    def _runner(job: jobs.Job) -> dict:
        job.append_log(f"binary={binary} date={date} cycle={cycle:02d} f{forecast_hour:03d}")
        job.append_log(f"bbox west={bb.west:.3f} east={bb.east:.3f} south={bb.south:.3f} north={bb.north:.3f}")
        result = run(env, binary, args, timeout=timeout)
        job.append_log(f"rc={result.returncode} seconds={result.seconds:.2f}")
        if not result.ok:
            for line in result.stderr.splitlines()[-10:]:
                job.append_log(f"stderr: {line}")
            raise RuntimeError(f"hrrr_ecape_grid_research rc={result.returncode}")
        out_payload: dict = {
            "ok": True,
            "output_json": str(output_json),
            "binary_seconds": round(result.seconds, 2),
        }
        if output_json.exists():
            try:
                data = json.loads(output_json.read_text(encoding="utf-8"))
                # Keep statistics summary in result; full grid data stays on disk.
                out_payload["statistics"] = data.get("statistics", data)
            except Exception as exc:
                out_payload["json_parse_error"] = str(exc)
        return out_payload

    if not background:
        # Synchronous — only sane for tiny boxes
        job = jobs.submit("ecape_grid", {"bbox": bb.__dict__, "model": model, "date": date,
                                          "cycle": cycle, "forecast_hour": forecast_hour}, _runner)
        # Wait for completion synchronously
        job.thread.join(timeout=timeout)  # type: ignore[union-attr]
        return job.to_payload(log_tail=40)

    job = jobs.submit("ecape_grid", {"bbox": bb.__dict__, "model": model, "date": date,
                                      "cycle": cycle, "forecast_hour": forecast_hour}, _runner)
    return {
        "ok": True,
        "job_id": job.job_id,
        "kind": job.kind,
        "state": job.state,
        "note": "Full-grid ECAPE typically takes ~17s per forecast hour. "
                "Poll wx_job_status with this job_id.",
    }


def ratio_map(
    env: RustwxEnv,
    *,
    region: str | None = None,
    location: str | dict | tuple | None = None,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    source: str = "aws",
    allow_large_heavy_domain: bool = False,
    background: bool = True,
    timeout: int = 1800,
) -> dict:
    """Render MLECAPE filled + ECAPE/CAPE ratio contours with magnitude mask.

    The binary always uses HRRR; `--model` is fixed. Region presets come from
    the binary's --region flag (gulf-to-kansas IS supported here).
    """
    binary = "hrrr_ecape_ratio_display"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built"}

    chosen_region = resolve_region(region, location, allow_extended=True)
    date, cycle = _resolve_run(run_str, model)
    out_dir = env.out_root / "ecape_ratio_map" / f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{chosen_region}"

    args = [
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        "--region", chosen_region,
        "--cache-dir", str(env.cache_dir.resolve()),
    ]
    if allow_large_heavy_domain:
        args.append("--allow-large-heavy-domain")

    def _runner(job: jobs.Job) -> dict:
        job.append_log(f"binary={binary} region={chosen_region} date={date} cycle={cycle:02d}")
        result = run(env, binary, args, out_dir=out_dir, timeout=timeout)
        job.append_log(f"rc={result.returncode} seconds={result.seconds:.2f} pngs={len(result.pngs)}")
        if not result.ok:
            for line in result.stderr.splitlines()[-10:]:
                job.append_log(f"stderr: {line}")
            raise RuntimeError(f"hrrr_ecape_ratio_display rc={result.returncode}")
        return {
            "ok": True,
            "out_dir": str(out_dir),
            "pngs": [str(p) for p in result.pngs],
            "png_count": len(result.pngs),
            "binary_seconds": round(result.seconds, 2),
        }

    if not background:
        job = jobs.submit("ecape_ratio_map", {"region": chosen_region, "date": date,
                                               "cycle": cycle, "forecast_hour": forecast_hour}, _runner)
        job.thread.join(timeout=timeout)  # type: ignore[union-attr]
        return job.to_payload(log_tail=40)

    job = jobs.submit("ecape_ratio_map", {"region": chosen_region, "date": date,
                                           "cycle": cycle, "forecast_hour": forecast_hour}, _runner)
    return {
        "ok": True,
        "job_id": job.job_id,
        "region": chosen_region,
        "kind": job.kind,
        "state": job.state,
        "note": "MLECAPE+ratio map renders in 30-60s for a region preset, "
                "longer for CONUS. Poll wx_job_status with this job_id.",
    }
