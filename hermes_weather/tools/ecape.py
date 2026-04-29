"""ECAPE specialty tools.

In rustwx 0.4 the agent-v1 contract handles every ECAPE map (sbecape,
mlecape, muecape, ratios, ECAPE-EHI, ECAPE-STP, ECAPE-SCP) through
`render_maps_json`. The single-point profile probe and the swath-scale
research statistics aren't in the agent-v1 contract yet; those use
optional rustwx proof binaries when available.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from .. import jobs
from ..geo import Bbox, bbox_from_center, resolve_location, resolve_to_domain
from ..rustwx import RustwxEnv, parse_run, render_maps, resolve_latest_run_for_hours, run


def _resolve_run(
    run_str: str,
    model: str = "hrrr",
    *,
    source: str = "aws",
    forecast_hour: int = 0,
) -> tuple[str, int]:
    if run_str == "latest":
        return resolve_latest_run_for_hours(
            model,
            source=source,
            forecast_hours=[forecast_hour],
            product="sfc",
        )
    return parse_run(run_str)


# ── Single-point ECAPE profile probe (optional binary) ──────────────────


def profile(
    env: RustwxEnv,
    *,
    location,
    run_str: str = "latest",
    forecast_hour: int = 1,
    model: str = "hrrr",
    source: str = "aws",
    crop_radius_deg: float = 1.0,
    include_input_column: bool = False,
    out_dir: str | None = None,
    timeout: int = 120,
) -> dict:
    """Per-profile ECAPE diagnostic at a (lat, lon).

    Uses the optional `hrrr_ecape_profile_probe` binary. Until rustwx
    exposes a Python API for this, build the binary from the workspace
    or this tool will return a clear "not built" error.
    """
    binary = "hrrr_ecape_profile_probe"
    if not env.has_binary(binary):
        return {
            "ok": False,
            "error": (
                f"{binary} binary not built. This specialty tool isn't in "
                "the rustwx agent-v1 contract yet; build the binary with: "
                f"cargo build --release --bin {binary}, then set "
                "HERMES_RUSTWX_BIN_DIR. (All map rendering tools work without it.)"
            ),
        }

    latlon = resolve_location(location)
    if latlon is None:
        return {"ok": False, "error": f"could not resolve location {location!r}"}
    lat, lon = latlon

    date, cycle = _resolve_run(run_str, model, source=source, forecast_hour=forecast_hour)
    out = Path(out_dir) if out_dir else (
        env.out_root / "ecape_profile" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}"
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
        "lat": lat, "lon": lon,
        "date": date, "cycle": cycle,
        "forecast_hour": forecast_hour, "model": model,
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
            payload["diagnostics"] = data
        except Exception as exc:
            payload["json_parse_error"] = str(exc)
    return payload


# ── Swath-scale grid research (optional binary) ─────────────────────────


def grid(
    env: RustwxEnv,
    *,
    bbox: dict | None = None,
    location=None,
    radius_km: float = 400.0,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 1,
    source: str = "aws",
    domain_slug: str = "custom_swath",
    background: bool = True,
    timeout: int = 1800,
) -> dict:
    """Full-grid ECAPE statistics over a swath.

    Uses the optional `hrrr_ecape_grid_research` binary. Background by
    default — poll wx_job_status with the returned job_id.
    """
    binary = "hrrr_ecape_grid_research"
    if not env.has_binary(binary):
        return {
            "ok": False,
            "error": (
                f"{binary} binary not built. Build with: "
                f"cargo build --release --bin {binary}"
            ),
        }

    if bbox is not None:
        try:
            bb = Bbox(
                west=float(bbox["west"]), east=float(bbox["east"]),
                south=float(bbox["south"]), north=float(bbox["north"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return {"ok": False, "error": f"bad bbox: {exc}"}
    else:
        latlon = resolve_location(location)
        if latlon is None:
            return {"ok": False, "error": "supply either bbox or location"}
        bb = bbox_from_center(*latlon, radius_km=radius_km)

    date, cycle = _resolve_run(run_str, model, source=source, forecast_hour=forecast_hour)
    out_dir = env.out_root / "ecape_grid" / f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{domain_slug}"
    out_dir.mkdir(parents=True, exist_ok=True)
    output_json = out_dir / f"{domain_slug}.json"

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        f"--west={bb.west:.4f}",
        f"--east={bb.east:.4f}",
        f"--south={bb.south:.4f}",
        f"--north={bb.north:.4f}",
        "--domain-slug", domain_slug,
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(output_json.resolve()),
    ]

    def _runner(job: jobs.Job) -> dict:
        job.append_log(f"binary={binary} date={date} cycle={cycle:02d} f{forecast_hour:03d}")
        result = run(env, binary, args, timeout=timeout)
        job.append_log(f"rc={result.returncode} seconds={result.seconds:.2f}")
        if not result.ok:
            for line in result.stderr.splitlines()[-10:]:
                job.append_log(f"stderr: {line}")
            raise RuntimeError(f"{binary} rc={result.returncode}")
        out_payload: dict = {
            "ok": True,
            "output_json": str(output_json),
            "binary_seconds": round(result.seconds, 2),
        }
        if output_json.exists():
            try:
                data = json.loads(output_json.read_text(encoding="utf-8"))
                out_payload["statistics"] = data.get("statistics", data)
            except Exception as exc:
                out_payload["json_parse_error"] = str(exc)
        return out_payload

    if not background:
        job = jobs.submit("ecape_grid", {"bbox": bb.__dict__}, _runner)
        if job.thread:
            job.thread.join(timeout=timeout)
        return job.to_payload(log_tail=40)

    job = jobs.submit("ecape_grid", {"bbox": bb.__dict__}, _runner)
    return {
        "ok": True,
        "job_id": job.job_id, "kind": job.kind, "state": job.state,
        "note": "Full-grid ECAPE swath research started. Poll wx_job_status.",
    }


# ── Ratio map — now goes through agent-v1 render_maps_json ──────────────


def ratio_map(
    env: RustwxEnv,
    *,
    region: str | None = None,
    domain: str | None = None,
    location=None,
    bounds: list[float] | None = None,
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    source: str = "aws",
    parcel: str = "ml",
    include_native_ratio: bool = False,
    out_dir: str | None = None,
    timeout: int = 1800,
) -> dict:
    """ECAPE/CAPE ratio render — routes through render_maps_json now.

    By default emits the ECAPE map plus the derived-CAPE ratio variant.
    Native-CAPE ratios are opt-in because not every rustwx build exposes
    the native CAPE fields needed by the heavy ECAPE renderer.
    """
    if not env.module_available:
        return {
            "ok": False,
            "error": "rustwx Python module not installed; run: pip install 'rustwx>=0.4.6'",
        }
    parcel = parcel.lower()
    if parcel not in ("sb", "ml", "mu"):
        return {"ok": False, "error": f"parcel must be sb/ml/mu, got {parcel!r}"}

    recipes = [
        f"{parcel}ecape",
        f"{parcel}cape",
        f"{parcel}_ecape_derived_cape_ratio",
    ]
    if include_native_ratio:
        recipes.append(f"{parcel}_ecape_native_cape_ratio")
    date, cycle = _resolve_run(run_str, model, source=source, forecast_hour=forecast_hour)
    domain_slug, computed_bounds = resolve_to_domain(
        env, region=region, domain=domain, location=location
    )
    if bounds is None and computed_bounds is not None:
        bounds = computed_bounds

    out_root = Path(out_dir) if out_dir else (
        env.out_root / "ecape_ratio" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{parcel}_{domain_slug or 'custom'}"
    )
    out_root.mkdir(parents=True, exist_ok=True)

    request: dict = {
        "date_yyyymmdd": date,
        "cycle_utc": cycle,
        "forecast_hour": forecast_hour,
        "model": model,
        "source": source,
        "products": recipes,
        "out_dir": str(out_root.resolve()).replace("\\", "/"),
        "place_label_density": "major",
    }
    if bounds:
        request["bounds"] = list(bounds)
    elif domain_slug:
        request["domain"] = domain_slug
    else:
        request["domain"] = "conus"

    started = time.time()
    try:
        result = render_maps(env, request)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    elapsed = time.time() - started

    pngs: list[str] = []
    for dom in result.get("domains", []) or []:
        for p in (dom.get("summary") or {}).get("output_paths", []) or []:
            pngs.append(str(p))
    for dom in (result.get("heavy_derived") or {}).get("domains", []) or []:
        for recipe in dom.get("recipes", []) or []:
            output_path = recipe.get("output_path")
            if output_path:
                pngs.append(str(output_path))

    return {
        "ok": len(pngs) > 0,
        "model": model, "date": date, "cycle": cycle,
        "forecast_hour": forecast_hour,
        "parcel": parcel,
        "domain": domain_slug, "bounds": bounds,
        "out_dir": str(out_root),
        "pngs": sorted(set(pngs)),
        "png_count": len(set(pngs)),
        "elapsed_s": round(elapsed, 2),
    }
