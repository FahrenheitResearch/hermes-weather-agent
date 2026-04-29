"""Fast HRRR cross sections through the rustwx pressure VolumeStore.

This is intentionally a local, agent-style workflow:

1. Build a small temporary HRRR pressure VolumeStore for the requested route
   and 1-3 forecast hours.
2. Render one or many cross-section products directly from that store.
3. Prune old temporary stores so users do not have to clean them manually.

It does not start a dashboard or sidecar server.
"""
from __future__ import annotations

import hashlib
import json
import math
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..geo import find_domain_for_string, parse_latlon, resolve_location
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run


VOLUME_PRODUCTS = [
    "temperature",
    "wind_speed",
    "theta_e",
    "rh",
    "q",
    "omega",
    "vorticity",
    "shear",
    "lapse_rate",
    "cloud",
    "cloud_total",
    "wetbulb",
    "icing",
    "frontogenesis",
    "vpd",
    "dewpoint_dep",
    "moisture_transport",
    "pv",
    "fire_wx",
]

PRODUCT_ALIASES = {
    "all": "all",
    "wxsection": "all",
    "relative-humidity": "rh",
    "relative_humidity": "rh",
    "specific-humidity": "q",
    "specific_humidity": "q",
    "theta-e": "theta_e",
    "thetae": "theta_e",
    "wind-speed": "wind_speed",
    "wet-bulb": "wetbulb",
    "wet_bulb": "wetbulb",
    "vapor-pressure-deficit": "vpd",
    "vapor_pressure_deficit": "vpd",
    "dewpoint-depression": "dewpoint_dep",
    "dewpoint_depression": "dewpoint_dep",
    "moisture-transport": "moisture_transport",
    "moisture_transport": "moisture_transport",
    "potential-vorticity": "pv",
    "potential_vorticity": "pv",
    "cloud-water": "cloud",
    "cloud_water": "cloud",
    "total-condensate": "cloud_total",
    "total_condensate": "cloud_total",
    "cloud-total": "cloud_total",
    "cloud_total": "cloud_total",
    "lapse-rate": "lapse_rate",
    "lapse_rate": "lapse_rate",
    "fire-weather": "fire_wx",
    "fire_weather": "fire_wx",
    "fire-wx": "fire_wx",
    "fire_wx": "fire_wx",
}

ROUTES: dict[str, tuple[str, tuple[float, float], tuple[float, float]]] = {
    "amarillo-chicago": ("Amarillo to Chicago", (35.2220, -101.8313), (41.8781, -87.6298)),
    "kansas-city-chicago": ("Kansas City to Chicago", (39.0997, -94.5786), (41.8781, -87.6298)),
    "san-francisco-tahoe": ("San Francisco to Tahoe", (37.7749, -122.4194), (39.0968, -120.0324)),
    "sacramento-reno": ("Sacramento to Reno", (38.5816, -121.4944), (39.5296, -119.8138)),
    "los-angeles-mojave": ("Los Angeles to Mojave", (34.0522, -118.2437), (35.0525, -118.1739)),
    "san-diego-imperial": ("San Diego to Imperial", (32.7157, -117.1611), (32.8476, -115.5694)),
    "socal-coast-desert": ("SoCal Coast to Desert", (34.0195, -118.4912), (33.8303, -116.5453)),
}


@dataclass(frozen=True)
class Route:
    route_id: str
    route_name: str
    start: tuple[float, float]
    end: tuple[float, float]


def volume_cross_section(
    env: RustwxEnv,
    *,
    products: list[str] | str | None = None,
    product: str | None = None,
    route: str | None = "socal-coast-desert",
    start=None,
    end=None,
    run_str: str = "latest",
    forecast_hour: int | None = None,
    forecast_hours: list[int] | None = None,
    forecast_hour_start: int | None = None,
    forecast_hour_end: int | None = None,
    source: str = "nomads",
    spacing_km: float = 10.0,
    width: int = 1400,
    height: int = 820,
    top_pressure_hpa: int = 100,
    bounds_padding_deg: float = 1.5,
    load_parallelism: int = 2,
    max_build_hours: int = 3,
    allow_more_hours: bool = False,
    store_ttl_hours: float = 6.0,
    keep_store: bool = True,
    out_dir: str | None = None,
    timeout: int = 900,
) -> dict:
    """Build a small temporary HRRR pressure VolumeStore and render sections."""
    if not env.has_binary("hrrr_pressure_volume_store"):
        return _missing("hrrr_pressure_volume_store")
    if not env.has_binary("volume_store_cross_section_render"):
        return _missing("volume_store_cross_section_render")

    try:
        selected_products = _normalize_products(products, product)
        hours = _forecast_hours(
            forecast_hour=forecast_hour,
            forecast_hours=forecast_hours,
            forecast_hour_start=forecast_hour_start,
            forecast_hour_end=forecast_hour_end,
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    span = max(hours) - min(hours) + 1
    if span > max_build_hours and not allow_more_hours:
        return {
            "ok": False,
            "error": (
                f"requested hour span {span} exceeds max_build_hours={max_build_hours}; "
                "set allow_more_hours=true to build a larger temporary store"
            ),
            "forecast_hours": hours,
        }

    date, cycle = resolve_latest_run("hrrr") if run_str == "latest" else parse_run(run_str)
    route_def = _resolve_route(env, route=route, start=start, end=end)
    if isinstance(route_def, dict):
        return route_def

    store_root = env.cache_dir / "volume_stores"
    _prune_old_stores(store_root, ttl_hours=store_ttl_hours)
    store_root.mkdir(parents=True, exist_ok=True)

    bounds = _route_bounds(route_def, padding_deg=bounds_padding_deg)
    key = _store_key(date, cycle, min(hours), max(hours), source, bounds, route_def)
    build_dir = store_root / key
    store_dir = build_dir / "store"

    out_root = Path(out_dir) if out_dir else (
        env.out_root
        / "volume_cross_section"
        / f"{date}_{cycle:02d}z_f{_hour_label(hours)}_{route_def.route_id}"
    )
    out_root.mkdir(parents=True, exist_ok=True)

    started = time.time()
    build_result = None
    if not (store_dir / "manifest.json").exists():
        build_cmd = [
            str(env.require_binary("hrrr_pressure_volume_store")),
            "--date", date,
            "--cycle", str(cycle),
            "--start-hour", str(min(hours)),
            "--end-hour", str(max(hours)),
            "--source", source,
            f"--west={bounds[0]:.6f}",
            f"--east={bounds[1]:.6f}",
            f"--south={bounds[2]:.6f}",
            f"--north={bounds[3]:.6f}",
            "--cache-dir", str(env.cache_dir.resolve()),
            "--out-dir", str(build_dir.resolve()),
            "--load-parallelism", str(max(1, int(load_parallelism))),
            f"--route-start-lat={route_def.start[0]:.8f}",
            f"--route-start-lon={route_def.start[1]:.8f}",
            f"--route-end-lat={route_def.end[0]:.8f}",
            f"--route-end-lon={route_def.end[1]:.8f}",
            "--route-spacing-km", str(spacing_km),
        ]
        build_result = _run_json_command(build_cmd, timeout=timeout)
        if not build_result["ok"]:
            return {
                "ok": False,
                "stage": "build",
                "error": build_result["error"],
                "command": build_cmd,
                "stderr_tail": build_result["stderr_tail"],
                "stdout_tail": build_result["stdout_tail"],
            }

    render_cmd = [
        str(env.require_binary("volume_store_cross_section_render")),
        "--store", str(store_dir.resolve()),
        "--out-dir", str(out_root.resolve()),
        "--products", ",".join(selected_products) if selected_products != ["all"] else "all",
        "--hours", ",".join(str(hour) for hour in hours),
        "--spacing-km", str(spacing_km),
        "--top-pressure-hpa", str(top_pressure_hpa),
        "--width", str(width),
        "--height", str(height),
        "--route-id", route_def.route_id,
        "--route-name", route_def.route_name,
        f"--start-lat={route_def.start[0]:.8f}",
        f"--start-lon={route_def.start[1]:.8f}",
        f"--end-lat={route_def.end[0]:.8f}",
        f"--end-lon={route_def.end[1]:.8f}",
    ]
    render_result = _run_json_command(render_cmd, timeout=timeout)
    if not render_result["ok"]:
        return {
            "ok": False,
            "stage": "render",
            "error": render_result["error"],
            "command": render_cmd,
            "stderr_tail": render_result["stderr_tail"],
            "stdout_tail": render_result["stdout_tail"],
            "store_dir": str(store_dir),
        }

    if not keep_store:
        _safe_remove_store(build_dir, store_root)

    pngs = sorted(str(path) for path in out_root.rglob("*.png"))
    webps = sorted(str(path) for path in out_root.rglob("*.webp"))
    summaries = sorted(str(path) for path in out_root.rglob("*.json"))
    report = render_result.get("json") or {}
    return {
        "ok": len(pngs) > 0 or len(webps) > 0,
        "engine": "rustwx-pressure-volume-store",
        "model": "hrrr",
        "date": date,
        "cycle": cycle,
        "forecast_hours": hours,
        "source": source,
        "route": {
            "id": route_def.route_id,
            "name": route_def.route_name,
            "start": {"lat": route_def.start[0], "lon": route_def.start[1]},
            "end": {"lat": route_def.end[0], "lon": route_def.end[1]},
        },
        "bounds": bounds,
        "products": selected_products,
        "out_dir": str(out_root),
        "store_dir": str(store_dir) if keep_store else None,
        "store_ttl_hours": store_ttl_hours,
        "pngs": pngs,
        "webps": webps,
        "summaries": summaries,
        "png_count": len(pngs),
        "webp_count": len(webps),
        "elapsed_s": round(time.time() - started, 2),
        "build": build_result.get("json") if build_result else {"cache_hit": True},
        "render": report,
    }


def _missing(binary: str) -> dict:
    return {
        "ok": False,
        "error": (
            f"{binary} binary not found. Build rustwx with: "
            "cargo build -p rustwx-cli --release --bin hrrr_pressure_volume_store "
            "--bin volume_store_cross_section_render"
        ),
    }


def _normalize_products(products: list[str] | str | None, product: str | None) -> list[str]:
    raw: list[str]
    if products is None:
        raw = [product] if product else ["all"]
    elif isinstance(products, str):
        raw = [item.strip() for item in products.split(",") if item.strip()]
    else:
        raw = [str(item).strip() for item in products if str(item).strip()]
    if not raw:
        return ["all"]
    out = []
    for item in raw:
        key = item.strip().lower().replace(" ", "_")
        key = PRODUCT_ALIASES.get(key, PRODUCT_ALIASES.get(key.replace("-", "_"), key))
        if key == "smoke":
            raise ValueError("smoke is not supported by the current HRRR pressure VolumeStore")
        if key == "all":
            return ["all"]
        if key not in VOLUME_PRODUCTS:
            raise ValueError(f"unknown VolumeStore cross-section product {item!r}")
        out.append(key)
    return sorted(set(out), key=out.index)


def _forecast_hours(
    *,
    forecast_hour: int | None,
    forecast_hours: list[int] | None,
    forecast_hour_start: int | None,
    forecast_hour_end: int | None,
) -> list[int]:
    if forecast_hours:
        hours = sorted(set(int(hour) for hour in forecast_hours))
    elif forecast_hour_start is not None or forecast_hour_end is not None:
        start = int(forecast_hour_start if forecast_hour_start is not None else forecast_hour or 0)
        end = int(forecast_hour_end if forecast_hour_end is not None else start)
        if start > end:
            raise ValueError("forecast_hour_start must be <= forecast_hour_end")
        hours = list(range(start, end + 1))
    else:
        hours = [int(forecast_hour if forecast_hour is not None else 0)]
    if any(hour < 0 or hour > 48 for hour in hours):
        raise ValueError("HRRR VolumeStore cross sections support forecast hours 0-48")
    return hours


def _resolve_route(env: RustwxEnv, *, route: str | None, start, end) -> Route | dict:
    if route:
        slug = route.strip().lower().replace("_", "-").replace(" ", "-")
        if slug not in ROUTES:
            return {"ok": False, "error": f"unknown route {route!r}", "available_routes": sorted(ROUTES)}
        name, s, e = ROUTES[slug]
        return Route(slug, name, s, e)

    if start is None or end is None:
        name, s, e = ROUTES["socal-coast-desert"]
        return Route("socal-coast-desert", name, s, e)

    sll = _resolve_endpoint(env, start)
    ell = _resolve_endpoint(env, end)
    if sll is None or ell is None:
        return {"ok": False, "error": f"could not resolve start/end: {start!r}, {end!r}"}
    route_id = _slug(f"custom-{sll[0]:.3f}-{sll[1]:.3f}-{ell[0]:.3f}-{ell[1]:.3f}")
    return Route(route_id, f"Custom {sll[0]:.3f},{sll[1]:.3f} to {ell[0]:.3f},{ell[1]:.3f}", sll, ell)


def _resolve_endpoint(env: RustwxEnv, value) -> tuple[float, float] | None:
    resolved = resolve_location(value)
    if resolved is not None:
        return resolved
    if isinstance(value, str):
        parsed = parse_latlon(value)
        if parsed is not None:
            return parsed
        domain = find_domain_for_string(env, value)
        bounds = domain.get("bounds") if domain else None
        if bounds and len(bounds) == 4:
            west, east, south, north = [float(v) for v in bounds]
            return ((south + north) / 2.0, (west + east) / 2.0)
    return None


def _route_bounds(route: Route, *, padding_deg: float) -> list[float]:
    lats = [route.start[0], route.end[0]]
    lons = [route.start[1], route.end[1]]
    pad = max(float(padding_deg), 0.1)
    west = min(lons) - pad
    east = max(lons) + pad
    south = min(lats) - pad
    north = max(lats) + pad
    return [
        max(-180.0, west),
        min(180.0, east),
        max(-90.0, south),
        min(90.0, north),
    ]


def _store_key(
    date: str,
    cycle: int,
    start_hour: int,
    end_hour: int,
    source: str,
    bounds: list[float],
    route: Route,
) -> str:
    payload = json.dumps(
        {
            "date": date,
            "cycle": cycle,
            "start_hour": start_hour,
            "end_hour": end_hour,
            "source": source,
            "bounds": [round(v, 4) for v in bounds],
            "route": {
                "id": route.route_id,
                "name": route.route_name,
                "start": route.start,
                "end": route.end,
            },
            "schema": 2,
        },
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    return f"hrrr_{date}_{cycle:02d}z_f{start_hour:03d}_f{end_hour:03d}_{route.route_id}_{digest}"


def _run_json_command(cmd: list[str], *, timeout: int) -> dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "seconds": round(time.time() - started, 3),
            "stdout_tail": [],
            "stderr_tail": [],
        }
    parsed = _parse_json_object(proc.stdout)
    return {
        "ok": proc.returncode == 0,
        "error": None if proc.returncode == 0 else f"exit {proc.returncode}",
        "returncode": proc.returncode,
        "seconds": round(time.time() - started, 3),
        "json": parsed,
        "stdout_tail": proc.stdout.splitlines()[-12:],
        "stderr_tail": proc.stderr.splitlines()[-12:],
    }


def _parse_json_object(text: str) -> Any:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


def _prune_old_stores(root: Path, *, ttl_hours: float) -> None:
    if ttl_hours <= 0 or not root.exists():
        return
    cutoff = time.time() - ttl_hours * 3600.0
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            if child.stat().st_mtime < cutoff:
                _safe_remove_store(child, root)
        except OSError:
            continue


def _safe_remove_store(path: Path, root: Path) -> None:
    root_resolved = root.resolve()
    path_resolved = path.resolve()
    if root_resolved == path_resolved or root_resolved not in path_resolved.parents:
        return
    shutil.rmtree(path_resolved, ignore_errors=True)


def _slug(value: str) -> str:
    keep = []
    for ch in value.lower():
        if ch.isalnum():
            keep.append(ch)
        elif ch in {"-", "_", "."}:
            keep.append("-")
    slug = "".join(keep).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug or "route"


def _hour_label(hours: list[int]) -> str:
    if len(hours) == 1:
        return f"{hours[0]:03d}"
    return f"{min(hours):03d}-{max(hours):03d}"
