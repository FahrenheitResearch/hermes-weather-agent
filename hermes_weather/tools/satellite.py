"""GOES satellite rendering via rustwx.render_goes_satellite_json."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from ..rustwx import RustwxEnv


SOCAL_BOUNDS = [-121.5, -113.5, 31.5, 36.8]
SOCAL_ALIASES = {"socal", "southern-california", "southern_california"}
DEFAULT_PRODUCTS = [
    "goes_geocolor",
    "goes_glm_fed_geocolor",
    "goes_airmass_rgb",
    "goes_sandwich_rgb",
    "goes_day_night_cloud_micro_combo_rgb",
    "goes_fire_temperature_rgb",
    "goes_dust_rgb",
    "goes_abi_band_01",
    "goes_abi_band_02",
    "goes_abi_band_03",
    "goes_abi_band_04",
    "goes_abi_band_05",
    "goes_abi_band_06",
    "goes_abi_band_07",
    "goes_abi_band_08",
    "goes_abi_band_09",
    "goes_abi_band_10",
    "goes_abi_band_11",
    "goes_abi_band_12",
    "goes_abi_band_13",
    "goes_abi_band_14",
    "goes_abi_band_15",
    "goes_abi_band_16",
]


def _collect_pngs(value: Any) -> list[str]:
    pngs: list[str] = []
    if isinstance(value, dict):
        for item in value.values():
            pngs.extend(_collect_pngs(item))
    elif isinstance(value, list):
        for item in value:
            pngs.extend(_collect_pngs(item))
    elif isinstance(value, str) and value.lower().endswith(".png"):
        pngs.append(value)
    return pngs


def _render_goes_satellite(env: RustwxEnv, request: dict) -> dict:
    if not env.module_available:
        raise RuntimeError(
            "rustwx Python module not installed. Install with: pip install 'rustwx>=0.4.6'"
        )
    import rustwx

    if not hasattr(rustwx, "render_goes_satellite_json"):
        raise RuntimeError(
            "installed rustwx does not expose render_goes_satellite_json; install rustwx>=0.4.6"
        )
    return json.loads(rustwx.render_goes_satellite_json(json.dumps(request, default=str)))


def satellite(
    env: RustwxEnv,
    *,
    satellite: str = "goes18",
    abi_product: str = "ABI-L2-CMIPC",
    domain: str = "pacific_southwest",
    bounds: list[float] | None = None,
    label: str | None = None,
    products: list[str] | None = None,
    width: int | None = None,
    height: int | None = None,
    scan_lookback_hours: int = 6,
    discovery_retries: int | None = None,
    retry_sleep_ms: int | None = None,
    use_cache: bool = True,
    download_glm: bool = False,
    glm_fetch_count: int | None = None,
    glm_lookback_hours: int | None = None,
    glm_max_age_min: float | None = None,
    high_speed_png: bool = True,
    skip_scan_id: str | None = None,
    out_dir: str | None = None,
) -> dict:
    """Render latest GOES ABI/GLM satellite products for a domain or bounds."""
    if not env.module_available:
        return {
            "ok": False,
            "error": "rustwx Python module not installed. Run: pip install 'rustwx>=0.4.6'",
        }

    domain_slug = domain.strip().lower().replace(" ", "-")
    if bounds is None and domain_slug in SOCAL_ALIASES:
        bounds = SOCAL_BOUNDS
        label = label or "Southern California"

    out_root = Path(out_dir) if out_dir else (
        env.out_root / "satellite" / satellite / domain.replace(" ", "-")
    )
    out_root.mkdir(parents=True, exist_ok=True)

    request: dict[str, Any] = {
        "satellite": satellite,
        "abi_product": abi_product,
        "domain": domain,
        "out_dir": str(out_root.resolve()).replace("\\", "/"),
        "cache_dir": str(env.cache_dir.resolve()).replace("\\", "/"),
        "scan_lookback_hours": scan_lookback_hours,
        "use_cache": use_cache,
        "download_glm": download_glm,
        "high_speed_png": high_speed_png,
    }
    if bounds:
        request["bounds"] = list(bounds)
        request.pop("domain", None)
    if label:
        request["label"] = label
    selected_products = list(products) if products else list(DEFAULT_PRODUCTS)
    request["products"] = selected_products
    if any("glm" in product.lower() for product in selected_products):
        request["download_glm"] = True
    if width:
        request["width"] = int(width)
    if height:
        request["height"] = int(height)
    if discovery_retries is not None:
        request["discovery_retries"] = int(discovery_retries)
    if retry_sleep_ms is not None:
        request["retry_sleep_ms"] = int(retry_sleep_ms)
    if glm_fetch_count is not None:
        request["glm_fetch_count"] = int(glm_fetch_count)
    if glm_lookback_hours is not None:
        request["glm_lookback_hours"] = int(glm_lookback_hours)
    if glm_max_age_min is not None:
        request["glm_max_age_min"] = float(glm_max_age_min)
    if skip_scan_id:
        request["skip_scan_id"] = skip_scan_id

    started = time.time()
    try:
        report = _render_goes_satellite(env, request)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "request": request}

    pngs = sorted(set(_collect_pngs(report)))
    if not pngs:
        pngs = [str(p) for p in sorted(out_root.rglob("*.png"))]
    return {
        "ok": bool(pngs),
        "satellite": satellite,
        "abi_product": abi_product,
        "domain": None if bounds else domain,
        "bounds": bounds,
        "products": request.get("products"),
        "out_dir": str(out_root),
        "pngs": pngs,
        "png_count": len(pngs),
        "elapsed_s": round(time.time() - started, 2),
        "request": request,
        "report": report,
    }
