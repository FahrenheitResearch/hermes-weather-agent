"""Point time-series / meteogram sampling via rustwx 0.4.4 Python APIs."""
from __future__ import annotations

import json
import time
from typing import Any

from ..geo import resolve_location
from ..rustwx import (
    RustwxEnv,
    parse_run,
    resolve_latest_run,
)


SOCAL_BOUNDS = [-121.5, -113.5, 31.5, 36.8]


def _resolve_point(location: Any = None, lat: float | None = None, lon: float | None = None):
    if lat is not None and lon is not None:
        return float(lat), float(lon)
    point = resolve_location(location)
    if point is not None:
        return point
    return None


def _hours_payload(
    forecast_hours: list[int] | None,
    forecast_hour_start: int | None,
    forecast_hour_end: int | None,
) -> dict:
    if forecast_hours:
        return {"forecast_hours": sorted({int(hour) for hour in forecast_hours})}
    payload: dict[str, int] = {}
    if forecast_hour_start is not None:
        payload["forecast_hour_start"] = int(forecast_hour_start)
    if forecast_hour_end is not None:
        payload["forecast_hour_end"] = int(forecast_hour_end)
    return payload


def _rustwx_json_call(env: RustwxEnv, function_name: str, request: dict) -> dict:
    if not env.module_available:
        raise RuntimeError(
            "rustwx Python module not installed. Install with: pip install 'rustwx>=0.4.4'"
        )
    import rustwx

    if not hasattr(rustwx, function_name):
        raise RuntimeError(
            f"installed rustwx does not expose {function_name}; install rustwx>=0.4.4"
        )
    function = getattr(rustwx, function_name)
    return json.loads(function(json.dumps(request, default=str)))


def meteogram(
    env: RustwxEnv,
    *,
    location=None,
    lat: float | None = None,
    lon: float | None = None,
    store_id: str | None = None,
    model: str = "hrrr",
    run_str: str = "latest",
    source: str = "nomads",
    forecast_hour_start: int | None = 0,
    forecast_hour_end: int | None = 3,
    forecast_hours: list[int] | None = None,
    variables: list[str] | None = None,
    method: str = "nearest",
    use_cache: bool = True,
) -> dict:
    """Sample a point forecast time series.

    If `store_id` is supplied, samples an already warmed rustwx in-memory
    grid store. Otherwise it fetches/decodes directly through
    sample_point_timeseries_json.
    """
    if not env.module_available:
        return {
            "ok": False,
            "error": "rustwx Python module not installed. Run: pip install 'rustwx>=0.4.4'",
        }

    point = _resolve_point(location, lat, lon)
    if point is None:
        return {
            "ok": False,
            "error": "location must be numeric 'lat,lon' or pass lat and lon separately",
        }
    lat_v, lon_v = point

    request: dict[str, Any] = {
        "lat": lat_v,
        "lon": lon_v,
        "method": method,
        **_hours_payload(forecast_hours, forecast_hour_start, forecast_hour_end),
    }
    if store_id:
        request["store_id"] = store_id
        function_name = "sample_point_timeseries_store_json"
    else:
        date, cycle = resolve_latest_run(model) if run_str == "latest" else parse_run(run_str)
        request.update(
            {
                "model": model,
                "date_yyyymmdd": date,
                "cycle_utc": cycle,
                "source": source,
                "cache_dir": str(env.cache_dir.resolve()).replace("\\", "/"),
                "use_cache": use_cache,
            }
        )
        function_name = "sample_point_timeseries_json"
    if variables and not store_id:
        request["variables"] = list(variables)

    started = time.time()
    try:
        report = _rustwx_json_call(env, function_name, request)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "request": request}

    return {
        "ok": True,
        "lat": lat_v,
        "lon": lon_v,
        "store_id": store_id,
        "elapsed_s": round(time.time() - started, 2),
        "request": request,
        "report": report,
    }


def warm_store(
    env: RustwxEnv,
    *,
    model: str = "hrrr",
    run_str: str = "latest",
    source: str = "nomads",
    bounds: list[float] | None = None,
    forecast_hour_start: int | None = 0,
    forecast_hour_end: int | None = 3,
    forecast_hours: list[int] | None = None,
    variables: list[str] | None = None,
    use_cache: bool = True,
) -> dict:
    """Warm an in-memory point-time-series grid store for repeated sampling."""
    if not env.module_available:
        return {
            "ok": False,
            "error": "rustwx Python module not installed. Run: pip install 'rustwx>=0.4.4'",
        }

    date, cycle = resolve_latest_run(model) if run_str == "latest" else parse_run(run_str)
    request: dict[str, Any] = {
        "model": model,
        "date_yyyymmdd": date,
        "cycle_utc": cycle,
        "source": source,
        "bounds": list(bounds or SOCAL_BOUNDS),
        "cache_dir": str(env.cache_dir.resolve()).replace("\\", "/"),
        "use_cache": use_cache,
        **_hours_payload(forecast_hours, forecast_hour_start, forecast_hour_end),
    }
    if variables:
        request["variables"] = list(variables)

    started = time.time()
    try:
        report = _rustwx_json_call(env, "warm_point_timeseries_store_json", request)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "request": request}

    return {
        "ok": True,
        "store_id": report.get("store_id"),
        "date": date,
        "cycle": cycle,
        "bounds": request["bounds"],
        "elapsed_s": round(time.time() - started, 2),
        "request": request,
        "report": report,
    }
