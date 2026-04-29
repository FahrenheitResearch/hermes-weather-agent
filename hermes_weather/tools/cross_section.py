"""Compatibility wrapper for HRRR VolumeStore cross sections.

The legacy proof renderer is intentionally not used here.
All Hermes cross-section calls now route through the pressure VolumeStore
path so future model support can extend that store architecture instead of
reviving the old proof binary.
"""
from __future__ import annotations

from ..rustwx import RustwxEnv
from .volume_cross_section import volume_cross_section


def cross_section(
    env: RustwxEnv,
    *,
    product: str = "temperature",
    route: str | None = "socal-coast-desert",
    start=None,
    end=None,
    run_str: str = "latest",
    forecast_hour: int | None = 0,
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
    **_legacy_options,
) -> dict:
    """Render one HRRR VolumeStore cross section.

    `model`, `palette`, `sample_count`, and `no_wind_overlay` from the old
    proof renderer are accepted via `**_legacy_options` and ignored.
    """
    return volume_cross_section(
        env,
        product=product,
        route=route,
        start=start,
        end=end,
        run_str=run_str,
        forecast_hour=forecast_hour,
        forecast_hours=forecast_hours,
        forecast_hour_start=forecast_hour_start,
        forecast_hour_end=forecast_hour_end,
        source=source,
        spacing_km=spacing_km,
        width=width,
        height=height,
        top_pressure_hpa=top_pressure_hpa,
        bounds_padding_deg=bounds_padding_deg,
        load_parallelism=load_parallelism,
        max_build_hours=max_build_hours,
        allow_more_hours=allow_more_hours,
        store_ttl_hours=store_ttl_hours,
        keep_store=keep_store,
        out_dir=out_dir,
        timeout=timeout,
    )
