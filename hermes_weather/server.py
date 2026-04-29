"""Hermes Weather MCP server.

Wires every tool into the MCP `Server` and dispatches calls. Every tool
returns a JSON-serialisable dict; the server packs it as TextContent so
MCP clients (Hermes, Claude Desktop, …) can parse the result.

Run modes:
  python -m hermes_weather.server            run as MCP stdio server
  python -m hermes_weather.server --list     list registered tools
  python -m hermes_weather.server --doctor   show binary discovery state
  python -m hermes_weather.server --test     smoke-test rendering
"""
from __future__ import annotations

import asyncio
import json
import sys

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from . import jobs
from .rustwx import RustwxBinaryMissing, discover
from .tools import (
    cache as cache_tool,
    catalog,
    data_packs as data_pack_tool,
    dataset as ds_tool,
    ecape as ecape_tool,
    fetch as fetch_tool,
    meteogram as meteogram_tool,
    radar as radar_tool,
    render as render_tool,
    research as research_tool,
    satellite as satellite_tool,
    sounding as sounding_tool,
    volume_cross_section as volume_cs_tool,
)

ENV = discover()
SERVER = Server("hermes-weather")


# ── Tool schema ─────────────────────────────────────────────────────────


def _tool_definitions() -> list[Tool]:
    location_schema: dict = {
        "type": "string",
        "description": "City name (e.g. 'Amarillo, TX'), 'lat,lon', or known landmark from the offline gazetteer.",
    }
    run_schema: dict = {
        "type": "string",
        "description": "Model run as 'YYYY-MM-DD/HHz' or 'latest'.",
        "default": "latest",
    }
    fhour_schema: dict = {"type": "integer", "default": 0, "minimum": 0, "maximum": 384}

    return [
        Tool(
            name="wx_models",
            description="List available weather models (HRRR, GFS, ECMWF Open Data, RRFS-A, WRF-GDEX) with sources, products, and forecast horizons.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_recipes",
            description="List rendering recipes (direct GRIB plots, derived thermodynamic plots), ECAPE products, and cross-section product/route catalogs. Live data when product_catalog binary is available, fallback otherwise.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_products",
            description=(
                "Live product catalog from the rustwx product_catalog binary. Returns 100+ products "
                "with kind (direct/derived/heavy/windowed), status (supported/partial/blocked), "
                "maturity (operational/experimental), runner binaries, per-model support, and "
                "provenance (proxy/native/etc). Filter by kind, status, model, or substring search."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": ["direct", "derived", "heavy", "windowed"]},
                    "status": {"type": "string", "enum": ["supported", "partial", "blocked"]},
                    "model": {"type": "string", "description": "Restrict to entries supporting this model id"},
                    "search": {"type": "string", "description": "Case-insensitive substring match against slug/title"},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_regions",
            description="List rustwx region presets (conus, midwest, southern-plains, etc.) with descriptions.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_doctor",
            description="Diagnose the local rustwx install: which binaries are reachable, where, and what's missing. Run this first if anything fails.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_latest",
            description="Resolve the latest available model run. Returns date/cycle/run-string.",
            inputSchema={
                "type": "object",
                "properties": {"model": {"type": "string", "default": "hrrr"}},
                "required": [],
            },
        ),
        Tool(
            name="wx_data_packs",
            description=(
                "Describe HRRR-first local data-pack tiers: what works without more downloads "
                "under 1/5/10/50 GB style budgets, and current cache/output disk usage."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "budget_gb": {
                        "type": "number",
                        "description": "Optional local storage budget to select the closest pack.",
                    },
                    "include_current_cache": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_fetch",
            description=(
                "Fetch model GRIB2 data via rustwx-cli with idx-byte-range selective download by default. "
                "Pass `variables` to filter (e.g. ['TMP:2 m above ground','UGRD:10 m above ground']). "
                "Set `full=true` to skip the idx and pull the whole file. "
                "Note: requires the rustwx-cli binary; render/ECAPE tools fetch internally."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "forecast_hour": fhour_schema,
                    "product": {"type": "string", "default": "default"},
                    "variables": {"type": "array", "items": {"type": "string"}},
                    "source": {"type": "string", "description": "aws/nomads/google/azure/ncei/ecmwf/gdex"},
                    "output": {"type": "string", "description": "Path to write the fetched GRIB."},
                    "full": {"type": "boolean", "default": False},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_render_recipe",
            description=(
                "Render any combination of direct, derived, windowed, and heavy recipes as PNGs. "
                "Recipes are looked up in the live product catalog (call wx_products to enumerate). "
                "Examples: direct → mslp_10m_winds, 2m_temperature, 500mb_height_winds, total_qpf; "
                "derived → sbcape, mlcape, mucape, sbecape, mlecape, muecape, "
                "ml_ecape_derived_cape_ratio, ecape_ehi_0_3km, ecape_stp, srh_0_1km, srh_0_3km, "
                "bulk_shear_0_6km, stp_fixed, lapse_rate_700_500; "
                "windowed → qpf_1h, qpf_6h, qpf_total, uh_2to5km_3h_max; "
                "heavy → severe_proof_panel. "
                "Region resolves from `region` (preset) or `location` (city/lat-lon nearest preset)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "recipes": {"type": "array", "items": {"type": "string"}},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "forecast_hour": fhour_schema,
                    "region": {"type": "string"},
                    "location": location_schema,
                    "source": {"type": "string", "default": "aws"},
                    "place_label_density": {"type": "string", "default": "major",
                                              "enum": ["none", "major", "major-and-aux", "dense"]},
                    "contour_mode": {"type": "string", "default": "automatic"},
                    "allow_large_heavy_domain": {"type": "boolean", "default": False},
                },
                "required": ["recipes"],
            },
        ),
        Tool(
            name="wx_windowed",
            description=(
                "Render time-window products: qpf_1h, qpf_6h, qpf_12h, qpf_24h, qpf_total, "
                "uh_2to5km_1h_max, uh_2to5km_3h_max, uh_2to5km_run_max. HRRR-only (uses "
                "hrrr_windowed_batch). Set forecast_hour to the last hour of the window."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "products": {"type": "array", "items": {"type": "string"}},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "forecast_hour": {"type": "integer", "default": 6},
                    "region": {"type": "string"}, "location": location_schema,
                    "source": {"type": "string", "default": "aws"},
                },
                "required": ["products"],
            },
        ),
        Tool(
            name="wx_severe_panel",
            description=(
                "Generate the heavy severe + ECAPE panel (multi-product) from one shared heavy "
                "thermodynamic load. Use this when you want the full severe-weather plate; for "
                "individual products call wx_render_recipe instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                    "source": {"type": "string", "default": "aws"},
                    "allow_large_heavy_domain": {"type": "boolean", "default": False},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_cape",
            description="Render SBCAPE/MLCAPE/MUCAPE map. parcel ∈ {sb, ml, mu}.",
            inputSchema={
                "type": "object",
                "properties": {
                    "parcel": {"type": "string", "enum": ["sb", "ml", "mu"], "default": "sb"},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_srh",
            description="Render storm-relative helicity map. layer_km ∈ {1, 3} for 0-1 km or 0-3 km SRH.",
            inputSchema={
                "type": "object",
                "properties": {
                    "layer_km": {"type": "integer", "enum": [1, 3], "default": 1},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_shear",
            description="Render 0-6 km bulk wind shear map.",
            inputSchema={
                "type": "object",
                "properties": {
                    "layer_km": {"type": "integer", "enum": [6], "default": 6},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_ecape",
            description=(
                "Render the first-class ECAPE map (sbecape / mlecape / muecape). Single product, "
                "single binary call. For ratio variants (e.g. ml_ecape_derived_cape_ratio) call "
                "wx_render_recipe instead, and for the full Figure-4 ratio panel use wx_ecape_ratio_map."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "parcel": {"type": "string", "enum": ["sb", "ml", "mu"], "default": "ml"},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_stp",
            description="Render fixed-layer Significant Tornado Parameter map.",
            inputSchema={
                "type": "object",
                "properties": {
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema, "forecast_hour": fhour_schema,
                    "region": {"type": "string"}, "location": location_schema,
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_ecape_profile",
            description=(
                "Per-profile ECAPE diagnostics at a (lat, lon). Returns ECAPE/CAPE/CIN/LFC/EL "
                "for SB/ML/MU × entraining/non-entraining × pseudoadiabatic/irreversible. "
                "Sub-second per profile (Rust solver, 5,600-13,000× faster than Python ecape-parcel)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "location": location_schema,
                    "run": run_schema,
                    "forecast_hour": {"type": "integer", "default": 1},
                    "model": {"type": "string", "default": "hrrr"},
                    "include_input_column": {"type": "boolean", "default": False},
                },
                "required": ["location"],
            },
        ),
        Tool(
            name="wx_ecape_grid",
            description=(
                "Full-grid ECAPE statistics over a lat/lon swath. ~17s per HRRR forecast hour for "
                "CONUS-scale boxes. "
                "Runs as a background job — poll wx_job_status with the returned job_id."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "bbox": {"type": "object",
                              "properties": {
                                  "west": {"type": "number"}, "east": {"type": "number"},
                                  "south": {"type": "number"}, "north": {"type": "number"}}},
                    "location": location_schema,
                    "radius_km": {"type": "number", "default": 400.0},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "forecast_hour": {"type": "integer", "default": 1},
                    "domain_slug": {"type": "string", "default": "custom_swath"},
                    "background": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_ecape_ratio_map",
            description=(
                "Render the showcase ECAPE/CAPE ratio map: MLECAPE filled + ECAPE/CAPE ratio "
                "contours with magnitude masking. HRRR only. "
                "Region presets include the wide gulf-to-kansas option useful for severe-weather setups. "
                "Background job — poll wx_job_status."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "region": {"type": "string"},
                    "location": location_schema,
                    "run": run_schema,
                    "forecast_hour": fhour_schema,
                    "source": {"type": "string", "default": "nomads"},
                    "allow_large_heavy_domain": {"type": "boolean", "default": False},
                    "background": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_cross_section",
            description=(
                "Compatibility alias for wx_volume_cross_section. HRRR-only; builds a small "
                "temporary pressure VolumeStore and renders PNG/WebP cross-section products. "
                "Future model support should extend the VolumeStore path, not the legacy proof renderer."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "products": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Product slugs, or omit/use ['all'] for all non-smoke VolumeStore products.",
                    },
                    "product": {"type": "string", "description": "Single-product shortcut."},
                    "route": {"type": "string"},
                    "start": location_schema,
                    "end": location_schema,
                    "run": run_schema,
                    "forecast_hour": fhour_schema,
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}},
                    "forecast_hour_start": {"type": "integer"},
                    "forecast_hour_end": {"type": "integer"},
                    "source": {"type": "string", "default": "nomads"},
                    "spacing_km": {"type": "number", "default": 10.0},
                    "width": {"type": "integer", "default": 1400},
                    "height": {"type": "integer", "default": 820},
                    "max_build_hours": {"type": "integer", "default": 3},
                    "allow_more_hours": {"type": "boolean", "default": False},
                    "keep_store": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_volume_cross_section",
            description=(
                "Fast HRRR-only cross sections through the new rustwx pressure VolumeStore. "
                "Builds a small temporary local store for the requested route/hour window "
                "(1-3 hours by default), renders PNG and WebP products, then prunes old stores. "
                "Products include all wxsection non-smoke styles: temperature, wind_speed, theta_e, "
                "rh, q, omega, vorticity, shear, lapse_rate, cloud, cloud_total, wetbulb, icing, "
                "frontogenesis, vpd, dewpoint_dep, moisture_transport, pv, fire_wx."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "products": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Product slugs, or omit/use ['all'] for all non-smoke VolumeStore products.",
                    },
                    "product": {"type": "string", "description": "Single-product shortcut."},
                    "route": {"type": "string", "default": "socal-coast-desert"},
                    "start": location_schema,
                    "end": location_schema,
                    "run": run_schema,
                    "forecast_hour": fhour_schema,
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}},
                    "forecast_hour_start": {"type": "integer"},
                    "forecast_hour_end": {"type": "integer"},
                    "source": {"type": "string", "default": "nomads"},
                    "spacing_km": {"type": "number", "default": 10.0},
                    "width": {"type": "integer", "default": 1400},
                    "height": {"type": "integer", "default": 820},
                    "top_pressure_hpa": {"type": "integer", "default": 100},
                    "bounds_padding_deg": {"type": "number", "default": 1.5},
                    "load_parallelism": {"type": "integer", "default": 2},
                    "max_build_hours": {"type": "integer", "default": 3},
                    "allow_more_hours": {"type": "boolean", "default": False},
                    "store_ttl_hours": {"type": "number", "default": 6.0},
                    "keep_store": {"type": "boolean", "default": True},
                    "out_dir": {"type": "string"},
                    "timeout": {"type": "integer", "default": 900},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_satellite",
            description=(
                "Render latest GOES satellite imagery via rustwx.render_goes_satellite_json. "
                "Default products match the CA Fire satellite lane: GeoColor, GLM overlay, "
                "RGB composites, and ABI Bands 1-16."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "satellite": {"type": "string", "default": "goes18"},
                    "abi_product": {"type": "string", "default": "ABI-L2-CMIPC"},
                    "domain": {"type": "string", "default": "pacific_southwest"},
                    "bounds": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "[west, east, south, north]; overrides domain",
                    },
                    "label": {"type": "string"},
                    "products": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": satellite_tool.DEFAULT_PRODUCTS,
                    },
                    "width": {"type": "integer"},
                    "height": {"type": "integer"},
                    "scan_lookback_hours": {"type": "integer", "default": 6},
                    "discovery_retries": {"type": "integer"},
                    "retry_sleep_ms": {"type": "integer"},
                    "use_cache": {"type": "boolean", "default": True},
                    "download_glm": {"type": "boolean", "default": False},
                    "glm_fetch_count": {"type": "integer"},
                    "glm_lookback_hours": {"type": "integer"},
                    "glm_max_age_min": {"type": "number"},
                    "high_speed_png": {"type": "boolean", "default": True},
                    "skip_scan_id": {"type": "string"},
                    "out_dir": {"type": "string"},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_meteogram",
            description=(
                "Sample a point forecast time series via rustwx.sample_point_timeseries_json. "
                "Pass store_id to sample a warmed point-timeseries store instead."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "location": location_schema,
                    "lat": {"type": "number"},
                    "lon": {"type": "number"},
                    "store_id": {"type": "string"},
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "source": {"type": "string", "default": "nomads"},
                    "forecast_hour_start": {"type": "integer", "default": 0},
                    "forecast_hour_end": {"type": "integer", "default": 3},
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}},
                    "variables": {"type": "array", "items": {"type": "string"}},
                    "method": {
                        "type": "string",
                        "enum": ["nearest", "nearest-cell", "inverse-distance-4", "idw4"],
                        "default": "nearest",
                    },
                    "use_cache": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_meteogram_warm_store",
            description=(
                "Warm an in-memory rustwx point-timeseries grid store for repeated meteogram sampling. "
                "Use the returned store_id with wx_meteogram."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "model": {"type": "string", "default": "hrrr"},
                    "run": run_schema,
                    "source": {"type": "string", "default": "nomads"},
                    "bounds": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "[west, east, south, north]; defaults to Southern California",
                    },
                    "forecast_hour_start": {"type": "integer", "default": 0},
                    "forecast_hour_end": {"type": "integer", "default": 3},
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}},
                    "variables": {"type": "array", "items": {"type": "string"}},
                    "use_cache": {"type": "boolean", "default": True},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_radar",
            description=(
                "Render NEXRAD Level-II radar PNGs and feature JSON with rustwx radar_export. "
                "Supports base, dual-pol, storm-relative velocity, VIL, echo tops, and all-product exports."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "site": {"type": "string", "description": "4-letter ICAO site ID, e.g. KTLX, KBMX"},
                    "lat": {"type": "number", "description": "Latitude; used with lon to choose nearest radar site"},
                    "lon": {"type": "number", "description": "Longitude; used with lat to choose nearest radar site"},
                    "location": location_schema,
                    "valid_time": {"type": "string", "description": "ISO 8601 UTC; default = now"},
                    "product": {
                        "type": "string",
                        "default": "ref",
                        "description": "ref, vel, sw, zdr, cc, phi, kdp, hca, srv, vil, et, or all",
                    },
                    "products": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                        "description": "Comma list or array. Use 'all' for every renderable product in the scan.",
                    },
                    "size": {"type": "integer", "default": 1024},
                    "min_value": {"type": "number"},
                    "include_tensor": {"type": "boolean", "default": False},
                    "max_tensor_gates": {"type": "integer", "default": 800},
                    "cache_dir": {"type": "string"},
                    "out_dir": {"type": "string"},
                    "timeout": {"type": "integer", "default": 180},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_sounding",
            description=(
                "Render a SHARPpy-style skew-T sounding at (lat, lon) with the native "
                "rustwx sounding_plot binary."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "location": location_schema,
                    "run": run_schema,
                    "forecast_hour": {"type": "integer", "default": 1},
                    "model": {"type": "string", "default": "hrrr"},
                    "source": {"type": "string", "default": "aws"},
                    "sample_method": {
                        "type": "string",
                        "enum": ["nearest", "inverse-distance4", "box-mean"],
                        "default": "nearest",
                    },
                    "box_radius_km": {
                        "type": "number",
                        "description": "Half-width for sample_method='box-mean'; defaults to 25 km.",
                    },
                    "box_radius_deg": {
                        "type": "number",
                        "description": "Optional lat/lon degree half-width for sample_method='box-mean'.",
                    },
                    "crop_radius_deg": {
                        "type": "number",
                        "description": "Model crop radius around the target point. Automatically expands for box soundings.",
                    },
                },
                "required": ["location"],
            },
        ),
        Tool(
            name="wx_build_dataset",
            description=(
                "Build a multi-day dataset of either rendered PNG products (mode='render') or "
                "ECAPE profile probes (mode='probe'). Background job — poll wx_job_status. "
                "Concurrency bounded by `workers` (default 4)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["render", "probe"], "default": "render"},
                    "model": {"type": "string", "default": "hrrr"},
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "cycles": {"type": "array", "items": {"type": "integer"}},
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}, "default": [0]},
                    "region": {"type": "string"},
                    "location": location_schema,
                    "direct_recipes": {"type": "array", "items": {"type": "string"}},
                    "derived_recipes": {"type": "array", "items": {"type": "string"}},
                    "profile_points": {"type": "array", "items": {"type": "string"}},
                    "workers": {"type": "integer", "default": 4},
                    "limit": {"type": "integer"},
                },
                "required": ["start_date", "end_date"],
            },
        ),
        Tool(
            name="wx_research_profile_sweep",
            description=(
                "Run a multi-point ECAPE profile sweep across (point × date × cycle × forecast_hour). "
                "Modes: 'targets' (explicit list of {label, location} or {label, lat, lon}); "
                "'random' (n_random uniform draws within bbox/location/region/CONUS); "
                "'stress' (curated edge-case profiles — high terrain, dry slot, cold pool, etc.). "
                "Writes per-probe JSON + an aggregated summary.csv with timing breakdown "
                "(raw_fetch_s / profile_extract_s / parcel_solver_s / render_s). Background job. "
                "Auto-trims the cache to cache_cap_gb before launch."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "mode": {"type": "string", "enum": ["targets", "random", "stress"], "default": "targets"},
                    "targets": {"type": "array", "items": {"type": "object"}},
                    "region": {"type": "string"},
                    "bbox": {"type": "object",
                              "properties": {
                                  "west": {"type": "number"}, "east": {"type": "number"},
                                  "south": {"type": "number"}, "north": {"type": "number"}}},
                    "location": location_schema,
                    "radius_km": {"type": "number", "default": 600.0},
                    "n_random": {"type": "integer", "default": 50},
                    "seed": {"type": "integer", "default": 20260426},
                    "model": {"type": "string", "default": "hrrr"},
                    "start_date": {"type": "string", "description": "YYYY-MM-DD; default today (UTC)"},
                    "end_date": {"type": "string", "description": "YYYY-MM-DD; default = start_date"},
                    "cycles": {"type": "array", "items": {"type": "integer"}},
                    "forecast_hours": {"type": "array", "items": {"type": "integer"}, "default": [1]},
                    "source": {"type": "string", "default": "aws"},
                    "workers": {"type": "integer", "default": 4},
                    "cache_cap_gb": {"type": "number", "default": 500.0},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_cache_status",
            description="Report rustwx cache disk usage, top file consumers, and per-subdir totals.",
            inputSchema={
                "type": "object",
                "properties": {"top_n": {"type": "integer", "default": 20}},
                "required": [],
            },
        ),
        Tool(
            name="wx_cache_evict",
            description=(
                "Evict least-recently-used files until cache size is below target_gb. "
                "Defaults to dry_run=true — set false to actually delete. "
                "`keep_pattern` is a substring matched against file paths; matching files are never evicted."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "target_gb": {"type": "number", "default": 400.0},
                    "dry_run": {"type": "boolean", "default": True},
                    "keep_pattern": {"type": "string"},
                },
                "required": [],
            },
        ),
        Tool(
            name="wx_job_status",
            description="Poll a background job by job_id. Returns state (pending/running/done/failed), elapsed time, progress, result, and a log tail.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "log_tail": {"type": "integer", "default": 20},
                },
                "required": ["job_id"],
            },
        ),
        Tool(
            name="wx_job_list",
            description="List recent background jobs (newest first).",
            inputSchema={
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 20}},
                "required": [],
            },
        ),
        Tool(
            name="wx_job_cancel",
            description="Mark a running background job as cancelled. Best-effort — Python threads can't be killed mid-flight, but the runner will see the flag at its next yield.",
            inputSchema={
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        ),
    ]


@SERVER.list_tools()
async def _list_tools() -> list[Tool]:
    return _tool_definitions()


@SERVER.call_tool()
async def _call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        result = _dispatch(name, arguments or {})
    except RustwxBinaryMissing as exc:
        result = {"ok": False, "error": str(exc), "binary_name": exc.binary_name}
    except Exception as exc:
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    text = json.dumps(result, default=str, indent=2)
    return [TextContent(type="text", text=text)]


def _dispatch(name: str, args: dict) -> dict | list:
    if name == "wx_models":
        return catalog.models(ENV)
    if name == "wx_recipes":
        return catalog.recipes(ENV)
    if name == "wx_products":
        return catalog.products(ENV, **args)
    if name == "wx_regions":
        return catalog.regions(ENV)
    if name == "wx_doctor":
        return catalog.doctor(ENV)

    if name == "wx_latest":
        return fetch_tool.latest(ENV, model=args.get("model", "hrrr"))
    if name == "wx_data_packs":
        return data_pack_tool.data_packs(
            ENV,
            budget_gb=args.get("budget_gb"),
            include_current_cache=bool(args.get("include_current_cache", True)),
        )
    if name == "wx_fetch":
        return fetch_tool.fetch(ENV, **_with_run(args))

    if name == "wx_render_recipe":
        return render_tool.render_recipe(ENV, **_with_run(args, key="run_str"))
    if name == "wx_windowed":
        return render_tool.windowed(ENV, **_with_run(args, key="run_str"))
    if name == "wx_severe_panel":
        return render_tool.severe_panel(ENV, **_with_run(args, key="run_str"))
    if name == "wx_cape":
        return render_tool.cape(ENV, **_with_run(args, key="run_str"))
    if name == "wx_ecape":
        return render_tool.ecape(ENV, **_with_run(args, key="run_str"))
    if name == "wx_srh":
        return render_tool.srh(ENV, **_with_run(args, key="run_str"))
    if name == "wx_shear":
        return render_tool.shear(ENV, **_with_run(args, key="run_str"))
    if name == "wx_stp":
        return render_tool.stp(ENV, **_with_run(args, key="run_str"))

    if name == "wx_ecape_profile":
        return ecape_tool.profile(ENV, **_with_run(args, key="run_str"))
    if name == "wx_ecape_grid":
        return ecape_tool.grid(ENV, **_with_run(args, key="run_str"))
    if name == "wx_ecape_ratio_map":
        return ecape_tool.ratio_map(ENV, **_with_run(args, key="run_str"))

    if name == "wx_cross_section":
        return volume_cs_tool.volume_cross_section(ENV, **_with_run(args, key="run_str"))
    if name == "wx_volume_cross_section":
        return volume_cs_tool.volume_cross_section(ENV, **_with_run(args, key="run_str"))

    if name == "wx_satellite":
        return satellite_tool.satellite(ENV, **args)
    if name == "wx_meteogram":
        return meteogram_tool.meteogram(ENV, **_with_run(args, key="run_str"))
    if name == "wx_meteogram_warm_store":
        return meteogram_tool.warm_store(ENV, **_with_run(args, key="run_str"))

    if name == "wx_radar":
        return radar_tool.radar(ENV, **args)

    if name == "wx_sounding":
        return sounding_tool.sounding(ENV, **_with_run(args, key="run_str"))

    if name == "wx_build_dataset":
        return ds_tool.build_dataset(ENV, **args)
    if name == "wx_research_profile_sweep":
        return research_tool.profile_sweep(ENV, **args)

    if name == "wx_cache_status":
        return cache_tool.status(ENV, top_n=args.get("top_n", 20))
    if name == "wx_cache_evict":
        return cache_tool.evict(
            ENV,
            target_gb=float(args.get("target_gb", 400.0)),
            dry_run=bool(args.get("dry_run", True)),
            keep_pattern=args.get("keep_pattern"),
        )

    if name == "wx_job_status":
        job = jobs.get(args["job_id"])
        if not job:
            return {"ok": False, "error": f"job {args['job_id']} not found"}
        return {"ok": True, **job.to_payload(log_tail=args.get("log_tail", 20))}
    if name == "wx_job_list":
        return {
            "ok": True,
            "jobs": [j.to_payload(log_tail=4) for j in jobs.list_recent(args.get("limit", 20))],
        }
    if name == "wx_job_cancel":
        return {"ok": jobs.cancel(args["job_id"])}

    return {"ok": False, "error": f"unknown tool: {name}"}


def _with_run(args: dict, key: str = "run") -> dict:
    """Translate the agent-facing `run` arg to the internal kwarg name."""
    out = dict(args)
    if "run" in out and key != "run":
        out[key] = out.pop("run")
    return out


# ── Entry points ────────────────────────────────────────────────────────


async def _serve():
    async with stdio_server() as (read, write):
        await SERVER.run(read, write, SERVER.create_initialization_options())


def _print_tool_list() -> None:
    tools = _tool_definitions()
    print(f"Hermes Weather — {len(tools)} MCP tools:\n")
    for t in tools:
        print(f"  {t.name}")
        # Print first sentence of description for the listing
        desc = (t.description or "").split(". ")[0]
        print(f"    {desc}\n")


def _print_doctor() -> None:
    print("Hermes Weather — local install diagnostics\n")
    print(json.dumps(catalog.doctor(ENV), indent=2))


def _smoke_test() -> int:
    """Render one MLECAPE map for southern-plains as a sanity check."""
    print("Smoke test: rendering MLECAPE for southern-plains...")
    result = render_tool.cape(
        ENV, parcel="ml", region="southern-plains",
        run_str="latest", forecast_hour=0,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


def run_cli() -> int:
    if "--list" in sys.argv:
        _print_tool_list()
        return 0
    if "--doctor" in sys.argv:
        _print_doctor()
        return 0
    if "--test" in sys.argv:
        return _smoke_test()
    asyncio.run(_serve())
    return 0


if __name__ == "__main__":
    sys.exit(run_cli())
