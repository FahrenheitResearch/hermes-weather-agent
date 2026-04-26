"""Catalog tools — live product inventory loaded from rustwx `product_catalog`.

The agent's source of truth is `product_catalog`, a rustwx binary that emits a
JSON inventory of every direct / derived / heavy / windowed product the
workspace currently supports, including kind, status (supported/partial/blocked),
maturity (operational/experimental), per-model fetch-mode, blockers, runner
list, and provenance flags (`proxy`, etc.).

We load the catalog lazily, cache it for the life of the process, and fall
back to a hard-coded mirror when the binary isn't built. The mirror covers
the recipe set listed in the rustwx product catalog at the time this code
was written; treat it as a backstop, not the authoritative list.
"""
from __future__ import annotations

import json
from typing import Any

from ..rustwx import RustwxEnv, run_json

# Model registry — mirrors rustwx-models::built_in_models()
MODELS = {
    "hrrr": {
        "name": "HRRR",
        "resolution_km": 3.0,
        "coverage": "CONUS",
        "frequency_hours": 1,
        "products": ["sfc", "prs", "nat", "subh"],
        "default_product": "sfc",
        "sources": ["aws", "nomads", "google", "azure"],
        "max_forecast_hour": 48,
    },
    "gfs": {
        "name": "GFS",
        "resolution_deg": 0.25,
        "coverage": "Global",
        "frequency_hours": 6,
        "products": ["pgrb2.0p25"],
        "default_product": "pgrb2.0p25",
        "sources": ["aws", "nomads", "google", "ncei"],
        "max_forecast_hour": 384,
    },
    "ecmwf-open-data": {
        "name": "ECMWF Open Data",
        "frequency_hours": 6,
        "products": ["oper"],
        "default_product": "oper",
        "sources": ["ecmwf", "aws"],
    },
    "rrfs-a": {
        "name": "RRFS-A",
        "resolution_km": 3.0,
        "coverage": "CONUS/NA",
        "frequency_hours": 1,
        "products": ["prs-conus", "nat-na", "prs-na"],
        "default_product": "prs-conus",
        "max_forecast_hour": 60,
    },
    "wrf-gdex": {
        "name": "WRF-GDEX (NetCDF via netcrust)",
        "sources": ["gdex"],
        "max_forecast_hour": 23,
    },
}

# rustwx region presets — comma-aligned with the binaries' --region enum
REGIONS = {
    "conus":             {"description": "Lower 48 (large heavy-domain runs may need allow-large-heavy-domain)"},
    "midwest":           {"description": "Iowa / Illinois / Indiana / Ohio"},
    "great-lakes":       {"description": "Michigan / Wisconsin / Minnesota"},
    "northeast":         {"description": "New England / Mid-Atlantic"},
    "southeast":         {"description": "Florida / Georgia / Carolinas / Alabama"},
    "southern-plains":   {"description": "Oklahoma / North Texas / Kansas — primary severe-weather domain"},
    "gulf-to-kansas":    {"description": "Texas Gulf coast through Kansas"},
    "california":        {"description": "California rectangular crop"},
    "california-square": {"description": "California square crop"},
    "reno-square":       {"description": "Reno / Sierra Nevada square crop"},
}

# Cross-section product palette (cross_section_proof --product <…>)
CROSS_SECTION_PRODUCTS = [
    "temperature", "relative-humidity", "specific-humidity", "theta-e",
    "wind-speed", "wet-bulb", "vapor-pressure-deficit",
    "dewpoint-depression", "moisture-transport", "fire-weather",
]
CROSS_SECTION_ROUTES = [
    "amarillo-chicago", "kansas-city-chicago",
    "san-francisco-tahoe", "sacramento-reno",
    "los-angeles-mojave", "san-diego-imperial",
]


# ── Hard-coded fallback mirror (matches the live catalog at write time) ──
# Used only when `product_catalog` binary is missing. Each entry is a slug
# and a tiny description; the live catalog has much richer metadata.

FALLBACK_DIRECT = {
    "200mb_height_winds": "200 mb height + winds",
    "300mb_height_winds": "300 mb height + winds",
    "250mb_height_winds": "250 mb height + winds",
    "500mb_height_winds": "500 mb height + winds",
    "700mb_height_winds": "700 mb height + winds",
    "850mb_height_winds": "850 mb height + winds",
    "200mb_temperature_height_winds": "200 mb temperature + height + winds",
    "300mb_temperature_height_winds": "300 mb temperature + height + winds",
    "250mb_temperature_height_winds": "250 mb temperature + height + winds",
    "500mb_temperature_height_winds": "500 mb temperature + height + winds",
    "850mb_temperature_height_winds": "850 mb temperature + height + winds",
    "700mb_temperature_height_winds": "700 mb temperature + height + winds",
    "2m_relative_humidity": "2 m relative humidity",
    "2m_relative_humidity_10m_winds": "2 m RH + 10 m winds",
    "2m_temperature": "2 m air temperature",
    "2m_temperature_10m_winds": "2 m T + 10 m winds",
    "2m_dewpoint": "2 m dewpoint",
    "2m_dewpoint_10m_winds": "2 m dewpoint + 10 m winds",
    "mslp_10m_winds": "MSLP + 10 m winds",
    "10m_wind_gusts": "10 m wind gusts",
    "precipitable_water": "Precipitable water",
    "cloud_cover": "Total cloud cover",
    "low_cloud_cover": "Low cloud cover",
    "middle_cloud_cover": "Mid cloud cover",
    "high_cloud_cover": "High cloud cover",
    "cloud_cover_levels": "Cloud cover by level",
    "visibility": "Visibility",
    "simulated_ir_satellite": "Simulated IR satellite (partial)",
    "total_qpf": "Total QPF",
    "categorical_rain": "Categorical rain (partial)",
    "categorical_freezing_rain": "Categorical freezing rain (partial)",
    "categorical_ice_pellets": "Categorical ice pellets (partial)",
    "categorical_snow": "Categorical snow (partial)",
    "precipitation_type": "Precip type (partial)",
    "700mb_dewpoint_height_winds": "700 mb dewpoint + height + winds (partial)",
    "850mb_dewpoint_height_winds": "850 mb dewpoint + height + winds (partial)",
    "200mb_rh_height_winds": "200 mb RH + height + winds",
    "300mb_rh_height_winds": "300 mb RH + height + winds",
    "500mb_rh_height_winds": "500 mb RH + height + winds",
    "700mb_rh_height_winds": "700 mb RH + height + winds",
    "850mb_rh_height_winds": "850 mb RH + height + winds",
    "200mb_absolute_vorticity_height_winds": "200 mb absolute vorticity + height + winds (partial)",
    "300mb_absolute_vorticity_height_winds": "300 mb absolute vorticity + height + winds (partial)",
    "500mb_absolute_vorticity_height_winds": "500 mb absolute vorticity + height + winds (partial)",
    "700mb_absolute_vorticity_height_winds": "700 mb absolute vorticity + height + winds (partial)",
    "850mb_absolute_vorticity_height_winds": "850 mb absolute vorticity + height + winds (partial)",
    "1km_reflectivity": "1 km reflectivity (partial)",
    "composite_reflectivity": "Composite reflectivity (partial)",
    "composite_reflectivity_uh": "Composite reflectivity + UH overlay (partial)",
    "uh_2to5km": "Updraft helicity 2-5 km (partial)",
    "smoke_pm25_native": "Surface smoke PM2.5 (partial)",
    "smoke_column": "Vertically-integrated smoke (partial)",
}

FALLBACK_DERIVED = {
    "sbcape": "Surface-based CAPE",
    "sbcin":  "Surface-based CIN",
    "sblcl":  "Surface-based LCL",
    "mlcape": "Mixed-layer CAPE",
    "mlcin":  "Mixed-layer CIN",
    "mucape": "Most-unstable CAPE",
    "mucin":  "Most-unstable CIN",
    "sbecape": "Surface-based ECAPE — full parcel-path entraining CAPE",
    "mlecape": "Mixed-layer ECAPE",
    "muecape": "Most-unstable ECAPE",
    "sb_ecape_derived_cape_ratio": "SB ECAPE / SB derived-CAPE ratio (experimental)",
    "ml_ecape_derived_cape_ratio": "ML ECAPE / ML derived-CAPE ratio (experimental)",
    "mu_ecape_derived_cape_ratio": "MU ECAPE / MU derived-CAPE ratio (experimental)",
    "sb_ecape_native_cape_ratio":  "SB ECAPE / SB native-CAPE ratio (experimental)",
    "ml_ecape_native_cape_ratio":  "ML ECAPE / ML native-CAPE ratio (experimental)",
    "mu_ecape_native_cape_ratio":  "MU ECAPE / MU native-CAPE ratio (experimental)",
    "sbncape": "Surface-based NCAPE (entraining)",
    "sbecin":  "Surface-based ECIN (entraining)",
    "mlecin":  "Mixed-layer ECIN (entraining)",
    "ecape_scp": "Supercell composite using ECAPE (experimental)",
    "ecape_ehi_0_1km": "ECAPE-EHI 0-1 km (experimental)",
    "ecape_ehi_0_3km": "ECAPE-EHI 0-3 km (experimental)",
    "ecape_stp": "ECAPE-based STP (experimental)",
    "theta_e_2m_10m_winds":  "2 m theta-e + 10 m winds",
    "vpd_2m":                "2 m vapor pressure deficit",
    "dewpoint_depression_2m": "2 m dewpoint depression",
    "wetbulb_2m":            "2 m wet-bulb temperature",
    "fire_weather_composite": "Fire-weather composite",
    "apparent_temperature_2m": "2 m apparent temperature",
    "heat_index_2m":         "2 m heat index",
    "wind_chill_2m":         "2 m wind chill",
    "lifted_index":          "Lifted index",
    "lapse_rate_700_500":    "700-500 mb lapse rate",
    "lapse_rate_0_3km":      "0-3 km lapse rate",
    "bulk_shear_0_1km":      "0-1 km bulk shear",
    "bulk_shear_0_6km":      "0-6 km bulk shear",
    "srh_0_1km":             "0-1 km storm-relative helicity",
    "srh_0_3km":             "0-3 km storm-relative helicity",
    "ehi_0_1km":             "0-1 km energy-helicity index",
    "ehi_0_3km":             "0-3 km energy-helicity index",
    "stp_fixed":             "Fixed-layer significant tornado parameter",
    "scp_mu_0_3km_0_6km_proxy": "Supercell composite proxy (MU CAPE, 0-3 km SRH, 0-6 km shear)",
    "temperature_advection_700mb": "700 mb temperature advection",
    "temperature_advection_850mb": "850 mb temperature advection",
}

FALLBACK_HEAVY = {
    "severe_proof_panel": "Multi-product severe + ECAPE panel from one heavy thermo load",
}

FALLBACK_WINDOWED = {
    "qpf_1h":  "1-hour QPF window",
    "qpf_6h":  "6-hour QPF window",
    "qpf_12h": "12-hour QPF window",
    "qpf_24h": "24-hour QPF window",
    "qpf_total": "Run-total QPF",
    "uh_2to5km_1h_max":  "1-hour 2-5 km UH max",
    "uh_2to5km_3h_max":  "3-hour 2-5 km UH max",
    "uh_2to5km_run_max": "Run-max 2-5 km UH",
}

# hrrr_windowed_batch's --product flag uses dash-separated slugs that don't
# match the catalog slugs exactly; this maps catalog → CLI flag form.
WINDOWED_CLI_SLUG = {
    "qpf_1h": "qpf1h",
    "qpf_6h": "qpf6h",
    "qpf_12h": "qpf12h",
    "qpf_24h": "qpf24h",
    "qpf_total": "qpf-total",
    "uh_2to5km_1h_max": "uh25km1h",
    "uh_2to5km_3h_max": "uh25km3h",
    "uh_2to5km_run_max": "uh25km-run-max",
}


# ── Live catalog loader ─────────────────────────────────────────────────


_LIVE_CATALOG: dict | None = None
_LIVE_LOAD_ERROR: str | None = None


def _load_live(env: RustwxEnv) -> dict | None:
    """Load and cache the rustwx product_catalog JSON. Returns None if the
    binary is missing or fails. Result is memoised across calls."""
    global _LIVE_CATALOG, _LIVE_LOAD_ERROR
    if _LIVE_CATALOG is not None:
        return _LIVE_CATALOG
    if not env.has("product_catalog"):
        _LIVE_LOAD_ERROR = "product_catalog binary not built"
        return None
    try:
        result = run_json(env, "product_catalog", [], timeout=30)
    except Exception as exc:
        _LIVE_LOAD_ERROR = f"product_catalog failed: {exc}"
        return None
    if not isinstance(result, dict):
        _LIVE_LOAD_ERROR = "product_catalog returned non-dict JSON"
        return None
    _LIVE_CATALOG = result
    _LIVE_LOAD_ERROR = None
    return _LIVE_CATALOG


def _live_recipe_index(env: RustwxEnv) -> dict[str, dict]:
    """Index every product slug → entry across direct/derived/heavy/windowed."""
    catalog = _load_live(env)
    if catalog is None:
        return {}
    out: dict[str, dict] = {}
    for kind in ("direct", "derived", "heavy", "windowed"):
        for entry in catalog.get(kind, []):
            slug = entry.get("slug")
            if slug:
                out[slug] = entry
    return out


def lookup_recipe(env: RustwxEnv, slug: str) -> dict | None:
    """Return the live catalog entry for `slug`, or None if missing."""
    return _live_recipe_index(env).get(slug)


def recipe_kind(env: RustwxEnv, slug: str) -> str | None:
    """Return 'direct'/'derived'/'heavy'/'windowed', or None if unknown.

    Falls back to the hard-coded mirror when the live catalog is unavailable.
    """
    entry = lookup_recipe(env, slug)
    if entry is not None:
        return entry.get("kind")
    if slug in FALLBACK_DIRECT:
        return "direct"
    if slug in FALLBACK_DERIVED:
        return "derived"
    if slug in FALLBACK_HEAVY:
        return "heavy"
    if slug in FALLBACK_WINDOWED:
        return "windowed"
    return None


def supports_model(env: RustwxEnv, slug: str, model: str) -> str:
    """Return the support status ('supported'/'partial'/'blocked'/'unknown')
    of `slug` on `model`."""
    entry = lookup_recipe(env, slug)
    if entry is None:
        return "unknown"
    for support in entry.get("support", []):
        if support.get("target") == model:
            return support.get("status", "unknown")
    return "unknown"


def runners_for(env: RustwxEnv, slug: str) -> list[str]:
    """Return the runner-binary list for `slug` (e.g. ['derived_batch','hrrr_derived_batch'])."""
    entry = lookup_recipe(env, slug)
    if entry is None:
        return []
    return list(entry.get("runners", []))


# ── Public catalog tools ────────────────────────────────────────────────


def models(env: RustwxEnv) -> dict:
    """Return the full model catalog. Tries `rustwx-cli list` for live data,
    falls back to the hard-coded mirror."""
    out: dict[str, Any] = {"models": MODELS, "source": "builtin"}
    if env.has("rustwx-cli"):
        try:
            # rustwx-cli list emits "id: description" lines, not JSON
            from ..rustwx import run as run_proc
            result = run_proc(env, "rustwx-cli", ["list"], timeout=20)
            if result.ok and result.stdout.strip():
                parsed = []
                for line in result.stdout.splitlines():
                    if ":" in line:
                        slug, desc = line.split(":", 1)
                        parsed.append({"id": slug.strip(), "description": desc.strip()})
                if parsed:
                    out["live_models"] = parsed
                    out["source"] = "rustwx-cli"
        except Exception:
            pass
    return out


def regions(_env: RustwxEnv) -> dict:
    """Return the rustwx region preset catalog."""
    return {"regions": REGIONS}


def recipes(env: RustwxEnv) -> dict:
    """Return the recipe catalog. Live data preferred, fallback to mirror.

    Live entries include status/maturity/runners/support — agents should
    inspect those before promising the user a render.
    """
    catalog = _load_live(env)
    if catalog is None:
        return {
            "source": "fallback",
            "fallback_reason": _LIVE_LOAD_ERROR,
            "direct": FALLBACK_DIRECT,
            "derived": FALLBACK_DERIVED,
            "heavy": FALLBACK_HEAVY,
            "windowed": FALLBACK_WINDOWED,
            "cross_section_products": CROSS_SECTION_PRODUCTS,
            "cross_section_routes": CROSS_SECTION_ROUTES,
        }
    return {
        "source": "live",
        "summary": catalog.get("summary"),
        "direct":   _trim_entries(catalog.get("direct", [])),
        "derived":  _trim_entries(catalog.get("derived", [])),
        "heavy":    _trim_entries(catalog.get("heavy", [])),
        "windowed": _trim_entries(catalog.get("windowed", [])),
        "cross_section_products": CROSS_SECTION_PRODUCTS,
        "cross_section_routes": CROSS_SECTION_ROUTES,
    }


def products(env: RustwxEnv, *, kind: str | None = None,
             status: str | None = None, model: str | None = None,
             search: str | None = None) -> dict:
    """Return the live product catalog with optional filters.

    kind:    'direct' | 'derived' | 'heavy' | 'windowed' | None for all
    status:  'supported' | 'partial' | 'blocked' | None for all
    model:   restrict to entries that have any support row matching this model
    search:  substring match against slug or title (case-insensitive)
    """
    catalog = _load_live(env)
    if catalog is None:
        return {
            "ok": False,
            "error": _LIVE_LOAD_ERROR or "product_catalog unavailable",
            "advice": "Build with: cargo build --release --bin product_catalog",
        }
    out_kinds: list[str] = ["direct", "derived", "heavy", "windowed"] if kind is None else [kind]
    rows: list[dict] = []
    needle = (search or "").strip().lower()
    for k in out_kinds:
        for entry in catalog.get(k, []):
            if status and entry.get("status") != status:
                continue
            if model:
                models_supported = {s.get("target") for s in entry.get("support", [])
                                     if s.get("status") in ("supported", "partial")}
                if model not in models_supported:
                    continue
            if needle:
                hay = (entry.get("slug", "") + " " + entry.get("title", "")).lower()
                if needle not in hay:
                    continue
            rows.append(_compact(entry))
    return {
        "ok": True,
        "count": len(rows),
        "summary": catalog.get("summary"),
        "filters": {"kind": kind, "status": status, "model": model, "search": search},
        "products": rows,
    }


def doctor(env: RustwxEnv) -> dict:
    """Diagnostic: which binaries did we find, where, and what's missing.

    Critical = the minimum set required for the showcase tools. Missing
    "nice-to-have" binaries (forecast_now, region galleries, etc.) are
    listed separately so the agent can degrade gracefully.
    """
    found = {name: str(p) for name, p in env.binaries.items()}
    critical_set = {
        "rustwx-cli", "product_catalog",
        "direct_batch", "derived_batch",
        "hrrr_direct_batch", "hrrr_derived_batch",
        "hrrr_windowed_batch", "heavy_panel_hour",
        "hrrr_ecape_ratio_display", "hrrr_ecape_grid_research",
        "hrrr_ecape_profile_probe", "cross_section_proof",
    }
    missing = sorted(critical_set - set(env.binaries))
    catalog = _load_live(env)
    return {
        "bin_dir": str(env.bin_dir) if env.bin_dir else None,
        "found": found,
        "missing_critical": missing,
        "netcdf_runtime": "not required by current rustwx builds; WRF NetCDF4 reads use netcrust",
        "cache_dir": str(env.cache_dir),
        "out_root": str(env.out_root),
        "rustwx_cli_available": env.has("rustwx-cli"),
        "product_catalog_loaded": catalog is not None,
        "product_catalog_summary": (catalog or {}).get("summary"),
        "advice": (
            f"Build missing binaries via cargo build --release --bin <name> "
            f"and set HERMES_RUSTWX_BIN_DIR. Missing: {missing}"
            if missing else
            "All critical rustwx binaries are reachable."
        ),
    }


# ── helpers ─────────────────────────────────────────────────────────────


def _trim_entries(entries: list[dict]) -> list[dict]:
    """Strip the heaviest fields from each entry for compact recipe listings."""
    return [_compact(e) for e in entries]


def _compact(entry: dict) -> dict:
    """Compact view of a product catalog entry for agent consumption."""
    support_summary = []
    for s in entry.get("support", []):
        support_summary.append({
            "model": s.get("target"),
            "status": s.get("status"),
            "blockers": s.get("blockers", []),
        })
    return {
        "slug": entry.get("slug"),
        "title": entry.get("title"),
        "kind": entry.get("kind"),
        "status": entry.get("status"),
        "maturity": entry.get("maturity"),
        "flags": entry.get("flags", []),
        "experimental": entry.get("experimental", False),
        "render_style": entry.get("render_style"),
        "runners": entry.get("runners", []),
        "support": support_summary,
        "category": (entry.get("product_metadata") or {}).get("category"),
        "lineage": (entry.get("product_metadata") or {})
                    .get("provenance", {}).get("lineage"),
    }
