"""Catalog tools — live from `rustwx.agent_capabilities_json()`.

The rustwx Python module is the source of truth: models, recipes
(direct / light_derived / heavy_derived / windowed), domains, request
schema. This module exposes thin filtered views suitable for an agent
to consume without re-reading the full capabilities JSON.

If `import rustwx` isn't installed, all functions degrade with a clear
"install rustwx>=0.4" error rather than crashing the MCP server.
"""
from __future__ import annotations

import json

from ..rustwx import RustwxEnv, list_domains as _list_domains

# Cross-section product palette — kept in code because the cross-section
# render path isn't part of agent-v1 yet.
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


def _require_caps(env: RustwxEnv) -> dict | None:
    """Return rustwx capabilities dict, or None if module unavailable."""
    return env.capabilities


def _model_entry(caps: dict, model_id: str) -> dict | None:
    for m in caps.get("models", []) or []:
        if m.get("id") == model_id or m.get("model_id") == model_id:
            return m
    return None


# ── Public catalog tools ────────────────────────────────────────────────


def models(env: RustwxEnv) -> dict:
    """List available models with sources/products/recipes."""
    caps = _require_caps(env)
    if caps is None:
        return _module_error()
    out_models = []
    for m in caps.get("models", []) or []:
        out_models.append({
            "id": m.get("id"),
            "default_product": m.get("default_product"),
            "default_render_product": m.get("default_render_product"),
            "products": m.get("products") or [],
            "sources": m.get("sources") or [],
            "max_forecast_hour": m.get("max_forecast_hour"),
            "direct_recipe_count": len(m.get("direct_recipes") or []),
            "light_derived_recipe_count": len(m.get("light_derived_recipes") or []),
            "heavy_derived_recipe_count": len(m.get("heavy_derived_recipes") or []),
            "windowed_recipe_count": len(m.get("windowed_products") or m.get("windowed_recipes") or []),
        })
    return {
        "agent_api": caps.get("agent_api"),
        "rustwx_version": env.module_version,
        "count": len(out_models),
        "models": out_models,
    }


def recipes(env: RustwxEnv, *, model: str = "hrrr") -> dict:
    """List recipe slugs for a given model, grouped by kind."""
    caps = _require_caps(env)
    if caps is None:
        return _module_error()
    m = _model_entry(caps, model)
    if not m:
        return {
            "ok": False,
            "error": f"unknown model {model!r}",
            "available": [m.get("id") for m in caps.get("models", [])],
        }
    return {
        "ok": True,
        "model": model,
        "direct": m.get("direct_recipes") or [],
        "light_derived": m.get("light_derived_recipes") or m.get("derived_recipes") or [],
        "heavy_derived": m.get("heavy_derived_recipes") or [],
        "windowed": m.get("windowed_products") or m.get("windowed_recipes") or [],
        "cross_section_products": CROSS_SECTION_PRODUCTS,
        "cross_section_routes": CROSS_SECTION_ROUTES,
    }


def products(
    env: RustwxEnv,
    *,
    kind: str | None = None,
    model: str = "hrrr",
    search: str | None = None,
) -> dict:
    """Return product slugs filtered by kind / model / substring search.

    `kind` ∈ {direct, derived, light_derived, heavy_derived, windowed}.
    'derived' (without prefix) returns light + heavy together.
    """
    caps = _require_caps(env)
    if caps is None:
        return _module_error()
    m = _model_entry(caps, model)
    if not m:
        return {"ok": False, "error": f"unknown model {model!r}"}

    buckets = {
        "direct": m.get("direct_recipes") or [],
        "light_derived": m.get("light_derived_recipes") or m.get("derived_recipes") or [],
        "heavy_derived": m.get("heavy_derived_recipes") or [],
        "windowed": m.get("windowed_products") or m.get("windowed_recipes") or [],
    }
    if kind == "derived":
        wanted = buckets["light_derived"] + buckets["heavy_derived"]
    elif kind in buckets:
        wanted = buckets[kind]
    elif kind is None:
        wanted = sum(buckets.values(), [])
    else:
        return {"ok": False, "error": f"unknown kind {kind!r}"}

    if search:
        needle = search.lower()
        wanted = [p for p in wanted if needle in p.lower()]

    return {
        "ok": True,
        "model": model,
        "kind": kind,
        "search": search,
        "count": len(wanted),
        "products": wanted,
    }


def regions(env: RustwxEnv) -> dict:
    """Return rustwx region domain presets (kind=region)."""
    return _list_domains(env, kind="region")


def domains(env: RustwxEnv, *, kind: str | None = None,
            limit: int | None = None) -> dict:
    """Return all rustwx domains. kind ∈ {country, region, metro, watch_area}."""
    return _list_domains(env, kind=kind, limit=limit)


def doctor(env: RustwxEnv) -> dict:
    """Diagnostic: rustwx module state, capabilities summary, optional binaries."""
    info: dict = {
        "rustwx_module_available": env.module_available,
        "rustwx_version": env.module_version,
        "cache_dir": str(env.cache_dir),
        "out_root": str(env.out_root),
    }
    if env.capabilities:
        caps = env.capabilities
        info["agent_api"] = caps.get("agent_api")
        info["models"] = [m.get("id") for m in caps.get("models", []) or []]
        info["domain_count"] = caps.get("domains", {}).get("count")
    else:
        info["error"] = (
            "rustwx Python module not found. Run: pip install 'rustwx>=0.4'"
        )

    info["optional_binaries"] = {
        name: str(p) for name, p in env.binaries.items()
    }
    info["specialty_tools"] = {
        "sounding":             env.has_binary("sounding_plot"),
        "cross_section":        env.has_binary("cross_section_proof"),
        "ecape_profile_probe":  env.has_binary("hrrr_ecape_profile_probe"),
        "ecape_grid_research":  env.has_binary("hrrr_ecape_grid_research"),
    }
    if env.module_available:
        info["advice"] = (
            "Map rendering (direct/derived/heavy ECAPE/windowed) is fully "
            "available via the rustwx agent-v1 API. Specialty tools "
            "(sounding, cross sections, ECAPE profile probe, ECAPE grid "
            "research) require the corresponding optional binaries built "
            "from the rustwx workspace; build them with cargo build "
            "--release --bin <name> and set HERMES_RUSTWX_BIN_DIR."
        )
    else:
        info["advice"] = "Install the rustwx Python module: pip install 'rustwx>=0.4'"
    return info


def _module_error() -> dict:
    return {
        "ok": False,
        "error": "rustwx Python module not installed",
        "fix": "pip install 'rustwx>=0.4'",
    }
