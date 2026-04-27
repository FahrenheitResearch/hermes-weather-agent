"""Map rendering — single agent-v1 entrypoint via `rustwx.render_maps_json`.

All product kinds (direct, light derived, heavy derived/ECAPE, HRRR
windowed) are sent through one rustwx call. The runner internally splits
heavy ECAPE products onto the canonical derived_batch path and keeps
everything else on the shared non-ECAPE path. This collapses the older
plugin's recipe-classifier + binary-router into 30 lines.

Region / domain selection: pass either `region` (preset slug like
"southern-plains" or "conus"), `domain` (any of the 77 built-in domain
slugs from rustwx.list_domains_json — country / region / metro /
watch_area), or `bounds` (custom [west,east,south,north]).
"""
from __future__ import annotations

import time
from pathlib import Path

from ..rustwx import RustwxEnv, parse_run, render_maps, resolve_latest_run

# Common region preset → domain slug mapping. rustwx 0.4 may accept the
# region names directly; we normalise hyphen forms.
REGION_ALIASES = {
    "conus": "conus",
    "midwest": "midwest",
    "southern-plains": "southern-plains",
    "great-lakes": "great-lakes",
    "northeast": "northeast",
    "southeast": "southeast",
    "california": "california",
    "california-square": "california-square",
    "reno-square": "reno-square",
    "gulf-to-kansas": "gulf-to-kansas",
}


def _resolve_run(run_str: str, model: str) -> tuple[str, int]:
    if run_str == "latest":
        return resolve_latest_run(model)
    return parse_run(run_str)


def _resolve_domain(
    region: str | None,
    domain: str | None,
    location: str | dict | tuple | None,
) -> str | None:
    """Pick a domain slug. Region preset > explicit domain > location lookup."""
    if domain:
        return domain.strip().lower().replace("_", "-")
    if region:
        r = region.strip().lower().replace("_", "-").replace(" ", "-")
        return REGION_ALIASES.get(r, r)
    if location is not None:
        # Try to resolve to a built-in domain via rustwx — done at the call site
        # for now; here we just return None and let the caller pass bounds
        # if it has lat/lon.
        return None
    return "conus"


def render_recipe(
    env: RustwxEnv,
    *,
    recipes: list[str],
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    region: str | None = None,
    domain: str | None = None,
    location: str | dict | tuple | None = None,
    bounds: list[float] | None = None,
    source: str = "aws",
    place_label_density: str = "major",
    out_dir: str | None = None,
    timeout: int = 900,
) -> dict:
    """Render any combination of direct / derived / heavy / windowed recipes.

    Sends a single rustwx.render_maps_json request. The runner routes
    light products through non_ecape_hour and heavy ECAPE products
    through derived_batch internally; the response splits results into
    `domains` (light) and `heavy_derived` (heavy) sections.
    """
    if not env.module_available:
        return {
            "ok": False,
            "error": (
                "rustwx Python module not installed. Run: pip install 'rustwx>=0.4'"
            ),
        }
    if not recipes:
        return {"ok": False, "error": "recipes is empty"}

    date, cycle = _resolve_run(run_str, model)
    domain_slug = _resolve_domain(region, domain, location)
    out_root = Path(out_dir) if out_dir else (
        env.out_root / "render" / model /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{domain_slug or 'custom'}"
    )
    out_root.mkdir(parents=True, exist_ok=True)

    request: dict = {
        "date_yyyymmdd": date,
        "cycle_utc": cycle,
        "forecast_hour": forecast_hour,
        "model": model,
        "source": source,
        "products": list(recipes),
        "out_dir": str(out_root.resolve()).replace("\\", "/"),
        "place_label_density": place_label_density,
    }
    if bounds:
        request["bounds"] = list(bounds)
    elif domain_slug:
        request["domain"] = domain_slug

    started = time.time()
    try:
        result = render_maps(env, request)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "request": request,
        }
    elapsed = time.time() - started

    pngs: list[str] = []
    blockers: list[dict] = []
    light_count = 0
    heavy_count = 0

    # Light products land under domains[].summary.output_paths
    for dom in result.get("domains", []) or []:
        s = dom.get("summary") or {}
        for p in s.get("output_paths", []) or []:
            pngs.append(str(p))
            light_count += 1
        for blk in (dom.get("direct") or {}).get("blockers", []) or []:
            blockers.append(blk)
        for blk in (dom.get("derived") or {}).get("blockers", []) or []:
            blockers.append(blk)

    # Heavy products land under heavy_derived.domains[].recipes[]
    heavy = result.get("heavy_derived") or {}
    for dom in heavy.get("domains", []) or []:
        for recipe in dom.get("recipes", []) or []:
            output_path = recipe.get("output_path")
            if output_path:
                pngs.append(str(output_path))
                heavy_count += 1
        for blk in dom.get("blockers", []) or []:
            blockers.append(blk)

    pngs = sorted(set(pngs))
    return {
        "ok": len(pngs) > 0,
        "model": model,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "domain": domain_slug,
        "bounds": bounds,
        "out_dir": str(out_root),
        "pngs": pngs,
        "png_count": len(pngs),
        "light_count": light_count,
        "heavy_count": heavy_count,
        "blockers": blockers,
        "elapsed_s": round(elapsed, 2),
        "agent_total_ms": result.get("agent_total_ms"),
        "shared_timing": result.get("shared_timing"),
    }


# ── Per-field shortcuts ──────────────────────────────────────────────────


def cape(env: RustwxEnv, *, parcel: str = "sb", **kwargs) -> dict:
    recipe = {"sb": "sbcape", "ml": "mlcape", "mu": "mucape"}.get(parcel.lower())
    if not recipe:
        return {"ok": False, "error": f"parcel must be sb/ml/mu, got {parcel!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def ecape(env: RustwxEnv, *, parcel: str = "ml", **kwargs) -> dict:
    recipe = {"sb": "sbecape", "ml": "mlecape", "mu": "muecape"}.get(parcel.lower())
    if not recipe:
        return {"ok": False, "error": f"parcel must be sb/ml/mu, got {parcel!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def srh(env: RustwxEnv, *, layer_km: int = 1, **kwargs) -> dict:
    recipe = {1: "srh_0_1km", 3: "srh_0_3km"}.get(int(layer_km))
    if not recipe:
        return {"ok": False, "error": f"layer_km must be 1 or 3, got {layer_km!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def shear(env: RustwxEnv, *, layer_km: int = 6, **kwargs) -> dict:
    recipe = {1: "bulk_shear_0_1km", 6: "bulk_shear_0_6km"}.get(int(layer_km))
    if not recipe:
        return {"ok": False, "error": f"layer_km must be 1 or 6, got {layer_km!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def stp(env: RustwxEnv, **kwargs) -> dict:
    return render_recipe(env, recipes=["stp_fixed"], **kwargs)


def windowed(env: RustwxEnv, *, products: list[str], forecast_hour: int = 6, **kwargs) -> dict:
    """QPF / UH window products go through the same render_maps_json call."""
    if not products:
        return {"ok": False, "error": "products list is empty"}
    return render_recipe(env, recipes=products, forecast_hour=forecast_hour, **kwargs)


def severe_panel(env: RustwxEnv, **kwargs) -> dict:
    """Multi-product severe panel — request the canonical severe family
    via the unified render call. The runner routes heavy ECAPE recipes
    through derived_batch and others through non_ecape_hour automatically.
    """
    severe_recipes = [
        # severe core
        "mlcape", "mucape", "sbcape",
        "srh_0_1km", "srh_0_3km",
        "bulk_shear_0_1km", "bulk_shear_0_6km",
        "stp_fixed",
        # heavy ECAPE
        "mlecape", "muecape", "sbecape",
        "ecape_ehi_0_3km", "ecape_stp",
    ]
    return render_recipe(env, recipes=severe_recipes, **kwargs)
