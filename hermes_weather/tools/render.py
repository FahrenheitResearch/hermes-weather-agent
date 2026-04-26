"""Map-rendering tools — direct, derived, windowed, and heavy panels.

Notes:
- The product catalog can overstate per-model support: a recipe may be
  listed as `supported` for a model, but the binary's planner reports a
  blocker at runtime ("missing GRIB selector ...", "direct planner missed
  fetch for canonical family ..."). When that happens the binary still
  exits rc=0 with an empty PNG list — we read the manifest and surface
  the blockers so the agent can pick a different recipe.


Recipes are looked up in the live `product_catalog`; the right binary is
chosen based on the recipe's `kind` (direct/derived/heavy/windowed) and the
model (HRRR-specialised binaries are preferred when available).

PNGs are written to disk; tools return file paths.
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path

from ..geo import resolve_region
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run
from .catalog import (
    FALLBACK_DERIVED, FALLBACK_DIRECT, FALLBACK_HEAVY, FALLBACK_WINDOWED,
    WINDOWED_CLI_SLUG, recipe_kind, runners_for, supports_model,
)


def _read_blockers(manifest_paths: list) -> list[tuple[str, str]]:
    """Read direct/derived manifest JSONs and return [(recipe_slug, reason), ...]."""
    blockers: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for mp in manifest_paths:
        try:
            data = json.loads(Path(str(mp)).read_text(encoding="utf-8"))
        except Exception:
            continue
        for b in data.get("blockers") or []:
            slug = b.get("recipe_slug") or "?"
            reason = (b.get("reason") or "").strip()
            key = (slug, reason)
            if key not in seen:
                seen.add(key)
                blockers.append(key)
    return blockers


# ── Recipe → binary routing ─────────────────────────────────────────────


def _classify(env: RustwxEnv, recipes: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    """Bucket recipes by kind, return (buckets, unknowns).

    Buckets keys: "direct", "derived", "windowed", "heavy".
    Unknowns are slugs the catalog doesn't know about.
    """
    buckets: dict[str, list[str]] = defaultdict(list)
    unknowns: list[str] = []
    for r in recipes:
        kind = recipe_kind(env, r)
        if kind in ("direct", "derived", "heavy", "windowed"):
            buckets[kind].append(r)
        else:
            unknowns.append(r)
    return buckets, unknowns


def _binary_for(env: RustwxEnv, kind: str, model: str, slug: str) -> str | None:
    """Select the best binary for (kind, model, slug)."""
    runners = runners_for(env, slug)
    if kind == "direct":
        # HRRR specialised first
        if model == "hrrr" and "hrrr_direct_batch" in runners and env.has("hrrr_direct_batch"):
            return "hrrr_direct_batch"
        if "direct_batch" in runners and env.has("direct_batch"):
            return "direct_batch"
        # Catalog miss — try HRRR specialised by default
        if model == "hrrr" and env.has("hrrr_direct_batch"):
            return "hrrr_direct_batch"
        if env.has("direct_batch"):
            return "direct_batch"
    elif kind == "derived":
        if model == "hrrr" and "hrrr_derived_batch" in runners and env.has("hrrr_derived_batch"):
            return "hrrr_derived_batch"
        if "derived_batch" in runners and env.has("derived_batch"):
            return "derived_batch"
        if model == "hrrr" and env.has("hrrr_derived_batch"):
            return "hrrr_derived_batch"
        if env.has("derived_batch"):
            return "derived_batch"
    elif kind == "windowed":
        if env.has("hrrr_windowed_batch"):
            return "hrrr_windowed_batch"
    elif kind == "heavy":
        if env.has("heavy_panel_hour"):
            return "heavy_panel_hour"
    return None


# ── Run-string helpers ──────────────────────────────────────────────────


def _resolve_run(run_str: str, model: str) -> tuple[str, int]:
    if run_str == "latest":
        return resolve_latest_run(model)
    return parse_run(run_str)


# ── Public entry point ──────────────────────────────────────────────────


def render_recipe(
    env: RustwxEnv,
    *,
    recipes: list[str],
    model: str = "hrrr",
    run_str: str = "latest",
    forecast_hour: int = 0,
    region: str | None = None,
    location: str | dict | tuple | None = None,
    source: str = "aws",
    place_label_density: str = "major",
    contour_mode: str = "automatic",
    allow_large_heavy_domain: bool = False,
    out_dir: str | None = None,
    timeout: int = 600,
) -> dict:
    """Render any combination of direct / derived / heavy / windowed recipes.

    Recipes are looked up in the live product catalog (with a fallback mirror).
    Each `kind` group is sent to its appropriate binary in one batched call.
    """
    if not recipes:
        return {"ok": False, "error": "recipes is empty"}

    buckets, unknowns = _classify(env, recipes)
    if unknowns:
        return {
            "ok": False,
            "error": f"unknown recipes: {unknowns}",
            "advice": "Call wx_products to list every supported slug, "
                       "or wx_recipes for the catalog summary.",
        }

    # Warn about products that are blocked or not supported on this model
    warnings: list[str] = []
    for r in recipes:
        st = supports_model(env, r, model)
        if st == "blocked":
            warnings.append(f"{r}: blocked on {model}")
        elif st == "unknown":
            # OK — fallback list won't have model support, just continue
            pass

    date, cycle = _resolve_run(run_str, model)
    chosen_region = resolve_region(region, location, allow_extended=True)

    out_root = Path(out_dir) if out_dir else (
        env.out_root / "render" / model /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{chosen_region}"
    )

    summary: dict = {
        "ok": True,
        "model": model,
        "date": date,
        "cycle": cycle,
        "forecast_hour": forecast_hour,
        "region": chosen_region,
        "out_dir": str(out_root),
        "warnings": warnings,
        "runs": [],
        "pngs": [],
        "manifests": [],
        "elapsed_s": 0.0,
    }

    started = time.time()

    # Group recipes that share the same target binary so we batch fewer calls
    by_binary: dict[str, list[str]] = defaultdict(list)
    for kind, slugs in buckets.items():
        for slug in slugs:
            bin_name = _binary_for(env, kind, model, slug)
            if not bin_name:
                summary["warnings"].append(f"{slug}: no runner available for kind={kind} model={model}")
                continue
            by_binary[bin_name].append(slug)

    if not by_binary:
        summary["ok"] = False
        summary["error"] = "no runnable recipes after routing"
        return summary

    for bin_name, slugs in by_binary.items():
        rr = _invoke(
            env, bin_name, model=model, date=date, cycle=cycle,
            forecast_hour=forecast_hour, source=source, region=chosen_region,
            slugs=slugs,
            place_label_density=place_label_density,
            contour_mode=contour_mode,
            allow_large_heavy_domain=allow_large_heavy_domain,
            out_dir=out_root, timeout=timeout,
        )
        run_payload = {"binary": bin_name, "slugs": slugs, **rr.to_payload()}

        # Even when rc=0, the binary may have written zero PNGs and a list of
        # blockers in its manifest (catalog says supported but planner missed
        # the fetch, or selector key not in the GRIB). Surface them.
        if rr.ok and not rr.pngs and rr.manifests:
            blockers = _read_blockers(rr.manifests)
            if blockers:
                run_payload["blockers"] = blockers
                summary["warnings"].extend(
                    f"{slug}: {reason}" for slug, reason in blockers
                )
                summary["ok"] = False

        summary["runs"].append(run_payload)
        summary["pngs"].extend([str(p) for p in rr.pngs])
        summary["manifests"].extend([str(p) for p in rr.manifests])
        if not rr.ok:
            summary["ok"] = False

    summary["pngs"] = sorted(set(summary["pngs"]))
    summary["manifests"] = sorted(set(summary["manifests"]))
    summary["png_count"] = len(summary["pngs"])
    summary["elapsed_s"] = round(time.time() - started, 2)
    return summary


def _invoke(
    env: RustwxEnv, bin_name: str, *,
    model: str, date: str, cycle: int, forecast_hour: int, source: str,
    region: str, slugs: list[str],
    place_label_density: str, contour_mode: str, allow_large_heavy_domain: bool,
    out_dir: Path, timeout: int,
):
    """Construct argv for the chosen binary and run it."""
    base = [
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        "--region", region,
        "--cache-dir", str(env.cache_dir.resolve()),
    ]

    if bin_name in ("direct_batch", "derived_batch"):
        # multi-model binaries — pass --model
        argv = ["--model", model, *base, "--recipe", *slugs]
        if bin_name == "derived_batch":
            # derived_batch's --place-label-density is numeric (0..3); the
            # named form is only supported by hrrr_derived_batch.
            density_map = {
                "none": "0", "major": "1", "major-and-aux": "2", "dense": "3",
            }
            argv.extend([
                "--source-mode", "canonical",
                "--place-label-density", density_map.get(place_label_density, "0"),
                "--contour-mode", contour_mode,
            ])
            if allow_large_heavy_domain:
                argv.append("--allow-large-heavy-domain")
        else:  # direct_batch
            argv.extend(["--contour-mode", contour_mode])
        return run(env, bin_name, argv, out_dir=out_dir, timeout=timeout)

    if bin_name in ("hrrr_direct_batch", "hrrr_derived_batch"):
        # HRRR-only — no --model flag
        argv = [*base, "--recipe", *slugs, "--contour-mode", contour_mode]
        if bin_name == "hrrr_derived_batch":
            argv.extend([
                "--source-mode", "canonical",
                "--place-label-density", place_label_density,
            ])
            if allow_large_heavy_domain:
                argv.append("--allow-large-heavy-domain")
        return run(env, bin_name, argv, out_dir=out_dir, timeout=timeout)

    if bin_name == "hrrr_windowed_batch":
        cli_slugs = [WINDOWED_CLI_SLUG.get(s, s) for s in slugs]
        argv = [*base, "--product", *cli_slugs]
        return run(env, bin_name, argv, out_dir=out_dir, timeout=timeout)

    if bin_name == "heavy_panel_hour":
        argv = ["--model", model, *base]
        if allow_large_heavy_domain:
            argv.append("--allow-large-heavy-domain")
        return run(env, bin_name, argv, out_dir=out_dir, timeout=timeout)

    raise ValueError(f"unhandled binary: {bin_name}")


# ── Per-field shortcuts ──────────────────────────────────────────────────


def cape(env: RustwxEnv, *, parcel: str = "sb", **kwargs) -> dict:
    """SBCAPE/MLCAPE/MUCAPE shortcut."""
    recipe = {"sb": "sbcape", "ml": "mlcape", "mu": "mucape"}.get(parcel.lower())
    if not recipe:
        return {"ok": False, "error": f"parcel must be sb/ml/mu, got {parcel!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def ecape(env: RustwxEnv, *, parcel: str = "ml", **kwargs) -> dict:
    """SBECAPE/MLECAPE/MUECAPE shortcut. Single first-class ECAPE map."""
    recipe = {"sb": "sbecape", "ml": "mlecape", "mu": "muecape"}.get(parcel.lower())
    if not recipe:
        return {"ok": False, "error": f"parcel must be sb/ml/mu, got {parcel!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def srh(env: RustwxEnv, *, layer_km: int = 1, **kwargs) -> dict:
    """0-1 km or 0-3 km storm-relative helicity."""
    recipe = {1: "srh_0_1km", 3: "srh_0_3km"}.get(int(layer_km))
    if not recipe:
        return {"ok": False, "error": f"layer_km must be 1 or 3, got {layer_km!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def shear(env: RustwxEnv, *, layer_km: int = 6, **kwargs) -> dict:
    """0-1 km or 0-6 km bulk shear."""
    recipe = {1: "bulk_shear_0_1km", 6: "bulk_shear_0_6km"}.get(int(layer_km))
    if not recipe:
        return {"ok": False, "error": f"layer_km must be 1 or 6, got {layer_km!r}"}
    return render_recipe(env, recipes=[recipe], **kwargs)


def stp(env: RustwxEnv, **kwargs) -> dict:
    """Fixed-layer Significant Tornado Parameter."""
    return render_recipe(env, recipes=["stp_fixed"], **kwargs)
