"""End-to-end Hermes Weather showcase.

Drives every MCP tool against a single canonical run, captures per-call
timing, and writes a manifest at outputs/showcase/manifest.json. Pair this
with `examples/showcase_html.py` to generate the HTML gallery.

Designed to be re-runnable. Uses the existing rustwx cache when present
(set HERMES_CACHE_DIR=C:/Users/drew/rustwx/proof/cache for warm runs).
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hermes_weather import jobs
from hermes_weather.rustwx import discover
from hermes_weather.tools import (
    cache as cache_tool,
    catalog,
    cross_section as cs_tool,
    dataset as ds_tool,
    ecape as ecape_tool,
    fetch as fetch_tool,
    radar as radar_tool,
    render as render_tool,
    research as research_tool,
    severe as severe_tool,
    sounding as sounding_tool,
    windowed as windowed_tool,
)


# ── Showcase configuration ─────────────────────────────────────────────

# A known cached HRRR run; everything routes through this for warm-cache speed.
HRRR_RUN = "2026-04-24/22z"
HRRR_FHOUR = 0
GFS_RUN = "2026-04-24/18z"
ECMWF_RUN = "2026-04-24/12z"
RRFS_RUN = "2026-04-24/23z"

REGION_PRIMARY = "southern-plains"
REGION_CONUS = "conus"
LOCATION_DEMO = "Norman, OK"

SHOWCASE_OUT = Path("outputs/showcase")
SHOWCASE_OUT.mkdir(parents=True, exist_ok=True)


# ── Bookkeeping ────────────────────────────────────────────────────────


@dataclass
class CallRecord:
    section: str
    name: str
    args: dict
    elapsed_s: float
    ok: bool
    pngs: list[str] = field(default_factory=list)
    json_files: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    error: str | None = None
    note: str | None = None
    raw_result: dict = field(default_factory=dict)


RECORDS: list[CallRecord] = []


def call(section: str, name: str, fn, *args, **kwargs) -> dict:
    """Time a tool call, capture result, and add to RECORDS."""
    print(f"  [{section}] {name} ...", end="", flush=True)
    note = kwargs.pop("_note", None)
    started = time.time()
    try:
        result = fn(*args, **kwargs)
        if not isinstance(result, dict):
            result = {"value": result}
        ok = bool(result.get("ok", True))
    except Exception as exc:
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        ok = False
    elapsed = time.time() - started

    pngs = result.get("pngs") or ([result["png"]] if result.get("png") else [])
    pngs = [str(p) for p in pngs if p]
    jsons = []
    for k in ("output_json", "csv", "index", "scan_path", "profile_json"):
        if k in result and result[k]:
            jsons.append(str(result[k]))
    metadata = {k: v for k, v in result.items()
                if k in ("model", "date", "cycle", "forecast_hour", "region",
                          "site", "lat", "lon", "binary", "elapsed_s",
                          "binary_seconds", "png_count", "result_count",
                          "ok_count", "kind", "state", "job_id")}

    rec = CallRecord(
        section=section, name=name,
        args={k: v for k, v in kwargs.items() if not k.startswith("_")},
        elapsed_s=round(elapsed, 2),
        ok=ok, pngs=pngs, json_files=jsons,
        metadata=metadata,
        error=result.get("error"),
        note=note,
        raw_result={k: v for k, v in result.items() if k != "pngs"},
    )
    RECORDS.append(rec)
    print(f" {'✓' if ok else '✗'} {rec.elapsed_s:>6.2f}s "
          f"{'(' + str(len(pngs)) + ' pngs)' if pngs else ''}"
          f"{' [' + (rec.error or '')[:50] + ']' if rec.error else ''}")
    return result


def wait_for_job(job_id: str, *, max_wait: int = 600, poll: float = 2.0) -> dict:
    """Block until a background job finishes, then return its payload."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        job = jobs.get(job_id)
        if job is None:
            return {"ok": False, "error": f"job {job_id} not found"}
        if job.state in ("done", "failed", "cancelled"):
            return job.to_payload(log_tail=20)
        time.sleep(poll)
    return {"ok": False, "error": f"timeout waiting for job {job_id}"}


# ── Sections ───────────────────────────────────────────────────────────


def run_discovery(env):
    print("\n── Discovery ──────────────────────────────────────")
    call("discovery", "wx_doctor", catalog.doctor, env)
    call("discovery", "wx_models", catalog.models, env)
    call("discovery", "wx_regions", catalog.regions, env)
    call("discovery", "wx_recipes", catalog.recipes, env)
    call("discovery", "wx_products(kind=derived,search=ecape)",
         catalog.products, env, kind="derived", search="ecape")
    call("discovery", "wx_latest(model=hrrr)", fetch_tool.latest, env, model="hrrr")


def run_hrrr_full_recipe_render(env):
    """Render every supported HRRR direct + derived recipe in two batched calls."""
    print("\n── HRRR Full Recipe Coverage (batched decode) ─────")
    cat = catalog._load_live(env) or {}
    direct_supported = [e["slug"] for e in cat.get("direct", [])
                         if any(s["target"] == "hrrr" and s["status"] == "supported"
                                 for s in e.get("support", []))]
    derived_supported = [e["slug"] for e in cat.get("derived", [])
                          if any(s["target"] == "hrrr" and s["status"] == "supported"
                                  for s in e.get("support", []))]

    if direct_supported:
        call("hrrr_direct", f"render {len(direct_supported)} HRRR direct recipes",
             render_tool.render_recipe, env,
             recipes=direct_supported, model="hrrr",
             run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
             region=REGION_PRIMARY,
             timeout=900)
    if derived_supported:
        call("hrrr_derived", f"render {len(derived_supported)} HRRR derived recipes",
             render_tool.render_recipe, env,
             recipes=derived_supported, model="hrrr",
             run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
             region=REGION_PRIMARY,
             timeout=900)


def run_hrrr_windowed(env):
    print("\n── HRRR Windowed (QPF / UH) ───────────────────────")
    call("hrrr_windowed", "all 8 windowed products",
         windowed_tool.windowed, env,
         products=["qpf_1h", "qpf_6h", "qpf_12h", "qpf_24h", "qpf_total",
                    "uh_2to5km_1h_max", "uh_2to5km_3h_max", "uh_2to5km_run_max"],
         region=REGION_PRIMARY, run_str=HRRR_RUN, forecast_hour=6, timeout=600)


def run_hrrr_severe_panel(env):
    print("\n── HRRR Heavy Severe + ECAPE Panel ────────────────")
    call("hrrr_severe", "heavy_panel_hour",
         severe_tool.severe_panel, env,
         model="hrrr", run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
         region=REGION_PRIMARY, timeout=900)


def run_hrrr_ecape_specials(env):
    print("\n── HRRR ECAPE Specials ────────────────────────────")
    # Profile probe (sub-second)
    call("ecape", "profile probe @ Norman, OK",
         ecape_tool.profile, env,
         location=LOCATION_DEMO, run_str=HRRR_RUN, forecast_hour=1,
         include_input_column=True, timeout=120)

    # Ratio display map (background)
    res = call("ecape", "ECAPE/CAPE ratio display (background)",
               ecape_tool.ratio_map, env,
               region=REGION_PRIMARY, run_str=HRRR_RUN,
               forecast_hour=HRRR_FHOUR, background=True, timeout=900)
    if res.get("job_id"):
        payload = wait_for_job(res["job_id"], max_wait=900)
        # Update record with completed job result
        if RECORDS:
            r = RECORDS[-1]
            r.raw_result["job_done"] = payload
            if payload.get("result"):
                pngs = payload["result"].get("pngs") or []
                r.pngs.extend(pngs)
                r.elapsed_s = payload.get("elapsed_s", r.elapsed_s)
            r.ok = payload.get("state") == "done"

    # Grid research (background)
    res = call("ecape", "grid research @ southern-plains bbox (background)",
               ecape_tool.grid, env,
               location=LOCATION_DEMO, radius_km=400.0,
               run_str=HRRR_RUN, forecast_hour=1,
               domain_slug="showcase_swath",
               background=True, timeout=900)
    if res.get("job_id"):
        payload = wait_for_job(res["job_id"], max_wait=900)
        if RECORDS:
            r = RECORDS[-1]
            r.raw_result["job_done"] = payload
            if payload.get("result"):
                ojson = payload["result"].get("output_json")
                if ojson:
                    r.json_files.append(str(ojson))
                r.elapsed_s = payload.get("elapsed_s", r.elapsed_s)
            r.ok = payload.get("state") == "done"


def run_multi_model(env):
    """Demonstrate multi-model coverage with a representative recipe each."""
    print("\n── Multi-Model Coverage ───────────────────────────")
    # GFS — direct and derived
    call("gfs", "GFS direct: mslp_10m_winds, 500mb, 2m_temperature",
         render_tool.render_recipe, env,
         recipes=["mslp_10m_winds", "500mb_temperature_height_winds", "2m_temperature"],
         model="gfs", run_str=GFS_RUN, forecast_hour=0,
         region=REGION_CONUS, timeout=600)

    call("gfs", "GFS derived: sbcape, mucape, lapse_rate_700_500",
         render_tool.render_recipe, env,
         recipes=["sbcape", "mucape", "lapse_rate_700_500"],
         model="gfs", run_str=GFS_RUN, forecast_hour=0,
         region=REGION_CONUS, timeout=600)

    # ECMWF — direct only (limited derived support in some configs)
    call("ecmwf", "ECMWF direct: mslp_10m_winds, 500mb_height_winds",
         render_tool.render_recipe, env,
         recipes=["mslp_10m_winds", "500mb_height_winds"],
         model="ecmwf-open-data", run_str=ECMWF_RUN, forecast_hour=0,
         region=REGION_CONUS, timeout=600)

    # RRFS-A — direct
    call("rrfs", "RRFS-A direct: 2m_temperature, mslp_10m_winds",
         render_tool.render_recipe, env,
         recipes=["2m_temperature", "mslp_10m_winds"],
         model="rrfs-a", run_str=RRFS_RUN, forecast_hour=0,
         region=REGION_CONUS, timeout=600)

    # WRF-GDEX — best-effort (THREDDS may be unreachable)
    call("wrf_gdex", "WRF-GDEX direct (best-effort, netcrust path)",
         render_tool.render_recipe, env,
         recipes=["2m_temperature", "mslp_10m_winds"],
         model="wrf-gdex", run_str="2026-04-24/00z", forecast_hour=0,
         region=REGION_CONUS, timeout=600,
         _note="Probes UCAR GDEX THREDDS; auth/network may block.")


def run_cross_sections(env):
    print("\n── Cross Sections ─────────────────────────────────")
    for product in ("temperature", "theta-e", "wind-speed", "fire-weather"):
        call("cross_section", f"amarillo→chicago: {product}",
             cs_tool.cross_section, env,
             product=product, route="amarillo-chicago",
             model="hrrr", run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
             timeout=600)
    # One custom (city-pair) cross section
    call("cross_section", "Norman→Memphis: relative-humidity (custom endpoints)",
         cs_tool.cross_section, env,
         product="relative-humidity",
         start="Norman, OK", end="Memphis, TN",
         model="hrrr", run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
         timeout=600)


def run_observations(env):
    print("\n── Observations ───────────────────────────────────")
    call("radar", "NEXRAD Level 2 fetch @ KTLX",
         radar_tool.radar, env,
         site="KTLX", _note="Renders to PNG only if ptx-radar-processor is installed.")

    call("sounding", "Skew-T @ Norman, OK (native rustwx)",
         sounding_tool.sounding, env,
         location=LOCATION_DEMO, run_str=HRRR_RUN, forecast_hour=1, timeout=180)


def run_research(env):
    print("\n── Research mode ──────────────────────────────────")
    res = call("research", "stress profile sweep (10 curated points, f1)",
               research_tool.profile_sweep, env,
               mode="stress",
               start_date="2026-04-24", end_date="2026-04-24",
               cycles=[22],
               forecast_hours=[1],
               workers=4,
               cache_cap_gb=500.0,
               background=True, timeout_per_probe=120)
    if res.get("job_id"):
        payload = wait_for_job(res["job_id"], max_wait=600)
        if RECORDS:
            r = RECORDS[-1]
            r.raw_result["job_done"] = payload
            if payload.get("result"):
                csv_path = payload["result"].get("csv")
                if csv_path:
                    r.json_files.append(str(csv_path))
                r.elapsed_s = payload.get("elapsed_s", r.elapsed_s)
            r.ok = payload.get("state") == "done"

    # Mini build_dataset (1 cycle, 1 forecast hour, 2 derived recipes)
    res = call("research", "mini dataset (1 cycle × 2 derived recipes)",
               ds_tool.build_dataset, env,
               mode="render", model="hrrr",
               start_date="2026-04-24", end_date="2026-04-24",
               cycles=[22], forecast_hours=[0],
               region=REGION_PRIMARY,
               derived_recipes=["sbcape", "mlcape"],
               workers=2, limit=1,
               background=True)
    if res.get("job_id"):
        payload = wait_for_job(res["job_id"], max_wait=300)
        if RECORDS:
            r = RECORDS[-1]
            r.raw_result["job_done"] = payload
            r.elapsed_s = payload.get("elapsed_s", r.elapsed_s)
            r.ok = payload.get("state") == "done"


def run_cache_and_jobs(env):
    print("\n── Cache & Jobs ───────────────────────────────────")
    call("cache", "wx_cache_status", cache_tool.status, env)
    call("cache", "wx_cache_evict (dry-run, target 600 GB)",
         cache_tool.evict, env, target_gb=600.0, dry_run=True)
    call("jobs", "wx_job_list (recent)", lambda: {
        "ok": True,
        "jobs": [j.to_payload(log_tail=2) for j in jobs.list_recent(20)],
    })


def run_fetch(env):
    print("\n── Fetch ──────────────────────────────────────────")
    out_path = SHOWCASE_OUT / "demo_fetch.grib2"
    call("fetch", "wx_fetch idx-byte-range (TMP/UGRD/VGRD @ 2 m / 10 m)",
         fetch_tool.fetch, env,
         model="hrrr", run=HRRR_RUN, forecast_hour=0,
         variables=[
             "TMP:2 m above ground",
             "UGRD:10 m above ground",
             "VGRD:10 m above ground",
         ],
         output=str(out_path))


# ── Manifest writer ────────────────────────────────────────────────────


def write_manifest(env, total_seconds: float):
    by_section: dict[str, list[dict]] = {}
    for r in RECORDS:
        by_section.setdefault(r.section, []).append({
            "name": r.name,
            "ok": r.ok,
            "elapsed_s": r.elapsed_s,
            "args": _safe_jsonable(r.args),
            "pngs": r.pngs,
            "json_files": r.json_files,
            "metadata": _safe_jsonable(r.metadata),
            "error": r.error,
            "note": r.note,
            "raw_result": _safe_jsonable(r.raw_result),
        })
    manifest = {
        "schema_version": "hermes_weather.showcase.v0",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "doctor": catalog.doctor(env),
        "total_seconds": round(total_seconds, 2),
        "section_count": len(by_section),
        "call_count": len(RECORDS),
        "ok_count": sum(1 for r in RECORDS if r.ok),
        "fail_count": sum(1 for r in RECORDS if not r.ok),
        "png_count": sum(len(r.pngs) for r in RECORDS),
        "sections": by_section,
    }
    out = SHOWCASE_OUT / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    print(f"\nManifest → {out}")
    return manifest


def _safe_jsonable(obj: Any) -> Any:
    try:
        json.dumps(obj, default=str)
        return obj
    except Exception:
        return str(obj)


# ── Main ──────────────────────────────────────────────────────────────


def main() -> int:
    env = discover()
    print("=" * 60)
    print("Hermes Weather — full showcase")
    print(f"Bin dir   : {env.bin_dir}")
    print(f"Cache dir : {env.cache_dir}")
    print(f"Out root  : {env.out_root}")
    print("=" * 60)

    if not env.bin_dir or not env.has("hrrr_derived_batch"):
        print("FATAL: rustwx binaries not discovered. "
               "Set HERMES_RUSTWX_BIN_DIR.")
        return 1

    started = time.time()

    run_discovery(env)
    run_fetch(env)
    run_hrrr_full_recipe_render(env)
    run_hrrr_windowed(env)
    run_hrrr_severe_panel(env)
    run_hrrr_ecape_specials(env)
    run_multi_model(env)
    run_cross_sections(env)
    run_observations(env)
    run_research(env)
    run_cache_and_jobs(env)

    total = time.time() - started
    manifest = write_manifest(env, total)

    print("\n" + "=" * 60)
    print(f"Total runtime       : {total:.1f}s")
    print(f"Sections            : {len(manifest['sections'])}")
    print(f"Calls               : {manifest['call_count']}  "
          f"(ok {manifest['ok_count']}, fail {manifest['fail_count']})")
    print(f"PNGs produced       : {manifest['png_count']}")
    print(f"Manifest            : {SHOWCASE_OUT / 'manifest.json'}")
    print("=" * 60)
    print("Now run: python examples/showcase_html.py "
           "to build the HTML gallery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
