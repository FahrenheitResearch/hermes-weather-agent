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
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hermes_weather import jobs
from hermes_weather.rustwx import discover, parse_run, resolve_latest_run_for_hours
from hermes_weather.tools import (
    cache as cache_tool,
    catalog,
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

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


# ── Showcase configuration ─────────────────────────────────────────────

# Default to live/latest model hours. Set these env vars to pin a reproducible
# historical run for benchmarks.
HRRR_RUN = os.environ.get("HERMES_SHOWCASE_HRRR_RUN", "latest")
HRRR_FHOUR = 0
GFS_RUN = os.environ.get("HERMES_SHOWCASE_GFS_RUN", "latest")
ECMWF_RUN = os.environ.get("HERMES_SHOWCASE_ECMWF_RUN", "latest")
RRFS_RUN = os.environ.get("HERMES_SHOWCASE_RRFS_RUN", "latest")

REGION_PRIMARY = "southern-plains"
REGION_CONUS = "conus"
LOCATION_DEMO = (35.2220, -97.4390)
LOCATION_DEMO_LABEL = "Norman, OK"
MEMPHIS_LOCATION = (35.1495, -90.0490)
SOCAL_LOCATION = (34.0522, -118.2437)
SOCAL_LOCATION_LABEL = "Los Angeles"
SATELLITE_BOUNDS = [-127.0, -111.0, 30.0, 44.5]
SATELLITE_PRODUCTS = [
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
SOCAL_BOUNDS = [-119.5, -116.0, 32.5, 34.8]
SOCAL_METEOGRAM_VARIABLES = [
    "temperature_2m_c",
    "relative_humidity_2m_pct",
    "wind_speed_10m_ms",
    "precip_hourly_mm",
]

SHOWCASE_OUT = Path("outputs/showcase")
SHOWCASE_OUT.mkdir(parents=True, exist_ok=True)


def _hrrr_research_date_cycle(forecast_hour: int = 1) -> tuple[str, int]:
    if HRRR_RUN == "latest":
        return resolve_latest_run_for_hours(
            "hrrr",
            source="aws",
            forecast_hours=[forecast_hour],
            product="sfc",
        )
    return parse_run(HRRR_RUN)


# ── Bookkeeping ────────────────────────────────────────────────────────


@dataclass
class CallRecord:
    section: str
    name: str
    args: dict
    elapsed_s: float
    ok: bool
    pngs: list[str] = field(default_factory=list)
    webps: list[str] = field(default_factory=list)
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
    webps = result.get("webps") or ([result["webp"]] if result.get("webp") else [])
    webps = [str(p) for p in webps if p]
    jsons = []
    for k in ("output_json", "csv", "index", "scan_path", "profile_json"):
        if k in result and result[k]:
            jsons.append(str(result[k]))
    metadata = {k: v for k, v in result.items()
                if k in ("model", "date", "cycle", "forecast_hour", "region",
                          "site", "lat", "lon", "binary", "elapsed_s",
                          "binary_seconds", "png_count", "result_count",
                          "webp_count", "ok_count", "kind", "state", "job_id")}

    rec = CallRecord(
        section=section, name=name,
        args={k: v for k, v in kwargs.items() if not k.startswith("_")},
        elapsed_s=round(elapsed, 2),
        ok=ok, pngs=pngs, webps=webps, json_files=jsons,
        metadata=metadata,
        error=result.get("error"),
        note=note,
        raw_result={k: v for k, v in result.items() if k not in ("pngs", "webps")},
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
    model = next(
        (m for m in (env.capabilities or {}).get("models", []) if m.get("id") == "hrrr"),
        {},
    )
    direct_supported = list(model.get("direct_recipes") or [])
    derived_supported = list(model.get("light_derived_recipes") or [])

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
         render_tool.windowed, env,
         products=["qpf_1h", "qpf_6h", "qpf_12h", "qpf_24h", "qpf_total",
                    "uh_2to5km_1h_max", "uh_2to5km_3h_max", "uh_2to5km_run_max"],
         region=REGION_PRIMARY, run_str=HRRR_RUN, forecast_hour=6, timeout=600)


def run_hrrr_severe_panel(env):
    print("\n── HRRR Heavy Severe + ECAPE Panel ────────────────")
    call("hrrr_severe", "heavy_panel_hour",
         render_tool.severe_panel, env,
         model="hrrr", run_str=HRRR_RUN, forecast_hour=HRRR_FHOUR,
         region=REGION_PRIMARY, timeout=900)


def run_hrrr_ecape_specials(env):
    print("\n── HRRR ECAPE Specials ────────────────────────────")
    # Profile probe (sub-second)
    call("ecape", f"profile probe @ {LOCATION_DEMO_LABEL}",
         ecape_tool.profile, env,
         location=LOCATION_DEMO, run_str=HRRR_RUN, forecast_hour=1,
         include_input_column=True, timeout=120)

    # Ratio display map.
    call("ecape", "ECAPE/CAPE derived-ratio display",
               ecape_tool.ratio_map, env,
               region=REGION_PRIMARY, run_str=HRRR_RUN,
               forecast_hour=HRRR_FHOUR, include_native_ratio=False, timeout=900)

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
    """Demonstrate multi-model coverage with a passing representative recipe."""
    print("\n── Multi-Model Coverage ───────────────────────────")
    # GFS — direct and derived
    call("gfs", "GFS direct: mslp_10m_winds, 500mb, 2m_temperature",
         render_tool.render_recipe, env,
         recipes=["mslp_10m_winds", "500mb_temperature_height_winds", "2m_temperature"],
         model="gfs", run_str=GFS_RUN, forecast_hour=0,
         region=REGION_CONUS, timeout=600)
    # Other model probes currently produce planner/source blockers on this
    # local agent-v1 path, so keep the runnable showcase to passing outputs.

def run_rustwx_044_integrations(env):
    print("\n-- rustwx 0.4.6 Integrations --------------------------------")
    call("volume_cross_section", "wx_volume_cross_section: SoCal all non-smoke products, f0",
         volume_cs_tool.volume_cross_section, env,
         products=["all"], route="socal-coast-desert",
         run_str="latest", forecast_hour=0,
         max_build_hours=1, load_parallelism=2,
         out_dir=str(SHOWCASE_OUT / "volume_cross_section_socal"),
         timeout=900,
         _note="Full showcase renders all supported non-smoke VolumeStore products for one HRRR hour.")

    call("satellite", "wx_satellite: GOES18 CA site full ABI/GLM product set",
         satellite_tool.satellite, env,
         satellite="goes18",
         bounds=SATELLITE_BOUNDS,
         label="Pacific Southwest Satellite",
         products=SATELLITE_PRODUCTS,
         width=900, height=700,
         scan_lookback_hours=6,
         download_glm=True,
         out_dir=str(SHOWCASE_OUT / "satellite_pacific_southwest"),
         _note="Matches the CA Fire satellite product set: GeoColor, GLM overlay, RGB composites, and ABI Bands 1-16.")

    call("meteogram", "wx_meteogram: Los Angeles HRRR f0-f3",
         meteogram_tool.meteogram, env,
         location=SOCAL_LOCATION,
         model="hrrr", run_str="latest",
         forecast_hour_start=0, forecast_hour_end=3,
         variables=SOCAL_METEOGRAM_VARIABLES)

    warm = call("meteogram", "wx_meteogram_warm_store: small SoCal HRRR f0-f3",
                meteogram_tool.warm_store, env,
                model="hrrr", run_str="latest",
                bounds=SOCAL_BOUNDS,
                forecast_hour_start=0, forecast_hour_end=3,
                variables=SOCAL_METEOGRAM_VARIABLES,
                _note="Small warm-store sample for repeated point queries over a SoCal bbox.")
    store_id = warm.get("store_id")
    if store_id:
        call("meteogram", "wx_meteogram: sample warmed store @ Los Angeles",
             meteogram_tool.meteogram, env,
             location=SOCAL_LOCATION,
             store_id=store_id,
             forecast_hour_start=0, forecast_hour_end=3)


def run_observations(env):
    print("\n── Observations ───────────────────────────────────")
    call("radar", "NEXRAD Level 2 fetch @ KTLX",
         radar_tool.radar, env,
         site="KTLX", products="all", timeout=240,
         _note="Uses the native rustwx radar_export renderer: base, dual-pol, SRV, VIL, echo tops, and feature JSON.")

    call("sounding", "Skew-T @ Norman, OK (native rustwx)",
         sounding_tool.sounding, env,
         location=LOCATION_DEMO, run_str=HRRR_RUN, forecast_hour=1, timeout=180)


def run_research(env):
    research_date, research_cycle = _hrrr_research_date_cycle(1)
    research_date_iso = f"{research_date[:4]}-{research_date[4:6]}-{research_date[6:]}"
    print("\n── Research mode ──────────────────────────────────")
    res = call("research", "stress profile sweep (10 curated points, f1)",
               research_tool.profile_sweep, env,
               mode="stress",
               start_date=research_date_iso, end_date=research_date_iso,
               cycles=[research_cycle],
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
               start_date=research_date_iso, end_date=research_date_iso,
               cycles=[research_cycle], forecast_hours=[0],
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
            "webps": r.webps,
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
        "webp_count": sum(len(r.webps) for r in RECORDS),
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

    if not env.module_available:
        print("FATAL: rustwx Python module not discovered. Install rustwx>=0.4.6.")
        return 1

    started = time.time()

    run_discovery(env)
    run_fetch(env)
    run_hrrr_full_recipe_render(env)
    run_hrrr_windowed(env)
    run_hrrr_severe_panel(env)
    run_hrrr_ecape_specials(env)
    run_multi_model(env)
    run_rustwx_044_integrations(env)
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
    print(f"WebPs produced      : {manifest['webp_count']}")
    print(f"Manifest            : {SHOWCASE_OUT / 'manifest.json'}")
    print("=" * 60)
    print("Now run: python examples/showcase_html.py "
           "to build the HTML gallery.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
