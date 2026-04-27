"""Research-mode primitives — multi-point profile sweeps with timing breakdown.

A profile sweep takes a list of (label, location, run, forecast_hour) tuples,
runs `hrrr_ecape_profile_probe` against each one, parses the JSON output,
extracts ECAPE/CAPE/CIN/LFC/EL diagnostics + per-stage timings, and emits a
single CSV index for downstream analysis.

Three convenience modes:

  * targets         — explicit list of (label, location) points (e.g. tornado-target catalog)
  * random          — N random lat/lon picks within a bbox or region
  * stress          — curated edge-case profiles (high terrain, dry slot, very cold ML, etc.)

Each is intentionally small; the orchestrator iterates over (point × cycle ×
forecast_hour) tuples and aggregates results.
"""
from __future__ import annotations

import csv
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from .. import jobs
from ..geo import Bbox, bbox_from_center, resolve_location
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run

# Curated stress fixtures — high-terrain, dry-slot, cold-pool, etc. Tracks
# the kinds of cases the ECAPE-RS paper used for stress sweeps.
STRESS_PROFILES: list[tuple[str, float, float]] = [
    ("Leadville_high_terrain",  39.249, -106.292),
    ("Death_Valley_extreme_dry", 36.462, -116.866),
    ("Mt_Washington_cold_top", 44.270,  -71.303),
    ("Big_Bend_dry_slot",       29.270, -103.250),
    ("North_Slope_arctic",      70.250, -148.350),
    ("Florida_Keys_marine",     24.555, -81.783),
    ("Caspian_subtropical",     36.500, -116.866),  # placeholder: arid foreland
    ("Reno_lee_subsidence",     39.530, -119.815),
    ("Norman_severe_classic",   35.222,  -97.439),
    ("Birmingham_dixie_alley",  33.518,  -86.810),
]


def _resolve_run(run_str: str, model: str = "hrrr") -> tuple[str, int]:
    if run_str == "latest":
        return resolve_latest_run(model)
    return parse_run(run_str)


def _expand_cycles(model: str, requested: list[int] | None) -> list[int]:
    if requested:
        return sorted({int(c) % 24 for c in requested})
    if model == "hrrr":
        return list(range(24))
    if model == "gfs":
        return [0, 6, 12, 18]
    return [0, 12]


def _date_range(start_iso: str, end_iso: str) -> list[str]:
    s = datetime.strptime(start_iso, "%Y-%m-%d").date()
    e = datetime.strptime(end_iso, "%Y-%m-%d").date()
    if e < s:
        s, e = e, s
    days = []
    cur = s
    while cur <= e:
        days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days


def _run_one_probe(
    env: RustwxEnv, *,
    label: str, lat: float, lon: float,
    model: str, date: str, cycle: int, forecast_hour: int,
    source: str, out_dir: Path, timeout: int,
) -> dict:
    binary = "hrrr_ecape_profile_probe"
    if not env.has_binary(binary):
        return {"ok": False, "label": label, "error": f"{binary} not built"}

    leaf = out_dir / f"{date}_{cycle:02d}z_f{forecast_hour:03d}" / f"{label}_{lat:.3f}_{lon:.3f}"
    leaf.mkdir(parents=True, exist_ok=True)
    output_json = leaf / "profile.json"

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        f"--lat={lat:.6f}",
        f"--lon={lon:.6f}",
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(output_json.resolve()),
    ]
    started = time.time()
    rr = run(env, binary, args, timeout=timeout)
    elapsed = time.time() - started

    payload: dict = {
        "ok": rr.ok,
        "label": label,
        "lat": lat, "lon": lon,
        "model": model, "date": date, "cycle": cycle,
        "forecast_hour": forecast_hour,
        "elapsed_wallclock_s": round(elapsed, 3),
        "binary_seconds": round(rr.seconds, 3),
        "output_json": str(output_json),
    }

    # Pull timings + diagnostics out of the JSON if available
    if output_json.exists():
        try:
            data = json.loads(output_json.read_text(encoding="utf-8"))
            timings = data.get("timings") or data.get("timing") or {}
            payload["timings"] = {
                "raw_fetch_s":      timings.get("raw_fetch_s"),
                "profile_extract_s": timings.get("profile_extract_s") or timings.get("extract_s"),
                "parcel_solver_s":  timings.get("parcel_solver_s") or timings.get("solver_s"),
                "render_s":         timings.get("render_s"),
            }
            # Surface the headline diagnostics
            payload["sb"] = data.get("sb") or data.get("surface_based")
            payload["ml"] = data.get("ml") or data.get("mixed_layer")
            payload["mu"] = data.get("mu") or data.get("most_unstable")
            payload["summary"] = data.get("summary")
        except Exception as exc:
            payload["json_parse_error"] = str(exc)

    if not rr.ok:
        payload["error"] = rr.stderr.strip()[-300:] or rr.stdout.strip()[-300:]
    return payload


def _resolve_targets(
    *, mode: str,
    targets: list[dict] | None,
    bbox: dict | None,
    location: str | dict | tuple | None,
    radius_km: float,
    n_random: int,
    seed: int | None,
    region: str | None,
) -> list[tuple[str, float, float]]:
    if mode == "targets":
        out: list[tuple[str, float, float]] = []
        for t in (targets or []):
            label = t.get("label") or t.get("name") or "point"
            ll = resolve_location(t.get("location") or (t.get("lat"), t.get("lon"))
                                  if "lat" in t and "lon" in t else t.get("location"))
            if ll is None:
                continue
            out.append((str(label), float(ll[0]), float(ll[1])))
        return out

    if mode == "stress":
        return list(STRESS_PROFILES)

    if mode == "random":
        rng = random.Random(seed)
        # Decide bbox
        if bbox:
            bb = Bbox(west=float(bbox["west"]), east=float(bbox["east"]),
                      south=float(bbox["south"]), north=float(bbox["north"]))
        elif location is not None:
            ll = resolve_location(location)
            if ll is None:
                return []
            bb = bbox_from_center(*ll, radius_km=radius_km)
        else:
            # Default to CONUS
            bb = Bbox(west=-125.0, east=-67.0, south=24.0, north=49.0)

        return [
            (f"random_{i:03d}",
             rng.uniform(bb.south, bb.north),
             rng.uniform(bb.west, bb.east))
            for i in range(n_random)
        ]

    return []


def profile_sweep(
    env: RustwxEnv,
    *,
    mode: str = "targets",
    targets: list[dict] | None = None,
    region: str | None = None,
    bbox: dict | None = None,
    location: str | dict | tuple | None = None,
    radius_km: float = 600.0,
    n_random: int = 50,
    seed: int | None = 20260426,
    model: str = "hrrr",
    start_date: str | None = None,
    end_date: str | None = None,
    cycles: list[int] | None = None,
    forecast_hours: list[int] = (1,),
    source: str = "aws",
    workers: int = 4,
    out_dir: str | None = None,
    background: bool = True,
    timeout_per_probe: int = 90,
    cache_cap_gb: float | None = 500.0,
) -> dict:
    """Run a profile sweep across (point × date × cycle × forecast_hour).

    `mode`:
      * "targets" — uses `targets` (list of {label, location, ...})
      * "random"  — n_random uniformly drawn lat/lon within bbox/location/region/CONUS
      * "stress"  — curated stress profiles

    Writes one JSON per probe + an aggregate CSV summary.
    Background by default — poll wx_job_status.
    """
    if mode not in ("targets", "random", "stress"):
        return {"ok": False, "error": f"mode must be targets/random/stress, got {mode!r}"}

    points = _resolve_targets(
        mode=mode, targets=targets, bbox=bbox, location=location,
        radius_km=radius_km, n_random=n_random, seed=seed, region=region,
    )
    if not points:
        return {"ok": False, "error": "no points resolved"}

    # Date range. Default to today (UTC) if neither bound supplied.
    if start_date is None and end_date is None:
        from datetime import timezone as _tz
        today = datetime.now(_tz.utc).strftime("%Y-%m-%d")
        start_date = end_date = today
    elif start_date is None:
        start_date = end_date
    elif end_date is None:
        end_date = start_date
    dates = _date_range(start_date, end_date)
    cyc = _expand_cycles(model, cycles)
    fhs = list(forecast_hours)
    total = len(points) * len(dates) * len(cyc) * len(fhs)

    sweep_root = Path(out_dir) if out_dir else (
        env.out_root / "research" / f"profile_sweep_{mode}_{start_date}_to_{end_date}_{int(time.time())}"
    )
    sweep_root.mkdir(parents=True, exist_ok=True)

    def _runner(job: jobs.Job) -> dict:
        # Optional cache trim before a heavy sweep
        if cache_cap_gb:
            from . import cache as cache_tool
            ev = cache_tool.evict_to(env.cache_dir, target_gb=cache_cap_gb, dry_run=False)
            if ev.get("evicted_files", 0) > 0:
                job.append_log(f"cache trim: evicted {ev['evicted_files']} files "
                                f"freed {ev.get('evicted_bytes', 0) / 1e9:.1f} GB")

        job.progress = {"total": total, "completed": 0, "failed": 0}
        job.append_log(
            f"sweep mode={mode} points={len(points)} dates={len(dates)} "
            f"cycles={len(cyc)} fhours={len(fhs)} total={total} workers={workers}"
        )
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = []
            for date in dates:
                for c in cyc:
                    for fh in fhs:
                        for label, lat, lon in points:
                            futs.append(pool.submit(
                                _run_one_probe, env,
                                label=label, lat=lat, lon=lon,
                                model=model, date=date, cycle=c, forecast_hour=fh,
                                source=source, out_dir=sweep_root,
                                timeout=timeout_per_probe,
                            ))
            for fut in as_completed(futs):
                if job.state == "cancelled":
                    break
                try:
                    r = fut.result()
                except Exception as exc:
                    r = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                results.append(r)
                job.progress["completed"] += 1
                if not r.get("ok"):
                    job.progress["failed"] += 1
                if job.progress["completed"] % 10 == 0:
                    job.append_log(
                        f"progress {job.progress['completed']}/{total} "
                        f"failed={job.progress['failed']}"
                    )

        # Write aggregate CSV
        csv_path = sweep_root / "summary.csv"
        cols = [
            "label", "lat", "lon", "model", "date", "cycle", "forecast_hour",
            "ok", "binary_seconds",
            "raw_fetch_s", "profile_extract_s", "parcel_solver_s", "render_s",
            "output_json", "error",
        ]
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in results:
                t = r.get("timings") or {}
                w.writerow({
                    **{k: r.get(k) for k in cols if k not in ("raw_fetch_s",
                       "profile_extract_s", "parcel_solver_s", "render_s")},
                    "raw_fetch_s":       t.get("raw_fetch_s"),
                    "profile_extract_s": t.get("profile_extract_s"),
                    "parcel_solver_s":   t.get("parcel_solver_s"),
                    "render_s":          t.get("render_s"),
                })

        index_path = sweep_root / "index.json"
        index_path.write_text(json.dumps({
            "mode": mode,
            "model": model,
            "start_date": start_date,
            "end_date": end_date,
            "cycles": cyc,
            "forecast_hours": fhs,
            "points": [{"label": l, "lat": la, "lon": lo} for l, la, lo in points],
            "result_count": len(results),
            "ok_count": sum(1 for r in results if r.get("ok")),
            "fail_count": sum(1 for r in results if not r.get("ok")),
        }, indent=2, default=str), encoding="utf-8")

        return {
            "ok": True,
            "out_dir": str(sweep_root),
            "csv": str(csv_path),
            "index": str(index_path),
            "result_count": len(results),
            "ok_count": sum(1 for r in results if r.get("ok")),
            "fail_count": sum(1 for r in results if not r.get("ok")),
        }

    if not background:
        job = jobs.submit("profile_sweep",
                          {"mode": mode, "points": len(points), "total": total},
                          _runner)
        if job.thread:
            job.thread.join()
        return job.to_payload(log_tail=40)

    job = jobs.submit("profile_sweep",
                      {"mode": mode, "points": len(points), "total": total},
                      _runner)
    return {
        "ok": True,
        "job_id": job.job_id,
        "kind": job.kind,
        "state": job.state,
        "total": total,
        "out_dir": str(sweep_root),
        "note": "Profile sweep started. Poll wx_job_status with this job_id.",
    }
