"""Dataset orchestration — multi-day, multi-cycle HRRR/GFS render & probe.

Two modes:
  * `render` — fan out direct/derived recipes across a date range,
    producing organised PNGs per (date, cycle, forecast_hour, region).
    Useful for building visual training corpora.
  * `probe`  — fan out hrrr_ecape_profile_probe across a date range at
    fixed lat/lon points (e.g. tornado target list). Produces JSON
    profiles plus a summary index.

Runs as a background job. Concurrency is bounded by ThreadPoolExecutor;
the rustwx binaries themselves use rayon internally, so don't oversubscribe.
"""
from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .. import jobs
from ..rustwx import RustwxEnv, run
from .render import render_recipe


@dataclass
class CycleSpec:
    date: str   # YYYYMMDD
    cycle: int  # 0..23
    forecast_hour: int


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


def _cycles_for_model(model: str, requested: list[int] | None) -> list[int]:
    if requested:
        return sorted({int(c) % 24 for c in requested})
    if model == "hrrr":
        return list(range(24))
    if model == "gfs":
        return [0, 6, 12, 18]
    return [0, 12]


def _expand_specs(
    *, model: str, start: str, end: str, cycles: list[int] | None,
    forecast_hours: list[int],
) -> list[CycleSpec]:
    out: list[CycleSpec] = []
    for d in _date_range(start, end):
        for c in _cycles_for_model(model, cycles):
            for fh in forecast_hours:
                out.append(CycleSpec(date=d, cycle=c, forecast_hour=fh))
    return out


def build_dataset(
    env: RustwxEnv,
    *,
    mode: str = "render",
    model: str = "hrrr",
    start_date: str,
    end_date: str,
    cycles: list[int] | None = None,
    forecast_hours: list[int] = (0,),
    region: str | None = None,
    location: str | dict | tuple | None = None,
    direct_recipes: list[str] | None = None,
    derived_recipes: list[str] | None = None,
    profile_points: list[str | dict | tuple] | None = None,
    out_dir: str | None = None,
    workers: int = 4,
    limit: int | None = None,
    background: bool = True,
    timeout_per_run: int = 600,
) -> dict:
    """Build a HRRR/GFS visualization or profile-probe dataset.

    `mode="render"`: produces PNGs for the recipe set across every (date,
    cycle, forecast_hour, region) tuple.
    `mode="probe"`: produces ECAPE-profile JSON for each (point, time)
    pair across the date range.
    """
    if mode not in ("render", "probe"):
        return {"ok": False, "error": f"mode must be 'render' or 'probe', got {mode!r}"}
    if mode == "render" and not (direct_recipes or derived_recipes):
        return {"ok": False, "error": "render mode requires direct_recipes or derived_recipes"}
    if mode == "probe" and not profile_points:
        return {"ok": False, "error": "probe mode requires profile_points list"}

    fhs = list(forecast_hours) if forecast_hours else [0]
    specs = _expand_specs(model=model, start=start_date, end=end_date,
                          cycles=cycles, forecast_hours=fhs)
    if limit:
        specs = specs[:limit]
    if not specs:
        return {"ok": False, "error": "no date×cycle×forecast_hour combinations"}

    base_out = Path(out_dir) if out_dir else (
        env.out_root / "dataset" / mode / f"{model}_{start_date}_to_{end_date}"
    )
    base_out.mkdir(parents=True, exist_ok=True)

    def _run_one_render(spec: CycleSpec) -> dict:
        spec_out = base_out / f"{spec.date}_{spec.cycle:02d}z_f{spec.forecast_hour:03d}"
        spec_out.mkdir(parents=True, exist_ok=True)
        recipes = list(direct_recipes or []) + list(derived_recipes or [])
        return render_recipe(
            env, recipes=recipes, model=model,
            run_str=f"{spec.date[:4]}-{spec.date[4:6]}-{spec.date[6:]}/{spec.cycle:02d}z",
            forecast_hour=spec.forecast_hour, region=region, location=location,
            out_dir=str(spec_out), timeout=timeout_per_run,
        )

    def _run_one_probe(spec: CycleSpec, point: str | dict | tuple) -> dict:
        from ..geo import resolve_location
        ll = resolve_location(point)
        if ll is None:
            return {"ok": False, "spec": asdict(spec), "point": str(point), "error": "unresolved point"}
        lat, lon = ll
        spec_out = base_out / f"{spec.date}_{spec.cycle:02d}z_f{spec.forecast_hour:03d}" / f"{lat:.3f}_{lon:.3f}"
        spec_out.mkdir(parents=True, exist_ok=True)
        output_json = spec_out / "profile.json"
        binary = "hrrr_ecape_profile_probe"
        if not env.has(binary):
            return {"ok": False, "error": f"{binary} not built"}
        args = [
            "--model", model,
            "--date", spec.date,
            "--cycle", str(spec.cycle),
            "--forecast-hour", str(spec.forecast_hour),
            "--source", "aws",
            f"--lat={lat:.6f}",
            f"--lon={lon:.6f}",
            "--cache-dir", str(env.cache_dir.resolve()),
            "--output", str(output_json.resolve()),
        ]
        result = run(env, binary, args, timeout=timeout_per_run)
        return {
            "ok": result.ok,
            "spec": asdict(spec),
            "point": str(point),
            "lat": lat, "lon": lon,
            "output_json": str(output_json),
            "binary_seconds": round(result.seconds, 2),
        }

    def _runner(job: jobs.Job) -> dict:
        results: list[dict] = []
        total = len(specs) * (len(profile_points) if mode == "probe" else 1)
        job.progress = {"total": total, "completed": 0, "failed": 0}
        job.append_log(f"mode={mode} model={model} specs={len(specs)} workers={workers}")

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            if mode == "render":
                for spec in specs:
                    futures.append(pool.submit(_run_one_render, spec))
            else:
                for spec in specs:
                    for pt in profile_points or []:
                        futures.append(pool.submit(_run_one_probe, spec, pt))
            for fut in as_completed(futures):
                if job.state == "cancelled":
                    break
                try:
                    res = fut.result()
                except Exception as exc:
                    res = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
                results.append(res)
                job.progress["completed"] += 1
                if not res.get("ok"):
                    job.progress["failed"] += 1
                if job.progress["completed"] % 5 == 0 or job.progress["completed"] == total:
                    job.append_log(
                        f"progress {job.progress['completed']}/{total} "
                        f"({job.progress['failed']} failed)"
                    )

        index = base_out / "index.json"
        index.write_text(json.dumps({
            "mode": mode,
            "model": model,
            "start_date": start_date,
            "end_date": end_date,
            "spec_count": len(specs),
            "result_count": len(results),
            "ok_count": sum(1 for r in results if r.get("ok")),
            "results": results,
        }, indent=2, default=str), encoding="utf-8")

        return {
            "ok": True,
            "mode": mode,
            "out_dir": str(base_out),
            "index": str(index),
            "result_count": len(results),
            "ok_count": sum(1 for r in results if r.get("ok")),
        }

    if not background:
        job = jobs.submit("dataset", {"mode": mode, "model": model,
                                       "start": start_date, "end": end_date}, _runner)
        # Wait synchronously
        if job.thread:
            job.thread.join()
        return job.to_payload(log_tail=40)

    job = jobs.submit("dataset", {"mode": mode, "model": model,
                                   "start": start_date, "end": end_date,
                                   "spec_count": len(specs)}, _runner)
    return {
        "ok": True,
        "job_id": job.job_id,
        "kind": job.kind,
        "state": job.state,
        "spec_count": len(specs),
        "out_dir": str(base_out),
        "note": "Dataset build started in background. Poll wx_job_status with this job_id.",
    }
