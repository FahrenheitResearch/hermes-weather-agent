"""Dataset builder — orchestrates mass HRRR download + processing + derived field computation."""
from __future__ import annotations

import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import rustmet

from .compute import compute_derived
from .data_fetch import UPPER_AIR_VARS, SURFACE_VARS, STANDARD_LEVELS


VAR_MAP = {
    "Temperature": "TMP", "U-Component of Wind": "UGRD",
    "V-Component of Wind": "VGRD", "Specific Humidity": "SPFH",
    "Geopotential Height": "HGT", "Pressure": "PRES",
}


def process_single_run(model: str, run: str, variables: list[str], levels: list[int],
                       surface_variables: list[str], derived_fields: list[str],
                       output_path: str) -> dict:
    # Suppress rustmet download noise
    import os
    _devnull = open(os.devnull, 'w')
    _old = os.dup(2)
    os.dup2(_devnull.fileno(), 2)
    try:
        return _process_single_run_inner(model, run, variables, levels,
                                         surface_variables, derived_fields, output_path)
    finally:
        os.dup2(_old, 2)
        _devnull.close()


def _process_single_run_inner(model, run, variables, levels,
                              surface_variables, derived_fields, output_path):
    """Fetch one model run, extract fields + compute derived, save as numpy."""
    t0 = time.time()
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Build selective var filter
    var_filter = []
    for var in variables:
        for lev in levels:
            var_filter.append(f"{var}:{lev} mb")
    var_filter.extend(surface_variables)

    try:
        grib = rustmet.fetch(model, run, fhour=0, product="prs", vars=var_filter)
    except Exception as e:
        return {"status": "error", "error": str(e), "run": run}

    if not grib.messages:
        return {"status": "error", "error": "no messages", "run": run}

    nx, ny = grib.messages[0].nx, grib.messages[0].ny
    msg_index = {}
    for m in grib.messages:
        msg_index[(m.variable, m.level_value)] = m

    # Extract upper-air
    channels = {}
    channel_names = []
    for var in variables:
        for lev in levels:
            lev_pa = lev * 100
            m = None
            for long_name, short in VAR_MAP.items():
                if short == var:
                    m = msg_index.get((long_name, lev_pa))
                    break
            if m:
                vals = m.values()
                if len(vals) == ny * nx:
                    key = f"{var}_{lev}"
                    channels[key] = vals.reshape(ny, nx).astype(np.float32)
                    channel_names.append(key)

    # Extract surface
    for sv in surface_variables:
        for m in grib.messages:
            level_str = m.level.lower() if hasattr(m, 'level') else ""
            var_short = VAR_MAP.get(m.variable, m.variable)
            if sv.split(":")[0] in (var_short, m.variable):
                if any(x in level_str for x in ("surface", "ground", "2 m", "10 m")):
                    vals = m.values()
                    if len(vals) == ny * nx:
                        safe = sv.replace(":", "_").replace(" ", "_")
                        channels[safe] = vals.reshape(ny, nx).astype(np.float32)
                        channel_names.append(safe)
                        break

    # Compute derived fields using metrust
    if derived_fields:
        try:
            derived = compute_derived(model, run, derived_fields)
            for key, arr in derived.items():
                channels[key] = arr
                channel_names.append(key)
        except Exception:
            pass

    if not channels:
        return {"status": "error", "error": "no fields", "run": run}

    data = np.stack([channels[k] for k in channel_names], axis=0)
    np.save(str(out), data)

    meta = out.parent / (out.stem + "_meta.json")
    meta.write_text(json.dumps({
        "run": run, "shape": list(data.shape), "channels": channel_names,
        "nx": nx, "ny": ny, "time_s": round(time.time() - t0, 2),
    }, indent=2))

    return {"status": "ok", "run": run, "shape": list(data.shape),
            "channels": len(channel_names), "time_s": round(time.time() - t0, 2)}


def build_dataset(config: dict) -> dict:
    """Build complete training dataset from config.

    Config keys: model, start_date, end_date, variables, levels,
    surface_variables, derived_fields, frequency_hours, lead_time_hours,
    output_dir, normalize, split, workers, limit
    """
    t0 = time.time()
    model = config["model"]
    start = datetime.strptime(config["start_date"], "%Y-%m-%d")
    end = datetime.strptime(config["end_date"], "%Y-%m-%d")
    variables = config.get("variables", UPPER_AIR_VARS)
    levels_key = str(config.get("levels", "13"))
    levels = STANDARD_LEVELS.get(levels_key, [int(x) for x in levels_key.split(",")])
    surface = config.get("surface_variables", SURFACE_VARS)
    derived = config.get("derived_fields", [])
    freq = config.get("frequency_hours", 1)
    lead = config.get("lead_time_hours", 1)
    out_dir = Path(config["output_dir"])
    workers = config.get("workers", 8)
    limit = config.get("limit")

    raw_dir = out_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Generate runs
    runs = []
    dt = start
    while dt <= end:
        for hour in range(0, 24, freq):
            runs.append(f"{dt:%Y-%m-%d}/{hour:02d}z")
        dt += timedelta(days=1)
    if limit:
        runs = runs[:limit]

    print(f"Building: {len(runs)} runs, {len(variables)} vars x {len(levels)} levels"
          f" + {len(surface)} surface + {len(derived)} derived")

    # Phase 1: Download + process
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for run_str in runs:
            out_path = raw_dir / f"{run_str.replace('/', '_').replace('z', '')}.npy"
            if out_path.exists():
                results.append({"status": "cached", "run": run_str})
                continue
            futures[pool.submit(process_single_run, model, run_str, variables,
                                levels, surface, derived, str(out_path))] = run_str

        for future in as_completed(futures):
            r = future.result()
            (results if r["status"] in ("ok", "cached") else errors).append(r)
            done = len(results) + len(errors)
            elapsed = time.time() - t0
            rate = done / elapsed * 60 if elapsed > 0 else 0
            eta = (len(runs) - done - len([x for x in results if x.get("status") == "cached"])) / rate if rate > 0 else 0
            sys.stdout.write(f"\r  [{done}/{len(runs)}] ok={len(results)} err={len(errors)} [{rate:.0f}/min ETA {eta:.0f}m]   ")
            sys.stdout.flush()
        print()

    # Phase 2: Build pairs
    run_to_file = {}
    for r in sorted(runs):
        f = raw_dir / f"{r.replace('/', '_').replace('z', '')}.npy"
        if f.exists():
            run_to_file[r] = str(f)

    pairs = []
    for r in sorted(run_to_file):
        dt = datetime.strptime(r, "%Y-%m-%d/%Hz")
        target = f"{(dt + timedelta(hours=lead)):%Y-%m-%d}/{(dt + timedelta(hours=lead)).hour:02d}z"
        if target in run_to_file:
            pairs.append({"input": run_to_file[r], "target": run_to_file[target],
                          "input_time": r, "target_time": target})

    # Phase 3: Normalization
    stats = None
    if config.get("normalize", True) and pairs:
        import random
        sample = random.sample(pairs, min(200, len(pairs)))
        sums, sq_sums, count = None, None, 0
        for p in sample:
            try:
                d = np.load(p["input"])
                if sums is None:
                    sums = np.zeros(d.shape[0]); sq_sums = np.zeros(d.shape[0])
                for c in range(d.shape[0]):
                    v = d[c][~np.isnan(d[c])]
                    if len(v): sums[c] += v.mean(); sq_sums[c] += (v**2).mean()
                count += 1
            except Exception:
                pass
        if count:
            mean = sums / count
            std = np.maximum(np.sqrt(sq_sums / count - mean**2), 1e-6)
            stats = {"mean": mean.tolist(), "std": std.tolist()}

    # Phase 4: Split
    import random; random.seed(42); random.shuffle(pairs)
    split = config.get("split", {"train": 0.8, "val": 0.1, "test": 0.1})
    n = len(pairs)
    t_end = int(n * split.get("train", 0.8))
    v_end = t_end + int(n * split.get("val", 0.1))
    splits = {"train": pairs[:t_end], "val": pairs[t_end:v_end], "test": pairs[v_end:]}

    # Get channel names
    channel_names = []
    for f in raw_dir.glob("*_meta.json"):
        channel_names = json.loads(f.read_text()).get("channels", [])
        break

    manifest = {
        "created": datetime.now(timezone.utc).isoformat(), "config": config,
        "total_samples": len(pairs), "splits": {k: len(v) for k, v in splits.items()},
        "shape": results[0].get("shape") if results and "shape" in results[0] else None,
        "channel_names": channel_names,
    }
    if stats:
        manifest["normalization"] = {**stats, "channel_names": channel_names}
    for name, data in splits.items():
        (out_dir / f"{name}.json").write_text(json.dumps(data, indent=2))
    (out_dir / "metadata.json").write_text(json.dumps(manifest, indent=2))

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} min — {len(pairs)} samples "
          f"({', '.join(f'{k}={len(v)}' for k, v in splits.items())})")

    return {"status": "ok", "path": str(out_dir), "total_samples": len(pairs),
            "splits": {k: len(v) for k, v in splits.items()},
            "shape": manifest["shape"], "channels": len(channel_names),
            "time_minutes": round(elapsed / 60, 1)}


# ── Background dataset building ────────────────────────────────────
_build_state = {
    "running": False,
    "progress": "",
    "result": None,
}


def get_build_status() -> dict:
    return {k: v for k, v in _build_state.items()}


def build_dataset_background(config: dict) -> dict:
    """Start dataset building in background thread."""
    if _build_state["running"]:
        return {"error": "Build already in progress", **get_build_status()}

    _build_state.update({"running": True, "progress": "starting...", "result": None})

    def run():
        try:
            result = build_dataset(config)
            _build_state["result"] = result
        except Exception as e:
            _build_state["result"] = {"error": str(e)}
        finally:
            _build_state["running"] = False

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return {"status": "started", "config": {k: v for k, v in config.items() if k != "derived_fields"}}
