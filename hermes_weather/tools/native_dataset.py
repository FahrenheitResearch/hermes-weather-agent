"""Rust-native multisource training-data tools.

These wrappers expose rustwx's native dataset planner/materializer and raw
observation quicklook renderer to MCP clients. The heavy work stays in Rust;
this module only normalizes agent arguments and records outputs.
"""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

from .. import jobs
from ..rustwx import RustwxEnv


def _csv(values: list[str] | tuple[str, ...] | str | None) -> str | None:
    if values is None:
        return None
    if isinstance(values, str):
        return values
    return ",".join(str(value).strip() for value in values if str(value).strip())


def _bounds_csv(bounds: list[float] | tuple[float, ...] | str | None) -> str | None:
    if bounds is None:
        return None
    if isinstance(bounds, str):
        return bounds
    if len(bounds) != 4:
        raise ValueError("bounds must be [west, east, south, north]")
    return ",".join(str(float(value)) for value in bounds)


def _run_binary(env: RustwxEnv, binary: str, args: list[str], *, timeout: int) -> dict:
    exe = env.require_binary(binary)
    started = time.time()
    proc = subprocess.run(
        [str(exe), *args],
        env=env.subprocess_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return {
        "ok": proc.returncode == 0,
        "binary": binary,
        "command": [str(exe), *args],
        "returncode": proc.returncode,
        "elapsed_s": round(time.time() - started, 3),
        "stdout_tail": proc.stdout.splitlines()[-40:],
        "stderr_tail": proc.stderr.splitlines()[-40:],
    }


def plan(
    env: RustwxEnv,
    *,
    dataset_name: str = "rustwx_hrrr_multisource_v1",
    case: str | None = None,
    cases: list[str] | None = None,
    tile_grid: str | None = None,
    tiles: list[str] | None = None,
    shard_index: int = 0,
    shard_count: int = 1,
    grid_size: int = 512,
    history_steps: int = 3,
    forecast_step_frames: int = 1,
    hrrr_fields: list[str] | str | None = None,
    mrms_fields: list[str] | str | None = None,
    goes_channels: list[str] | str | None = None,
    goes_product_family: str | None = None,
    goes_sector: str | None = None,
    level2_products: list[str] | str | None = None,
    out: str | None = None,
    print_plan: bool = False,
    timeout: int = 120,
) -> dict:
    """Write a rustwx native dataset plan with selectable source categories."""
    out_path = Path(out) if out else env.out_root / "native_dataset" / "dataset_plan.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    args = [
        "--dataset-name", dataset_name,
        "--shard-index", str(int(shard_index)),
        "--shard-count", str(int(shard_count)),
        "--grid-size", str(int(grid_size)),
        "--history-steps", str(int(history_steps)),
        "--forecast-step-frames", str(int(forecast_step_frames)),
        "--out", str(out_path),
    ]
    for item in cases or ([case] if case else []):
        args.extend(["--case", item])
    if tile_grid:
        args.extend(["--tile-grid", tile_grid])
    for item in tiles or []:
        args.extend(["--tile", item])
    for flag, values in (
        ("--hrrr-fields", hrrr_fields),
        ("--mrms-fields", mrms_fields),
        ("--goes-channels", goes_channels),
        ("--level2-products", level2_products),
    ):
        value = _csv(values)
        if value:
            args.extend([flag, value])
    if goes_product_family:
        args.extend(["--goes-product-family", goes_product_family])
    if goes_sector:
        args.extend(["--goes-sector", goes_sector])
    if print_plan:
        args.append("--print")

    result = _run_binary(env, "native_dataset_plan", args, timeout=timeout)
    result["plan_path"] = str(out_path)
    if out_path.exists():
        try:
            plan_json = json.loads(out_path.read_text(encoding="utf-8"))
            config = plan_json.get("config", {})
            result["dataset_name"] = plan_json.get("dataset_name")
            result["case_count"] = len(config.get("cases", []))
            result["tile_count"] = len(config.get("tiles", []))
            result["source_count"] = len(config.get("sources", []))
            result["sources"] = config.get("sources", [])
        except Exception as exc:
            result["plan_parse_error"] = f"{type(exc).__name__}: {exc}"
    return result


def run_plan(
    env: RustwxEnv,
    *,
    plan_path: str,
    source_root: str | None = None,
    cache_root: str | None = None,
    shard_out: str | None = None,
    progress_out: str | None = None,
    report_out: str | None = None,
    allow_missing_sources: bool = False,
    fetch_hrrr: bool = False,
    fetch_obs: bool = False,
    fetch_radar: bool = False,
    max_attempts: int = 3,
    continue_on_error: bool = False,
    rayon_threads: int = 0,
    background: bool = True,
    timeout: int = 3600,
) -> dict:
    """Run or queue the native dataset runner for an existing plan."""
    plan = Path(plan_path)
    args = ["--plan", str(plan)]
    if progress_out:
        args.extend(["--progress-out", progress_out])
    if report_out:
        args.extend(["--report-out", report_out])
    if source_root:
        args.extend(["--source-root", source_root])
    if cache_root:
        args.extend(["--cache-root", cache_root])
    if shard_out:
        args.extend(["--shard-out", shard_out])
    if allow_missing_sources:
        args.append("--allow-missing-sources")
    if fetch_hrrr:
        args.append("--fetch-hrrr")
    if fetch_obs:
        args.append("--fetch-obs")
    if fetch_radar:
        args.append("--fetch-radar")
    args.extend(["--max-attempts", str(int(max_attempts))])
    if continue_on_error:
        args.append("--continue-on-error")
    if rayon_threads:
        args.extend(["--rayon-threads", str(int(rayon_threads))])

    def _runner(job: jobs.Job) -> dict:
        job.append_log(f"native_dataset_runner plan={plan}")
        result = _run_binary(env, "native_dataset_runner", args, timeout=timeout)
        if report_out and Path(report_out).exists():
            result["report_path"] = report_out
        if progress_out and Path(progress_out).exists():
            result["progress_path"] = progress_out
        if shard_out and Path(shard_out).exists():
            result["shards"] = [str(path) for path in sorted(Path(shard_out).rglob("*")) if path.is_file()]
        return result

    if background:
        job = jobs.submit("native-dataset-run", {"plan": str(plan)}, _runner)
        return {
            "ok": True,
            "job_id": job.job_id,
            "kind": job.kind,
            "state": job.state,
            "note": "Native dataset runner started in background. Poll wx_job_status with this job_id.",
        }
    inline = jobs.Job(job_id="inline", kind="native-dataset-run", args={"plan": str(plan)})
    inline.started_at = time.time()
    return _runner(inline)


def preview(
    env: RustwxEnv,
    *,
    kind: str,
    input: str,
    out: str | None = None,
    size: int = 512,
    bounds: list[float] | str | None = None,
    channel: str | None = None,
    product: str | None = None,
    radar_site: str | None = None,
    center_lat: float | None = None,
    center_lon: float | None = None,
    span_km: float = 512.0,
    min_value: float | None = None,
    max_value: float | None = None,
    dealias: str = "auto",
    timeout: int = 300,
) -> dict:
    """Render a raw GOES, MRMS, or NEXRAD Level-II file to a PNG quicklook."""
    input_path = Path(input)
    if out:
        out_path = Path(out)
    else:
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in f"{kind}_{input_path.stem}")
        out_path = env.out_root / "native_obs_preview" / f"{safe}.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    args = [
        "--kind", kind,
        "--input", str(input_path),
        "--out", str(out_path),
        "--size", str(int(size)),
    ]
    value = _bounds_csv(bounds)
    if value:
        args.extend(["--bounds", value])
    if channel:
        args.extend(["--channel", channel])
    if product:
        args.extend(["--product", product])
    if radar_site:
        args.extend(["--radar-site", radar_site])
    if center_lat is not None:
        args.extend(["--center-lat", str(float(center_lat))])
    if center_lon is not None:
        args.extend(["--center-lon", str(float(center_lon))])
    if span_km:
        args.extend(["--span-km", str(float(span_km))])
    if min_value is not None:
        args.extend(["--min", str(float(min_value))])
    if max_value is not None:
        args.extend(["--max", str(float(max_value))])
    if dealias:
        args.extend(["--dealias", dealias])

    result = _run_binary(env, "native_obs_preview", args, timeout=timeout)
    result["png"] = str(out_path)
    result["json"] = str(out_path.with_suffix(".json"))
    if out_path.with_suffix(".json").exists():
        try:
            result["preview_report"] = json.loads(out_path.with_suffix(".json").read_text(encoding="utf-8"))
        except Exception as exc:
            result["preview_report_error"] = f"{type(exc).__name__}: {exc}"
    return result
