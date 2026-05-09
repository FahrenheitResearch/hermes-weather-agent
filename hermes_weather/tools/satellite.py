"""GOES satellite rendering via rustwx.render_goes_satellite_json."""
from __future__ import annotations

import json
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from ..rustwx import RustwxEnv


SOCAL_BOUNDS = [-121.5, -113.5, 31.5, 36.8]
SOCAL_ALIASES = {"socal", "southern-california", "southern_california"}
DEFAULT_PRODUCTS = [
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
FULL_DISK_SAFE_PRODUCTS = [
    "goes_abi_band_13",
]
AUTO_BOUNDS_SECTORS = {
    "full",
    "full_disk",
    "fulldisk",
    "full_disc",
    "fulldisc",
    "fd",
    "f",
    "meso",
    "mesoscale",
    "meso1",
    "mesoscale1",
    "mesoscale_1",
    "m1",
    "meso2",
    "mesoscale2",
    "mesoscale_2",
    "m2",
}


def _collect_pngs(value: Any) -> list[str]:
    pngs: list[str] = []
    if isinstance(value, dict):
        for item in value.values():
            pngs.extend(_collect_pngs(item))
    elif isinstance(value, list):
        for item in value:
            pngs.extend(_collect_pngs(item))
    elif isinstance(value, str) and value.lower().endswith(".png"):
        pngs.append(value)
    return pngs


def _render_goes_satellite(env: RustwxEnv, request: dict) -> dict:
    if not env.module_available:
        raise RuntimeError(
            "rustwx Python module not installed. Install with: pip install 'rustwx>=0.5.0'"
        )
    import rustwx

    if not hasattr(rustwx, "render_goes_satellite_json"):
        raise RuntimeError(
            "installed rustwx does not expose render_goes_satellite_json; install rustwx>=0.5.0"
        )
    return json.loads(rustwx.render_goes_satellite_json(json.dumps(request, default=str)))


def _render_goes_native_sequence(env: RustwxEnv, request: dict) -> tuple[dict | None, list[str], list[str]]:
    try:
        import rustwx  # type: ignore
    except Exception:
        rustwx = None
    if rustwx is not None and hasattr(rustwx, "render_goes_native_sequence_json"):
        report = json.loads(rustwx.render_goes_native_sequence_json(json.dumps(request, default=str)))
        return report, [], []

    exe = env.require_binary("goes_native_sequence")
    args = [
        str(exe),
        "--satellite", str(request["satellite"]),
        "--abi-product", str(request["abi_product"]),
        "--product", str(request["product"]),
        "--domain", str(request["domain_slug"]),
        "--label", str(request["domain_label"]),
        "--west", str(float(request["west"])),
        "--east", str(float(request["east"])),
        "--south", str(float(request["south"])),
        "--north", str(float(request["north"])),
        "--out-dir", str(Path(request["out_dir"]).resolve()),
        "--cache-dir", str(Path(request["cache_dir"]).resolve()),
        "--latest-count", str(max(1, int(request["latest_count"]))),
        "--scan-lookback-hours", str(max(1, int(request["scan_lookback_hours"]))),
        "--downsample", str(float(request["downsample"])),
        "--download-workers", str(max(0, int(request["download_workers"]))),
        "--render-workers", str(max(0, int(request["render_workers"]))),
        "--png-compression", str(request["png_compression"]),
    ]
    if request.get("abi_sector"):
        args.extend(["--sector", str(request["abi_sector"])])
    if request.get("start_time_utc"):
        args.extend(["--start", str(request["start_time_utc"])])
    if request.get("end_time_utc"):
        args.extend(["--end", str(request["end_time_utc"])])
    if request.get("min_step_minutes"):
        args.extend(["--min-step-minutes", str(int(request["min_step_minutes"]))])
    if not request.get("use_cache", True):
        args.append("--no-cache")
    if request.get("max_width"):
        args.extend(["--max-width", str(int(request["max_width"]))])
    if request.get("max_height"):
        args.extend(["--max-height", str(int(request["max_height"]))])

    proc = subprocess.run(
        args,
        env=env.subprocess_env(),
        capture_output=True,
        text=True,
        timeout=int(request.get("timeout", 1800)),
    )
    report: dict[str, Any] | None = None
    if proc.stdout.strip():
        try:
            report = json.loads(proc.stdout)
        except Exception:
            report = None
    return report, proc.stdout.splitlines()[-40:], proc.stderr.splitlines()[-40:]


def _write_gif(
    pngs: list[str],
    gif_path: Path,
    *,
    fps: float = 8.0,
    width: int | None = 1200,
) -> dict:
    gif_path.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg:
        concat_path = gif_path.with_suffix(".ffconcat")
        duration = 1.0 / max(0.1, float(fps))
        lines = ["ffconcat version 1.0"]
        for png in pngs:
            escaped = str(Path(png)).replace("\\", "/").replace("'", "'\\''")
            lines.append(f"file '{escaped}'")
            lines.append(f"duration {duration:.6f}")
        if pngs:
            escaped = str(Path(pngs[-1])).replace("\\", "/").replace("'", "'\\''")
            lines.append(f"file '{escaped}'")
        concat_path.write_text("\n".join(lines) + "\n", encoding="ascii")
        scale = f"scale={int(width)}:-1:flags=lanczos," if width else ""
        vf = (
            f"fps={float(fps):g},{scale}"
            "split[s0][s1];[s0]palettegen=max_colors=192[p];"
            "[s1][p]paletteuse=dither=bayer:bayer_scale=3"
        )
        started = time.time()
        proc = subprocess.run(
            [
                ffmpeg,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_path),
                "-vf",
                vf,
                str(gif_path),
            ],
            capture_output=True,
            text=True,
            timeout=900,
        )
        return {
            "ok": proc.returncode == 0 and gif_path.exists(),
            "method": "ffmpeg",
            "path": str(gif_path),
            "elapsed_s": round(time.time() - started, 3),
            "returncode": proc.returncode,
            "stderr_tail": proc.stderr.splitlines()[-20:],
        }

    from PIL import Image

    started = time.time()
    frames = []
    for png in pngs:
        image = Image.open(png).convert("P", palette=Image.Palette.ADAPTIVE, colors=192)
        if width and image.width != width:
            height = max(1, round(image.height * (width / image.width)))
            image = image.resize((width, height))
        frames.append(image)
    if not frames:
        return {"ok": False, "method": "pillow", "path": str(gif_path), "error": "no frames"}
    frames[0].save(
        gif_path,
        save_all=True,
        append_images=frames[1:],
        duration=round(1000 / max(0.1, float(fps))),
        loop=0,
        optimize=False,
    )
    return {
        "ok": gif_path.exists(),
        "method": "pillow",
        "path": str(gif_path),
        "elapsed_s": round(time.time() - started, 3),
    }


def _normalize_slug(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def satellite(
    env: RustwxEnv,
    *,
    satellite: str = "goes18",
    abi_product: str = "ABI-L2-CMIPC",
    sector: str | None = None,
    domain: str = "pacific_southwest",
    bounds: list[float] | None = None,
    label: str | None = None,
    products: list[str] | None = None,
    width: int | None = None,
    height: int | None = None,
    scan_lookback_hours: int = 6,
    discovery_retries: int | None = None,
    retry_sleep_ms: int | None = None,
    use_cache: bool = True,
    download_glm: bool = False,
    glm_fetch_count: int | None = None,
    glm_lookback_hours: int | None = None,
    glm_max_age_min: float | None = None,
    high_speed_png: bool = True,
    auto_bounds: bool | None = None,
    allow_high_resolution_full_disk: bool = False,
    sequence_count: int | None = None,
    sequence_gif: bool = False,
    sequence_gif_delay_ms: int | None = None,
    skip_scan_id: str | None = None,
    out_dir: str | None = None,
) -> dict:
    """Render latest GOES ABI/GLM satellite products for a domain or bounds."""
    if not env.module_available:
        return {
            "ok": False,
            "error": "rustwx Python module not installed. Run: pip install 'rustwx>=0.5.0'",
        }

    sector_slug = _normalize_slug(sector) if sector else None
    sector_auto_domain = (
        sector_slug in AUTO_BOUNDS_SECTORS and bounds is None and domain == "pacific_southwest"
    )
    domain_slug = domain.strip().lower().replace(" ", "-")
    path_domain = domain
    if sector_auto_domain:
        full_disk_aliases = {"full", "full_disk", "fulldisk", "full_disc", "fulldisc", "fd", "f"}
        path_domain = "goes-full-disk" if sector_slug in full_disk_aliases else "goes-mesoscale"
        domain_slug = path_domain
    if bounds is None and domain_slug in SOCAL_ALIASES:
        bounds = SOCAL_BOUNDS
        label = label or "Southern California"

    out_root = Path(out_dir) if out_dir else (
        env.out_root / "satellite" / satellite / path_domain.replace(" ", "-")
    )
    out_root.mkdir(parents=True, exist_ok=True)

    request: dict[str, Any] = {
        "satellite": satellite,
        "abi_product": abi_product,
        "out_dir": str(out_root.resolve()).replace("\\", "/"),
        "cache_dir": str(env.cache_dir.resolve()).replace("\\", "/"),
        "scan_lookback_hours": scan_lookback_hours,
        "use_cache": use_cache,
        "download_glm": download_glm,
        "high_speed_png": high_speed_png,
    }
    if not sector_auto_domain:
        request["domain"] = domain
    if sector:
        request["sector"] = sector
    if auto_bounds is not None:
        request["auto_bounds"] = bool(auto_bounds)
    elif sector_slug in AUTO_BOUNDS_SECTORS:
        request["auto_bounds"] = True
    if allow_high_resolution_full_disk:
        request["allow_high_resolution_full_disk"] = True
    if sequence_count is not None:
        request["sequence_count"] = max(1, int(sequence_count))
    if sequence_gif:
        request["sequence_gif"] = True
    if sequence_gif_delay_ms is not None:
        request["sequence_gif_delay_ms"] = int(sequence_gif_delay_ms)
    if bounds:
        request["bounds"] = list(bounds)
        request.pop("domain", None)
    if label:
        request["label"] = label
    if products:
        selected_products = list(products)
    elif sector_slug in {"full", "full_disk", "fulldisk", "full_disc", "fulldisc", "fd", "f"}:
        selected_products = list(FULL_DISK_SAFE_PRODUCTS)
    else:
        selected_products = list(DEFAULT_PRODUCTS)
    request["products"] = selected_products
    if any("glm" in product.lower() for product in selected_products):
        request["download_glm"] = True
    if width:
        request["width"] = int(width)
    if height:
        request["height"] = int(height)
    if discovery_retries is not None:
        request["discovery_retries"] = int(discovery_retries)
    if retry_sleep_ms is not None:
        request["retry_sleep_ms"] = int(retry_sleep_ms)
    if glm_fetch_count is not None:
        request["glm_fetch_count"] = int(glm_fetch_count)
    if glm_lookback_hours is not None:
        request["glm_lookback_hours"] = int(glm_lookback_hours)
    if glm_max_age_min is not None:
        request["glm_max_age_min"] = float(glm_max_age_min)
    if skip_scan_id:
        request["skip_scan_id"] = skip_scan_id

    started = time.time()
    try:
        report = _render_goes_satellite(env, request)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "request": request}

    pngs = sorted(set(_collect_pngs(report)))
    if not pngs:
        pngs = [str(p) for p in sorted(out_root.rglob("*.png"))]
    return {
        "ok": bool(pngs),
        "satellite": satellite,
        "abi_product": abi_product,
        "sector": sector,
        "domain": None if bounds else (None if sector_auto_domain else domain),
        "bounds": bounds,
        "products": request.get("products"),
        "out_dir": str(out_root),
        "pngs": pngs,
        "png_count": len(pngs),
        "elapsed_s": round(time.time() - started, 2),
        "request": request,
        "report": report,
    }


def native_sequence(
    env: RustwxEnv,
    *,
    satellite: str = "goes18",
    abi_product: str = "ABI-L2-CMIPC",
    sector: str | None = None,
    product: str = "geocolor",
    bounds: list[float] | None = None,
    west: float | None = None,
    east: float | None = None,
    south: float | None = None,
    north: float | None = None,
    domain: str = "native_crop",
    label: str | None = None,
    start: str | None = None,
    end: str | None = None,
    latest_count: int = 1,
    scan_lookback_hours: int = 6,
    min_step_minutes: int | None = None,
    use_cache: bool = True,
    downsample: float = 1.0,
    max_width: int | None = None,
    max_height: int | None = None,
    download_workers: int = 8,
    render_workers: int = 0,
    png_compression: str = "fast",
    make_gif: bool = False,
    gif_fps: float = 8.0,
    gif_width: int | None = 1200,
    gif_path: str | None = None,
    out_dir: str | None = None,
    timeout: int = 1800,
) -> dict:
    """Render fast native-grid GOES crops over an explicit bbox and time window."""
    if bounds is not None:
        if len(bounds) != 4:
            return {"ok": False, "error": "bounds must be [west, east, south, north]"}
        west, east, south, north = [float(value) for value in bounds]
    if west is None or east is None or south is None or north is None:
        return {"ok": False, "error": "provide bounds or west/east/south/north"}

    out_root = Path(out_dir) if out_dir else env.out_root / "satellite_native_sequence" / satellite / domain
    out_root.mkdir(parents=True, exist_ok=True)
    request = {
        "satellite": satellite,
        "abi_product": abi_product,
        "abi_sector": sector,
        "product": product,
        "domain_slug": domain,
        "domain_label": label or domain.replace("_", " ").replace("-", " ").title(),
        "west": float(west),
        "east": float(east),
        "south": float(south),
        "north": float(north),
        "bounds": [float(west), float(east), float(south), float(north)],
        "out_dir": str(out_root.resolve()),
        "cache_dir": str(env.cache_dir.resolve()),
        "start_time_utc": start,
        "end_time_utc": end,
        "latest_count": max(1, int(latest_count)),
        "scan_lookback_hours": max(1, int(scan_lookback_hours)),
        "min_step_minutes": int(min_step_minutes) if min_step_minutes else None,
        "use_cache": bool(use_cache),
        "downsample": float(downsample),
        "max_width": int(max_width) if max_width else None,
        "max_height": int(max_height) if max_height else None,
        "download_workers": max(0, int(download_workers)),
        "render_workers": max(0, int(render_workers)),
        "png_compression": png_compression,
        "timeout": int(timeout),
    }

    started = time.time()
    try:
        report, stdout_tail, stderr_tail = _render_goes_native_sequence(env, request)
        returncode = 0
    except Exception as exc:
        return {
            "ok": False,
            "binary": "goes_native_sequence",
            "request": request,
            "error": str(exc),
            "elapsed_s": round(time.time() - started, 3),
        }
    pngs = sorted(str(path) for path in out_root.rglob("*.png"))
    gif: dict[str, Any] | None = None
    if make_gif and pngs:
        target_gif = (
            Path(gif_path)
            if gif_path
            else out_root / f"{domain}_{product}_{len(pngs)}frames.gif"
        )
        gif = _write_gif(pngs, target_gif, fps=gif_fps, width=gif_width)
    return {
        "ok": returncode == 0 and bool(pngs) and (gif is None or gif.get("ok", False)),
        "renderer": "rustwx.render_goes_native_sequence_json_or_binary",
        "returncode": returncode,
        "elapsed_s": round(time.time() - started, 3),
        "out_dir": str(out_root),
        "pngs": pngs,
        "png_count": len(pngs),
        "gif": gif,
        "gif_path": gif.get("path") if gif else None,
        "request": request,
        "report": report,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }
