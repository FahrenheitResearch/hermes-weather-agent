"""NEXRAD Level 2 radar export via the optional rustwx `radar_export` binary."""
from __future__ import annotations

import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

from ..geo import resolve_location
from ..rustwx import RustwxEnv

S3_BUCKET = "https://unidata-nexrad-level2.s3.amazonaws.com"
RADAR_EXPORT = "radar_export"


def _list_scans(site: str, target: datetime) -> list[tuple[str, datetime]]:
    prefix = f"{target:%Y/%m/%d}/{site}/"
    try:
        resp = requests.get(S3_BUCKET, params={"list-type": "2", "prefix": prefix}, timeout=30)
        resp.raise_for_status()
    except Exception:
        return []
    entries: list[tuple[str, datetime]] = []
    for elem in ET.fromstring(resp.text).iter():
        if not elem.tag.endswith("Key"):
            continue
        key = elem.text or ""
        if "_MDM" in key:
            continue
        name = Path(key).name
        rest = name[len(site):].lstrip("_")
        if len(rest) >= 15 and rest[8] == "_":
            dp, tp = rest[:8], rest[9:15]
            if dp.isdigit() and tp.isdigit():
                ts = datetime.strptime(dp + tp, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                entries.append((key, ts))
    return entries


def _download(key: str, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / Path(key).name
    if target.exists() and target.stat().st_size > 0:
        return target
    with requests.get(f"{S3_BUCKET}/{key}", stream=True, timeout=180) as r:
        r.raise_for_status()
        with target.open("wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    return target


def _find_radar_export(env: RustwxEnv) -> Path | None:
    if env.has_binary(RADAR_EXPORT):
        return env.binaries[RADAR_EXPORT]

    names = [RADAR_EXPORT, f"{RADAR_EXPORT}.exe"]
    if env.bin_dir is not None:
        for name in names:
            candidate = env.bin_dir / name
            if candidate.exists():
                return candidate

    for root in (
        Path.home() / "rustwx" / "target" / "release",
        Path.home() / "rustwx" / "target" / "debug",
        Path.home() / ".cargo" / "bin",
    ):
        for name in names:
            candidate = root / name
            if candidate.exists():
                return candidate

    for name in names:
        hit = shutil.which(name)
        if hit:
            return Path(hit)
    return None


def _missing_binary_error() -> str:
    return (
        "rustwx binary 'radar_export' not found. Build it with "
        "`cargo build --release --bin radar_export` or set HERMES_RUSTWX_BIN_DIR "
        "to the rustwx target/release directory."
    )


def _normalize_products(value: str | list[str]) -> str:
    aliases = {
        "dbz": "ref",
        "reflectivity": "ref",
        "velocity": "vel",
        "spectrum_width": "sw",
        "rho": "cc",
        "echotops": "et",
        "echo_tops": "et",
    }
    parts = value if isinstance(value, list) else str(value).split(",")
    normalized = []
    for part in parts:
        item = str(part).strip()
        if not item:
            continue
        normalized.append(aliases.get(item.lower(), item))
    return ",".join(normalized) if normalized else "ref"


def radar(
    env: RustwxEnv,
    *,
    site: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    location: dict | None = None,
    valid_time: str | None = None,
    product: str = "ref",
    products: str | list[str] | None = None,
    out_dir: str | None = None,
    timeout: int = 180,
    cache_dir: str | None = None,
    size: int = 1024,
    min_value: float | None = None,
    include_tensor: bool = False,
    max_tensor_gates: int = 800,
) -> dict:
    """Render NEXRAD Level-II PNG/JSON outputs with rustwx `radar_export`.

    With `valid_time`, the wrapper downloads the nearest archived scan for the
    requested site and passes it to `radar_export --input`. Otherwise
    `radar_export` fetches the latest public scan for `--site` or `--lat/--lon`.
    """
    if location is not None and (lat is None or lon is None):
        resolved = resolve_location(location)
        if resolved is not None:
            loc_lat, loc_lon = resolved
            lat = lat if lat is not None else loc_lat
            lon = lon if lon is not None else loc_lon

    site = site.upper() if site else None
    if not site and (lat is None or lon is None):
        return {"ok": False, "error": "provide site or both lat and lon"}

    if valid_time:
        try:
            target = datetime.fromisoformat(valid_time.replace("Z", "+00:00"))
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
        except ValueError as exc:
            return {"ok": False, "error": f"bad valid_time: {exc}"}
    else:
        target = datetime.now(timezone.utc)

    started = time.time()
    scan_path: Path | None = None
    best_ts: datetime | None = None

    binary = _find_radar_export(env)
    if binary is None:
        payload = {
            "ok": False,
            "error": _missing_binary_error(),
            "elapsed_s": round(time.time() - started, 2),
        }
        if site:
            entries = _list_scans(site, target)
            if entries:
                best_key, best_ts = min(entries, key=lambda e: abs(e[1] - target))
                scan_path = _download(best_key, Path(cache_dir) if cache_dir else env.cache_dir / "radar")
                payload.update({
                    "fallback": "raw_level2_scan",
                    "site": site,
                    "scan_path": str(scan_path),
                    "scan_time_utc": best_ts.isoformat(),
                    "requested_time_utc": target.isoformat(),
                })
        return payload

    if valid_time:
        if not site:
            return {"ok": False, "error": "valid_time archive lookup requires site"}
        entries = _list_scans(site, target)
        if not entries:
            return {"ok": False, "error": f"no scans listed for {site} on {target:%Y-%m-%d}"}
        best_key, best_ts = min(entries, key=lambda e: abs(e[1] - target))
        scan_path = _download(best_key, Path(cache_dir) if cache_dir else env.cache_dir / "radar")

    requested_products = _normalize_products(products if products is not None else product)
    multi_product = requested_products.lower() == "all" or "," in requested_products

    stamp = best_ts or target
    label = site or f"{lat:.3f}_{lon:.3f}"
    out_root = (Path(out_dir) if out_dir else (env.out_root / "radar" / label / f"{stamp:%Y%m%d_%H%M%S}")).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    safe_product = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in requested_products)
    png_target = out_root if multi_product else out_root / f"radar_{safe_product.lower()}.png"
    json_path = out_root / "radar.json"

    cmd = [str(binary)]
    if site:
        cmd.extend(["--site", site])
    else:
        cmd.extend(["--lat", str(lat), "--lon", str(lon)])
    if scan_path is not None:
        cmd.extend(["--input", str(scan_path)])
    cmd.extend(["--products" if multi_product else "--product", requested_products])
    cmd.extend(["--png", str(png_target), "--json", str(json_path), "--size", str(size)])
    if min_value is not None:
        cmd.extend(["--min-value", str(min_value)])
    if include_tensor:
        cmd.append("--include-tensor")
    cmd.extend(["--max-tensor-gates", str(max_tensor_gates)])

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env.subprocess_env(),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "error": f"radar_export timed out after {timeout}s",
            "binary": str(binary),
            "out_dir": str(out_root),
            "stdout_tail": (exc.stdout or "").splitlines()[-12:],
            "stderr_tail": (exc.stderr or "").splitlines()[-12:],
            "elapsed_s": round(time.time() - started, 2),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": f"radar_export failed to start: {exc}",
            "binary": str(binary),
            "out_dir": str(out_root),
            "elapsed_s": round(time.time() - started, 2),
        }

    pngs = sorted(out_root.rglob("*.png"))
    jsons = sorted(out_root.rglob("*.json"))
    payload = {
        "ok": proc.returncode == 0,
        "binary": str(binary),
        "returncode": proc.returncode,
        "site": site,
        "lat": lat,
        "lon": lon,
        "product": product,
        "products": requested_products,
        "scan_path": str(scan_path) if scan_path else None,
        "scan_time_utc": best_ts.isoformat() if best_ts else None,
        "requested_time_utc": target.isoformat(),
        "out_dir": str(out_root),
        "pngs": [str(p) for p in pngs],
        "png_count": len(pngs),
        "jsons": [str(p) for p in jsons],
        "json_count": len(jsons),
        "stdout_tail": proc.stdout.splitlines()[-12:] if proc.stdout else [],
        "stderr_tail": proc.stderr.splitlines()[-12:] if proc.stderr else [],
        "elapsed_s": round(time.time() - started, 2),
    }
    if proc.returncode != 0:
        payload["error"] = "radar_export failed"
    return payload
