"""NEXRAD Level 2 radar fetch — minimal, network-only.

rustwx does not currently render NEXRAD Level 2; this tool downloads the
nearest scan from the public AWS bucket and returns the path. If a local
`ptx-radar-processor` binary exists it will be used to render a PNG;
otherwise the raw scan path is returned and the agent can fall back to
another tool.
"""
from __future__ import annotations

import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

from ..rustwx import RustwxEnv

S3_BUCKET = "https://unidata-nexrad-level2.s3.amazonaws.com"


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


def radar(
    env: RustwxEnv,
    *,
    site: str,
    valid_time: str | None = None,
    product: str = "DBZ",
    out_dir: str | None = None,
) -> dict:
    """Fetch the NEXRAD Level 2 scan nearest to `valid_time` (default: now)
    for radar `site` (e.g. KTLX). Renders to PNG if ptx-radar-processor is
    available, otherwise returns the raw scan path.
    """
    site = site.upper()
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
    entries = _list_scans(site, target)
    if not entries:
        return {"ok": False, "error": f"no scans listed for {site} on {target:%Y-%m-%d}"}

    best_key, best_ts = min(entries, key=lambda e: abs(e[1] - target))
    cache_dir = env.cache_dir / "radar"
    scan_path = _download(best_key, cache_dir)

    out_root = Path(out_dir) if out_dir else (env.out_root / "radar" / site / f"{best_ts:%Y%m%d_%H%M%S}")
    out_root.mkdir(parents=True, exist_ok=True)

    payload = {
        "ok": True,
        "site": site,
        "scan_path": str(scan_path),
        "scan_time_utc": best_ts.isoformat(),
        "requested_time_utc": target.isoformat(),
        "out_dir": str(out_root),
        "elapsed_s": round(time.time() - started, 2),
    }

    ptx = shutil.which("ptx-radar-processor") or shutil.which("ptx-radar-processor.exe")
    if ptx:
        try:
            cmd = [
                ptx,
                "--input", str(scan_path),
                "--out-dir", str(out_root),
                "--product", product,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                                  env=env.subprocess_env())
            if proc.returncode == 0:
                pngs = sorted(out_root.glob("*.png"))
                payload["pngs"] = [str(p) for p in pngs]
                payload["png_count"] = len(pngs)
                payload["renderer"] = "ptx-radar-processor"
            else:
                payload["render_error"] = proc.stderr.strip()[-300:]
        except Exception as exc:
            payload["render_error"] = str(exc)
    else:
        payload["note"] = (
            "Rendered PNGs unavailable: install `ptx-radar-processor` "
            "and ensure it is on PATH. Raw Level 2 scan saved at scan_path."
        )
    return payload
