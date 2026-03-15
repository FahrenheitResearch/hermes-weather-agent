"""Weather data fetching — HRRR, GFS, radar, soundings.

Uses rustmet for GRIB2 I/O (fast Rust parser with selective idx download).
"""
from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
import rustmet


S3_NEXRAD = "https://unidata-nexrad-level2.s3.amazonaws.com"

MODEL_INFO = {
    "hrrr": {"resolution": "3km", "coverage": "CONUS", "frequency": "1h",
             "grid": (1799, 1059), "projection": "Lambert Conformal"},
    "gfs": {"resolution": "0.25deg", "coverage": "Global", "frequency": "6h",
            "grid": (1440, 721), "projection": "Lat/Lon"},
    "rap": {"resolution": "13km", "coverage": "CONUS", "frequency": "1h",
            "grid": (451, 337), "projection": "Lambert Conformal"},
}

STANDARD_LEVELS = {
    "13": [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50],
    "37": [1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 725, 700,
           675, 650, 625, 600, 575, 550, 525, 500, 475, 450, 425, 400, 375,
           350, 325, 300, 275, 250, 225, 200, 175, 150, 125, 100],
}

UPPER_AIR_VARS = ["TMP", "UGRD", "VGRD", "SPFH", "HGT"]
SURFACE_VARS = ["TMP:2 m above ground", "UGRD:10 m above ground",
                "VGRD:10 m above ground", "PRES:surface"]


def fetch_model(model: str, run: str, fhour: int = 0,
                product: str = "prs", variables: list[str] = None):
    """Fetch model data using rustmet (fast Rust GRIB2 parser).

    Args:
        model: "hrrr", "gfs", "rap"
        run: "YYYY-MM-DD/HHz"
        fhour: Forecast hour
        product: "prs", "sfc", "nat"
        variables: Variable filter list

    Returns:
        rustmet GribFile with .messages
    """
    return rustmet.fetch(model, run, fhour=fhour, product=product, vars=variables)


def fetch_radar_scan(site: str, target_time: datetime) -> Path | None:
    """Download nearest NEXRAD Level 2 scan from S3.

    Args:
        site: Radar site ID (e.g. "KTLX")
        target_time: Target datetime (UTC)

    Returns:
        Path to downloaded file, or None
    """
    prefix = f"{target_time:%Y/%m/%d}/{site}/"
    try:
        resp = requests.get(S3_NEXRAD, params={"list-type": "2", "prefix": prefix}, timeout=30)
        resp.raise_for_status()
    except Exception:
        return None

    entries = []
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

    if not entries:
        return None

    best_key, _ = min(entries, key=lambda x: abs(x[1] - target_time))

    # Download
    cache_dir = Path("cache/radar")
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / Path(best_key).name
    if not target.exists():
        with requests.get(f"{S3_NEXRAD}/{best_key}", stream=True, timeout=180) as r:
            r.raise_for_status()
            with target.open("wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
    return target


def fetch_sounding(lat: float, lon: float, valid_time: datetime) -> dict | None:
    """Fetch nearest historical radiosonde sounding from IEM.

    Returns dict with pressure, temperature, dewpoint, wind arrays.
    """
    # Find nearest launch time (00Z or 12Z)
    candidates = [
        valid_time.replace(hour=0, minute=0, second=0),
        valid_time.replace(hour=12, minute=0, second=0),
        (valid_time - timedelta(days=1)).replace(hour=12, minute=0, second=0),
    ]
    launch = min(candidates, key=lambda c: abs(c - valid_time))
    ts = launch.strftime("%Y%m%d%H%M")

    # Find nearest RAOB station (simplified — use a few major stations)
    # In production, load full station list from rustmet
    import math
    stations = _load_raob_stations()

    def dist(s):
        return math.sqrt((s["lat"] - lat)**2 + (s["lon"] - lon)**2)

    nearest = min(stations, key=dist)

    for prefix in [f"K{nearest['icao']}", nearest["icao"]]:
        try:
            resp = requests.get(
                f"https://mesonet.agron.iastate.edu/json/raob.py?station={prefix}&ts={ts}",
                timeout=20
            )
            if resp.ok:
                profiles = resp.json().get("profiles", [])
                if profiles:
                    levels = []
                    for lev in profiles[0].get("profile", []):
                        p, t, td = lev.get("pres"), lev.get("tmpc"), lev.get("dwpc")
                        if p and t is not None and td is not None and 100 <= p <= 1050:
                            levels.append({
                                "pressure": p, "temperature": t, "dewpoint": td,
                                "wind_speed_kt": lev.get("sknt") or 0,
                                "wind_dir": lev.get("drct") or 0,
                                "height_m": lev.get("hght") or 0,
                            })
                    if len(levels) >= 10:
                        return {
                            "station": nearest, "launch_time": launch.isoformat(),
                            "levels": sorted(levels, key=lambda l: l["pressure"], reverse=True),
                        }
        except Exception:
            pass
    return None


def _load_raob_stations() -> list[dict]:
    """Load RAOB stations from rustmet source."""
    import re
    path = Path(__file__).resolve().parents[1] / ".." / "rustmet" / "crates" / "wx-sounding" / "src" / "raob_stations.rs"
    if not path.exists():
        # Fallback: minimal station list
        return [
            {"icao": "OUN", "name": "Norman", "lat": 35.18, "lon": -97.44},
            {"icao": "BMX", "name": "Birmingham", "lat": 33.17, "lon": -86.77},
            {"icao": "SGF", "name": "Springfield", "lat": 37.23, "lon": -93.40},
            {"icao": "DVN", "name": "Davenport", "lat": 41.61, "lon": -90.58},
            {"icao": "OAX", "name": "Omaha", "lat": 41.32, "lon": -96.37},
        ]
    stations = []
    for m in re.finditer(
        r'wmo:\s*"(\d+)",\s*icao:\s*"(\w+)",\s*name:\s*"([^"]*)",\s*state:\s*"(\w+)",\s*lat:\s*([-\d.]+),\s*lon:\s*([-\d.]+)',
        path.read_text()
    ):
        stations.append({"icao": m.group(2), "name": m.group(3),
                          "lat": float(m.group(5)), "lon": float(m.group(6))})
    return stations
