"""Native 3D meteorological computations using metrust (Rust-powered MetPy replacement).

All grid-level computations use metrust.calc which is 10-93,000x faster than MetPy.
GRIB2 I/O uses rustmet for fast parsing and selective download.
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import rustmet
import metrust.calc as mc


DERIVED_FIELDS = {
    "sbcape": "Surface-based CAPE (full column)",
    "sb3cape": "Surface-based 3km CAPE",
    "mlcape": "Mixed-layer CAPE (100mb mean parcel)",
    "sb3cin": "Surface-based CIN",
    "lcl": "Lifted condensation level (m AGL)",
    "lfc": "Level of free convection (m AGL)",
    "shear_01": "0-1km bulk wind shear (m/s)",
    "shear_03": "0-3km bulk wind shear (m/s)",
    "shear_06": "0-6km bulk wind shear (m/s)",
    "srh_01": "0-1km storm-relative helicity (m2/s2)",
    "srh_03": "0-3km storm-relative helicity (m2/s2)",
    "stp": "Significant Tornado Parameter",
}


def fetch_native_3d(model: str, run: str, fhour: int = 0) -> dict | None:
    """Fetch native-level model data and build 3D arrays for computation.

    Downloads the wrfnatf (native level) HRRR file when possible for full
    50-level vertical resolution. Falls back to prs product.

    Returns dict with temp_3d, qvapor_3d, height_3d, pres_3d, u_3d, v_3d,
    psfc, t2, q2, nx, ny, nz — all as numpy float64 arrays.
    """
    import os

    # Suppress rustmet download noise — redirect fd 2 to devnull for the
    # duration of the fetch, then restore it so later logging still works.
    _devnull = open(os.devnull, 'w')
    _old_stderr = os.dup(2)
    os.dup2(_devnull.fileno(), 2)

    try:
        return _fetch_native_3d_inner(model, run, fhour)
    finally:
        os.dup2(_old_stderr, 2)
        os.close(_old_stderr)
        _devnull.close()


def _fetch_native_3d_inner(model, run, fhour):
    import os
    import requests
    import tempfile
    from datetime import datetime
    # Try native file first (wrfnatf)
    dt = datetime.strptime(run, "%Y-%m-%d/%Hz")
    fstr = f"{fhour:02d}"
    native_urls = [
        f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{dt:%Y%m%d}/conus/hrrr.t{dt:%H}z.wrfnatf{fstr}.grib2",
        f"https://noaa-hrrr-bdp-pds.s3.amazonaws.com/hrrr.{dt:%Y%m%d}/conus/hrrr.t{dt:%H}z.wrfnatf{fstr}.grib2",
    ]

    messages = None
    for url in native_urls:
        try:
            resp = requests.get(url, stream=True, timeout=300)
            if resp.status_code != 200:
                continue
            with tempfile.NamedTemporaryFile(suffix=".grib2", delete=False) as f:
                for chunk in resp.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
                tmp = f.name
            grib = rustmet.open(tmp)
            os.unlink(tmp)
            messages = grib.messages
            break
        except Exception:
            continue

    if messages is None or len(messages) < 20:
        # Fallback to prs
        try:
            grib = rustmet.fetch(model, run, fhour=fhour, product="prs",
                                 vars=["TMP", "SPFH", "UGRD", "VGRD", "HGT"])
            messages = grib.messages
        except Exception:
            return None

    # Index messages
    msg_by = {}
    for m in messages:
        msg_by[(m.variable, m.level_value)] = m

    nx = messages[0].nx
    ny = messages[0].ny

    # Find temperature levels — hybrid or pressure
    temp_levels = sorted(set(
        m.level_value for m in messages
        if m.variable == "Temperature"
        and "Hybrid" in (m.level if hasattr(m, 'level') else "")
    ))
    if not temp_levels:
        temp_levels = sorted(set(
            m.level_value for m in messages
            if m.variable == "Temperature"
            and m.level_value > 100
            and "Ground" not in (m.level if hasattr(m, 'level') else "")
            and "Height" not in (m.level if hasattr(m, 'level') else "")
        ))

    nz = len(temp_levels)
    if nz < 5:
        return None

    # Build 3D arrays
    size = nz * ny * nx
    T = np.zeros(size, dtype=np.float64)
    Q = np.zeros(size, dtype=np.float64)
    H = np.zeros(size, dtype=np.float64)
    P = np.zeros(size, dtype=np.float64)
    U = np.zeros(size, dtype=np.float64)
    V = np.zeros(size, dtype=np.float64)

    for zi, lev in enumerate(temp_levels):
        off = zi * ny * nx
        for var_long, arr, conv in [
            ("Temperature", T, -273.15), ("Specific Humidity", Q, 0),
            ("Geopotential Height", H, 0), ("Pressure", P, 0),
            ("U-Component of Wind", U, 0), ("V-Component of Wind", V, 0),
        ]:
            m = msg_by.get((var_long, lev))
            if m:
                vals = m.values()
                if len(vals) == ny * nx:
                    if conv:
                        arr[off:off + ny * nx] = vals + conv
                    else:
                        arr[off:off + ny * nx] = vals

    # Surface fields (t2 in KELVIN for metrust)
    psfc_m = t2_m = q2_m = None
    for m in messages:
        level_str = m.level if hasattr(m, 'level') else ""
        if m.variable == "Pressure" and ("Ground" in level_str or "Surface" in level_str):
            psfc_m = m
        elif m.variable == "Temperature" and "Height" in level_str and "2 " in level_str:
            t2_m = m
        elif m.variable == "Specific Humidity" and "Height" in level_str and "2 " in level_str:
            q2_m = m

    psfc = psfc_m.values().astype(np.float64) if psfc_m else np.full(ny * nx, 100000.0)
    t2 = t2_m.values().astype(np.float64) if t2_m else (T[:ny * nx] + 273.15).copy()
    q2 = q2_m.values().astype(np.float64) if q2_m else Q[:ny * nx].copy()

    # Heights to AGL
    sfc_h = H[:ny * nx].copy()
    for zi in range(nz):
        H[zi * ny * nx:(zi + 1) * ny * nx] -= sfc_h

    return {
        "temp_3d": T, "qvapor_3d": Q, "height_3d": H, "pres_3d": P,
        "u_3d": U, "v_3d": V, "psfc": psfc, "t2": t2, "q2": q2,
        "nx": nx, "ny": ny, "nz": nz, "levels": temp_levels,
    }


def _resolve_run(run: str) -> str:
    """Resolve 'latest' to an actual run string.

    Probes the NOMADS HRRR directory listing to find the most recent
    date directory and the latest run hour within it.
    """
    if run != "latest":
        return run

    import re
    import requests as req
    from datetime import datetime, timedelta, timezone

    base = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
    now = datetime.now(timezone.utc)

    for day_offset in range(2):
        date = now - timedelta(days=day_offset)
        date_str = date.strftime("%Y%m%d")
        dir_url = f"{base}/hrrr.{date_str}/conus/"
        try:
            r = req.get(dir_url, timeout=15)
            if not r.ok:
                continue
        except Exception:
            continue

        hours = sorted(set(re.findall(
            r'hrrr\.t(\d{2})z\.wrf(?:sfc|prs|nat)f\d{2}\.grib2', r.text
        )))
        if hours:
            return f"{date:%Y-%m-%d}/{hours[-1]}z"

    fb = now - timedelta(hours=3)
    return f"{fb:%Y-%m-%d}/{fb.hour:02d}z"


def compute_derived(model: str, run: str, fields: list[str],
                    fhour: int = 0) -> dict[str, np.ndarray]:
    """Compute multiple derived fields from one model fetch.

    Uses metrust for CAPE/CIN (Rust-powered, full parcel integration)
    and rustmet for shear/SRH (native grid computation).

    Returns dict of field_name → numpy array (ny, nx).
    """
    run = _resolve_run(run)
    data = fetch_native_3d(model, run, fhour)
    if data is None:
        return {}

    nx, ny, nz = data["nx"], data["ny"], data["nz"]
    p3d = data["pres_3d"].reshape(nz, ny, nx)
    t3d = data["temp_3d"].reshape(nz, ny, nx)
    q3d = data["qvapor_3d"].reshape(nz, ny, nx)
    h3d = data["height_3d"].reshape(nz, ny, nx)
    psfc = data["psfc"].reshape(ny, nx)
    t2 = data["t2"].reshape(ny, nx)
    q2 = data["q2"].reshape(ny, nx)

    results = {}

    # CAPE fields — metrust native grid computation
    cape_fields = {"sbcape", "sb3cape", "sb3cin", "mlcape", "lcl", "lfc", "stp"}
    if cape_fields & set(fields):
        top = 3000 if "sb3cape" in fields else None
        cape, cin, lcl_h, lfc_h = mc.compute_cape_cin(
            p3d, t3d, q3d, h3d, psfc, t2, q2, top_m=top
        )
        if "sbcape" in fields or "sb3cape" in fields:
            key = "sb3cape" if "sb3cape" in fields else "sbcape"
            results[key] = cape.magnitude.astype(np.float32)
        if "sb3cin" in fields:
            results["sb3cin"] = cin.magnitude.astype(np.float32)
        if "lcl" in fields:
            results["lcl"] = lcl_h.magnitude.astype(np.float32)
        if "lfc" in fields:
            results["lfc"] = lfc_h.magnitude.astype(np.float32)

    if "mlcape" in fields:
        ml_cape, _, _, _ = mc.compute_cape_cin(
            p3d, t3d, q3d, h3d, psfc, t2, q2, parcel_type="mixed"
        )
        results["mlcape"] = ml_cape.magnitude.astype(np.float32)

    # Shear — rustmet native grid
    for f in fields:
        if f.startswith("shear_"):
            top = int(f.split("_")[1]) * 1000
            results[f] = rustmet.compute_shear(
                data["u_3d"], data["v_3d"], data["height_3d"], nx, ny, nz, 0, top
            ).reshape(ny, nx).astype(np.float32)

    # SRH — rustmet native grid
    for f in fields:
        if f.startswith("srh_"):
            top = int(f.split("_")[1]) * 1000
            results[f] = rustmet.compute_srh(
                data["u_3d"], data["v_3d"], data["height_3d"], nx, ny, nz, top
            ).reshape(ny, nx).astype(np.float32)

    # STP — composed from CAPE + SRH + shear
    if "stp" in fields:
        cape_arr = results.get("sbcape", results.get("sb3cape"))
        if cape_arr is None:
            c, _, lcl_h, _ = mc.compute_cape_cin(p3d, t3d, q3d, h3d, psfc, t2, q2)
            cape_arr = c.magnitude.astype(np.float32)
            lcl_arr = lcl_h.magnitude.flatten()
        else:
            lcl_arr = results.get("lcl", np.zeros((ny, nx), dtype=np.float32)).flatten()

        srh = results.get("srh_01")
        if srh is None:
            srh = rustmet.compute_srh(data["u_3d"], data["v_3d"], data["height_3d"], nx, ny, nz, 1000)
        else:
            srh = srh.flatten()
        shear = results.get("shear_06")
        if shear is None:
            shear = rustmet.compute_shear(data["u_3d"], data["v_3d"], data["height_3d"], nx, ny, nz, 0, 6000)
        else:
            shear = shear.flatten()

        results["stp"] = rustmet.compute_stp(
            cape_arr.flatten().astype(np.float64),
            lcl_arr.flatten().astype(np.float64) if hasattr(lcl_arr, 'astype') else np.array(lcl_arr, dtype=np.float64),
            srh.flatten().astype(np.float64) if hasattr(srh, 'astype') else np.array(srh, dtype=np.float64),
            shear.flatten().astype(np.float64) if hasattr(shear, 'astype') else np.array(shear, dtype=np.float64),
        ).reshape(ny, nx).astype(np.float32)

    return results
