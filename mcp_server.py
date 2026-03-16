#!/usr/bin/env python3
"""
Weather Training MCP Server — plug into Hermes Agent for AI-driven weather model training.

Exposes tools for fetching weather data, computing derived fields (SB3CAPE, shear, SRH)
from native 3D model data using metrust, rendering to terminal, and building ML datasets.

Install: pip install rustmet metrust mcp numpy Pillow requests
Run:     python mcp_server.py

Configure in ~/.hermes/config.yaml:
    mcp_servers:
      weather:
        command: "python"
        args: ["path/to/mcp_server.py"]
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
import mcp.types as types

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

server = Server("weather-training")


def _resolve_latest(model: str = "hrrr") -> str:
    """Find the actual latest available model run by probing NOMADS.

    Walks the NOMADS directory listing for today and yesterday, finds the
    latest date directory that exists, then picks the latest run hour
    within that directory.  Returns ``YYYY-MM-DD/HHz``.
    """
    import re
    import requests as req
    from datetime import timedelta

    base = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod"
    now = datetime.now(timezone.utc)

    # 1. Find the latest date directory — try today then yesterday
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

        # 2. Find every run hour advertised in that directory (any product)
        hours = sorted(set(re.findall(r'hrrr\.t(\d{2})z\.wrf(?:sfc|prs|nat)f\d{2}\.grib2', r.text)))
        if hours:
            latest_hour = hours[-1]
            return f"{date:%Y-%m-%d}/{latest_hour}z"

    # Fallback — guess ~3 hours behind wallclock
    fb = now - timedelta(hours=3)
    return f"{fb:%Y-%m-%d}/{fb.hour:02d}z"


# ══════════════════════════════════════════════════════════════════════
# TOOLS
# ══════════════════════════════════════════════════════════════════════

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="wx_models",
            description="List available weather models, standard level presets, and derived fields that can be computed from native 3D data using metrust",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_fetch",
            description="Fetch weather model data (HRRR/GFS/RAP) for a specific run time. Returns available variables and grid info.",
            inputSchema={"type": "object", "properties": {
                "model": {"type": "string", "description": "hrrr, gfs, or rap"},
                "run": {"type": "string", "description": "Run time as YYYY-MM-DD/HHz (e.g. 2026-03-15/12z)"},
                "fhour": {"type": "integer", "description": "Forecast hour", "default": 0},
                "variables": {"type": "array", "items": {"type": "string"}, "description": "Variable filter list"},
            }, "required": ["model", "run"]},
        ),
        Tool(
            name="wx_compute",
            description="Compute derived meteorological fields from native 3D model data using metrust (Rust, 10-93000x faster than MetPy). SB3CAPE uses full 50-level parcel integration. Available: sbcape, sb3cape, mlcape, shear_01/03/06, srh_01/03, stp",
            inputSchema={"type": "object", "properties": {
                "model": {"type": "string", "description": "Model name"},
                "run": {"type": "string", "description": "YYYY-MM-DD/HHz"},
                "fields": {"type": "array", "items": {"type": "string"}, "description": "Fields: sbcape, sb3cape, mlcape, shear_06, srh_03, stp"},
                "output_dir": {"type": "string", "description": "Save numpy arrays here"},
            }, "required": ["model", "run", "fields"]},
        ),
        Tool(
            name="wx_build_dataset",
            description="Build a complete ML training dataset in the background. Downloads HRRR data, extracts fields, computes derived fields from native 3D data, normalizes, splits. Runs in background — use wx_build_status to check progress. Use wx_train to start training once complete.",
            inputSchema={"type": "object", "properties": {
                "model": {"type": "string", "description": "hrrr, gfs, or rap"},
                "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                "end_date": {"type": "string", "description": "YYYY-MM-DD"},
                "variables": {"type": "array", "items": {"type": "string"}, "description": "Upper-air vars: TMP, UGRD, VGRD, SPFH, HGT"},
                "levels": {"type": "string", "description": "Preset '13' or '37', or comma-separated pressure levels"},
                "derived_fields": {"type": "array", "items": {"type": "string"}, "description": "Native 3D computed: sbcape, sb3cape, shear_06, srh_03, stp"},
                "frequency_hours": {"type": "integer", "default": 1},
                "lead_time_hours": {"type": "integer", "default": 1},
                "output_dir": {"type": "string"},
                "workers": {"type": "integer", "default": 4},
                "limit": {"type": "integer", "description": "Max samples for testing"},
            }, "required": ["model", "start_date", "end_date", "output_dir"]},
        ),
        Tool(
            name="wx_render_terminal",
            description="Fetch and render a weather field as ANSI art. Use exact variable names from rustmet.\n\nPre-computed fields (fast, from GRIB): 'Convective Available Potential Energy', 'Convective Inhibition', 'Composite Reflectivity', 'Storm Relative Helicity', 'Temperature', 'Dewpoint Temperature', 'Wind Speed (Gust)', 'Visibility', 'Precipitable Water', 'MSLP (MAPS System Reduction)'\n\nDerived fields (computed from native 3D data via metrust, slower but more accurate): 'sbcape', 'sb3cape', 'mlcape', 'shear_01', 'shear_03', 'shear_06', 'srh_01', 'srh_03', 'stp'\n\nFor pre-computed fields, also specify level. Common levels: '0 Ground or Water Surface' (for CAPE/CIN), '2 Specified Height Level Above Ground' (for 2m temp), '10 Specified Height Level Above Ground' (for 10m wind), '0 Entire Atmosphere' (for composite reflectivity).",
            inputSchema={"type": "object", "properties": {
                "variable": {"type": "string", "description": "Exact variable name from the list above"},
                "level": {"type": "string", "description": "Exact level string, e.g. '0 Ground or Water Surface'", "default": ""},
                "model": {"type": "string", "default": "hrrr"},
                "run": {"type": "string", "description": "YYYY-MM-DD/HHz or 'latest'", "default": "latest"},
                "width": {"type": "integer", "default": 120},
                "lat": {"type": "number", "description": "Center latitude for regional crop"},
                "lon": {"type": "number", "description": "Center longitude for regional crop"},
                "radius_km": {"type": "number", "description": "Crop radius in km (default 500)", "default": 500},
            }, "required": ["variable"]},
        ),
        Tool(
            name="wx_radar_terminal",
            description="Fetch latest NEXRAD radar scan and render PPI as ANSI art in terminal",
            inputSchema={"type": "object", "properties": {
                "site": {"type": "string", "description": "Radar site ID (e.g. KTLX, KBMX)"},
                "product": {"type": "string", "default": "DBZ"},
                "width": {"type": "integer", "default": 120},
            }, "required": ["site"]},
        ),
        Tool(
            name="wx_sounding_terminal",
            description="Fetch atmospheric sounding at a location and render skew-T diagram as ANSI art",
            inputSchema={"type": "object", "properties": {
                "lat": {"type": "number"},
                "lon": {"type": "number"},
                "width": {"type": "integer", "default": 120},
            }, "required": ["lat", "lon"]},
        ),
        Tool(
            name="wx_build_status",
            description="Check status of background dataset build",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_train",
            description="Start training a UNet diffusion weather model in the background. Runs on GPU if available. Prints epoch progress with loss sparkline to terminal. You can continue using other tools while training runs.",
            inputSchema={"type": "object", "properties": {
                "dataset_dir": {"type": "string", "description": "Path to dataset built by wx_build_dataset"},
                "output_dir": {"type": "string", "description": "Where to save model checkpoints", "default": "./model_output"},
                "epochs": {"type": "integer", "default": 50},
                "lr": {"type": "number", "default": 0.001},
                "batch_size": {"type": "integer", "default": 4},
            }, "required": ["dataset_dir"]},
        ),
        Tool(
            name="wx_train_status",
            description="Check the status of background training (epoch, loss, best loss)",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="wx_train_stop",
            description="Stop background training",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        result = _dispatch(name, arguments)
        text = json.dumps(result, default=str) if isinstance(result, dict) else str(result)
        return [TextContent(type="text", text=text)]
    except Exception as e:
        return [TextContent(type="text", text=f"Error: {e}")]


def _dispatch(name: str, args: dict):
    if name == "wx_models":
        from tools.data_fetch import MODEL_INFO, STANDARD_LEVELS
        from tools.compute import DERIVED_FIELDS
        return {"models": MODEL_INFO, "standard_levels": STANDARD_LEVELS,
                "derived_fields": DERIVED_FIELDS,
                "note": "Derived fields computed from native 3D data using metrust (Rust). "
                        "SB3CAPE: full 50-level parcel integration, 2s for full CONUS vs 30+min MetPy."}

    elif name == "wx_fetch":
        from tools.data_fetch import fetch_model
        run = _resolve_latest(args["model"]) if args["run"] == "latest" else args["run"]
        grib = fetch_model(args["model"], run, args.get("fhour", 0), "prs", args.get("variables"))
        vars_found = {}
        for m in grib.messages:
            if m.variable not in vars_found: vars_found[m.variable] = []
            vars_found[m.variable].append(m.level)
        return {"messages": len(grib.messages), "grid": [grib.messages[0].nx, grib.messages[0].ny],
                "variables": vars_found}

    elif name == "wx_compute":
        import numpy as np
        from tools.compute import compute_derived
        run = _resolve_latest(args["model"]) if args["run"] == "latest" else args["run"]
        t0 = time.time()
        results = compute_derived(args["model"], run, args["fields"], args.get("fhour", 0))
        summary = {}
        for k, arr in results.items():
            summary[k] = {"shape": list(arr.shape), "min": float(np.nanmin(arr)),
                          "max": float(np.nanmax(arr)), "mean": float(np.nanmean(arr))}
            if args.get("output_dir"):
                p = Path(args["output_dir"]); p.mkdir(parents=True, exist_ok=True)
                np.save(str(p / f"{k}.npy"), arr)
        return {"fields": list(results.keys()), "time_s": round(time.time() - t0, 2), "details": summary}

    elif name == "wx_build_dataset":
        from tools.dataset import build_dataset_background
        return build_dataset_background(args)

    elif name == "wx_build_status":
        from tools.dataset import get_build_status
        return get_build_status()

    elif name == "wx_render_terminal":
        import os
        import numpy as np, rustmet
        from tools.ansi_render import rgba_to_ansi
        from tools.compute import DERIVED_FIELDS, compute_derived
        from PIL import Image as PILImage

        variable = args["variable"]; model = args.get("model", "hrrr")
        level = args.get("level", ""); run = args.get("run", "latest")
        width = args.get("width", 120)
        if run == "latest":
            run = _resolve_latest(model)

        _dn = open(os.devnull, 'w'); _old = os.dup(2); os.dup2(_dn.fileno(), 2)
        try:
            if variable in DERIVED_FIELDS:
                # Derived field — metrust native 3D computation
                results = compute_derived(model, run, [variable])
                if variable not in results:
                    return {"error": f"Failed to compute {variable}"}
                arr = results[variable]; ny, nx = arr.shape
                cmap = "cape" if "cape" in variable else "helicity" if "srh" in variable else "wind"
                vmax = 5000 if "cape" in variable else 500 if "srh" in variable else 50
                rgba = rustmet.render(arr.flatten().astype(np.float32), nx, ny,
                                      colormap=cmap, vmin=0, vmax=vmax)
                extra = {"max": float(np.nanmax(arr)), "mean": float(np.nanmean(arr)),
                         "source": "metrust native 3D computation"}
            else:
                # Pre-computed GRIB field — exact match by variable name + level
                grib = rustmet.fetch(model, run, fhour=0, product="sfc")
                # Also try severe product
                try:
                    grib2 = rustmet.fetch(model, run, fhour=0, product="prs", vars="severe")
                    all_msgs = list(grib.messages) + list(grib2.messages)
                except Exception:
                    all_msgs = list(grib.messages)

                msg = None
                for m in all_msgs:
                    if m.variable == variable:
                        if not level or (hasattr(m, 'level') and level in m.level):
                            if len(m.values()) > 0:
                                msg = m; break

                if not msg:
                    avail = sorted(set(f"{m.variable} @ {m.level}" for m in all_msgs if len(m.values()) > 0))
                    return {"error": f"'{variable}' @ '{level}' not found. Available:\n" + "\n".join(avail)}

                vals = msg.values(); nx, ny = msg.nx, msg.ny
                mv = msg.variable.lower()
                cmap = ("cape" if "convective" in mv else "reflectivity" if "refl" in mv
                        else "dewpoint" if "dewpoint" in mv else "helicity" if "helic" in mv
                        else "wind" if "wind" in mv else "temperature")
                vmin = 0 if cmap in ("cape","helicity","reflectivity") else None
                vmax = 5000 if cmap == "cape" else 500 if cmap == "helicity" else None
                rgba = rustmet.render(vals, nx, ny, colormap=cmap, vmin=vmin, vmax=vmax)
                extra = {"matched": msg.variable, "level": msg.level, "source": "GRIB pre-computed"}
        finally:
            os.dup2(_old, 2); os.close(_old); _dn.close()

        img = PILImage.fromarray(rgba.reshape(ny, nx, 4) if rgba.ndim != 3 else rgba)
        img = img.transpose(PILImage.FLIP_TOP_BOTTOM)

        # Regional crop if lat/lon specified
        crop_lat = args.get("lat")
        crop_lon = args.get("lon")
        if crop_lat is not None and crop_lon is not None:
            radius_km = args.get("radius_km", 500)
            # Approximate pixel crop from lat/lon
            # HRRR grid: ~3km resolution, ~21-53 lat, ~-134 to -60 lon (as 226-300 in 0-360)
            lat_range = 53 - 21  # degrees
            lon_range = 74  # degrees
            km_per_px_y = (lat_range * 111) / ny
            km_per_px_x = (lon_range * 85) / nx  # rough cos(37) factor
            crop_px_y = int(radius_km / km_per_px_y)
            crop_px_x = int(radius_km / km_per_px_x)
            # Convert lat/lon to pixel
            cy = int((53 - crop_lat) / lat_range * ny)
            cx = int((crop_lon + 134) / lon_range * nx)
            x1 = max(0, cx - crop_px_x)
            x2 = min(nx, cx + crop_px_x)
            y1 = max(0, cy - crop_px_y)
            y2 = min(ny, cy + crop_px_y)
            img = img.crop((x1, y1, x2, y2))

        ansi = rgba_to_ansi(np.array(img), img.width, img.height, width)
        # Print directly — never return ANSI in the result (it's 500KB+ of escape codes)
        sys.stderr.write(ansi + "\n")
        sys.stderr.flush()
        return {"rendered": True, "variable": variable, "run": run, **extra}

    elif name == "wx_radar_terminal":
        import numpy as np, subprocess
        from tools.ansi_render import rgba_to_ansi
        from tools.data_fetch import fetch_radar_scan
        scan = fetch_radar_scan(args["site"], datetime.now(timezone.utc))
        if not scan: return {"error": f"No radar for {args['site']}"}
        # Find rustmet-train or wx-pro binary
        import shutil
        bin_path = None
        for candidate in [
            ROOT / ".." / "rustmet-train" / "target" / "release" / "rustmet-train.exe",
            ROOT / ".." / "rustmet-train" / "target" / "release" / "rustmet-train",
            Path("/tmp/rustmet/target/release/wx-pro"),
            shutil.which("wx-pro"),
        ]:
            if candidate and Path(str(candidate)).exists():
                bin_path = Path(str(candidate)); break
        if not bin_path: return {"error": "No radar rendering binary found (need wx-pro or rustmet-train)"}
        # Use wx-pro radar-image with ANSI output
        wx_pro = None
        for candidate in [
            ROOT / ".." / "rustmet" / "target" / "release" / "wx-pro.exe",
            ROOT / ".." / "rustmet" / "target" / "release" / "wx-pro",
            Path("/tmp/rustmet/target/release/wx-pro"),
            shutil.which("wx-pro"),
        ]:
            if candidate and Path(str(candidate)).exists():
                wx_pro = str(candidate); break
        if not wx_pro: return {"error": "wx-pro binary not found"}

        result = subprocess.run(
            [wx_pro, "radar-image", "--site", args["site"],
             "--product", args.get("product", "ref").lower(),
             "--ansi", "--ansi-mode", "block", "--ansi-width", str(args.get("width", 120))],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=60)
        ansi = result.stdout.decode("utf-8", errors="replace")
        if ansi.strip():
            sys.stderr.write(ansi + "\n")
            sys.stderr.flush()
            return {"rendered": True, "site": args["site"]}
        return {"error": f"Radar render failed for {args['site']}"}

    elif name == "wx_sounding_terminal":
        import numpy as np, rustmet, subprocess, json as j
        from tools.ansi_render import rgba_to_ansi
        from PIL import Image
        import shutil
        wx = None
        for candidate in [
            ROOT / ".." / "rustmet" / "target" / "release" / "wx.exe",
            ROOT / ".." / "rustmet" / "target" / "release" / "wx",
            Path("/tmp/rustmet/target/release/wx"),
            shutil.which("wx"),
        ]:
            if candidate and Path(str(candidate)).exists():
                wx = str(candidate); break
        if not wx: return {"error": "wx binary not found"}
        r = subprocess.run([wx, "sounding", "--lat", str(args["lat"]), "--lon", str(args["lon"])],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=30)
        if r.returncode != 0: return {"error": "Sounding failed"}
        s = j.loads(r.stdout); levels = s["levels"]
        p = np.array([l["pressure_hpa"] for l in levels], dtype=np.float64)
        t = np.array([l["temperature_c"] for l in levels], dtype=np.float64)
        td = np.array([l["dewpoint_c"] for l in levels], dtype=np.float64)
        ws = np.array([l["wind_speed_kt"] for l in levels], dtype=np.float64)
        wd = np.array([l["wind_dir"] for l in levels], dtype=np.float64)
        raw = rustmet.render_skewt(p, t, td, wind_speed=ws, wind_dir=wd, width=400, height=400)
        img = Image.frombytes("RGBA", (400, 400), bytes(raw))
        ansi = rgba_to_ansi(np.array(img), 400, 400, args.get("width", 120))
        sys.stderr.write(ansi + "\n")
        sys.stderr.flush()
        return {"rendered": True, "indices": s.get("indices", {})}

    elif name == "wx_train":
        from tools.trainer import start_training
        return start_training(
            dataset_dir=args["dataset_dir"],
            output_dir=args.get("output_dir", "./model_output"),
            epochs=args.get("epochs", 50),
            lr=args.get("lr", 1e-3),
            batch_size=args.get("batch_size", 4),
        )

    elif name == "wx_train_status":
        from tools.trainer import get_training_status
        return get_training_status()

    elif name == "wx_train_stop":
        from tools.trainer import stop_training
        return stop_training()

    return {"error": f"Unknown tool: {name}"}


async def main():
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    if "--list" in sys.argv:
        import asyncio
        tools = asyncio.run(list_tools())
        print("Weather Training MCP Tools:\n")
        for t in tools:
            print(f"  {t.name}")
            print(f"    {t.description}\n")
    elif "--test" in sys.argv:
        print("Testing wx_compute (SB3CAPE)...")
        r = _dispatch("wx_compute", {"model": "hrrr", "run": "latest", "fields": ["sbcape"]})
        print(json.dumps(r, indent=2, default=str))
    else:
        import asyncio
        asyncio.run(main())


def run_cli():
    """Entry point for weather-mcp console script."""
    if "--list" in sys.argv:
        import asyncio
        tools = asyncio.run(list_tools())
        print("Weather Training MCP Tools:\n")
        for t in tools:
            print(f"  {t.name}")
            print(f"    {t.description}\n")
    elif "--test" in sys.argv:
        print("Testing wx_compute (SBCAPE)...")
        r = _dispatch("wx_compute", {"model": "hrrr", "run": "latest", "fields": ["sbcape"]})
        print(json.dumps(r, indent=2, default=str))
    else:
        import asyncio
        asyncio.run(main())
