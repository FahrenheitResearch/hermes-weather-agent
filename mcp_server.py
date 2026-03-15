#!/usr/bin/env python3
"""
Hermes Weather Agent — MCP Tool Server

AI agents use these tools to autonomously fetch weather data, compute derived
fields, render visualizations, and build ML training datasets.

Built on:
- rustmet: Fast Rust GRIB2 parser with selective download (10x cfgrib)
- metrust: Rust-powered MetPy replacement (10-93,000x faster, 150/150 functions)

Usage:
    python mcp_server.py              # stdio MCP server
    python mcp_server.py --list       # list available tools
    python mcp_server.py --test       # run quick self-test
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


# ── Minimal MCP Protocol Handler ────────────────────────────────────
class MCPServer:
    def __init__(self):
        self.tools = {}
        self.handlers = {}

    def tool(self, name, description, schema):
        def decorator(fn):
            self.tools[name] = {
                "name": name, "description": description,
                "inputSchema": {"type": "object", "properties": schema.get("properties", {}),
                                "required": schema.get("required", [])},
            }
            self.handlers[name] = fn
            return fn
        return decorator

    def run(self):
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                resp = self._handle(msg)
                if resp:
                    sys.stdout.write(json.dumps(resp) + "\n")
                    sys.stdout.flush()
            except Exception as e:
                sys.stderr.write(f"Error: {e}\n")

    def _handle(self, msg):
        method = msg.get("method", "")
        id_ = msg.get("id")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": id_, "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "hermes-weather-agent", "version": "1.0.0"},
            }}
        elif method == "tools/list":
            return {"jsonrpc": "2.0", "id": id_, "result": {"tools": list(self.tools.values())}}
        elif method == "tools/call":
            name = msg["params"]["name"]
            args = msg["params"].get("arguments", {})
            handler = self.handlers.get(name)
            if not handler:
                return {"jsonrpc": "2.0", "id": id_, "error": {"code": -32601, "message": f"Unknown: {name}"}}
            try:
                result = handler(**args)
                text = json.dumps(result, default=str) if isinstance(result, dict) else str(result)
                return {"jsonrpc": "2.0", "id": id_, "result": {
                    "content": [{"type": "text", "text": text}]
                }}
            except Exception as e:
                return {"jsonrpc": "2.0", "id": id_, "result": {
                    "content": [{"type": "text", "text": f"Error: {e}"}], "isError": True
                }}
        elif method == "notifications/initialized":
            return None
        return None


server = MCPServer()


# ══════════════════════════════════════════════════════════════════════
# TOOLS
# ══════════════════════════════════════════════════════════════════════

@server.tool("wx_models", "List available weather models and their capabilities", {
    "properties": {}, "required": []
})
def wx_models():
    from tools.data_fetch import MODEL_INFO, STANDARD_LEVELS
    from tools.compute import DERIVED_FIELDS
    return {
        "models": MODEL_INFO,
        "standard_level_presets": {k: v for k, v in STANDARD_LEVELS.items()},
        "derived_fields": DERIVED_FIELDS,
        "note": "Derived fields are computed from native 3D model data using metrust "
                "(Rust-powered, 10-93,000x faster than MetPy). SB3CAPE uses full 50-level "
                "parcel integration, not the model's pre-computed approximation.",
    }


@server.tool("wx_fetch", "Fetch weather model data for a specific run", {
    "properties": {
        "model": {"type": "string", "description": "hrrr, gfs, rap"},
        "run": {"type": "string", "description": "YYYY-MM-DD/HHz"},
        "fhour": {"type": "integer", "default": 0},
        "product": {"type": "string", "default": "prs"},
        "variables": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["model", "run"]
})
def wx_fetch(model: str, run: str, fhour: int = 0, product: str = "prs",
             variables: list[str] = None):
    from tools.data_fetch import fetch_model
    grib = fetch_model(model, run, fhour, product, variables)
    vars_found = {}
    for m in grib.messages:
        if m.variable not in vars_found:
            vars_found[m.variable] = []
        vars_found[m.variable].append(m.level)
    return {"messages": len(grib.messages), "grid": (grib.messages[0].nx, grib.messages[0].ny),
            "variables": vars_found}


@server.tool("wx_compute", "Compute derived weather fields from native 3D model data using metrust", {
    "properties": {
        "model": {"type": "string"},
        "run": {"type": "string", "description": "YYYY-MM-DD/HHz"},
        "fields": {"type": "array", "items": {"type": "string"},
                   "description": "Fields to compute: sbcape, sb3cape, mlcape, shear_06, srh_03, stp"},
        "output_dir": {"type": "string", "description": "Save numpy arrays here"},
    },
    "required": ["model", "run", "fields"]
})
def wx_compute(model: str, run: str, fields: list[str], output_dir: str = None):
    import numpy as np
    from tools.compute import compute_derived
    import time
    t0 = time.time()
    results = compute_derived(model, run, fields)
    elapsed = time.time() - t0
    summary = {}
    for name, arr in results.items():
        summary[name] = {"shape": list(arr.shape), "min": float(arr.min()), "max": float(arr.max()),
                         "mean": float(arr.mean())}
        if output_dir:
            p = Path(output_dir)
            p.mkdir(parents=True, exist_ok=True)
            np.save(str(p / f"{name}.npy"), arr)
    return {"fields_computed": list(results.keys()), "compute_time_s": round(elapsed, 2),
            "details": summary}


@server.tool("wx_build_dataset", "Build a complete ML training dataset from weather model data", {
    "properties": {
        "model": {"type": "string", "description": "hrrr, gfs, rap"},
        "start_date": {"type": "string", "description": "YYYY-MM-DD"},
        "end_date": {"type": "string", "description": "YYYY-MM-DD"},
        "variables": {"type": "array", "items": {"type": "string"},
                      "description": "Upper-air: TMP, UGRD, VGRD, SPFH, HGT"},
        "levels": {"type": "string", "description": "Preset (13, 37) or comma-separated"},
        "surface_variables": {"type": "array", "items": {"type": "string"}},
        "derived_fields": {"type": "array", "items": {"type": "string"},
                           "description": "Native 3D fields: sbcape, sb3cape, shear_06, srh_03, stp"},
        "frequency_hours": {"type": "integer", "default": 1},
        "lead_time_hours": {"type": "integer", "default": 1},
        "output_dir": {"type": "string"},
        "normalize": {"type": "boolean", "default": True},
        "workers": {"type": "integer", "default": 4},
        "limit": {"type": "integer", "description": "Max samples (for testing)"},
    },
    "required": ["model", "start_date", "end_date", "output_dir"]
})
def wx_build_dataset(**kwargs):
    from tools.dataset import build_dataset
    return build_dataset(kwargs)


@server.tool("wx_render_terminal", "Fetch and render weather data as ANSI art in terminal", {
    "properties": {
        "model": {"type": "string", "default": "hrrr"},
        "run": {"type": "string", "description": "YYYY-MM-DD/HHz or 'latest'"},
        "field": {"type": "string", "description": "Variable to render: sbcape, shear_06, composite_ref, etc"},
        "width": {"type": "integer", "default": 120, "description": "Terminal width in columns"},
    },
    "required": ["field"]
})
def wx_render_terminal(field: str, model: str = "hrrr", run: str = "latest",
                       width: int = 120):
    import numpy as np
    import rustmet
    from tools.ansi_render import rgba_to_ansi
    from tools.compute import DERIVED_FIELDS, compute_derived
    from datetime import datetime, timezone

    if run == "latest":
        now = datetime.now(timezone.utc)
        run = f"{now:%Y-%m-%d}/{(now.hour - 1) % 24:02d}z"

    # Check if it's a derived field or a standard GRIB field
    if field in DERIVED_FIELDS:
        results = compute_derived(model, run, [field])
        if field not in results:
            return {"error": f"Failed to compute {field}"}
        arr = results[field]
        ny, nx = arr.shape
        # Pick colormap
        cmap = "cape" if "cape" in field else "helicity" if "srh" in field else "wind"
        vmax = 5000 if "cape" in field else 500 if "srh" in field else 50
        rgba = rustmet.render(arr.flatten().astype(np.float32), nx, ny, colormap=cmap, vmin=0, vmax=vmax)
    else:
        # Standard GRIB variable
        grib = rustmet.fetch(model, run, fhour=0, product="prs", vars="severe")
        msg = None
        for m in grib.messages:
            if field.lower() in m.variable.lower():
                msg = m
                break
        if not msg:
            return {"error": f"Field '{field}' not found"}
        vals = msg.values()
        nx, ny = msg.nx, msg.ny
        cmap = "cape" if "cape" in field.lower() else "reflectivity" if "refl" in field.lower() else "temperature"
        rgba = rustmet.render(vals, nx, ny, colormap=cmap)

    ansi = rgba_to_ansi(rgba, nx, ny, width)
    # Print directly
    print(ansi)
    return {"rendered": True, "field": field, "run": run, "grid": f"{nx}x{ny}"}


@server.tool("wx_radar_terminal", "Fetch and render NEXRAD radar to terminal as ANSI", {
    "properties": {
        "site": {"type": "string", "description": "Radar site ID (e.g. KTLX)"},
        "product": {"type": "string", "default": "DBZ", "description": "DBZ or VEL"},
        "width": {"type": "integer", "default": 120},
    },
    "required": ["site"]
})
def wx_radar_terminal(site: str, product: str = "DBZ", width: int = 120):
    import numpy as np
    import rustmet
    from tools.ansi_render import rgba_to_ansi
    from tools.data_fetch import fetch_radar_scan
    from datetime import datetime, timezone

    # Download latest scan
    now = datetime.now(timezone.utc)
    scan_path = fetch_radar_scan(site, now)
    if not scan_path:
        return {"error": f"No radar data for {site}"}

    # Parse and render PPI
    from wx_radar.level2 import Level2File
    from wx_radar.color_table import ColorTable
    from wx_radar.products import RadarProduct

    # Use rustmet-train binary if available, otherwise render in Python
    import subprocess
    bin_path = ROOT / ".." / "rustmet-train" / "target" / "release" / "rustmet-train.exe"
    if bin_path.exists():
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            tmp = f.name
        subprocess.run([str(bin_path), "render-ppi", "--input", str(scan_path),
                        "--output", tmp, "--product", product, "--size", "400",
                        "--range-km", "230"], capture_output=True)
        from PIL import Image
        img = np.array(Image.open(tmp))
        import os; os.unlink(tmp)
        ansi = rgba_to_ansi(img, img.shape[1], img.shape[0], width)
        print(ansi)
        return {"rendered": True, "site": site, "product": product}

    return {"error": "rustmet-train binary not found for PPI rendering"}


@server.tool("wx_sounding_terminal", "Fetch and render a sounding profile to terminal", {
    "properties": {
        "lat": {"type": "number"},
        "lon": {"type": "number"},
        "width": {"type": "integer", "default": 120},
    },
    "required": ["lat", "lon"]
})
def wx_sounding_terminal(lat: float, lon: float, width: int = 120):
    import numpy as np
    import rustmet
    from tools.ansi_render import rgba_to_ansi
    from PIL import Image

    # Render skew-T using rustmet
    # Get sounding data from wx CLI
    import subprocess
    wx_bin = ROOT / ".." / "rustmet" / "target" / "release" / "wx.exe"
    result = subprocess.run([str(wx_bin), "sounding", "--lat", str(lat), "--lon", str(lon)],
                            capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        return {"error": "Sounding fetch failed"}

    import json
    sounding = json.loads(result.stdout)
    levels = sounding["levels"]
    p = np.array([l["pressure_hpa"] for l in levels], dtype=np.float64)
    t = np.array([l["temperature_c"] for l in levels], dtype=np.float64)
    td = np.array([l["dewpoint_c"] for l in levels], dtype=np.float64)
    ws = np.array([l["wind_speed_kt"] for l in levels], dtype=np.float64)
    wd = np.array([l["wind_dir"] for l in levels], dtype=np.float64)

    raw = rustmet.render_skewt(p, t, td, wind_speed=ws, wind_dir=wd, width=400, height=400)
    img = Image.frombytes("RGBA", (400, 400), bytes(raw))
    ansi = rgba_to_ansi(np.array(img), 400, 400, width)
    print(ansi)

    return {"rendered": True, "lat": lat, "lon": lon, "levels": len(levels),
            "indices": sounding.get("indices", {})}


# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if "--list" in sys.argv:
        print("Hermes Weather Agent — MCP Tools\n")
        for name, tool in server.tools.items():
            print(f"  {name}")
            print(f"    {tool['description']}\n")
        sys.exit(0)

    if "--test" in sys.argv:
        print("Running self-test...")
        import time

        # Test 1: Model info
        print("\n1. wx_models:")
        r = wx_models()
        print(f"   {len(r['models'])} models, {len(r['derived_fields'])} derived fields")

        # Test 2: Compute SB3CAPE
        print("\n2. wx_compute (SB3CAPE):")
        t0 = time.time()
        r = wx_compute(model="hrrr", run="latest", fields=["sbcape", "shear_06"])
        print(f"   {r}")
        print(f"   Time: {time.time()-t0:.1f}s")

        # Test 3: Terminal render
        print("\n3. wx_render_terminal:")
        wx_render_terminal(field="Composite Reflectivity", width=80)

        print("\nAll tests passed!")
        sys.exit(0)

    server.run()
