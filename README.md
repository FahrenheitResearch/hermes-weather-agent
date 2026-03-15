# Hermes Weather Agent

MCP tool server that lets AI agents autonomously build weather model training datasets, compute derived meteorological fields, and visualize weather data in the terminal.

## What It Does

An AI agent says **"build me a training dataset from HRRR data with native SB3CAPE"** and the tools handle everything:

1. **Mass downloads** HRRR/GFS/RAP data with selective idx-based fetching (only the fields you need)
2. **Computes derived fields** like SB3CAPE from full 50-level 3D native data — not the model's pre-computed approximation
3. **Renders weather data** to terminal using high-quality ANSI art (radar, model fields, soundings)
4. **Builds normalized datasets** with train/val/test splits ready for ML training

## Why This Matters

### Native 3D CAPE vs Model Output

The CAPE that comes in HRRR output files is computed by the model using internal shortcuts. When you compute it yourself from the full 3D temperature/moisture/pressure fields:

- **Consistent** across model versions (HRRR v3→v4 changed their CAPE computation)
- **Configurable** — mixed-layer, most-unstable, or surface-based with custom integration limits
- **Accurate** — full parcel integration through all 50 native levels, not sigma-level approximations

### Speed

| Operation | MetPy | metrust | Speedup |
|---|---|---|---|
| SBCAPE (full CONUS, 1.9M points × 50 levels) | ~30+ minutes | **2 seconds** | **~900x** |
| GRIB2 parse (700MB file) | ~8s (cfgrib) | **0.7s** (rustmet) | **11x** |
| 1 year hourly HRRR dataset | ~17 days | **2-4 hours** | **~100x** |

## MCP Tools

| Tool | Description |
|---|---|
| `wx_models` | List available models, levels, derived fields |
| `wx_fetch` | Fetch model data for a specific run |
| `wx_compute` | Compute derived fields (CAPE, shear, SRH, STP) from native 3D data |
| `wx_build_dataset` | Build complete ML training dataset with normalization + splits |
| `wx_render_terminal` | Render weather fields as ANSI terminal art |
| `wx_radar_terminal` | Render NEXRAD radar PPI to terminal |
| `wx_sounding_terminal` | Render atmospheric sounding to terminal |

## Quick Start

```bash
pip install rustmet metrust numpy Pillow requests

# List tools
python mcp_server.py --list

# Self-test (fetches real data, computes CAPE, renders to terminal)
python mcp_server.py --test

# Run as MCP server
python mcp_server.py
```

## Example: Agent Builds a Training Dataset

```python
# Agent calls wx_build_dataset:
{
    "model": "hrrr",
    "start_date": "2024-01-01",
    "end_date": "2024-12-31",
    "variables": ["TMP", "UGRD", "VGRD", "SPFH", "HGT"],
    "levels": "13",
    "derived_fields": ["sb3cape", "shear_06", "srh_03"],
    "frequency_hours": 1,
    "lead_time_hours": 1,
    "output_dir": "/data/hrrr_training",
    "workers": 8
}

# Result: 8,760 samples, each (C, 1059, 1799) with:
# - 5 vars × 13 levels = 65 upper-air channels
# - 4 surface channels
# - 3 derived channels (SB3CAPE, 0-6km shear, 0-3km SRH)
# - Per-channel normalization stats
# - 80/10/10 train/val/test split
```

## Built With

- **[rustmet](https://github.com/FahrenheitResearch/rustmet)** — Rust GRIB2 parser with streaming decode and selective download
- **[metrust](https://github.com/FahrenheitResearch/metrust-py)** — Drop-in MetPy replacement in Rust, 150/150 functions, 10-93,000x faster
- **[Hermes Agent](https://github.com/NousResearch/hermes-agent)** — MCP-compatible AI agent framework

## License

MIT
