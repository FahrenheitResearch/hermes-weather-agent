# Hermes Weather Agent

**A standalone Weather MCP server powered by rustwx.**

Hermes Weather Agent gives Hermes Agent, Claude, and any MCP-speaking client direct access to [`rustwx`](https://github.com/FahrenheitResearch/rustwx), a pure-Rust weather workspace. It is meant to be a general weather engine for agents: model maps, satellite imagery, radar products, meteograms, soundings, cross sections, latest-run discovery, local cache management, and severe-weather research tools from one server.

The compute and rendering live in `rustwx`. PNG output goes through the pure-Rust `rustwx-render` contour engine: no matplotlib for maps, no ANSI rendering, and no Python in the hot path. WRF NetCDF4 reads use `netcrust` (pure-Rust, feature-gated) so **no `netcdf.dll` runtime is required for the standard agent workflows.**

## Why this matters

* **One MCP server for weather work.** Agents can request maps, satellite, radar, meteograms, soundings, cross sections, cache status, and background jobs without a custom web app.
* **Latest data by default.** HRRR requests resolve against advertised forecast-hour availability; longer-range HRRR requests use the newest synoptic cycle with the requested hours.
* **Efficient local data use.** rustwx uses `.idx` byte-range fetches where possible, and Hermes exposes data-pack guidance so users can choose how much disk to reserve.
* **Live product discovery.** 100+ products with status/maturity/runner/per-model support metadata are queried from `rustwx product_catalog`; the agent can pick products the model actually supports.
* **Advanced severe-weather research when needed.** ECAPE maps, profile probes, profile sweeps, ratio maps, and severe panels are available as regular tools, not as a separate research-only app.

## What an agent can do with this

```
"Render latest HRRR 2 m temperature and 10 m wind over Los Angeles."
"Show me the latest GOES-West GeoColor image with GLM overlay for southern California."
"Make a point meteogram for Redding for the next 18 hours."
"Render KTLX base reflectivity and dual-pol products."
"Cross-section theta-e from Amarillo to Chicago at f06 using the HRRR VolumeStore."
"What does the latest CONUS QPF look like at f24?"

"Render MLECAPE and 0-3 km SRH over the southern plains for the latest HRRR."
"Probe the ECAPE profile at Norman, OK at the latest f01."
"Build a 7-day dataset of MLECAPE / SRH 0-3 / STP renders for southern-plains."
"What's currently in the cache, and what would a 400 GB eviction free?"
```

## Advanced Research Mode

The general weather tools are the main surface area. On top of that, Hermes Weather Agent includes ECAPE and severe-weather research tools for deeper work:

* full-parcel ECAPE maps at HRRR-grid scale
* per-profile ECAPE diagnostics from the Rust solver
* random/target/stress profile sweeps
* severe-weather panels and ECAPE/CAPE ratio products
* multi-day render/probe datasets for verification workflows

The ECAPE path is strong for exploratory and product-triage research, not yet calibrated forecast skill. Calibration is downstream of the verification work these tools enable.

## Install

```bash
pip install -U rustwx hermes-weather-agent
weather-mcp --doctor
```

That's it for every map-rendering tool — the rustwx PyPI wheel ships the `rustwx-agent-v1` Python API used by this plugin (no Rust toolchain, no separate binaries, no `netcdf.dll`). `rustwx>=0.4.6` is recommended for the current public release.

`weather-mcp --doctor` should report `rustwx_module_available: true`, `agent_api: rustwx-agent-v1`, and a nonzero `domain_count`.

### Optional specialty tools

A small set of tools sit *outside* the agent-v1 contract today and need rustwx workspace binaries to be built locally:

- `wx_sounding` (skew-T renderer)
- `wx_cross_section` / `wx_volume_cross_section` (HRRR pressure VolumeStore renderer; all non-smoke wxsection styles)
- `wx_radar` (NEXRAD Level-II renderer)
- `wx_ecape_profile` (single-point ECAPE probe)
- `wx_ecape_grid` (full-grid ECAPE swath research)

If you want those too:

```bash
git clone https://github.com/FahrenheitResearch/rustwx
cd rustwx
cargo build -p rustwx-cli --release \
  --bin sounding_plot \
  --bin hrrr_pressure_volume_store \
  --bin volume_store_cross_section_render \
  --bin radar_export \
  --bin hrrr_ecape_profile_probe \
  --bin hrrr_ecape_grid_research

export HERMES_RUSTWX_BIN_DIR="$(pwd)/target/release"
```

These will fold into the agent-v1 contract in a future rustwx release; until then, the corresponding MCP tools degrade with a clear "build the binary" error rather than blocking the rest of the plugin.

## Configure

```yaml
# Hermes Agent — ~/.hermes/config.yaml
mcp_servers:
  weather:
    command: weather-mcp
    env:
      HERMES_RUSTWX_BIN_DIR: /path/to/rustwx/target/release
      HERMES_CACHE_DIR: /path/to/weather-cache         # default: ./cache/rustwx
      HERMES_OUT_DIR:   /path/to/weather-outputs       # default: ./outputs
```

```json
// Claude Desktop / generic MCP — claude_desktop_config.json
{
  "mcpServers": {
    "weather": {
      "command": "weather-mcp",
      "env": { "HERMES_RUSTWX_BIN_DIR": "/path/to/rustwx/target/release" }
    }
  }
}
```

## CLI

```bash
weather-mcp --list      # every MCP tool with one-line descriptions
weather-mcp --doctor    # binary discovery + product catalog state
weather-mcp --test      # smoke-test render
```

## Tools (33 total)

### Discovery
| Tool | Purpose |
|---|---|
| `wx_models` | Available models, sources, products, forecast horizons |
| `wx_products` | **Live product catalog** — 100+ entries with status/maturity/runners/per-model support |
| `wx_recipes` | Compact recipe summary (live with fallback mirror) |
| `wx_regions` | rustwx region presets |
| `wx_doctor` | Local install diagnostics |
| `wx_latest` | Resolve the latest available run for a model |
| `wx_data_packs` | HRRR-first local storage tiers: what works without more downloads at 1/5/10/50 GB style budgets |

Hermes is HRRR-first for local operational use. Tool defaults use `run="latest"` and resolve against advertised forecast-hour availability; requests for f019-f048 use the newest HRRR synoptic cycle that actually has those hours. Local data-pack tiers are cache/retention guidance: rustwx uses `.idx` byte-range fetches wherever the requested fields can be safely subset, while larger tiers simply retain more warmed hours, routes, and artifacts.

### Direct & derived rendering
| Tool | Purpose |
|---|---|
| `wx_render_recipe` | Render any combination of direct / derived / windowed / heavy recipes (auto-routes to the right binary) |
| `wx_cape` | SBCAPE / MLCAPE / MUCAPE shortcut |
| `wx_ecape` | First-class ECAPE map (sbecape / mlecape / muecape) |
| `wx_srh` | 0-1 km or 0-3 km SRH |
| `wx_shear` | 0-1 km or 0-6 km bulk shear |
| `wx_stp` | Fixed-layer Significant Tornado Parameter |
| `wx_windowed` | Time-window products: QPF (1/6/12/24h, total) and 2-5 km UH (1h/3h/run-max) |
| `wx_severe_panel` | Multi-product severe + ECAPE plate from one shared heavy thermo load |

### ECAPE specialists
| Tool | Purpose |
|---|---|
| `wx_ecape_profile` | Per-profile ECAPE diagnostics at a (lat, lon) — sub-millisecond Rust solver |
| `wx_ecape_grid` | Full-grid ECAPE research over a swath (background) |
| `wx_ecape_ratio_map` | MLECAPE filled + ECAPE/CAPE ratio contours w/ magnitude mask |

### Vertical / observations
| Tool | Purpose |
|---|---|
| `wx_cross_section` | Compatibility alias for HRRR pressure VolumeStore cross sections |
| `wx_volume_cross_section` | HRRR pressure VolumeStore cross sections; builds a short-lived 1-3 hour local store and renders PNG/WebP for all non-smoke wxsection styles |
| `wx_satellite` | Latest GOES satellite imagery via `rustwx.render_goes_satellite_json`; supports GeoColor, GLM overlay, RGB composites, and ABI Bands 1-16 |
| `wx_meteogram` | Point forecast time series via `rustwx.sample_point_timeseries_json`, or a warmed store when `store_id` is supplied |
| `wx_meteogram_warm_store` | Warm a point-timeseries grid store for repeated meteogram sampling |
| `wx_radar` | Native rustwx NEXRAD Level-II rendering via `radar_export`: base, dual-pol, SRV, VIL, echo tops, and feature JSON |
| `wx_sounding` | Skew-T at (lat, lon) rendered by rustwx's native `sounding_plot` binary; supports `sample_method="box-mean"` with `box_radius_km` |

### Research mode
| Tool | Purpose |
|---|---|
| `wx_research_profile_sweep` | Multi-point ECAPE sweep across (point × date × cycle × fhour). Modes: `targets` / `random` / `stress`. Aggregated CSV with timing breakdown |
| `wx_build_dataset` | Multi-day batch renders or profile probes (background) |

### Cache & jobs
| Tool | Purpose |
|---|---|
| `wx_cache_status` | Disk usage + top consumers + per-subdir totals |
| `wx_cache_evict` | LRU eviction to bring cache below `target_gb` (dry-run by default) |
| `wx_job_status` / `wx_job_list` / `wx_job_cancel` | Background-job control |

## Showcase gallery

The repo ships a one-shot script that exercises every tool against a single canonical run, captures per-call timing, and emits a self-contained HTML gallery:

```bash
python examples/showcase_full.py        # ~3 min with warm cache, ~5 min cold
python examples/showcase_html.py        # builds outputs/showcase/index.html
```

The showcase includes HRRR VolumeStore cross sections, the GOES18 product set, direct point meteograms, warmed point-timeseries store sampling, and native rustwx radar export. Open `outputs/showcase/index.html` after running.

Top wall-time consumers from the canonical run:

```
42.7 s  HRRR derived  — 44 recipes / one shared decode    72 PNGs
41.3 s  HRRR severe panel (heavy_panel_hour)              24 PNGs
35.1 s  ECAPE/CAPE ratio display (background)              6 PNGs
 8.0 s  mini research dataset (1 cycle × 2 recipes)
 7.9 s  HRRR windowed — 8 QPF/UH products                  6 PNGs
 6.6 s  HRRR direct — 52 recipes / one shared decode      72 PNGs
 6.6 s  ECAPE grid research (background)
 5.9 s  stress profile sweep (10 curated points)
 5.1 s  GFS derived (sbcape/mucape/lapse)                  6 PNGs
 4.7 s ×5  cross sections (4 routes + 1 city pair)
```

The HRRR direct + derived passes each render dozens of products from one shared thermodynamic decode — that's where the per-product cost approaches zero.

## Programmatic use (without an LLM)

Every tool is also a plain Python function:

```python
from hermes_weather.rustwx import discover
from hermes_weather.tools import (
    render, ecape, volume_cross_section,
    satellite, meteogram, radar, catalog,
)

env = discover()

# Single first-class ECAPE map
out = render.ecape(env, parcel="ml", region="southern-plains",
                   run_str="latest", forecast_hour=0)
print(out["pngs"])

# Per-profile ECAPE in <1ms (Rust kernel)
prof = ecape.profile(env, location="Norman, OK",
                     run_str="latest", forecast_hour=1,
                     include_input_column=True)
print(prof["diagnostics"]["parcels"][1]["ratio_ecape_to_undiluted_cape"])

# HRRR VolumeStore cross sections: temporary 1-hour SoCal cube,
# all non-smoke wxsection styles, PNG + WebP outputs.
fast_xs = volume_cross_section.volume_cross_section(
    env, products=["all"], route="socal-coast-desert",
    run_str="latest", forecast_hour=0,
)

# GOES satellite set, short point meteogram, and native radar export.
sat = satellite.satellite(env, products=satellite.DEFAULT_PRODUCTS)
met = meteogram.meteogram(env, location=(34.0522, -118.2437),
                          forecast_hour_start=0, forecast_hour_end=3)
rad = radar.radar(env, site="KTLX", products="all")

# Discover what every recipe is and where it works
products = catalog.products(env, kind="derived", search="ecape")
for p in products["products"]:
    supported_models = [s["model"] for s in p["support"] if s["status"] == "supported"]
    print(f"{p['slug']:35s} {p['maturity']:14s} {supported_models}")
```

## Speed reality check (ECAPE-RS validation paper, April 2026)

| Operation | Python (`ecape-parcel` + MetPy) | Rust (`rustwx-calc`) | Speedup |
|---|---|---|---|
| ECAPE per HRRR profile (one parcel/config) | 2.8–6.1 s | 0.45–0.65 ms | **5,600–13,000×** |
| ECAPE full-grid (272,955 HRRR cells / forecast hour) | impractical | 17.6–18.4 s | — |

Per-profile ECAPE is essentially free; full-grid is the heavy one. Heavy tools (`wx_ecape_grid`, `wx_ecape_ratio_map` for CONUS, `wx_research_profile_sweep` over many cycles) run as background jobs. Profile-sweep results separate `raw_fetch_s` / `profile_extract_s` / `parcel_solver_s` / `render_s` so you can attribute time honestly.

## License

MIT.

## Acknowledgements

Built on top of [`rustwx`](https://github.com/FahrenheitResearch/rustwx) (Rust meteorology workspace, Fahrenheit Research) and the public `ecape-parcel` reference implementation. Validation methodology described in *Validation and Acceleration of an ecape-parcel-Compatible Solver for HRRR-Scale Entraining-CAPE Diagnostics*, ECAPE-RS Project, April 2026.
