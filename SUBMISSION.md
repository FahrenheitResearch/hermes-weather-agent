# Hermes Weather Agent — Hackathon Submission

## Tweet (280 chars)

Hermes Weather Agent: 7 MCP tools that let an AI agent autonomously build weather model training datasets. Rust-powered GRIB2 parsing (10x cfgrib) + derived field computation (900x MetPy). Full 50-level SB3CAPE in 2s. Terminal ANSI weather viz. @NousResearch #HermesHackathon

## Video Demo Description

The demo shows the Hermes Weather Agent in action: an AI agent receives the prompt "build me a 1-year HRRR dataset with native CAPE and shear," then autonomously downloads HRRR data, computes derived fields from native 3D model levels, normalizes, and produces train/val/test splits -- all through MCP tool calls. Terminal ANSI art renders radar, model fields, and skew-T soundings inline.

## Writeup

### Hermes Weather Agent

Seven MCP tools that give an AI agent everything it needs to build production-quality weather model training datasets from scratch. Ask it to "build me a 1-year HRRR dataset with native CAPE and shear" and it handles the entire pipeline: selective GRIB2 downloads, pressure-level extraction, derived field computation from full 3D native model data, per-channel normalization, and train/val/test splitting.

The core advantage is speed. GRIB2 I/O runs through **rustmet**, a Rust parser that decodes a 700MB HRRR file in 0.7 seconds (10x faster than cfgrib). Derived meteorological fields -- CAPE, CIN, wind shear, storm-relative helicity, significant tornado parameter -- are computed by **metrust**, a Rust replacement for MetPy covering all 150 functions at 10-93,000x the speed. Surface-based CAPE computed from full 50-level native HRRR data across the entire CONUS grid (1.9 million points) finishes in 2 seconds. The same computation in MetPy takes over 30 minutes.

This matters because the CAPE shipped in HRRR output files is a model-internal approximation that changed between HRRR v3 and v4. Computing it yourself from the native 3D temperature, moisture, and pressure fields gives consistent, configurable, physically accurate values -- critical for training diffusion and transformer weather models.

The agent also renders weather data directly in the terminal as ANSI art: CONUS-scale model fields, NEXRAD radar PPI scans, and skew-T sounding diagrams, all using half-block characters for high-resolution output. Seven tools total: `wx_models`, `wx_fetch`, `wx_compute`, `wx_build_dataset`, `wx_render_terminal`, `wx_radar_terminal`, and `wx_sounding_terminal`.

A 1-year hourly HRRR dataset that would take ~17 days with Python tooling completes in 2-4 hours.
