"""Hermes Weather — MCP plugin built on top of rustwx.

A thin Python adapter that lets an AI agent drive rustwx-cli's batch binaries:
fetch HRRR/GFS/RAP grids, render publication-quality maps, full-grid ECAPE,
profile diagnostics, cross sections, NEXRAD radar, and dataset assembly.

All compute and rendering happens inside Rust; this package is glue.
"""

__version__ = "0.2.0"
