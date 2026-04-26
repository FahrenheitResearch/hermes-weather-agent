"""Sounding tool — placeholder using the ECAPE profile probe's column output.

rustwx-sounding exists as a Rust crate (write_full_sounding_png /
render_full_sounding_png) but no rustwx-cli binary drives it from a
lat/lon today. As a stop-gap we use hrrr_ecape_profile_probe with
--include-input-column to extract the HRRR column at (lat, lon), then
render a basic skew-T with matplotlib.

Replace this with a rustwx-native sounding renderer when one ships.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from ..geo import resolve_location
from ..rustwx import RustwxEnv, parse_run, resolve_latest_run, run

# Skew-T transform (Beers-Whitney style with skew=45°)
_SKEW_DEG = 45.0


def _skew(t_c: float, p_hpa: float, p_top: float = 100.0) -> tuple[float, float]:
    """Skew-T projection: x = T + skew·log(p0/p), y = log(p0/p)."""
    if p_hpa <= 0:
        return (t_c, 0.0)
    y = math.log(1000.0 / max(p_hpa, p_top))
    x = t_c + math.tan(math.radians(_SKEW_DEG)) * y
    return (x, y)


def _render_skewt(profile_data: dict, out_path: Path, title: str) -> Path:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    column = profile_data.get("input_column") or profile_data.get("column") or {}
    # Native units from hrrr_ecape_profile_probe: pressure_pa, temperature_k,
    # dewpoint_k. Fall back to *_hpa/_c if a future upstream change ships them.
    p_pa = column.get("pressure_pa") or []
    t_k = column.get("temperature_k") or []
    td_k = column.get("dewpoint_k") or []

    if p_pa and t_k and td_k:
        p = [v / 100.0 for v in p_pa]
        t = [v - 273.15 for v in t_k]
        td = [v - 273.15 for v in td_k]
    else:
        p = column.get("pressure_hpa") or column.get("pressure") or []
        t = column.get("temperature_c") or column.get("temperature") or []
        td = column.get("dewpoint_c") or column.get("dewpoint") or []

    if not p or not t or not td or len(p) != len(t) or len(p) != len(td):
        raise RuntimeError(
            f"Insufficient column data to render sounding "
            f"(p:{len(p)} t:{len(t)} td:{len(td)} keys:{sorted(column.keys())[:6]})"
        )

    # Probe writes top-down; ensure pressure decreases monotonically for plotting.
    if len(p) >= 2 and p[0] < p[-1]:
        p = list(reversed(p)); t = list(reversed(t)); td = list(reversed(td))

    fig, ax = plt.subplots(figsize=(7, 7), dpi=120)
    pts_t = [_skew(tt, pp) for tt, pp in zip(t, p)]
    pts_td = [_skew(dd, pp) for dd, pp in zip(td, p)]
    ax.plot([x for x, _ in pts_t], [y for _, y in pts_t], color="#d33", lw=1.6, label="T")
    ax.plot([x for x, _ in pts_td], [y for _, y in pts_td], color="#1a6", lw=1.6, label="Td")

    # Pressure ticks
    plevels = [1000, 850, 700, 500, 300, 200, 150, 100]
    pmax = math.log(1000.0 / 100.0)
    ax.set_yticks([math.log(1000.0 / pp) for pp in plevels])
    ax.set_yticklabels([str(pp) for pp in plevels])
    ax.set_ylim(0, pmax)
    ax.set_xlim(-40, 40)
    ax.invert_yaxis()  # Pressure decreases up — but we already have log(1000/p) so up is up

    # Dry adiabats (rough guide lines)
    for t0 in range(-40, 50, 10):
        xs, ys = [], []
        for pp in [1000, 850, 700, 500, 300, 200]:
            tk = (t0 + 273.15) * (pp / 1000.0) ** 0.286 - 273.15
            x, y = _skew(tk, pp)
            xs.append(x); ys.append(y)
        ax.plot(xs, ys, color="#888", lw=0.4, ls="--", alpha=0.5)

    ax.set_xlabel("Temperature (°C, skewed)")
    ax.set_ylabel("Pressure (hPa)")
    ax.set_title(title)
    ax.grid(True, lw=0.3, alpha=0.4)
    ax.legend(loc="upper right", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return out_path


def sounding(
    env: RustwxEnv,
    *,
    location: str | dict | tuple,
    run_str: str = "latest",
    forecast_hour: int = 1,
    model: str = "hrrr",
    source: str = "aws",
    out_dir: str | None = None,
    timeout: int = 120,
) -> dict:
    """Render a basic skew-T sounding at (lat, lon) from HRRR.

    NOTE: this uses matplotlib until rustwx-sounding gains a CLI driver.
    Output is functional but not styled to NWS-grade quality yet.
    """
    binary = "hrrr_ecape_profile_probe"
    if not env.has(binary):
        return {"ok": False, "error": f"{binary} binary not built (needed to extract HRRR column)"}

    latlon = resolve_location(location)
    if latlon is None:
        return {"ok": False, "error": f"could not resolve location {location!r}"}
    lat, lon = latlon

    date, cycle = (resolve_latest_run(model) if run_str == "latest" else parse_run(run_str))
    out = Path(out_dir) if out_dir else (
        env.out_root / "sounding" /
        f"{date}_{cycle:02d}z_f{forecast_hour:03d}_{lat:.3f}_{lon:.3f}"
    )
    out.mkdir(parents=True, exist_ok=True)
    profile_json = out / "profile.json"
    png_path = out / "skewt.png"

    args = [
        "--model", model,
        "--date", date,
        "--cycle", str(cycle),
        "--forecast-hour", str(forecast_hour),
        "--source", source,
        f"--lat={lat:.6f}",
        f"--lon={lon:.6f}",
        "--cache-dir", str(env.cache_dir.resolve()),
        "--output", str(profile_json.resolve()),
        "--include-input-column",
    ]

    result = run(env, binary, args, timeout=timeout)
    if not result.ok:
        return {"ok": False,
                "error": result.stderr.strip()[-400:] or f"profile probe rc={result.returncode}"}

    if not profile_json.exists():
        return {"ok": False, "error": "profile probe did not write column JSON"}

    try:
        data = json.loads(profile_json.read_text(encoding="utf-8"))
        title = f"{model.upper()} f{forecast_hour:03d}  {lat:.2f}°,{lon:.2f}°  {date} {cycle:02d}z"
        _render_skewt(data, png_path, title)
    except Exception as exc:
        return {"ok": False, "error": f"sounding render failed: {exc}",
                "profile_json": str(profile_json)}

    return {
        "ok": True,
        "lat": lat, "lon": lon,
        "date": date, "cycle": cycle, "forecast_hour": forecast_hour,
        "model": model,
        "out_dir": str(out),
        "profile_json": str(profile_json),
        "png": str(png_path),
        "note": "Stop-gap matplotlib render. Will be replaced when rustwx-sounding ships a CLI.",
    }
