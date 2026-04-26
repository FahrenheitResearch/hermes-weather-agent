"""Showcase demo — drives the plugin programmatically (no LLM required).

Renders the headline products for a severe-weather setup:
  1. MLECAPE / ECAPE / ratio map for southern-plains
  2. ECAPE profile at Norman, OK
  3. Amarillo → Chicago cross section
  4. CAPE / SRH / STP / shear panel for southern-plains

Run from the repo root after `pip install -e .`:
    python examples/showcase_demo.py
"""
from __future__ import annotations

import json
import sys
import time

from hermes_weather.rustwx import discover
from hermes_weather.tools import (
    catalog,
    cross_section as cs_tool,
    ecape as ecape_tool,
    render as render_tool,
)
from hermes_weather import jobs


def section(title: str) -> None:
    print(f"\n{'═' * 70}\n {title}\n{'═' * 70}")


def main() -> int:
    env = discover()
    section("Doctor")
    print(json.dumps(catalog.doctor(env), indent=2))

    section("Latest run")
    from hermes_weather.tools.fetch import latest
    print(json.dumps(latest(env, model="hrrr"), indent=2))

    section("Severe-weather panel: SBCAPE / MLCAPE / SRH 0-1 / STP / 0-6 shear")
    panel = render_tool.render_recipe(
        env,
        recipes=["sbcape", "mlcape", "srh_0_1km", "stp_fixed", "bulk_shear_0_6km"],
        region="southern-plains",
        run_str="latest",
        forecast_hour=0,
    )
    print(json.dumps(panel, indent=2, default=str))

    section("ECAPE profile @ Norman, OK")
    prof = ecape_tool.profile(env, location="Norman, OK", run_str="latest", forecast_hour=1)
    diagnostics = (prof or {}).get("diagnostics") or {}
    keep = {k: diagnostics.get(k) for k in ("sb", "ml", "mu", "summary", "cape_cin")
            if k in diagnostics}
    print(json.dumps({"ok": prof.get("ok"), "elapsed_s": prof.get("elapsed_s"), **keep}, indent=2, default=str))

    section("ECAPE/CAPE ratio map (Figure 4) — southern-plains")
    submitted = ecape_tool.ratio_map(env, region="southern-plains",
                                     run_str="latest", forecast_hour=0)
    print(json.dumps(submitted, indent=2, default=str))
    if submitted.get("job_id"):
        for _ in range(60):
            time.sleep(2.0)
            job = jobs.get(submitted["job_id"])
            if job and job.state in ("done", "failed", "cancelled"):
                print(json.dumps(job.to_payload(log_tail=10), indent=2, default=str))
                break
            elif job:
                print(f"  ...{job.state} elapsed={time.time() - (job.started_at or time.time()):.1f}s")
        else:
            print("  (timeout waiting on ratio map)")

    section("Cross section: Amarillo → Chicago, theta-e")
    xs = cs_tool.cross_section(
        env, product="theta-e", route="amarillo-chicago",
        run_str="latest", forecast_hour=0,
    )
    print(json.dumps(xs, indent=2, default=str))

    return 0


if __name__ == "__main__":
    sys.exit(main())
