"""HRRR-first local data-pack guidance for Hermes agents.

The goal is not to hide downloads. It is to tell an agent/user what a local
cache tier can do before the next request triggers more data movement.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..rustwx import RustwxEnv


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        try:
            return path.stat().st_size
        except OSError:
            return 0
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except OSError:
                pass
    return total


def _gb(value: int | float) -> float:
    return round(float(value) / 1024.0 / 1024.0 / 1024.0, 3)


DATA_PACKS: list[dict[str, Any]] = [
    {
        "id": "hrrr-tiny-1gb",
        "budget_gb": 1,
        "target_device": "Raspberry Pi, small VPS, constrained laptop",
        "retained_data": [
            "No full-cycle retention.",
            "Keep only small selective HRRR byte-range cache entries.",
            "Keep at most one or two small route VolumeStores, usually f000 only.",
            "Aggressively prune rendered images and old temporary stores.",
        ],
        "works_without_more_downloads": [
            "Repeat the same already-rendered maps/cross sections.",
            "Repeat point meteograms for hours already fetched into the cache.",
            "Render from an existing small route VolumeStore if the exact route/hour/store is present.",
        ],
        "will_download_or_refetch": [
            "New forecast hours.",
            "New map fields not already cached.",
            "New arbitrary cross-section routes unless a matching store exists.",
            "Satellite full product sets and fresh radar scans.",
        ],
        "defaults": {
            "hrrr_hours": "f000-f003 on demand",
            "volume_store": "route-scoped, keep_store=false or TTL <= 1h",
            "satellite": "single product only",
            "meteogram": "direct point sampling, no warmed regional store",
        },
        "notes": "Best as a lightweight agent helper, not an operational weather workstation.",
    },
    {
        "id": "hrrr-local-5gb",
        "budget_gb": 5,
        "target_device": "low-end mini PC or laptop",
        "retained_data": [
            "Latest HRRR surface/pressure selective cache for a few recent hours.",
            "Several route-scoped pressure VolumeStores.",
            "Recent radar scan cache and a small satellite still cache.",
        ],
        "works_without_more_downloads": [
            "Common f000-f006 HRRR maps after first use.",
            "Point meteograms for cached latest hours.",
            "All non-smoke VolumeStore cross-section styles for cached route/hour stores.",
            "Cached radar all-product renders for a previously fetched scan.",
        ],
        "will_download_or_refetch": [
            "48h loops.",
            "Large new domains.",
            "Full CA satellite product history.",
            "New pressure hours not already in the cache.",
        ],
        "defaults": {
            "hrrr_hours": "f000-f006 or f000-f012 on demand",
            "volume_store": "route-scoped, TTL 6h",
            "satellite": "latest stills, selected products",
            "meteogram": "direct or small warmed area",
        },
        "notes": "Good default for casual local agent use.",
    },
    {
        "id": "hrrr-ca-core-10gb",
        "budget_gb": 10,
        "target_device": "Mac mini, desktop, 16 GB RAM laptop",
        "retained_data": [
            "One latest partial HRRR cycle for California-style use.",
            "CA/small-region surface fields for maps and meteograms.",
            "Route-scoped pressure VolumeStores for common drawn cross sections.",
            "Recent satellite/radar products.",
        ],
        "works_without_more_downloads": [
            "Most f000-f018 HRRR maps after warmup.",
            "Meteograms for cached points/region hours.",
            "Cross-section products for cached route/hour stores.",
            "Short loops over already warmed hours.",
        ],
        "will_download_or_refetch": [
            "Full f000-f048 loops unless the selected synoptic cycle was warmed.",
            "CONUS VolumeStores.",
            "New satellite scans and radar scans.",
        ],
        "defaults": {
            "hrrr_hours": "latest contiguous f000-f018 when available",
            "volume_store": "latest partial route stores; keep 1-2 recent",
            "satellite": "latest CA/Pacific Southwest stills",
            "meteogram": "small regional warmed store",
        },
        "notes": "This is the practical HRRR-first laptop tier.",
    },
    {
        "id": "hrrr-ca-operational-50gb",
        "budget_gb": 50,
        "target_device": "Mac mini / workstation / modest server",
        "retained_data": [
            "Newest synoptic HRRR cycle with f048 available.",
            "Optionally previous synoptic cycle if cleanup is strict.",
            "CA pressure VolumeStore f000-f048.",
            "Surface/meteogram cache for CA f000-f048.",
            "Recent satellite loops and radar scans.",
        ],
        "works_without_more_downloads": [
            "Full 48h CA cross-section loops from warmed pressure stores.",
            "Most CA static maps for warmed products/hours.",
            "Point meteograms across f000-f048 for warmed domains.",
            "Repeated satellite/radar viewing for cached scans.",
        ],
        "will_download_or_refetch": [
            "CONUS-wide pressure stores unless separately built.",
            "New cycles as they arrive.",
            "Smoke/native products until a native/hybrid data pack exists.",
        ],
        "defaults": {
            "hrrr_hours": "latest complete synoptic f000-f048",
            "volume_store": "CA region store, keep newest 1-2 completed stores",
            "satellite": "latest stills plus short WebP loops",
            "meteogram": "regional warmed store",
        },
        "notes": "Closest local analogue to the CA Fire production node, but still needs cleanup policy.",
    },
]


def data_packs(
    env: RustwxEnv,
    *,
    budget_gb: float | None = None,
    include_current_cache: bool = True,
) -> dict[str, Any]:
    """Return local HRRR data-pack tiers and current cache usage."""
    packs = DATA_PACKS
    selected = None
    if budget_gb is not None:
        fits = [pack for pack in packs if float(pack["budget_gb"]) <= float(budget_gb)]
        selected = fits[-1] if fits else packs[0]

    current: dict[str, Any] | None = None
    if include_current_cache:
        cache_dir = env.cache_dir
        out_root = env.out_root
        current = {
            "cache_dir": str(cache_dir),
            "cache_gb": _gb(_dir_size(cache_dir)),
            "output_root": str(out_root),
            "output_gb": _gb(_dir_size(out_root)),
            "subdirs": {},
        }
        for child in sorted(cache_dir.iterdir()) if cache_dir.exists() else []:
            if child.is_dir():
                current["subdirs"][child.name] = _gb(_dir_size(child))

    return {
        "ok": True,
        "model_priority": "hrrr-first",
        "policy": (
            "Use latest available model hours by default. For f019-f048, choose "
            "the newest synoptic HRRR cycle that actually advertises those hours."
        ),
        "budget_gb": budget_gb,
        "selected_pack": selected,
        "packs": packs,
        "current_usage": current,
        "caveats": [
            "Sizes are planning tiers, not hard guarantees; exact cache use depends on domain, products, and hours.",
            "Rendered PNG/WebP artifacts are separate from model-data cache and need their own cleanup policy.",
            "A Raspberry Pi tier should stay on demand-driven selective downloads; a Mac mini can run the 10 GB tier and often the 50 GB CA tier.",
        ],
    }
