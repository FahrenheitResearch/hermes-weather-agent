"""Cache management — disk usage report + LRU eviction.

Heavy research sweeps will fill a 500 GB cache fast. These helpers report
top consumers and evict least-recently-used files until usage falls below
a target. Always preview with `dry_run=True` before committing.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from ..rustwx import RustwxEnv


def _walk(root: Path) -> list[tuple[Path, os.stat_result]]:
    out: list[tuple[Path, os.stat_result]] = []
    if not root.exists():
        return out
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            p = Path(dirpath) / name
            try:
                st = p.stat()
            except OSError:
                continue
            out.append((p, st))
    return out


def status(env: RustwxEnv, *, top_n: int = 20) -> dict:
    """Report cache disk usage + top file consumers."""
    root = env.cache_dir
    files = _walk(root)
    total_bytes = sum(st.st_size for _p, st in files)
    files.sort(key=lambda x: x[1].st_size, reverse=True)
    biggest = [
        {
            "path": str(p),
            "size_mb": round(st.st_size / 1e6, 2),
            "age_hours": round((time.time() - st.st_mtime) / 3600, 1),
        }
        for p, st in files[:top_n]
    ]

    # Bucketed totals by top-level subdir
    bucket_totals: dict[str, int] = {}
    for p, st in files:
        try:
            rel = p.relative_to(root)
        except ValueError:
            continue
        parts = rel.parts
        bucket = parts[0] if parts else "(root)"
        bucket_totals[bucket] = bucket_totals.get(bucket, 0) + st.st_size

    return {
        "ok": True,
        "cache_dir": str(root),
        "exists": root.exists(),
        "file_count": len(files),
        "total_gb": round(total_bytes / 1e9, 2),
        "by_subdir_gb": {k: round(v / 1e9, 2) for k, v in
                          sorted(bucket_totals.items(), key=lambda kv: -kv[1])},
        "biggest_files": biggest,
    }


def evict_to(
    cache_dir: Path | str,
    *,
    target_gb: float,
    dry_run: bool = False,
    keep_pattern: str | None = None,
) -> dict:
    """Evict least-recently-used files until usage falls below target_gb.

    Returns counts of files evicted and bytes freed. With dry_run=True, no
    files are deleted — useful for previewing what an eviction would do.
    `keep_pattern` is a substring match against the file path; matching
    files are never evicted.
    """
    root = Path(cache_dir)
    if not root.exists():
        return {"ok": True, "evicted_files": 0, "evicted_bytes": 0,
                "note": "cache dir does not exist"}

    files = _walk(root)
    total_bytes = sum(st.st_size for _p, st in files)
    target_bytes = int(target_gb * 1e9)
    if total_bytes <= target_bytes:
        return {
            "ok": True,
            "evicted_files": 0,
            "evicted_bytes": 0,
            "current_gb": round(total_bytes / 1e9, 2),
            "target_gb": target_gb,
            "note": "already under target",
        }

    # Oldest mtime first (LRU)
    files.sort(key=lambda x: x[1].st_mtime)
    bytes_to_free = total_bytes - target_bytes
    evicted_files = 0
    evicted_bytes = 0
    sample_evicted: list[str] = []
    for p, st in files:
        if evicted_bytes >= bytes_to_free:
            break
        if keep_pattern and keep_pattern in str(p):
            continue
        try:
            if not dry_run:
                p.unlink()
            evicted_files += 1
            evicted_bytes += st.st_size
            if len(sample_evicted) < 8:
                sample_evicted.append(str(p))
        except OSError:
            continue

    return {
        "ok": True,
        "dry_run": dry_run,
        "starting_gb": round(total_bytes / 1e9, 2),
        "target_gb": target_gb,
        "evicted_files": evicted_files,
        "evicted_bytes": evicted_bytes,
        "evicted_gb": round(evicted_bytes / 1e9, 2),
        "ending_gb": round((total_bytes - evicted_bytes) / 1e9, 2),
        "sample_evicted": sample_evicted,
    }


def evict(env: RustwxEnv, *, target_gb: float = 400.0, dry_run: bool = True,
          keep_pattern: str | None = None) -> dict:
    """MCP-facing eviction. Defaults to dry_run for safety."""
    return evict_to(env.cache_dir, target_gb=target_gb,
                    dry_run=dry_run, keep_pattern=keep_pattern)
