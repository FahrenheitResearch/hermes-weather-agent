"""Geographic helpers — lat/lon parsing, domain lookup, bounds.

Built-in domain catalog comes from `rustwx.list_domains_json()` (77+ entries
covering country / region / metro / watch_area). Plugin caches them at
discovery time; agents resolve "Norman, OK" / "Amarillo, TX" / region
preset slugs via `find_domain_for_location`.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass


@dataclass
class Bbox:
    west: float
    east: float
    south: float
    north: float

    @property
    def center(self) -> tuple[float, float]:
        return ((self.south + self.north) / 2.0, (self.west + self.east) / 2.0)

    def contains(self, lat: float, lon: float) -> bool:
        return self.west <= lon <= self.east and self.south <= lat <= self.north

    def to_list(self) -> list[float]:
        return [self.west, self.east, self.south, self.north]


_LATLON_RE = re.compile(
    r"^\s*\(?\s*(-?\d+(?:\.\d+)?)\s*[,\s]\s*(-?\d+(?:\.\d+)?)\s*\)?\s*$"
)


def parse_latlon(s: str) -> tuple[float, float] | None:
    """Parse '35.2,-97.4' or '(35.2, -97.4)' or '35.2 -97.4'."""
    if not s:
        return None
    m = _LATLON_RE.match(s)
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))


def resolve_location(loc) -> tuple[float, float] | None:
    """Best-effort lat/lon resolver. Accepts (lat,lon), {'lat','lon'}, or string."""
    if loc is None:
        return None
    if isinstance(loc, (tuple, list)) and len(loc) == 2:
        return float(loc[0]), float(loc[1])
    if isinstance(loc, dict):
        if "lat" in loc and "lon" in loc:
            return float(loc["lat"]), float(loc["lon"])
        return None
    if isinstance(loc, str):
        # Try numeric first
        parsed = parse_latlon(loc)
        if parsed is not None:
            return parsed
        # Otherwise the caller has an env to look up domain centroids
        return None
    return None


def bbox_from_center(lat: float, lon: float, radius_km: float = 400.0) -> Bbox:
    """Square-ish lat/lon bbox around a center point."""
    dlat = radius_km / 111.0
    cos_lat = max(math.cos(math.radians(lat)), 0.1)
    dlon = radius_km / (111.0 * cos_lat)
    return Bbox(west=lon - dlon, east=lon + dlon,
                south=lat - dlat, north=lat + dlat)


def find_domain_for_string(env, query: str) -> dict | None:
    """Best-effort lookup of a built-in domain by label / slug substring.

    Returns the rustwx domain dict (with bounds / kind / label) or None.
    """
    if not env.module_available or not query:
        return None
    needle = query.strip().lower().replace(",", "").replace("  ", " ")
    try:
        from .rustwx import list_domains  # local import to avoid cycle
    except Exception:
        return None
    catalog = list_domains(env, limit=None)
    candidates = catalog.get("domains") or []

    # Exact slug match
    for d in candidates:
        if d.get("slug", "").lower() == needle.replace(" ", "_"):
            return d
    # Label match (case-insensitive substring)
    for d in candidates:
        label = (d.get("label") or "").lower()
        if needle in label or label in needle:
            return d
    # Word-set overlap heuristic for "norman ok" -> "Norman, OK"
    words = set(needle.split())
    best = None
    best_score = 0
    for d in candidates:
        label_words = set((d.get("label") or "").lower().replace(",", "").split())
        overlap = len(words & label_words)
        if overlap > best_score:
            best_score = overlap
            best = d
    return best if best_score >= 1 else None


def find_domain_containing(env, lat: float, lon: float,
                           prefer_kind: str = "metro") -> dict | None:
    """Find smallest rustwx domain whose bounds contain (lat, lon).

    Prefers metros, then regions, then watch areas, then countries.
    """
    if not env.module_available:
        return None
    try:
        from .rustwx import list_domains
    except Exception:
        return None
    catalog = list_domains(env, limit=None)
    rows = catalog.get("domains") or []
    kind_priority = ["metro", "watch_area", "region", "country"]
    if prefer_kind in kind_priority:
        kind_priority = [prefer_kind] + [k for k in kind_priority if k != prefer_kind]
    for kind in kind_priority:
        matches = []
        for d in rows:
            if d.get("kind") != kind:
                continue
            b = d.get("bounds")
            if not b or len(b) != 4:
                continue
            west, east, south, north = b
            if west <= lon <= east and south <= lat <= north:
                matches.append((abs(east - west) * abs(north - south), d))
        if matches:
            matches.sort(key=lambda kv: kv[0])
            return matches[0][1]
    return None


def resolve_to_domain(
    env,
    *,
    region: str | None = None,
    domain: str | None = None,
    location=None,
) -> tuple[str | None, list[float] | None]:
    """Pick a domain slug + optional bounds for a render request.

    Returns (domain_slug, bounds). At most one of those should be set;
    if both are None, the caller should default to "conus".
    """
    if domain:
        return domain.strip().lower().replace("_", "-"), None
    if region:
        return region.strip().lower().replace("_", "-").replace(" ", "-"), None
    if location is None:
        return None, None
    # First try numeric lat/lon
    if isinstance(location, str):
        parsed = parse_latlon(location)
        if parsed is None:
            d = find_domain_for_string(env, location)
            if d:
                return d.get("slug"), None
        else:
            d = find_domain_containing(env, *parsed, prefer_kind="metro")
            if d:
                return d.get("slug"), None
            # Fall back to a custom bbox around the point
            return None, bbox_from_center(*parsed, radius_km=400).to_list()
    if isinstance(location, (tuple, list)) and len(location) == 2:
        d = find_domain_containing(env, float(location[0]), float(location[1]))
        if d:
            return d.get("slug"), None
        return None, bbox_from_center(float(location[0]), float(location[1])).to_list()
    if isinstance(location, dict) and "lat" in location and "lon" in location:
        d = find_domain_containing(env, float(location["lat"]), float(location["lon"]))
        if d:
            return d.get("slug"), None
        return None, bbox_from_center(
            float(location["lat"]), float(location["lon"])
        ).to_list()
    return None, None
