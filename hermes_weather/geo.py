"""Geographic helpers — city/lat-lon parsing, region resolution, bbox math.

The rustwx batch binaries take either a `--region` preset (midwest, conus,
southern-plains, …) or, for the few that support it, an explicit
west/east/south/north bbox. We accept any of:

  * a region preset string ("southern-plains", "conus", …)
  * a city name from the small built-in gazetteer ("amarillo, tx")
  * a (lat, lon) tuple plus optional radius_km
  * an explicit bbox dict {"west": …, "east": …, "south": …, "north": …}

…and resolve them to whichever form the target binary needs.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

# rustwx region presets — taken from --help output of the binaries.
# Common to most binaries:
COMMON_REGIONS = {
    "midwest", "conus", "california", "california-square", "reno-square",
    "southeast", "southern-plains", "northeast", "great-lakes",
}
# Some binaries also accept "gulf-to-kansas":
EXTENDED_REGIONS = COMMON_REGIONS | {"gulf-to-kansas"}

# Approximate centers + bounding boxes for each region preset, used to
# resolve "lat/lon → nearest region" queries.
REGION_CENTERS = {
    "conus":             (39.0, -97.0),
    "midwest":           (41.0, -89.0),
    "great-lakes":       (44.0, -85.0),
    "northeast":         (42.0, -73.0),
    "southeast":         (33.0, -83.0),
    "southern-plains":   (35.0, -98.0),
    "gulf-to-kansas":    (33.0, -94.0),
    "california":        (37.0, -120.0),
    "california-square": (37.0, -120.0),
    "reno-square":       (39.5, -119.8),
}

# Tiny offline gazetteer — enough for showcase demos without a network call.
# Add more via update_gazetteer() at runtime if you need wider coverage.
GAZETTEER: dict[str, tuple[float, float]] = {
    # Severe-weather usual suspects
    "norman, ok":         (35.222, -97.439),
    "oklahoma city, ok":  (35.467, -97.516),
    "amarillo, tx":       (35.222, -101.831),
    "dallas, tx":         (32.776, -96.797),
    "fort worth, tx":     (32.756, -97.330),
    "wichita, ks":        (37.687, -97.330),
    "tulsa, ok":          (36.154, -95.993),
    "kansas city, mo":    (39.099, -94.578),
    "omaha, ne":          (41.257, -95.995),
    "des moines, ia":     (41.587, -93.624),
    "minneapolis, mn":    (44.978, -93.265),
    "chicago, il":        (41.878, -87.630),
    "st. louis, mo":      (38.627, -90.199),
    "memphis, tn":        (35.149, -90.049),
    "birmingham, al":     (33.518, -86.810),
    "atlanta, ga":        (33.749, -84.388),
    "huntsville, al":     (34.730, -86.586),
    "jackson, ms":        (32.299, -90.184),
    "shreveport, la":     (32.525, -93.750),
    "houston, tx":        (29.760, -95.369),
    "san antonio, tx":    (29.424, -98.494),
    "austin, tx":         (30.267, -97.743),
    "denver, co":         (39.739, -104.990),
    "albuquerque, nm":    (35.085, -106.651),
    "phoenix, az":        (33.448, -112.074),
    "los angeles, ca":    (34.052, -118.244),
    "san francisco, ca":  (37.775, -122.419),
    "sacramento, ca":     (38.582, -121.494),
    "reno, nv":           (39.530, -119.815),
    "seattle, wa":        (47.606, -122.332),
    "portland, or":       (45.515, -122.679),
    "boise, id":          (43.615, -116.202),
    "salt lake city, ut": (40.760, -111.891),
    "billings, mt":       (45.787, -108.502),
    "fargo, nd":          (46.877, -96.789),
    "nashville, tn":      (36.163, -86.781),
    "louisville, ky":     (38.253, -85.759),
    "indianapolis, in":   (39.768, -86.158),
    "cincinnati, oh":     (39.103, -84.512),
    "cleveland, oh":      (41.499, -81.694),
    "detroit, mi":        (42.331, -83.046),
    "pittsburgh, pa":     (40.441, -79.996),
    "philadelphia, pa":   (39.953, -75.165),
    "washington, dc":     (38.907, -77.037),
    "new york, ny":       (40.713, -74.006),
    "boston, ma":         (42.360, -71.058),
    "miami, fl":          (25.762, -80.192),
    "tampa, fl":          (27.951, -82.458),
    "orlando, fl":        (28.538, -81.379),
    "new orleans, la":    (29.951, -90.072),
    "charleston, sc":     (32.776, -79.931),
    "raleigh, nc":        (35.779, -78.638),
    "charlotte, nc":      (35.227, -80.843),
    "richmond, va":       (37.541, -77.435),
}


@dataclass
class Bbox:
    west: float
    east: float
    south: float
    north: float

    @property
    def center(self) -> tuple[float, float]:
        return ((self.south + self.north) / 2.0, (self.west + self.east) / 2.0)

    def to_args(self) -> list[str]:
        # Use --flag=value form so negative coordinates aren't parsed as flags by clap.
        return [
            f"--west={self.west:.4f}",
            f"--east={self.east:.4f}",
            f"--south={self.south:.4f}",
            f"--north={self.north:.4f}",
        ]


def update_gazetteer(more: dict[str, tuple[float, float]]) -> None:
    """Extend the offline city gazetteer at runtime."""
    GAZETTEER.update({k.lower().strip(): v for k, v in more.items()})


def lookup_city(name: str) -> tuple[float, float] | None:
    """Look up a city name in the offline gazetteer. Case-insensitive."""
    if not name:
        return None
    key = name.lower().strip()
    if key in GAZETTEER:
        return GAZETTEER[key]
    # Try partial match (e.g. "norman" → "norman, ok")
    for k, v in GAZETTEER.items():
        if k.startswith(key + ",") or k.split(",")[0].strip() == key:
            return v
    return None


_LATLON_RE = re.compile(
    r"^\s*\(?\s*(-?\d+(?:\.\d+)?)\s*[,\s]\s*(-?\d+(?:\.\d+)?)\s*\)?\s*$"
)


def parse_latlon(s: str) -> tuple[float, float] | None:
    """Parse a lat/lon literal: '35.2,-97.4' or '(35.2, -97.4)' or '35.2 -97.4'."""
    if not s:
        return None
    m = _LATLON_RE.match(s)
    if not m:
        return None
    return float(m.group(1)), float(m.group(2))


def resolve_location(loc: str | dict | tuple | list | None) -> tuple[float, float] | None:
    """Best-effort location resolver. Returns (lat, lon) or None.

    Accepts: "Norman, OK" (gazetteer), "35.2,-97.4" (literal),
    {"lat": 35.2, "lon": -97.4}, (35.2, -97.4).
    """
    if loc is None:
        return None
    if isinstance(loc, (tuple, list)) and len(loc) == 2:
        return float(loc[0]), float(loc[1])
    if isinstance(loc, dict):
        if "lat" in loc and "lon" in loc:
            return float(loc["lat"]), float(loc["lon"])
        return None
    if isinstance(loc, str):
        parsed = parse_latlon(loc)
        if parsed is not None:
            return parsed
        return lookup_city(loc)
    return None


def bbox_from_center(lat: float, lon: float, radius_km: float = 400.0) -> Bbox:
    """Build a square-ish lat/lon bbox around a center point.

    Uses 1° lat ≈ 111 km and 1° lon ≈ 111·cos(lat) km. radius_km is the
    half-extent — total span is ~2*radius_km.
    """
    dlat = radius_km / 111.0
    cos_lat = max(math.cos(math.radians(lat)), 0.1)
    dlon = radius_km / (111.0 * cos_lat)
    return Bbox(
        west=lon - dlon,
        east=lon + dlon,
        south=lat - dlat,
        north=lat + dlat,
    )


def nearest_region(lat: float, lon: float) -> str:
    """Pick the closest rustwx region preset to (lat, lon)."""
    def hav(a_lat, a_lon, b_lat, b_lon):
        # Spherical-law-of-cosines (good enough for ranking)
        rlat1 = math.radians(a_lat); rlat2 = math.radians(b_lat)
        rlon1 = math.radians(a_lon); rlon2 = math.radians(b_lon)
        return math.acos(
            min(1.0, math.sin(rlat1) * math.sin(rlat2) +
                math.cos(rlat1) * math.cos(rlat2) * math.cos(rlon2 - rlon1))
        )
    return min(REGION_CENTERS.items(), key=lambda kv: hav(lat, lon, *kv[1]))[0]


def resolve_region(
    region: str | None,
    location: str | dict | tuple | list | None = None,
    *,
    allow_extended: bool = True,
) -> str:
    """Pick a rustwx region preset.

    Priority:
      1. explicit `region` if it's a known preset
      2. nearest preset to the resolved location (city/lat-lon)
      3. fall back to "conus"
    """
    valid = EXTENDED_REGIONS if allow_extended else COMMON_REGIONS
    if region:
        r = region.strip().lower().replace("_", "-").replace(" ", "-")
        if r in valid:
            return r
    if location is not None:
        ll = resolve_location(location)
        if ll is not None:
            chosen = nearest_region(*ll)
            if chosen in valid:
                return chosen
    return "conus"
