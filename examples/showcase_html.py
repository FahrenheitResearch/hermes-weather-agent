"""Generate outputs/showcase/index.html from the showcase manifest.

Self-contained: no external CSS/JS, no network. Uses file:// for image
sources so you can open the HTML directly in a browser. PNGs are copied
into outputs/showcase/images/<section>/ and referenced relatively, so the
showcase folder is portable.
"""
from __future__ import annotations

import html as html_mod
import json
import shutil
from pathlib import Path

SHOWCASE_DIR = Path("outputs/showcase")
MANIFEST = SHOWCASE_DIR / "manifest.json"
IMAGES_DIR = SHOWCASE_DIR / "images"
HTML_OUT = SHOWCASE_DIR / "index.html"


SECTION_TITLES = {
    "discovery":      ("Discovery", "🧭"),
    "fetch":          ("Selective GRIB Fetch", "📥"),
    "hrrr_direct":    ("HRRR — Direct (one shared decode)", "🗺️"),
    "hrrr_derived":   ("HRRR — Derived thermodynamics (one shared decode)", "🌪️"),
    "hrrr_windowed":  ("HRRR — Windowed (QPF / UH)", "⏱️"),
    "hrrr_severe":    ("HRRR — Severe + ECAPE heavy panel", "⚡"),
    "ecape":          ("ECAPE specialists", "🔥"),
    "gfs":            ("GFS", "🌐"),
    "ecmwf":          ("ECMWF Open Data", "🌍"),
    "rrfs":           ("RRFS-A", "🛰️"),
    "wrf_gdex":       ("WRF-GDEX (NetCDF via netcrust)", "🧪"),
    "cross_section":  ("Vertical cross sections", "✂️"),
    "radar":          ("NEXRAD Level 2", "📡"),
    "sounding":       ("Sounding (skew-T)", "🌡️"),
    "research":       ("Research mode", "🧬"),
    "cache":          ("Cache management", "💾"),
    "jobs":           ("Background jobs", "🧵"),
}


# ── HTML helpers ────────────────────────────────────────────────────────


CSS = """
:root {
  --bg: #0b0d12;
  --panel: #11151c;
  --panel2: #161b25;
  --border: #1f2734;
  --text: #e6e9ef;
  --muted: #8a93a4;
  --accent: #ffb547;
  --accent2: #5fc8ff;
  --green: #4ade80;
  --red: #f87171;
  --yellow: #facc15;
  --mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font: 14px/1.5 ui-sans-serif, system-ui, -apple-system, sans-serif;
  display: grid;
  grid-template-columns: 240px 1fr;
  min-height: 100vh;
}
nav.side {
  position: sticky; top: 0; align-self: start;
  height: 100vh; overflow-y: auto;
  background: var(--panel); border-right: 1px solid var(--border);
  padding: 1.25rem 0.75rem;
}
nav.side h1 {
  font: 600 14px var(--mono);
  letter-spacing: 0.04em; margin: 0 0.5rem 1.25rem;
}
nav.side a {
  display: block; padding: 0.45rem 0.75rem;
  color: var(--muted); text-decoration: none; font: 13px var(--mono);
  border-radius: 6px;
}
nav.side a:hover { background: var(--panel2); color: var(--text); }
main { padding: 2rem 2.5rem 4rem; max-width: 1500px; }
.hero {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 2rem;
  padding: 1.75rem 2rem;
  background: linear-gradient(135deg, #1a2233 0%, #0e1320 100%);
  border: 1px solid var(--border);
  border-radius: 14px;
  margin-bottom: 2rem;
}
.hero h1 {
  margin: 0 0 0.25rem;
  font: 700 26px var(--mono); letter-spacing: -0.02em;
}
.hero .sub { color: var(--muted); font: 13px var(--mono); margin-bottom: 1.25rem; }
.hero .meta { display: flex; gap: 0.75rem; flex-wrap: wrap; font: 12px var(--mono); }
.hero .meta span { padding: 0.25rem 0.6rem; background: var(--panel2); border: 1px solid var(--border); border-radius: 999px; color: var(--muted); }
.stats { display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.75rem; align-self: center; }
.stat {
  background: var(--panel2); border: 1px solid var(--border);
  padding: 1rem 1.25rem; border-radius: 10px;
  min-width: 140px; text-align: right;
}
.stat .num { font: 700 24px var(--mono); color: var(--accent); }
.stat .lbl { font: 11px var(--mono); color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
section.block {
  margin-bottom: 2.25rem;
  padding-top: 0.5rem;
}
section.block h2 {
  font: 600 18px var(--mono);
  margin: 0 0 0.25rem;
  color: var(--text);
  display: flex; align-items: baseline; gap: 0.5rem;
}
section.block h2 .emoji { font-size: 22px; }
section.block .sub {
  font: 12px var(--mono); color: var(--muted);
  margin-bottom: 1rem; display: flex; gap: 1rem; flex-wrap: wrap;
}
section.block .sub b { color: var(--accent2); }
.cards { display: grid; gap: 1rem; }
.cards.grid-img { grid-template-columns: repeat(auto-fill, minmax(360px, 1fr)); }
.cards.grid-text { grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); }
.card {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: 10px; padding: 0.85rem;
  transition: border-color 120ms;
}
.card:hover { border-color: #2a3447; }
.card .title {
  display: flex; justify-content: space-between; gap: 0.5rem;
  align-items: baseline; margin-bottom: 0.5rem;
}
.card .name { font: 600 13px var(--mono); color: var(--text); }
.card .timing { font: 12px var(--mono); color: var(--muted); white-space: nowrap; }
.card .timing.fast { color: var(--green); }
.card .timing.slow { color: var(--yellow); }
.card .timing.veryslow { color: var(--red); }
.card .meta { font: 11px var(--mono); color: var(--muted); margin-bottom: 0.6rem; }
.card .meta span { display: inline-block; padding: 1px 7px; margin-right: 4px; background: var(--panel2); border: 1px solid var(--border); border-radius: 4px; }
.card img {
  width: 100%; height: auto; display: block;
  border: 1px solid var(--border); border-radius: 6px;
  background: #fff;
}
.card.failure { border-color: rgba(248, 113, 113, 0.35); }
.card .err {
  font: 11px var(--mono); color: var(--red);
  background: rgba(248, 113, 113, 0.08);
  padding: 0.35rem 0.5rem; border-radius: 4px;
  white-space: pre-wrap; word-break: break-word;
}
.card .note {
  font: 11px var(--mono); color: var(--muted);
  margin-top: 0.4rem; font-style: italic;
}
details.json {
  margin-top: 0.4rem;
  background: var(--bg); border: 1px solid var(--border); border-radius: 6px;
  padding: 0.4rem 0.6rem;
}
details.json summary { font: 11px var(--mono); color: var(--muted); cursor: pointer; }
details.json pre {
  max-height: 240px; overflow: auto;
  font: 11px var(--mono); color: var(--text); margin: 0.5rem 0 0;
}
table.timing {
  width: 100%; border-collapse: collapse;
  background: var(--panel); border-radius: 10px; overflow: hidden;
}
table.timing th, table.timing td {
  padding: 0.5rem 0.85rem; text-align: left;
  border-bottom: 1px solid var(--border); font: 12px var(--mono);
}
table.timing th { background: var(--panel2); color: var(--muted); font-weight: 600; }
table.timing tr:last-child td { border-bottom: none; }
table.timing td.bar {
  width: 40%;
  background-clip: content-box;
}
table.timing .barfill {
  height: 16px; border-radius: 4px;
  background: linear-gradient(90deg, #ffb547, #f87171);
}
.badge {
  display: inline-block;
  padding: 1px 7px; border-radius: 4px;
  font: 10px var(--mono); text-transform: uppercase; letter-spacing: 0.04em;
  border: 1px solid var(--border);
}
.badge.ok    { color: var(--green);    background: rgba(74,222,128,0.08); border-color: rgba(74,222,128,0.3); }
.badge.fail  { color: var(--red);      background: rgba(248,113,113,0.08); border-color: rgba(248,113,113,0.3); }
.badge.partial { color: var(--yellow); background: rgba(250,204,21,0.08); border-color: rgba(250,204,21,0.3); }
"""


def page(manifest: dict, body: str) -> str:
    summary = manifest["doctor"].get("product_catalog_summary") or {}
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Hermes Weather — Showcase</title>
<style>{CSS}</style>
</head><body>
<nav class="side">
  <h1>Hermes Weather</h1>
  {_nav(manifest)}
</nav>
<main>
  <div class="hero">
    <div>
      <h1>End-to-end showcase</h1>
      <div class="sub">All-Rust weather pipeline · {manifest['generated_at']}</div>
      <div class="meta">
        <span>{summary.get('total_entries', 0)} catalog products</span>
        <span>{summary.get('supported_entries', 0)} supported</span>
        <span>{summary.get('experimental_entries', 0)} experimental</span>
        <span>rustwx-cli: {'✓' if manifest['doctor'].get('rustwx_cli_available') else '✗'}</span>
        <span>netcdf runtime: not required</span>
      </div>
    </div>
    <div class="stats">
      <div class="stat"><div class="num">{manifest['call_count']}</div><div class="lbl">Tool calls</div></div>
      <div class="stat"><div class="num">{manifest['png_count']}</div><div class="lbl">PNGs</div></div>
      <div class="stat"><div class="num">{manifest['ok_count']}</div><div class="lbl">Succeeded</div></div>
      <div class="stat"><div class="num">{manifest['total_seconds']:.0f}s</div><div class="lbl">Total runtime</div></div>
    </div>
  </div>
  {body}
</main>
</body></html>
"""


def _nav(manifest: dict) -> str:
    lines = []
    for sect in manifest["sections"].keys():
        title, emoji = SECTION_TITLES.get(sect, (sect, "·"))
        lines.append(f'<a href="#{sect}">{emoji} &nbsp;{html_mod.escape(title)}</a>')
    lines.append('<a href="#timing">⏲ &nbsp;Timing breakdown</a>')
    return "".join(lines)


def _timing_class(seconds: float) -> str:
    if seconds < 5: return "fast"
    if seconds < 60: return ""
    if seconds < 300: return "slow"
    return "veryslow"


def render_section(section: str, calls: list[dict]) -> str:
    title, emoji = SECTION_TITLES.get(section, (section, "·"))
    total = sum(c.get("elapsed_s", 0) for c in calls)
    ok = sum(1 for c in calls if c.get("ok"))

    has_pngs = any(c.get("pngs") for c in calls)
    grid_class = "grid-img" if has_pngs else "grid-text"
    cards = []

    for c in calls:
        cards.append(_render_card(section, c))

    return f"""
<section class="block" id="{html_mod.escape(section)}">
  <h2><span class="emoji">{emoji}</span> {html_mod.escape(title)}</h2>
  <div class="sub">
    <span><b>{len(calls)}</b> calls</span>
    <span><b>{ok}/{len(calls)}</b> ok</span>
    <span><b>{total:.1f}s</b> total</span>
  </div>
  <div class="cards {grid_class}">
    {''.join(cards)}
  </div>
</section>
"""


def _render_card(section: str, c: dict) -> str:
    name = html_mod.escape(c.get("name", ""))
    elapsed = c.get("elapsed_s", 0.0)
    timing_cls = _timing_class(elapsed)
    badge = '<span class="badge ok">ok</span>' if c.get("ok") else '<span class="badge fail">fail</span>'
    fail_class = "" if c.get("ok") else " failure"

    md = c.get("metadata") or {}
    meta_chips = []
    for key in ("model", "date", "cycle", "forecast_hour", "region", "binary",
                  "kind", "site", "lat", "lon", "png_count", "result_count"):
        if key in md and md[key] is not None:
            v = md[key]
            if isinstance(v, float):
                v = f"{v:.3f}"
            meta_chips.append(f"<span>{html_mod.escape(str(key))}: {html_mod.escape(str(v))}</span>")
    meta_html = ('<div class="meta">' + "".join(meta_chips) + '</div>') if meta_chips else ""

    pngs_html = ""
    for png in c.get("pngs", [])[:8]:  # limit to 8 per card
        rel = _ensure_image(section, c.get("name", ""), png)
        if rel:
            pngs_html += f'<img src="{html_mod.escape(rel)}" loading="lazy" alt="{html_mod.escape(Path(png).name)}">'

    err_html = ""
    if c.get("error"):
        err_html = f'<div class="err">{html_mod.escape(c["error"])[:600]}</div>'

    note_html = ""
    if c.get("note"):
        note_html = f'<div class="note">{html_mod.escape(c["note"])}</div>'

    json_summary = _json_inspector(c)

    return f"""
<div class="card{fail_class}">
  <div class="title">
    <div class="name">{name} {badge}</div>
    <div class="timing {timing_cls}">{elapsed:.2f}s</div>
  </div>
  {meta_html}
  {pngs_html}
  {err_html}
  {note_html}
  {json_summary}
</div>
"""


def _ensure_image(section: str, name: str, png_path: str) -> str | None:
    """Copy png_path into showcase/images/<section>/ and return its relative URL."""
    src = Path(png_path)
    if not src.exists():
        return None
    safe_section = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in section)
    dest_dir = IMAGES_DIR / safe_section
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if not dest.exists() or dest.stat().st_size != src.stat().st_size:
        shutil.copy2(src, dest)
    return f"images/{safe_section}/{src.name}"


def _json_inspector(c: dict) -> str:
    """Show a small inspectable JSON for tools that don't produce PNGs."""
    raw = c.get("raw_result") or {}
    # Strip the heavy fields that already appear above
    skim = {k: v for k, v in raw.items()
            if k not in ("pngs", "stderr_tail", "manifests", "runs", "stdout")
            and not (isinstance(v, list) and len(v) > 50)}
    if not skim:
        return ""
    try:
        text = json.dumps(skim, indent=2, default=str)
    except Exception:
        text = str(skim)
    if len(text) > 4000:
        text = text[:4000] + "\n…(truncated)"
    return f"""
<details class="json">
  <summary>Result JSON ({len(text)} chars)</summary>
  <pre>{html_mod.escape(text)}</pre>
</details>
"""


def render_timing_table(manifest: dict) -> str:
    rows = []
    flat: list[tuple[str, str, float]] = []
    for sect, calls in manifest["sections"].items():
        for c in calls:
            flat.append((sect, c.get("name", ""), c.get("elapsed_s", 0.0)))
    flat.sort(key=lambda x: x[2], reverse=True)
    if not flat:
        return ""
    max_t = max(t for _s, _n, t in flat)
    for sect, name, t in flat[:40]:
        bar_pct = (t / max_t) * 100 if max_t else 0
        rows.append(
            f'<tr><td>{html_mod.escape(sect)}</td>'
            f'<td>{html_mod.escape(name)}</td>'
            f'<td>{t:.2f}s</td>'
            f'<td class="bar"><div class="barfill" style="width:{bar_pct:.1f}%"></div></td></tr>'
        )
    return f"""
<section class="block" id="timing">
  <h2><span class="emoji">⏲</span> Timing breakdown — top 40 calls by wall time</h2>
  <div class="sub"><span>longest call dominates the gradient</span></div>
  <table class="timing">
    <thead><tr><th>Section</th><th>Call</th><th>Elapsed</th><th></th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</section>
"""


def main() -> int:
    if not MANIFEST.exists():
        print(f"Missing manifest: {MANIFEST}. Run examples/showcase_full.py first.")
        return 1
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    body_parts = []
    for sect, calls in manifest["sections"].items():
        body_parts.append(render_section(sect, calls))
    body_parts.append(render_timing_table(manifest))

    HTML_OUT.write_text(page(manifest, "\n".join(body_parts)), encoding="utf-8")
    print(f"HTML → {HTML_OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
