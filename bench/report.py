#!/usr/bin/env python3
"""Turn bench/matrix.sh output into charts and tables (no dependencies).

    python3 bench/report.py bench/results/matrix.jsonl

Writes docs/benchmarks/{throughput,latency}.svg and regenerates the results section of
docs/benchmarks.md (between the `results:start` / `results:end` markers).
"""

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "docs" / "benchmarks"
DOC = ROOT / "docs" / "benchmarks.md"

TARGETS = ["ingot", "inngest"]
BACKENDS = ["sqlite", "postgres", "redis", "redis-postgres"]
BACKEND_LABEL = {"sqlite": "SQLite", "postgres": "Postgres", "redis": "Redis", "redis-postgres": "Redis + Postgres"}

# Categorical slots 1-2 (validated light + dark), chart chrome and ink tokens.
STYLE = """
  svg { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }
  .surface { fill: #fcfcfb; }
  .s1 { fill: #2a78d6; } .s2 { fill: #eb6834; }
  .t1 { fill: #0b0b0b; } .t2 { fill: #52514e; } .t3 { fill: #898781; }
  .grid { stroke: #e1e0d9; } .axis { stroke: #c3c2b7; }
  @media (prefers-color-scheme: dark) {
    .surface { fill: #1a1a19; }
    .s1 { fill: #3987e5; } .s2 { fill: #d95926; }
    .t1 { fill: #f0efec; } .t2 { fill: #c3c2b7; }
    .grid { stroke: #2c2c2a; } .axis { stroke: #383835; }
  }
"""


def load(path):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    runs = [r for r in rows if r["mode"] != "memory"]
    mem = {}
    for r in rows:
        if r["mode"] == "memory":
            k = (r["target"], r["backend"])
            mem[k] = max(mem.get(k, 0), r["peak_anon_mb"])
    versions = {r["target"]: r["version"] for r in runs}
    return runs, mem, versions


def median(runs, target, backend, mode, kind, field):
    vals = [
        r[field]
        for r in runs
        if (r["target"], r["backend"], r["mode"], r["kind"]) == (target, backend, mode, kind)
        and r.get(field) is not None
    ]
    return statistics.median(vals) if vals else None


def ticks(v):
    """Round axis ticks (steps of 1/2/5 x 10^k) covering 0..v."""
    k = 1
    while k * 10 <= v / 5:
        k *= 10
    step = next(m * k for m in (1, 2, 2.5, 5, 10) if m * k >= v / 5)
    n = int(-(-v // step))
    return [step * i for i in range(n + 1)]


def compact(v):
    if v >= 1000:
        s = f"{v / 1000:.2f}".rstrip("0").rstrip(".")
        return f"{s}K"
    return f"{v:g}"


def fmt(v):
    return f"{v:,.0f}" if v >= 10 or v == int(v) else f"{v:,.1f}"


def bar_chart(title, subtitle, panels, unit, labels, path):
    """Horizontal grouped bars: one group per backend, one bar per target, one panel per measure."""
    W, left, right = 680, 128, 64
    plot_w = W - left - right
    bar_h, gap, group_gap = 16, 2, 18
    group_h = len(TARGETS) * bar_h + (len(TARGETS) - 1) * gap
    panel_h = 34 + len(BACKENDS) * group_h + (len(BACKENDS) - 1) * group_gap + 26
    top = 92
    H = top + len(panels) * panel_h + 8
    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
        f'role="img" aria-label="{title}">',
        f"<style>{STYLE}</style>",
        f'<rect class="surface" width="{W}" height="{H}" rx="8"/>',
        f'<text class="t1" x="24" y="34" font-size="17" font-weight="600">{title}</text>',
        f'<text class="t2" x="24" y="56" font-size="13">{subtitle}</text>',
    ]
    # Legend.
    lx = 24
    for i, t in enumerate(TARGETS):
        o.append(f'<rect class="s{i + 1}" x="{lx}" y="68" width="12" height="12" rx="3"/>')
        o.append(f'<text class="t2" x="{lx + 18}" y="78.5" font-size="13">{labels[t]}</text>')
        lx += 30 + 7.2 * len(labels[t])

    y0 = top
    for ptitle, values in panels:
        tick_vals = ticks(max(v for v in values.values() if v is not None) * 1.08)
        scale = plot_w / tick_vals[-1]
        o.append(
            f'<text x="24" y="{y0 + 14}" font-size="14"><tspan class="t1" font-weight="600">{ptitle}</tspan>'
            f'<tspan class="t3" dx="6">{unit}</tspan></text>'
        )
        gy0, gy1 = y0 + 28, y0 + panel_h - 26
        for j, v in enumerate(tick_vals):
            x = left + v * scale
            o.append(f'<line class="{"axis" if j == 0 else "grid"}" x1="{x:.1f}" x2="{x:.1f}" y1="{gy0}" y2="{gy1}" stroke-width="1"/>')
            o.append(f'<text class="t3" x="{x:.1f}" y="{gy1 + 16}" font-size="11" text-anchor="middle" style="font-variant-numeric: tabular-nums">{compact(v)}</text>')
        y = gy0 + 6
        for b in BACKENDS:
            o.append(f'<text class="t2" x="{left - 10}" y="{y + group_h / 2 + 4.5}" font-size="13" text-anchor="end">{BACKEND_LABEL[b]}</text>')
            for i, t in enumerate(TARGETS):
                v = values.get((t, b))
                by = y + i * (bar_h + gap)
                if v is None:
                    o.append(f'<text class="t3" x="{left + 6}" y="{by + 12}" font-size="12">n/a</text>')
                    continue
                w = max(v * scale, 3)
                r = min(4, w / 2)
                # Rounded data end, square at the baseline.
                o.append(
                    f'<path class="s{i + 1}" d="M{left},{by} h{w - r:.1f} a{r},{r} 0 0 1 {r},{r} '
                    f'v{bar_h - 2 * r} a{r},{r} 0 0 1 -{r},{r} h-{w - r:.1f} z">'
                    f"<title>{labels[t]} · {BACKEND_LABEL[b]}: {fmt(v)} {unit}</title></path>"
                )
                o.append(f'<text class="t1" x="{left + w + 6:.1f}" y="{by + 12}" font-size="12" style="font-variant-numeric: tabular-nums">{fmt(v)}</text>')
            y += group_h + group_gap
        y0 += panel_h
    o.append("</svg>")
    path.write_text("\n".join(o) + "\n")


def tables(runs, mem, versions, labels):
    def cell(t, b, mode, kind, field):
        v = median(runs, t, b, mode, kind, field)
        return "–" if v is None else fmt(v)

    def ratio(b, mode, kind, field, higher_better=True):
        a = median(runs, "ingot", b, mode, kind, field)
        r = median(runs, "inngest", b, mode, kind, field)
        if not a or not r:
            return "–"
        x = a / r if higher_better else r / a
        return f"**{x:,.0f}×**" if x >= 10 else f"**{x:,.1f}×**"

    reps = max(r["rep"] for r in runs)
    n = max(r["n"] for r in runs if r["mode"] == "throughput")
    out = []
    out.append(f"Medians of {reps} repetitions. Throughput runs send {n:,} events in batches of 200.\n")
    out.append("### Throughput (runs/s, higher is better)\n")
    out.append(f"| Backend | Function | {labels['ingot']} | {labels['inngest']} | Ingot advantage |")
    out.append("|---|---|--:|--:|--:|")
    for b in BACKENDS:
        for kind, kl in (("one", "no steps"), ("three", "3 steps")):
            out.append(
                f"| {BACKEND_LABEL[b]} | {kl} | {cell('ingot', b, 'throughput', kind, 'runs_per_sec')} "
                f"| {cell('inngest', b, 'throughput', kind, 'runs_per_sec')} | {ratio(b, 'throughput', kind, 'runs_per_sec')} |"
            )
    rate = next((r for r in runs if r["mode"] == "latency"), None)
    if rate:
        out.append("\n### Latency at light load (ms, lower is better)\n")
        out.append(
            "Event sent → last step of a 3-step function finished, "
            f"{rate['n']} events sent one at a time at a steady rate (so p99 is the slowest run).\n"
        )
        out.append("| Backend | Server | p50 | p95 | p99 |")
        out.append("|---|---|--:|--:|--:|")
        for b in BACKENDS:
            for t in TARGETS:
                out.append(
                    f"| {BACKEND_LABEL[b]} | {labels[t]} | "
                    + " | ".join(cell(t, b, "latency", "three", f) for f in ("p50_ms", "p95_ms", "p99_ms"))
                    + " |"
                )
    out.append("\n### Resource use\n")
    out.append(
        "CPU is the total CPU time spent by the server container plus the database container "
        "(Postgres/Redis), divided by runs completed, during the 3-step throughput run. Memory is the "
        "server container's peak anonymous (heap) memory; it excludes the "
        "database container.\n"
    )
    out.append("| Backend | Server | CPU per run (ms) | Peak server memory (MB) |")
    out.append("|---|---|--:|--:|")
    for b in BACKENDS:
        for t in TARGETS:
            c = median(runs, t, b, "throughput", "three", "cpu_ms_per_run")
            m = mem.get((t, b))
            out.append(
                f"| {BACKEND_LABEL[b]} | {labels[t]} | {'–' if c is None else f'{c:.2f}'} | {'–' if m is None else m} |"
            )
    return "\n".join(out)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else ROOT / "bench" / "results" / "matrix.jsonl"
    runs, mem, versions = load(src)
    iv = versions.get("inngest", "").split("-")[0]
    labels = {"ingot": f"Ingot {versions.get('ingot', '')}".strip(), "inngest": f"Inngest {iv}".strip()}
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    def series(mode, kind, field):
        return {(t, b): median(runs, t, b, mode, kind, field) for t in TARGETS for b in BACKENDS}

    bar_chart(
        "Throughput",
        "Completed function runs per second, 2,000-event burst (higher is better)",
        [("Function with no steps", series("throughput", "one", "runs_per_sec")),
         ("Function with 3 steps", series("throughput", "three", "runs_per_sec"))],
        "runs/s",
        labels,
        OUT_DIR / "throughput.svg",
    )
    bar_chart(
        "Latency at light load",
        "Event sent → 3-step function finished, steady 5 events/s (lower is better)",
        [("Median (p50)", series("latency", "three", "p50_ms")),
         ("p95", series("latency", "three", "p95_ms"))],
        "ms",
        labels,
        OUT_DIR / "latency.svg",
    )

    doc = DOC.read_text()
    start, end = "<!-- results:start -->", "<!-- results:end -->"
    a, b = doc.index(start) + len(start), doc.index(end)
    DOC.write_text(doc[:a] + "\n" + tables(runs, mem, versions, labels) + "\n" + doc[b:])
    print(f"wrote {OUT_DIR}/throughput.svg, {OUT_DIR}/latency.svg, {DOC}")


if __name__ == "__main__":
    main()
