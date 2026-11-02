"""Run the corpus through the Go oracle and write the differential fixture.

    python3 regen.py <oracle binary> <corpus.txt> <datasets.json> <out.jsonl>

Each output row is {"e": expr, "d": dataset, "t": Go type, "v": Go %v, "err": true if
evaluation failed, "r": passes the restrictive policy (absent when the expression does not
pass default validation)}. Rows that are the same for every dataset collapse to "d": "*".
"""

import json
import subprocess
import sys
from collections import defaultdict

# Intentional deviations from Go (see the ingot-expr crate docs); kept out of the fixture.
SKIP = {
    "'''multi",
    'line\'\'\' == "multi\\nline"',
    "date(event.ts)",
    'now_plus("0") > now_minus("1ns")',
    '"" + [1, "a", null]',
    '"""x"y""" == \'x"y\'',
    'b"" + 1',
    "{null: 1}",
}


def main(oracle, corpus, datasets_path, out):
    datasets = json.load(open(datasets_path))
    exprs = [l.rstrip("\n") for l in open(corpus) if l.strip() and not l.startswith("#")]
    inputs = [(e, name) for e in exprs for name in datasets]
    stdin = "".join(json.dumps({"expr": e, "data": datasets[name]}) + "\n" for e, name in inputs)
    res = subprocess.run([oracle], input=stdin, capture_output=True, text=True, check=True)
    outs = [json.loads(l) for l in res.stdout.splitlines()]
    assert len(outs) == len(inputs), (len(outs), len(inputs))

    rows, seen = [], set()
    for (e, name), o in zip(inputs, outs):
        assert o["expr"] == e
        if e in SKIP or "panic" in o:
            continue
        row = {"e": e, "d": name, "t": o["type"], "v": o["v"]}
        if "err" in o:
            row["err"] = True
        if "checkerr" not in o:
            row["r"] = o["restrictive"]
        key = json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            rows.append(row)

    by_expr = defaultdict(list)
    for r in rows:
        by_expr[r["e"]].append(r)
    with open(out, "w") as f:
        for rs in by_expr.values():
            same = {json.dumps({k: v for k, v in r.items() if k != "d"}, sort_keys=True) for r in rs}
            if len(same) == 1 and len(rs) == len(datasets):
                rs = [dict(rs[0], d="*")]
            for r in rs:
                f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")
    print(f"{len(exprs)} expressions x {len(datasets)} datasets -> {out}")


if __name__ == "__main__":
    main(*sys.argv[1:])
