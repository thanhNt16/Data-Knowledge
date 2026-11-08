#!/usr/bin/env python3
"""Deterministically shuffle QuizCard :questions= options in docs/<track>/*.md.

Excludes docs/fundamentals/** and docs/dataops/dbt.md.
Parses each JS array via node -e, shuffles each question's options with a
seed derived from file+question index (stable across reruns), recomputes
`correct`, rewrites the line single-line with &quot; escaping preserved,
then re-validates every rewritten binding via node.
"""
import hashlib, json, random, re, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXCLUDE_DIRS = set()
EXCLUDE_FILES = set()
ATTR = ':questions="'


def files():
    out = []
    for p in sorted(ROOT.glob("docs/*/*.md")):
        rel = p.relative_to(ROOT)
        if rel.parts[1] in EXCLUDE_DIRS or rel in EXCLUDE_FILES:
            continue
        out.append(p)
    return out


def js_eval(node_args):
    """Eval a JS expr (stdin) -> JSON list, via node."""
    src, expr = node_args
    r = subprocess.run(
        ["node", "-e",
         "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{"
         f"const v=eval({expr});process.stdout.write(JSON.stringify(v))}});"],
        input=src, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"node eval failed: {r.stderr}\n--- input head: {src[:200]}")
    return json.loads(r.stdout)


def q(s):
    """Serialize a JS string in single quotes, using &quot; for double quotes."""
    out = s.replace("\\", "\\\\").replace("'", "\\'").replace('"', "&quot;")
    out = out.replace("\n", "\\n")
    return f"'{out}'"


def serialize(questions, quoted_keys):
    kp = "'" if quoted_keys else ""
    parts = []
    for it in questions:
        opts = ", ".join(q(o) for o in it["options"])
        parts.append(
            f"{{ {kp}q{kp}: {q(it['q'])}, {kp}options{kp}: [{opts}], "
            f"{kp}correct{kp}: {it['correct']}, {kp}explain{kp}: {q(it['explain'])} }}")
    return "[" + ", ".join(parts) + "]"


def seed_for(path, qi, qtext):
    return int.from_bytes(hashlib.sha256(f"{path}::{qi}::{qtext}".encode()).digest()[:8], "big")


def process(path):
    text = path.read_text()
    lines = text.splitlines(keepends=True)
    changed = False
    rewrites = []
    for i, line in enumerate(lines):
        if ATTR not in line:
            continue
        pre, rest = line.split(ATTR, 1)
        # array runs to the closing '"' of the attribute (last '"' on the line)
        arr_end = rest.rindex('"')
        arr_src, suffix = rest[:arr_end], rest[arr_end:]
        qs = js_eval((arr_src, "s"))
        quoted_keys = bool(re.match(r"\[\s*\{\s*'", arr_src))
        for qi, item in enumerate(qs):
            opts = list(item["options"])
            correct_opt = opts[item["correct"]]
            others = [o for o in opts if o is not correct_opt]
            # content-keyed ordering: stable regardless of current option order
            ckey = lambda s: int.from_bytes(hashlib.sha256(s.encode()).digest()[:8], "big")
            others.sort(key=ckey)
            pos = seed_for(str(path), qi, item["q"]) % len(opts)
            item["options"] = others[:pos] + [correct_opt] + others[pos:]
            item["correct"] = pos
        new_arr = serialize(qs, quoted_keys)
        if new_arr != arr_src:
            lines[i] = pre + ATTR + new_arr + suffix
            changed = True
            rewrites.append(new_arr)
    if changed:
        path.write_text("".join(lines))
    return rewrites


def validate(arrs):
    src = json.dumps(arrs)
    r = subprocess.run(
        ["node", "-e",
         "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{"
         "const arrs=JSON.parse(s);let bad=[],n=0;"
         "for(const [ai,a] of arrs.entries()){const v=eval(a);"
         "for(const [qi,q] of v.entries()){n++;"
         "if(!q||!Array.isArray(q.options)||typeof q.correct!=='number'"
         "||q.correct<0||q.correct>=q.options.length||!q.explain)bad.push(ai+':'+qi)}}"
         "console.log(JSON.stringify({questions:n,bad}))});"],
        input=src, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout)


def main():
    all_rewrites = []
    for p in files():
        rw = process(p)
        print(f"{p.relative_to(ROOT)}: {len(rw)} binding(s) rewritten")
        all_rewrites.extend(rw)
    res = validate(all_rewrites)
    print("validation:", res)
    if res["bad"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
