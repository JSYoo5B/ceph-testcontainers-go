#!/usr/bin/env python3
"""Static inventory of fixed waits in the Go sources (issue 181).

The scanner lists every call site that introduces a fixed wait or time budget
and every Ceph timing option a test sets, with a literal duration when one can
be resolved. It is a source inventory only: it measures nothing at runtime, it
does not know how often a call site runs, and a nominal duration is not elapsed
time. Durations it cannot resolve stay null and are counted separately; they
are never treated as zero.

Categories (what the call site is, not whether it is necessary):

  sleep          time.Sleep: an unconditional pause.
  poll_interval  time.NewTicker / time.Tick: the period of a polling loop.
  timer          time.After / time.NewTimer / time.AfterFunc: a fixed timer,
                 usually the arm of a select; whether it is a retry delay or
                 an upper bound depends on the surrounding code.
  budget         context.WithTimeout / context.WithDeadline: an upper bound
                 that a successful run does not wait out.
  config_timing  a Ceph option literal (Name: "...") whose name looks like a
                 timer, delay, interval, grace, lease or similar and whose value
                 is numeric. The scanner reports the configured value, not the
                 daemon behavior; the unit is the option's own (usually seconds).

Usage:
  wait_inventory.py scan [--root DIR] [--output FILE]
  wait_inventory.py summary [--root DIR] [--top N]
"""
import argparse
from fractions import Fraction
import json
import re
import statistics
import sys
from pathlib import Path

UNITS = {
    "Nanosecond": 1,
    "Microsecond": 1000,
    "Millisecond": 1000000,
    "Second": 1000000000,
    "Minute": 60000000000,
    "Hour": 3600000000000,
}

CALLS = {
    "time.Sleep": ("sleep", 0),
    "time.NewTicker": ("poll_interval", 0),
    "time.Tick": ("poll_interval", 0),
    "time.After": ("timer", 0),
    "time.NewTimer": ("timer", 0),
    "time.AfterFunc": ("timer", 0),
    "context.WithTimeout": ("budget", 1),
    "context.WithDeadline": ("budget", 1),
}

CALL_RE = re.compile(r"(?<![\w.])(" + "|".join(re.escape(c) for c in CALLS) + r")\s*\(")

# Option names that look like a wait. Matched against the literal after Name:.
CONFIG_NAME_RE = re.compile(
    r"^[a-z][a-z0-9_/]*(_interval|_timeout|_sleep|_delay|_grace|_ttl|_period|_lease|_tick|_backoff|_wait|_retry_interval)(_[a-z]+)?$"
)
CONFIG_ENTRY_RE = re.compile(r'Name:\s*"([^"]+)"\s*,\s*Value:\s*"([^"]*)"')
TEMP_CONFIG_RE = re.compile(r'temporaryConfig\(\s*"([^"]+)"\s*,\s*"([^"]*)"\s*\)')

ASSIGN_RE = re.compile(
    r"^\s*(?:(?:const|var)\s+)?([A-Za-z_]\w*)\s*(?:time\.Duration\s*)?=\s*(.+?)\s*$"
)


def blank_comments_and_strings(src):
    """Return (code, text): src with comments blanked; code also blanks string
    and rune literal contents. Newlines and offsets are preserved."""
    n = len(src)
    code = list(src)
    text = list(src)
    i = 0

    def blank(buf, a, b):
        for k in range(a, b):
            if buf[k] != "\n":
                buf[k] = " "

    while i < n:
        c = src[i]
        two = src[i:i + 2]
        if two == "//":
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(code, i, j)
            blank(text, i, j)
            i = j
        elif two == "/*":
            j = src.find("*/", i + 2)
            j = n if j < 0 else j + 2
            blank(code, i, j)
            blank(text, i, j)
            i = j
        elif c == "`":
            j = src.find("`", i + 1)
            j = n if j < 0 else j + 1
            blank(code, i + 1, j - 1)
            i = j
        elif c in "\"'":
            j = i + 1
            while j < n and src[j] != c and src[j] != "\n":
                j += 2 if src[j] == "\\" else 1
            j = min(j + 1, n)
            blank(code, i + 1, j - 1)
            i = j
        else:
            i += 1
    return "".join(code), "".join(text)


def split_args(code, open_idx):
    """Return the top-level argument strings of the call whose '(' is at
    open_idx, or None when the parentheses never balance."""
    depth = 0
    args = []
    start = open_idx + 1
    for k in range(open_idx, len(code)):
        ch = code[k]
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                args.append(code[start:k])
                return [a.strip() for a in args]
        elif ch == "," and depth == 1:
            args.append(code[start:k])
            start = k + 1
    return None


TOKEN_RE = re.compile(r"\s*(?:(\d+(?:\.\d+)?)|([A-Za-z_][\w]*(?:\.[A-Za-z_]\w*)*)|(.))")


def tokenize(expr):
    out = []
    pos = 0
    while pos < len(expr):
        m = TOKEN_RE.match(expr, pos)
        if not m or m.end() == pos:
            break
        pos = m.end()
        if m.group(1):
            out.append(("num", m.group(1)))
        elif m.group(2):
            out.append(("id", m.group(2)))
        elif m.group(3) and not m.group(3).isspace():
            out.append(("op", m.group(3)))
    return out if pos >= len(expr.rstrip()) else None


MAX_DURATION_NS = 2**63 - 1


class Unresolved(Exception):
    pass


def evaluate(expr, consts, depth=0):
    """Evaluate a duration expression to seconds, or raise Unresolved.

    The arithmetic follows Go constant rules for time.Duration: the value is
    counted in nanoseconds, an untyped number is a plain count (so a bare 5 is
    5 ns, not 5 s), the time units are Durations, and Durations multiply as raw
    nanosecond counts (time.Duration(3) * time.Second is 3 s) unless the
    result overflows int64. Only the final value is
    converted to seconds."""
    ns = _eval_ns(expr, consts, depth)[0]
    return float(ns) / 1e9


def _eval_ns(expr, consts, depth):
    """Return (nanoseconds as Fraction, is_duration)."""
    if depth > 8:
        raise Unresolved
    toks = tokenize(expr)
    if toks is None or not toks:
        raise Unresolved
    pos = [0]

    def peek():
        return toks[pos[0]] if pos[0] < len(toks) else (None, None)

    def take():
        t = peek()
        pos[0] += 1
        return t

    def factor():
        kind, val = take()
        if kind == "num":
            return Fraction(val), False
        if kind == "op" and val == "(":
            v = additive()
            if take() != ("op", ")"):
                raise Unresolved
            return v
        if kind == "id":
            if val.startswith("time.") and val[5:] in UNITS:
                return Fraction(UNITS[val[5:]]), True
            if val == "time.Duration":
                if take() != ("op", "("):
                    raise Unresolved
                v = additive()
                if take() != ("op", ")"):
                    raise Unresolved
                if v[1]:
                    return v
                if v[0].denominator != 1:
                    raise Unresolved  # a constant conversion must be integral
                return v[0], True
            if val in consts:
                sub = consts[val]
                if sub is None:
                    raise Unresolved
                return _eval_ns(sub, consts, depth + 1)
        raise Unresolved

    def term():
        v = factor()
        while peek() == ("op", "*"):
            take()
            r = factor()
            v = (v[0] * r[0], v[1] or r[1])
            if abs(v[0]) > MAX_DURATION_NS:
                raise Unresolved  # does not fit a Duration: a Go compile error
        return v

    def additive():
        v = term()
        while peek() in (("op", "+"), ("op", "-")):
            _, op = take()
            r = term()
            v = (v[0] + r[0] if op == "+" else v[0] - r[0], v[1] or r[1])
        return v

    v = additive()
    if pos[0] != len(toks):
        raise Unresolved
    return v


FUNC_RE = re.compile(r"\bfunc\b")
WORD_RE = re.compile(r"[A-Za-z_]\w*")
VAR_DECL_RE = re.compile(r"^\s*var\s+([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)\s+[^=\s][^=]*$")
MUTATE_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*(?:\+=|-=|\*=|/=)")


def shadowed_names(code):
    """Names that some binding other than a single constant definition may
    take: function and closure parameters, results and receivers, short
    variable declarations (including range, if and for clauses), var
    declarations without a value, and compound assignments. The inventory has
    no scopes, so any such name stops resolving in its whole package directory.
    Over-invalidating only turns a duration into null."""
    names = set()
    for m in FUNC_RE.finditer(code):
        i, depth = m.end(), 0
        while i < len(code):
            ch = code[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0 and ch in "{\n;":
                if ch == "\n" and code[m.end():i].strip() == "":
                    i += 1
                    continue
                break
            elif depth > 0:
                j = i
                while j < len(code) and (code[j].isalnum() or code[j] == "_"):
                    j += 1
                if j > i:
                    names.add(code[i:j])
                    i = j
                    continue
            i += 1
    for line in code.splitlines():
        if ":=" in line:
            for part in line.split(":=")[:-1]:
                names.update(WORD_RE.findall(part))
        m = VAR_DECL_RE.match(line)
        if m:
            names.update(WORD_RE.findall(m.group(1)))
        m = MUTATE_RE.match(line)
        if m:
            names.add(m.group(1))
    return names


def collect_consts(files):
    """Map directory -> {name: expr or None}. A name defined more than once
    with different expressions, or shadowed or mutated anywhere in the
    directory (see shadowed_names), is ambiguous (None)."""
    per_dir = {}
    shadow = {}
    for path, text, code in files:
        table = per_dir.setdefault(path.parent, {})
        shadow.setdefault(path.parent, set()).update(shadowed_names(code))
        for line in text.splitlines():
            m = ASSIGN_RE.match(line)
            if not m or "time." not in m.group(2) and not re.fullmatch(r"[\d.\s*+()\w]+", m.group(2)):
                continue
            name, expr = m.group(1), m.group(2).rstrip(",;")
            if name in table and table[name] != expr:
                table[name] = None
            elif name not in table:
                table[name] = expr
    for directory, names in shadow.items():
        for name in names:
            if name in per_dir[directory]:
                per_dir[directory][name] = None
    return per_dir


def deadline_arg(arg):
    m = re.search(r"\.Add\((.*)\)\s*$", arg)
    return m.group(1) if m else None


LOOP_RE = re.compile(r"\bfor\b")
BOUND_RE = re.compile(r"time\.Now\(\)\.After\(|time\.Since\(|\.Done\(\)|\.Err\(\)|\bdeadline\b|\bctx\.|\bcontext\.")
WINDOW = 14


def wait_context(code_lines, line):
    """Heuristic placement of a sleep/timer: 'poll_loop' when a for loop and a
    deadline or context check appear in the preceding lines (a bounded
    condition poll whose sleep is only the retry delay), else 'bare' (a pause
    with no visible condition). Reading the surrounding code is still needed."""
    window = code_lines[max(0, line - 1 - WINDOW):line - 1]
    if any(LOOP_RE.search(w) for w in window) and any(BOUND_RE.search(w) for w in window):
        return "poll_loop"
    return "bare"


def area_of(rel):
    parts = rel.parts
    if parts[0] == "internal" and len(parts) > 1:
        sub = "/".join(parts[:2])
        if sub == "internal/integration":
            return "native-scenarios"
        return sub
    return parts[0] if len(parts) > 1 else "(root)"


def line_of(src, idx):
    return src.count("\n", 0, idx) + 1


def scan(root):
    root = Path(root)
    paths = sorted(
        p for p in root.rglob("*.go")
        if not any(part.startswith(".") or part in ("vendor", "testdata") for part in p.relative_to(root).parts)
    )
    sources = []
    for p in paths:
        src = p.read_text(encoding="utf-8", errors="replace")
        code, text = blank_comments_and_strings(src)
        sources.append((p, src, code, text))
    consts = collect_consts([(p, text, code) for p, _, code, text in sources])
    records = []
    for p, src, code, text in sources:
        rel = p.relative_to(root)
        base = {
            "file": rel.as_posix(),
            "area": area_of(rel),
            "test_file": p.name.endswith("_test.go"),
        }
        table = consts.get(p.parent, {})
        code_lines = code.splitlines()
        for m in CALL_RE.finditer(code):
            name = m.group(1)
            category, index = CALLS[name]
            args = split_args(code, m.end() - 1)
            expr = None
            if args is not None and index < len(args):
                expr = args[index]
                if name == "context.WithDeadline":
                    expr = deadline_arg(expr)
            seconds = None
            if expr:
                try:
                    seconds = evaluate(expr, table)
                except (Unresolved, ZeroDivisionError):
                    seconds = None
            line = line_of(src, m.start())
            rec = dict(base, line=line, kind=name, category=category,
                       expr=" ".join((expr or "").split()) or None, seconds=seconds)
            if category in ("sleep", "timer"):
                rec["context"] = wait_context(code_lines, line)
            records.append(rec)
        seen = set()
        for rx in (CONFIG_ENTRY_RE, TEMP_CONFIG_RE):
            for m in rx.finditer(text):
                key, value = m.group(1), m.group(2)
                if not CONFIG_NAME_RE.match(key) or (m.start(), key) in seen:
                    continue
                seen.add((m.start(), key))
                try:
                    seconds = float(value)
                except ValueError:
                    continue  # booleans and enum values are not durations
                records.append(dict(base, line=line_of(src, m.start()), kind="config:" + key,
                                    category="config_timing", expr=value, seconds=seconds))
    records.sort(key=lambda r: (r["file"], r["line"], r["kind"]))
    return records


def fmt(seconds):
    if seconds is None:
        return "unresolved"
    if seconds >= 60 and seconds % 60 == 0:
        return "%dm" % (seconds // 60)
    if seconds >= 1:
        return ("%g" % seconds) + "s"
    return ("%g" % (seconds * 1000)) + "ms"


def summarize(records, top):
    lines = []
    cats = ["sleep", "poll_interval", "timer", "budget", "config_timing"]
    native = [r for r in records if r["area"] == "native-scenarios"]
    lines.append("Call sites: %d total, %d in native scenarios." % (len(records), len(native)))
    lines.append("")
    lines.append("| Area | Scope | " + " | ".join(cats) + " | unresolved |")
    lines.append("| --- | --- | " + " | ".join("---:" for _ in cats) + " | ---: |")
    areas = sorted({(r["area"], r["test_file"]) for r in records})
    for area, is_test in areas:
        rows = [r for r in records if r["area"] == area and r["test_file"] == is_test]
        counts = [sum(1 for r in rows if r["category"] == c) for c in cats]
        unresolved = sum(1 for r in rows if r["seconds"] is None)
        lines.append("| %s | %s | %s | %d |" % (area, "test" if is_test else "non-test",
                                               " | ".join(str(c) for c in counts), unresolved))
    lines.append("")
    lines.append("Resolved durations per category (all areas; per call site, not per execution):")
    lines.append("")
    lines.append("| Category | Sites | Resolved | Min | Median | Max |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
    for c in cats:
        rows = [r for r in records if r["category"] == c]
        vals = [r["seconds"] for r in rows if r["seconds"] is not None]
        if vals:
            lines.append("| %s | %d | %d | %s | %s | %s |" % (c, len(rows), len(vals), fmt(min(vals)),
                                                            fmt(statistics.median(vals)), fmt(max(vals))))
        else:
            lines.append("| %s | %d | 0 | - | - | - |" % (c, len(rows)))
    lines.append("")
    lines.append("Sleep and timer placement in native scenarios (heuristic, see wait_context):")
    lines.append("")
    lines.append("| Category | poll_loop sites | bare sites |")
    lines.append("| --- | ---: | ---: |")
    for c in ("sleep", "timer"):
        rows = [r for r in native if r["category"] == c]
        lines.append("| %s | %d | %d |" % (c, sum(1 for r in rows if r["context"] == "poll_loop"),
                                           sum(1 for r in rows if r["context"] == "bare")))
    lines.append("")
    for c, ctxname in (("sleep", "bare"), ("sleep", "poll_loop"), ("timer", "bare"), ("poll_interval", None)):
        rows = sorted((r for r in native if r["category"] == c and r["seconds"] is not None
                       and (ctxname is None or r["context"] == ctxname)),
                      key=lambda r: -r["seconds"])[:top]
        c = c if ctxname is None else "%s (%s)" % (c, ctxname)
        lines.append("Longest resolved %s sites in native scenarios:" % c)
        lines.append("")
        for r in rows:
            lines.append("- %s %s:%d `%s`" % (fmt(r["seconds"]), r["file"], r["line"], r["expr"]))
        if not rows:
            lines.append("- none")
        lines.append("")
    cfg = {}
    for r in records:
        if r["category"] == "config_timing":
            cfg.setdefault(r["kind"][len("config:"):], []).append(r)
    lines.append("Timing options set by tests:")
    lines.append("")
    lines.append("| Option | Sites | Values |")
    lines.append("| --- | ---: | --- |")
    for key in sorted(cfg):
        values = sorted({r["expr"] for r in cfg[key]})
        lines.append("| `%s` | %d | %s |" % (key, len(cfg[key]), ", ".join("`%s`" % v for v in values)))
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("scan", "summary"):
        p = sub.add_parser(name)
        p.add_argument("--root", default=".")
        if name == "scan":
            p.add_argument("--output")
        else:
            p.add_argument("--top", type=int, default=10)
    args = ap.parse_args(argv)
    records = scan(args.root)
    if args.cmd == "scan":
        out = json.dumps(records, indent=1, sort_keys=True) + "\n"
        if args.output:
            Path(args.output).write_text(out)
        else:
            sys.stdout.write(out)
    else:
        sys.stdout.write(summarize(records, args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())
