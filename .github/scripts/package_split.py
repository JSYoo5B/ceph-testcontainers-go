#!/usr/bin/env python3
"""Load and validate the package-split rewrite shards.

The retained native producer sources were recorded before the public API moved
between packages. Comparing them with today's source applies a recorded list of
rewrites first. Each package move owns one shard,
.github/scripts/fixtures/package-split/<move-id>.json, so two moves never edit
the same file. See docs/contributing/package-split-shards.md.

A shard is a JSON object:

    {
     "description": "what this move did and why the rewrites are exact",
     "identifiers": {"old.Name": "new.Name"},
     "calls": [["regular expression", "replacement"]],
     "note": "optional prose"
    }

`description` is required and at least one of `identifiers` and `calls` must be
non-empty. Any other field is refused.

The effective rewrite is defined by the stages, not by the shard order:

1. Every identifier of every shard is applied as a qualified-name rewrite, the
   longest key first and equal lengths in key order. Keys are unique across the
   shards and no target may contain a key, so the identifiers form a set.
2. Every call is a regular expression applied after all identifiers. The calls
   of the shards are concatenated in shard-name order, each shard's calls in
   its own order. A set of shards is accepted only when the result does not
   depend on that shard order for the retained producer sources.

The directory is read with lstat and never follows a symbolic link. Every
problem is reported; a rejected layout never yields a partial result.
"""
import itertools
import json
import os
import re
import stat
import sys

SHARD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "package-split")
CORPUS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
CORPUS_SUFFIX = ".source.go.txt"
SHARD_NAME = re.compile(r"([a-z0-9][a-z0-9-]{0,63})\.json")
QUALIFIED = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+")
FIELDS = ("description", "identifiers", "calls", "note")
MAX_BYTES = 256 * 1024
# Above this many shards that carry calls, every permutation is too many to
# apply; the check then applies every ordered pair of shards.
MAX_PERMUTED = 6


class ShardError(Exception):
    """One or more problems that make the layout unusable."""

    def __init__(self, problems):
        self.problems = list(problems)
        super().__init__("\n".join(self.problems))


class Shard:
    def __init__(self, name, description, identifiers, calls, note):
        self.name = name
        self.description = description
        self.identifiers = identifiers
        self.calls = calls
        self.note = note


def shard_stem(name):
    """The move identifier of a file name, or None unless it is exactly <move-id>.json."""
    match = SHARD_NAME.fullmatch(name)
    return match.group(1) if match else None


def _no_duplicate_keys(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            raise ValueError("duplicate JSON key %r" % key)
        seen[key] = value
    return seen


def _refuse_constant(token):
    raise ValueError("non-standard JSON constant %s" % token)


def parse_shard(name, text):
    """Return a Shard for one file's text, or raise ShardError."""
    problems = []
    try:
        data = json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_refuse_constant)
    except ValueError as err:
        raise ShardError(["%s: invalid JSON: %s" % (name, err)])
    if not isinstance(data, dict):
        raise ShardError(["%s: the top level must be an object" % name])
    for field in sorted(set(data) - set(FIELDS)):
        problems.append("%s: unknown field %r" % (name, field))
    for field in ("description", "note"):
        if field in data and (not isinstance(data[field], str) or not data[field].strip()):
            problems.append("%s: %s must be a non-empty string" % (name, field))
    if "description" not in data:
        problems.append("%s: missing required field 'description'" % name)

    identifiers = data.get("identifiers", {})
    if not isinstance(identifiers, dict):
        problems.append("%s: identifiers must be an object" % name)
        identifiers = {}
    for old, new in identifiers.items():
        if not isinstance(new, str) or not QUALIFIED.fullmatch(old) or not QUALIFIED.fullmatch(new):
            problems.append("%s: identifier %r -> %r must map one qualified name to another" % (name, old, new))
        elif old == new:
            problems.append("%s: identifier %r maps to itself" % (name, old))

    calls = data.get("calls", [])
    clean_calls = []
    if not isinstance(calls, list):
        problems.append("%s: calls must be a list" % name)
        calls = []
    for index, call in enumerate(calls):
        label = "%s: calls[%d]" % (name, index)
        if not isinstance(call, list) or len(call) != 2 or not all(isinstance(item, str) for item in call):
            problems.append("%s must be a [pattern, replacement] pair of strings" % label)
            continue
        pattern, replacement = call
        if not pattern:
            problems.append("%s has an empty pattern" % label)
            continue
        try:
            re.compile(pattern).sub(replacement, "")
        except re.error as err:
            problems.append("%s is not a valid rewrite: %s" % (label, err))
            continue
        clean_calls.append((pattern, replacement))

    if not problems and not identifiers and not clean_calls:
        problems.append("%s: an empty shard rewrites nothing; give it identifiers or calls" % name)
    if problems:
        raise ShardError(problems)
    return Shard(name, data["description"], dict(identifiers), clean_calls, data.get("note"))


def discover(directory=SHARD_DIR):
    """Return the sorted shard file names, or raise ShardError.

    Every directory entry must be a regular file named exactly <move-id>.json;
    anything else (a README, a symbolic link, a subdirectory, a name with a
    trailing newline) is refused rather than skipped, so a misnamed shard is
    never silently ignored.
    """
    try:
        entries = sorted(os.listdir(directory))
    except FileNotFoundError:
        raise ShardError(["%s: the shard directory does not exist" % directory])
    problems = []
    names = []
    for entry in entries:
        if shard_stem(entry) is None:
            problems.append("%r: not a shard name; use <move-id>.json (lowercase a-z, 0-9 and -, at most 64 characters before .json)" % entry)
            continue
        mode = os.lstat(os.path.join(directory, entry)).st_mode
        if not stat.S_ISREG(mode):
            problems.append("%r: a shard must be a regular file, not a link or directory" % entry)
            continue
        names.append(entry)
    if problems:
        raise ShardError(problems)
    if not names:
        raise ShardError(["%s: no shards" % directory])
    return names


def load_shards(directory=SHARD_DIR):
    """Read every shard in `directory` in name order, or raise ShardError."""
    problems = []
    shards = []
    for name in discover(directory):
        path = os.path.join(directory, name)
        if os.lstat(path).st_size > MAX_BYTES:
            problems.append("%s: larger than %d bytes" % (name, MAX_BYTES))
            continue
        with open(path, encoding="utf-8", newline="") as handle:
            text = handle.read()
        try:
            shards.append(parse_shard(name, text))
        except ShardError as err:
            problems.extend(err.problems)
    if problems:
        raise ShardError(problems)
    return shards


def identifier_pattern(key):
    return re.compile(r"(?<![\w.])" + re.escape(key) + r"\b")


def check_identifiers(shards):
    """Problems for duplicate keys and for any chain between identifiers."""
    problems = []
    owner = {}
    for shard in shards:
        for key in shard.identifiers:
            if key in owner:
                problems.append("identifier %r is defined by both %s and %s" % (key, owner[key], shard.name))
            else:
                owner[key] = shard.name
    patterns = {key: identifier_pattern(key) for key in owner}
    for shard in shards:
        for key, target in sorted(shard.identifiers.items()):
            for other, pattern in sorted(patterns.items()):
                if pattern.search(target):
                    problems.append(
                        "chain: the target %r of %r (%s) contains the key %r (%s); "
                        "edit the earlier shard's target in the same pull request instead"
                        % (target, key, shard.name, other, owner[other]))
    return problems


def ordered_identifiers(shards):
    """Every identifier as (old, new): longest key first, equal lengths by key."""
    merged = {}
    for shard in shards:
        merged.update(shard.identifiers)
    return sorted(merged.items(), key=lambda item: (-len(item[0]), item[0]))


def ordered_calls(shards):
    """The calls of the shards, concatenated in shard-name order."""
    return [call for shard in sorted(shards, key=lambda s: s.name) for call in shard.calls]


def apply_identifiers(source, identifiers):
    for old, new in identifiers:
        source = identifier_pattern(old).sub(lambda _match, new=new: new, source)
    return source


def apply_calls(source, calls):
    for pattern, replacement in calls:
        source = re.sub(pattern, replacement, source)
    return source


def apply_shards(source, shards):
    """Rewrite `source` with the effective rewrites (before gofmt)."""
    return apply_calls(apply_identifiers(source, ordered_identifiers(shards)), ordered_calls(shards))


def check_order_independence(shards, corpus):
    """Problems when the result depends on the order of the shards' calls.

    Identifiers are applied as a set (see the module text), so only the calls
    can depend on shard order. Two checks run. First, no call's replacement
    text may be matched by a call of another shard (a call whose output feeds
    another), whatever the corpus contains. Second, the calls of the shards are
    applied in every order to every source in `corpus` after the identifiers
    (with more than MAX_PERMUTED shards that carry calls, in every ordered pair
    instead) and all orders must give the same text. Two calls that rewrite
    overlapping text without feeding each other are detected only when the
    corpus contains that text.
    """
    problems = []
    seen = {}
    for shard in shards:
        for pattern, _ in shard.calls:
            if pattern in seen:
                problems.append("call pattern %r is defined by both %s and %s" % (pattern, seen[pattern], shard.name))
            seen[pattern] = shard.name
    carrying = [shard for shard in sorted(shards, key=lambda s: s.name) if shard.calls]
    problems.extend(_call_output_feeds_another_shard(carrying))
    if len(carrying) < 2:
        return problems
    identifiers = ordered_identifiers(shards)
    if len(carrying) <= MAX_PERMUTED:
        orders = list(itertools.permutations(range(len(carrying))))
    else:
        orders = [pair + tuple(i for i in range(len(carrying)) if i not in pair)
                  for pair in itertools.permutations(range(len(carrying)), 2)]
    for label, source in corpus:
        base = apply_identifiers(source, identifiers)
        # Applying one shard's calls to the same text is deterministic, so the
        # intermediate texts are shared between the orders.
        memo = {}

        def step(text, index):
            key = (text, index)
            if key not in memo:
                memo[key] = apply_calls(text, carrying[index].calls)
            return memo[key]

        reference = base
        for index in range(len(carrying)):
            reference = step(reference, index)
        for order in orders:
            result = base
            for index in order:
                result = step(result, index)
            if result != reference:
                problems.append("order dependence: applying the calls in the order %s gives a different %s than the name order"
                                % (", ".join(carrying[i].name for i in order), label))
                break
    return problems


def _call_output_feeds_another_shard(carrying):
    """Problems where a call's replacement text matches a call of another shard.

    A group reference in the replacement is replaced by a plain identifier
    first. This needs no sample text, so it also covers calls that the
    retained sources never exercise.
    """
    problems = []
    for shard in carrying:
        for pattern, replacement in shard.calls:
            produced = re.sub(r"\\(?:\d+|g<\w+>)", "x", replacement)
            for other in carrying:
                if other is shard:
                    continue
                for other_pattern, _ in other.calls:
                    if re.search(other_pattern, produced):
                        problems.append(
                            "order dependence: the replacement %r of %s (pattern %r) is matched by the call pattern %r of %s"
                            % (replacement, shard.name, pattern, other_pattern, other.name))
    return problems


def load_corpus(directory=CORPUS_DIR):
    """The retained producer sources the rewrites apply to, as (name, text)."""
    names = sorted(name for name in os.listdir(directory) if name.endswith(CORPUS_SUFFIX))
    corpus = []
    for name in names:
        with open(os.path.join(directory, name), encoding="utf-8", newline="") as handle:
            corpus.append((name, handle.read()))
    return corpus


def validate(shards, corpus):
    problems = check_identifiers(shards)
    problems.extend(check_order_independence(shards, corpus))
    return problems


def load_effective(directory=SHARD_DIR, corpus=None):
    """Load, validate and return the shards, or raise ShardError."""
    shards = load_shards(directory)
    problems = validate(shards, load_corpus() if corpus is None else corpus)
    if problems:
        raise ShardError(problems)
    return shards


def main(argv=None):
    try:
        shards = load_effective()
    except ShardError as err:
        for problem in err.problems:
            print("package-split: " + problem, file=sys.stderr)
        return 1
    print("package-split: %d shards, %d identifiers, %d calls"
          % (len(shards), len(ordered_identifiers(shards)), len(ordered_calls(shards))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
