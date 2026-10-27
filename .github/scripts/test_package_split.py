"""Exercise the package-split shard loader and its validation; never start Docker."""

import itertools
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import unittest

import package_split as ps


SCRIPTS = Path(__file__).resolve().parent
SHARDS = SCRIPTS / "fixtures" / "package-split"
BASELINE = SCRIPTS / "fixtures" / "migration" / "package-split.single-file-7015c62.json"

GOOD = {"description": "move one"}


def shard(**fields):
    return dict(GOOD, **fields)


def legacy_apply(split, source):
    """The rewrite exactly as the single-file reader applied it (before gofmt)."""
    for old, new in sorted(split["identifiers"].items(), key=lambda item: -len(item[0])):
        source = re.sub(r"(?<![\w.])" + re.escape(old) + r"\b", new, source)
    for pattern, replacement in split["calls"]:
        source = re.sub(pattern, replacement, source)
    return source


class Layout:
    """A temporary shard directory: {file name: object or raw text}."""

    def __init__(self, test, files):
        self.dir = tempfile.mkdtemp()
        test.addCleanup(shutil.rmtree, self.dir, True)
        for name, content in files.items():
            text = content if isinstance(content, str) else json.dumps(content)
            with open(os.path.join(self.dir, name), "w", encoding="utf-8", newline="") as handle:
                handle.write(text)


def problems_of(layout, corpus=()):
    try:
        shards = ps.load_shards(layout.dir)
    except ps.ShardError as err:
        return err.problems
    return ps.validate(shards, list(corpus))


class ShardFiles(unittest.TestCase):
    def accepted(self, files, corpus=()):
        self.assertEqual(problems_of(Layout(self, files), corpus), [])

    def refused(self, files, fragment, corpus=()):
        found = problems_of(Layout(self, files), corpus)
        self.assertTrue(any(fragment in problem for problem in found), (fragment, found))

    def test_control_table(self):
        ident = {"identifiers": {"a.Old": "b.New"}}
        accepted = {
            "identifiers only": {"one.json": shard(**ident)},
            "calls only": {"one.json": shard(calls=[["x", "y"]])},
            "note allowed": {"one.json": shard(note="n", **ident)},
            "disjoint keys in two shards": {"one.json": shard(**ident),
                                            "two.json": shard(identifiers={"a.Other": "b.Other"})},
            "digit and hyphen name": {"0-a-1.json": shard(**ident)},
        }
        for case, files in accepted.items():
            with self.subTest(case=case):
                self.accepted(files)
        refused = [
            ("duplicate key across shards", {"one.json": shard(**ident), "two.json": shard(identifiers={"a.Old": "b.Other"})}, "defined by both"),
            ("duplicate key with equal target", {"one.json": shard(**ident), "two.json": shard(**ident)}, "defined by both"),
            ("chain across shards", {"one.json": shard(**ident), "two.json": shard(identifiers={"b.New": "c.New"})}, "chain"),
            ("chain in reverse name order", {"one.json": shard(identifiers={"b.New": "c.New"}), "two.json": shard(**ident)}, "chain"),
            ("chain inside one shard", {"one.json": shard(identifiers={"a.Old": "b.New", "b.New": "c.New"})}, "chain"),
            ("target contains a key", {"one.json": shard(identifiers={"a.Old": "b.New.Sub"}), "two.json": shard(identifiers={"b.New": "c.New"})}, "chain"),
            ("identity rewrite", {"one.json": shard(identifiers={"a.Old": "a.Old"})}, "maps to itself"),
            ("unknown field", {"one.json": shard(extra=1, **ident)}, "unknown field 'extra'"),
            ("legacy field name", {"one.json": shard(role_image_routing_note="n", **ident)}, "unknown field"),
            ("missing description", {"one.json": {"identifiers": {"a.Old": "b.New"}}}, "missing required field 'description'"),
            ("blank description", {"one.json": {"description": " ", "calls": [["x", "y"]]}}, "non-empty string"),
            ("empty shard", {"one.json": shard()}, "empty shard"),
            ("empty maps", {"one.json": shard(identifiers={}, calls=[])}, "empty shard"),
            ("null identifiers", {"one.json": shard(identifiers=None)}, "identifiers must be an object"),
            ("unqualified key", {"one.json": shard(identifiers={"Old": "b.New"})}, "qualified name"),
            ("non-string target", {"one.json": shard(identifiers={"a.Old": 3})}, "qualified name"),
            ("bad call shape", {"one.json": shard(calls=[["x"]])}, "pair of strings"),
            ("empty call pattern", {"one.json": shard(calls=[["", "y"]])}, "empty pattern"),
            ("invalid regular expression", {"one.json": shard(calls=[["(", "y"]])}, "not a valid rewrite"),
            ("dangling group reference", {"one.json": shard(calls=[["(a)", "\\2"]])}, "not a valid rewrite"),
            ("duplicate call pattern", {"one.json": shard(calls=[["x", "y"]]), "two.json": shard(calls=[["x", "z"]])}, "call pattern"),
            ("duplicate JSON key", {"one.json": '{"description": "d", "identifiers": {"a.Old": "b.A", "a.Old": "b.B"}}'}, "duplicate JSON key"),
            ("NaN constant", {"one.json": '{"description": "d", "calls": [["x", "y"]], "note": NaN}'}, "non-standard JSON constant"),
            ("not an object", {"one.json": "[]"}, "top level must be an object"),
            ("invalid JSON", {"one.json": "{"}, "invalid JSON"),
            ("empty file", {"one.json": ""}, "invalid JSON"),
        ]
        for case, files, fragment in refused:
            with self.subTest(case=case):
                self.refused(files, fragment)

    def test_file_names_are_matched_exactly(self):
        body = shard(identifiers={"a.Old": "b.New"})
        for name in ("One.json", "one.json.bak", "one.JSON", "one json.json", "-one.json", "one.json\n", "one\n.json",
                     "one.json ", ".json", "README.md", "_one.json", "x" * 65 + ".json", "é.json"):
            with self.subTest(name=name):
                self.refused({name: body}, "not a shard name")
        # The maximum length and a bare digit are accepted.
        self.accepted({"1.json": body})
        self.accepted({"a" * 64 + ".json": body})
        # One misnamed file is refused even next to a good one, never skipped.
        self.refused({"good.json": body, "stray.txt": "x"}, "not a shard name")

    def test_links_and_directories_are_refused(self):
        layout = Layout(self, {"real.json": shard(identifiers={"a.Old": "b.New"})})
        os.symlink("real.json", os.path.join(layout.dir, "link.json"))
        os.mkdir(os.path.join(layout.dir, "dir.json"))
        found = problems_of(layout)
        self.assertEqual(sorted(p.split(":")[0] for p in found), ["'dir.json'", "'link.json'"])
        self.assertTrue(all("regular file" in p for p in found))

    def test_missing_and_empty_directories_are_refused(self):
        with self.assertRaisesRegex(ps.ShardError, "does not exist"):
            ps.load_shards(os.path.join(tempfile.gettempdir(), "package-split-absent-" + str(os.getpid())))
        with self.assertRaisesRegex(ps.ShardError, "no shards"):
            ps.load_shards(Layout(self, {}).dir)

    def test_every_problem_is_reported_not_just_the_first(self):
        found = problems_of(Layout(self, {"a.json": shard(extra=1, identifiers={"a.Old": "b.New"}),
                                          "b.json": shard(), "c.json": "{"}))
        self.assertGreaterEqual(len(found), 3, found)

    def test_shard_stem(self):
        self.assertEqual(ps.shard_stem("rgw-multisite.json"), "rgw-multisite")
        for name in ("rgw-multisite.json\n", "Rgw.json", "a/b.json", "", "a.json.json"):
            self.assertIsNone(ps.shard_stem(name), name)


class OrderIndependence(unittest.TestCase):
    SOURCE = "x := make(); x.Alpha(ctx)\n"

    def shards(self, **calls):
        out = []
        for name, pairs in sorted(calls.items()):
            out.append(ps.Shard(name + ".json", "d", {}, pairs, None))
        return out

    def check(self, shards, source=None):
        return ps.check_order_independence(shards, [("corpus", self.SOURCE if source is None else source)])

    def test_commuting_calls_are_accepted(self):
        shards = self.shards(a=[(r"\.Alpha\(", ".A(")], b=[(r"\.Beta\(", ".B(")])
        self.assertEqual(self.check(shards, "x.Alpha(ctx)\nx.Beta(ctx)\n"), [])

    def test_a_call_whose_output_feeds_another_is_refused(self):
        # a.json turns Alpha into Beta and b.json turns Beta into Gamma: the
        # result is Gamma in name order and Beta in the other order.
        shards = self.shards(a=[(r"\.Alpha\(", ".Beta(")], b=[(r"\.Beta\(", ".Gamma(")])
        found = self.check(shards)
        self.assertEqual(len(found), 2, found)
        self.assertTrue(all("order dependence" in problem for problem in found), found)
        self.assertTrue(any("b.json, a.json" in problem for problem in found), found)
        # The static check needs no sample text at all.
        static = ps.check_order_independence(shards, [])
        self.assertEqual(len(static), 1, static)
        self.assertIn("is matched by the call pattern", static[0])
        # A group reference in the replacement is not mistaken for text.
        grouped = self.shards(a=[(r"(\w+)\.Alpha\(", r"\1.Beta(")], b=[(r"^zzz$", "y")])
        self.assertEqual(ps.check_order_independence(grouped, []), [])

    def test_calls_that_rewrite_the_same_text_are_refused_on_the_corpus(self):
        shards = self.shards(a=[(r"\.Alpha\(", ".A(")], b=[(r"\.Alpha\(", ".B(")])
        # Identical patterns are refused outright, with or without a corpus.
        self.assertIn("call pattern", ps.check_order_independence(shards, [])[0])
        overlap = self.shards(a=[(r"\.Alpha\(ctx\)", ".A(ctx)")], b=[(r"\.Alpha\(", ".B(")])
        found = self.check(overlap)
        self.assertEqual(len(found), 1, found)
        self.assertIn("order dependence", found[0])

    def test_dependence_within_one_shard_is_the_shards_own_order(self):
        shards = self.shards(a=[(r"\.Alpha\(", ".Beta("), (r"\.Beta\(", ".Gamma(")])
        self.assertEqual(self.check(shards), [])

    def test_a_shard_without_calls_never_takes_part(self):
        shards = self.shards(a=[(r"\.Alpha\(", ".Beta(")], b=[])
        self.assertEqual(self.check(shards), [])

    def test_many_shards_are_checked_pairwise(self):
        count = ps.MAX_PERMUTED + 2
        # Every shard rewrites the same text, with a distinct pattern each.
        overlapping = self.shards(**{"s%02d" % i: [(r"\.Alpha(?=\(%s)" % ("c" * (i + 1)), ".A%d" % i)] for i in range(count)})
        found = self.check(overlapping, "x.Alpha(" + "c" * count + ")\n")
        self.assertTrue(found and "order dependence" in found[0], found)
        independent = self.shards(**{"s%02d" % i: [(r"\.T%d\(" % i, ".U%d(" % i)] for i in range(count)})
        self.assertEqual(self.check(independent, "x.T0(ctx)\n"), [])

    def test_the_corpus_decides(self):
        # Interference the corpus never exercises is not detected: the check is
        # stated to cover the retained producer sources only.
        shards = self.shards(a=[(r"\.Alpha\(ctx\)", ".A(ctx)")], b=[(r"\.Alpha\(", ".B(")])
        self.assertEqual(self.check(shards, "x.Other(ctx)\n"), [])
        self.assertEqual(len(self.check(shards, "x.Alpha(ctx)\n")), 1)

    def test_calls_are_concatenated_in_shard_name_order_whatever_the_load_order(self):
        z = ps.Shard("z.json", "d", {}, [("z1", "r"), ("z2", "r")], None)
        a = ps.Shard("a.json", "d", {}, [("a1", "r"), ("a2", "r")], None)
        want = [("a1", "r"), ("a2", "r"), ("z1", "r"), ("z2", "r")]
        self.assertEqual(ps.ordered_calls([z, a]), want)
        self.assertEqual(ps.ordered_calls([a, z]), want)

    def test_identifiers_do_not_depend_on_the_shard_order(self):
        keys = {"p.Foo": "q.Foo", "p.FooBar": "q.FooBar", "p.FooBarBaz": "q.Baz", "r.X": "s.X"}
        shards = [ps.Shard("%d.json" % i, "d", {k: v}, [], None) for i, (k, v) in enumerate(keys.items())]
        source = "p.Foo p.FooBar p.FooBarBaz r.X p.Foo.Bar x.p.Foo\n"
        want = ps.apply_shards(source, shards)
        for order in itertools.permutations(shards):
            self.assertEqual(ps.apply_shards(source, list(order)), want)
        # Qualified boundaries: no rewrite inside a longer name or a selector.
        self.assertEqual(want, "q.Foo q.FooBar q.Baz s.X q.Foo.Bar x.p.Foo\n")


class RepositoryLayout(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shards = ps.load_effective()
        cls.corpus = ps.load_corpus()

    def test_the_checked_in_layout_is_accepted(self):
        self.assertEqual(ps.validate(self.shards, self.corpus), [])
        self.assertEqual(ps.main(), 0)

    def test_the_corpus_is_the_retained_producers_and_which_calls_it_exercises(self):
        names = [name for name, _ in self.corpus]
        self.assertIn("cephfs-quiescence-native.source.go.txt", names)
        self.assertIn("cephfs-recovery-native.source.go.txt", names)
        rewritten = {name: ps.apply_identifiers(text, ps.ordered_identifiers(self.shards)) for name, text in self.corpus}
        # Only these calls change the retained sources; the others are covered
        # by the synthetic lines of the migration test and the static check.
        exercised = [pattern for pattern, _ in ps.ordered_calls(self.shards)
                     if any(re.search(pattern, text) for text in rewritten.values())]
        self.assertEqual(sorted(exercised), sorted([
            r"\b([A-Za-z_]\w*)\.(ControlContainer|ClientContainer)Context\(ctx\)",
            r"\b([A-Za-z_]\w*)\.StartCephFS\(ctx\)",
            r"\bcephfsmirror\.Run\(ctx, source\.ControlImage\(\), ",
        ]))

    def test_every_shard_documents_its_move(self):
        for shard_ in self.shards:
            self.assertTrue(ps.shard_stem(shard_.name))
            self.assertGreater(len(shard_.description), 40, shard_.name)

    def broken(self, mutate):
        """The checked-in shards copied to a scratch directory and mutated."""
        files = {path.name: json.loads(path.read_text()) for path in SHARDS.iterdir()}
        mutate(files)
        return problems_of(Layout(self, files), self.corpus)

    def test_mutated_copies_of_the_real_layout_are_refused(self):
        def duplicate(files):
            files["rbd-mirror.json"]["identifiers"]["multicluster.RunRGWMultisite"] = "rbdmirror.Run2"

        def chain(files):
            files["rbd-mirror.json"]["identifiers"]["rbd.Namespace"] = "rbdmirror.Namespace2"
            files["ceph-to-rbd.json"]["identifiers"]["ceph.RBDNamespace"] = "rbd.Namespace"

        def interfering_call(files):
            files["rgw-multisite.json"]["calls"] = [[r"\bcephfs\.Start\(ctx, ", "cephfs.Begin(ctx, "]]

        def unknown(files):
            files["rbd-mirror.json"]["extra"] = True

        def empty(files):
            files["rbd-mirror.json"] = {"description": "x" * 50}

        for name, mutate, fragment in (("duplicate key", duplicate, "defined by both"),
                                       ("chain", chain, "chain"),
                                       ("call consuming another call's output", interfering_call, "order dependence"),
                                       ("unknown field", unknown, "unknown field"),
                                       ("empty shard", empty, "empty shard")):
            with self.subTest(mutation=name):
                found = self.broken(mutate)
                self.assertTrue(any(fragment in problem for problem in found), found)
        self.assertEqual(self.broken(lambda files: None), [])


class MigrationFromTheSingleFile(unittest.TestCase):
    """Pins the shards to the single file that preceded them.

    The baseline is package-split.json as it stood at 7015c62. This test proves
    the migration lost and added nothing. Delete it with the baseline file in
    the first change that deliberately retargets an existing key; adding a
    shard never needs to touch either.
    """

    @classmethod
    def setUpClass(cls):
        cls.base = json.loads(BASELINE.read_text())
        cls.shards = ps.load_effective()
        cls.corpus = ps.load_corpus()

    def test_the_effective_identifiers_are_the_old_ones_in_the_old_order(self):
        old = sorted(self.base["identifiers"].items(), key=lambda item: -len(item[0]))
        self.assertEqual(len(old), 131)
        self.assertEqual(ps.ordered_identifiers(self.shards), old)

    def test_the_effective_calls_are_the_old_calls(self):
        old = [tuple(call) for call in self.base["calls"]]
        new = ps.ordered_calls(self.shards)
        self.assertEqual(len(old), 10)
        self.assertEqual(sorted(new), sorted(old))
        # Only the cross-shard order changed. Say so, and prove it harmless
        # below, rather than leaving the permutation implicit.
        self.assertNotEqual(new, old)

    def test_each_shard_keeps_the_old_relative_order_of_its_calls(self):
        old = [tuple(call) for call in self.base["calls"]]
        for shard_ in self.shards:
            positions = [old.index(call) for call in shard_.calls]
            self.assertEqual(positions, sorted(positions), shard_.name)

    def test_the_old_call_order_and_the_new_one_rewrite_identically(self):
        identifiers = ps.ordered_identifiers(self.shards)
        old = [tuple(call) for call in self.base["calls"]]
        new = ps.ordered_calls(self.shards)
        extra = "\n".join(self.synthetic_call_inputs())
        for name, text in self.corpus + [("synthetic", extra)]:
            base = ps.apply_identifiers(text, identifiers)
            self.assertEqual(ps.apply_calls(base, new), ps.apply_calls(base, old), name)
        # Swapping any two of the ten calls also leaves the synthetic lines alone.
        base = ps.apply_identifiers(extra, identifiers)
        want = ps.apply_calls(base, old)
        for i, j in itertools.permutations(range(len(old)), 2):
            swapped = list(old)
            swapped[i], swapped[j] = swapped[j], swapped[i]
            self.assertEqual(ps.apply_calls(base, swapped), want, (old[i][0], old[j][0]))

    def synthetic_call_inputs(self):
        """One line per call that the call matches, plus lines that must not change."""
        lines = [
            "fs, err := src.StartCephFS(ctx)",
            "fs, err := src.StartCephFSWithConfig(ctx, cfg)",
            "gw, err := src.StartRGW(ctx)",
            "gw, err := src.StartRGWWithConfig(ctx, cfg)",
            "err := src.RemoveRGW(ctx, gw)",
            "c, err := src.ControlContainerContext(ctx)",
            "c, err := src.ClientContainerContext(ctx)",
            "names := src.Filesystems()",
            "names := src.Gateways()",
            "err := src.InitRBDPool(ctx, pool)",
            "m, err := multicluster.RunCephFSMirror(ctx, source.ControlImage(), cfg)",
            "m, err := cephfsmirror.Run(ctx, source.ControlImage(), cfg)",
            "keep := untouched.Filesystems2()",
        ]
        return lines

    def test_the_whole_rewrite_equals_the_old_algorithm(self):
        extra = "\n".join(self.synthetic_call_inputs()) + "\n" + "\n".join(self.base["identifiers"]) + "\n"
        for name, text in self.corpus + [("synthetic", extra)]:
            self.assertEqual(ps.apply_shards(text, self.shards), legacy_apply(self.base, text), name)
        # The corpus really is rewritten, so equality is not vacuous.
        for name, text in self.corpus:
            self.assertNotEqual(legacy_apply(self.base, text), text, name)

    def test_no_entry_is_lost_added_or_moved_between_keys(self):
        merged = {}
        for shard_ in self.shards:
            merged.update(shard_.identifiers)
        self.assertEqual(merged, self.base["identifiers"])
        self.assertEqual(sum(len(s.identifiers) for s in self.shards), len(self.base["identifiers"]))

    def test_the_prose_is_kept(self):
        notes = [s.note for s in self.shards if s.note]
        self.assertEqual(notes, [self.base["role_image_routing_note"]])
        self.assertTrue(all(s.description for s in self.shards))

    def test_the_baseline_itself_has_no_chain_or_duplicate(self):
        # The layout rules would have accepted the single file as one shard.
        shard_ = ps.Shard("base.json", "d", self.base["identifiers"], [tuple(c) for c in self.base["calls"]], None)
        self.assertEqual(ps.check_identifiers([shard_]), [])


if __name__ == "__main__":
    unittest.main()
