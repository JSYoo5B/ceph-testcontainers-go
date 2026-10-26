import tempfile
import unittest
from pathlib import Path

import wait_inventory as wi


def scan_files(files):
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        for name, body in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body)
        return wi.scan(root)


def only(records, kind):
    got = [r for r in records if r["kind"] == kind]
    assert len(got) == 1, got
    return got[0]


class EvaluateTest(unittest.TestCase):
    def test_literals_and_units(self):
        for expr, want in [
            ("time.Second", 1.0), ("2 * time.Minute", 120.0), ("500*time.Millisecond", 0.5),
            ("(1 + 2) * time.Second", 3.0), ("time.Duration(3) * time.Second", 3.0),
            ("1.5 * time.Second", 1.5), ("time.Minute - 30*time.Second", 30.0),
        ]:
            self.assertAlmostEqual(wi.evaluate(expr, {}), want, msg=expr)

    def test_named_constants_resolve_and_chain(self):
        consts = {"a": "2 * time.Second", "b": "3 * a"}
        self.assertEqual(wi.evaluate("b", consts), 6.0)

    def test_unknown_or_ambiguous_is_unresolved_never_zero(self):
        for expr, consts in [("delay", {}), ("x", {"x": None}), ("", {}), ("time.Bogus", {}),
                             ("2 *", {}), ("(1 + 2", {}), ("r.bounds.Query", {})]:
            with self.assertRaises(wi.Unresolved, msg=expr):
                wi.evaluate(expr, consts)

    def test_constant_cycle_is_unresolved(self):
        with self.assertRaises(wi.Unresolved):
            wi.evaluate("a", {"a": "b", "b": "a"})


class MaskTest(unittest.TestCase):
    def test_comments_and_strings_are_not_calls(self):
        src = (
            "package p\n"
            "// time.Sleep(time.Second)\n"
            "/* time.Sleep(time.Second) */\n"
            'var s = "time.Sleep(time.Second)"\n'
            "var r = `time.Sleep(time.Second)`\n"
            "func f() { time.Sleep(2 * time.Second) }\n"
        )
        recs = scan_files({"a.go": src})
        self.assertEqual([(r["kind"], r["line"], r["seconds"]) for r in recs],
                         [("time.Sleep", 6, 2.0)])

    def test_escaped_quote_does_not_end_string(self):
        src = 'package p\nvar s = "a\\" time.Sleep(time.Second)"\nfunc f() { time.Sleep(time.Millisecond) }\n'
        recs = scan_files({"a.go": src})
        self.assertEqual([r["line"] for r in recs], [3])


class ScanTest(unittest.TestCase):
    def test_categories_and_arguments(self):
        src = """package p
const pause = 4 * time.Second
func f(ctx context.Context) {
	time.Sleep(pause)
	t := time.NewTicker(250 * time.Millisecond)
	_ = t
	select { case <-time.After(time.Second): }
	c, cancel := context.WithTimeout(ctx, 2*time.Minute)
	_, _ = context.WithDeadline(ctx, time.Now().Add(7*time.Second))
	_ = c; _ = cancel
	time.AfterFunc(3*time.Second, func() {})
}
"""
        recs = scan_files({"a.go": src})
        got = {r["kind"]: (r["category"], r["seconds"]) for r in recs}
        self.assertEqual(got, {
            "time.Sleep": ("sleep", 4.0),
            "time.NewTicker": ("poll_interval", 0.25),
            "time.After": ("timer", 1.0),
            "context.WithTimeout": ("budget", 120.0),
            "context.WithDeadline": ("budget", 7.0),
            "time.AfterFunc": ("timer", 3.0),
        })

    def test_context_argument_is_not_taken_as_the_duration(self):
        recs = scan_files({"a.go": "package p\nfunc f(ctx C) { context.WithTimeout(ctx, time.Second) }\n"})
        self.assertEqual(only(recs, "context.WithTimeout")["seconds"], 1.0)

    def test_unresolved_stays_null(self):
        recs = scan_files({"a.go": "package p\nfunc f(delay D) { time.Sleep(delay) }\n"})
        r = only(recs, "time.Sleep")
        self.assertIsNone(r["seconds"])
        self.assertEqual(r["expr"], "delay")

    def test_unbalanced_call_is_null_not_zero(self):
        recs = scan_files({"a.go": "package p\nfunc f() { time.Sleep(\n"})
        self.assertIsNone(only(recs, "time.Sleep")["seconds"])
        self.assertIsNone(only(recs, "time.Sleep")["expr"])

    def test_ambiguous_constant_in_same_directory_is_unresolved(self):
        recs = scan_files({
            "a.go": "package p\nfunc f() {\n\tvar delay = time.Second\n\ttime.Sleep(delay)\n}\n",
            "b.go": "package p\nfunc g() {\n\tvar delay = time.Minute\n\ttime.Sleep(delay)\n}\n",
        })
        self.assertEqual([r["seconds"] for r in recs], [None, None])

    def test_single_definition_resolves(self):
        recs = scan_files({"a.go": "package p\nfunc f() {\n\tvar delay = time.Second\n\ttime.Sleep(delay)\n}\n"})
        self.assertEqual(only(recs, "time.Sleep")["seconds"], 1.0)

    def test_constants_do_not_leak_across_directories(self):
        recs = scan_files({
            "x/a.go": "package x\nconst d = time.Second\n",
            "y/b.go": "package y\nfunc f() { time.Sleep(d) }\n",
        })
        self.assertIsNone(only(recs, "time.Sleep")["seconds"])

    def test_area_and_test_scope(self):
        recs = scan_files({
            "internal/integration/n_test.go": "package i\nfunc f() { time.Sleep(time.Second) }\n",
            "ceph/c.go": "package c\nfunc f() { time.Sleep(time.Second) }\n",
            "internal/tools/t.go": "package t\nfunc f() { time.Sleep(time.Second) }\n",
        })
        self.assertEqual({(r["file"], r["area"], r["test_file"]) for r in recs}, {
            ("internal/integration/n_test.go", "native-scenarios", True),
            ("ceph/c.go", "ceph", False),
            ("internal/tools/t.go", "internal/tools", False),
        })

    def test_hidden_vendor_and_testdata_are_skipped(self):
        recs = scan_files({
            ".hidden/a.go": "package p\nfunc f() { time.Sleep(time.Second) }\n",
            "vendor/a.go": "package p\nfunc f() { time.Sleep(time.Second) }\n",
            "testdata/a.go": "package p\nfunc f() { time.Sleep(time.Second) }\n",
        })
        self.assertEqual(recs, [])

    def test_method_named_like_a_call_is_not_matched(self):
        recs = scan_files({"a.go": "package p\nfunc f() { clock.time.Sleep(1); mytime.Sleep(1) }\n"})
        self.assertEqual(recs, [])


class ContextTest(unittest.TestCase):
    def test_sleep_in_bounded_loop_is_poll_loop(self):
        src = """package p
func f() {
	deadline := time.Now().Add(time.Minute)
	for {
		if ok() { break }
		if time.Now().After(deadline) { return }
		time.Sleep(time.Second)
	}
}
"""
        self.assertEqual(only(scan_files({"a.go": src}), "time.Sleep")["context"], "poll_loop")

    def test_loop_without_a_bound_check_is_bare(self):
        src = "package p\nfunc f() {\n\tfor i := 0; i < 3; i++ {\n\t\ttime.Sleep(time.Second)\n\t}\n}\n"
        self.assertEqual(only(scan_files({"a.go": src}), "time.Sleep")["context"], "bare")

    def test_bound_without_loop_is_bare(self):
        src = "package p\nfunc f(ctx C) {\n\t_ = ctx.Err()\n\ttime.Sleep(time.Second)\n}\n"
        self.assertEqual(only(scan_files({"a.go": src}), "time.Sleep")["context"], "bare")

    def test_loop_in_a_comment_does_not_count(self):
        src = "package p\nfunc f(ctx C) {\n\t// for each retry check ctx.Err()\n\ttime.Sleep(time.Second)\n}\n"
        self.assertEqual(only(scan_files({"a.go": src}), "time.Sleep")["context"], "bare")

    def test_budget_has_no_context_field(self):
        recs = scan_files({"a.go": "package p\nfunc f(c C) { context.WithTimeout(c, time.Second) }\n"})
        self.assertNotIn("context", recs[0])


class ConfigTest(unittest.TestCase):
    def test_numeric_timing_options_are_listed(self):
        src = """package p
var a = ConfigSetting{Section: "osd", Name: "osd_recovery_sleep", Value: "0.5"}
var b = temporaryConfig("mgr/volumes/snapshot_clone_delay", "30")
"""
        recs = scan_files({"a.go": src})
        got = {r["kind"]: r["seconds"] for r in recs if r["category"] == "config_timing"}
        self.assertEqual(got, {"config:osd_recovery_sleep": 0.5, "config:mgr/volumes/snapshot_clone_delay": 30.0})

    def test_non_numeric_and_non_timing_options_are_excluded(self):
        src = """package p
var a = ConfigSetting{Name: "mon_osd_adjust_down_out_interval", Value: "false"}
var b = ConfigSetting{Name: "osd_op_queue", Value: "wpq"}
var c = ConfigSetting{Name: "osd_pool_default_size", Value: "3"}
"""
        self.assertEqual(scan_files({"a.go": src}), [])

    def test_option_in_comment_is_ignored(self):
        src = '// {Name: "osd_recovery_sleep", Value: "0.5"}\npackage p\n'
        self.assertEqual(scan_files({"a.go": src}), [])


class SummaryTest(unittest.TestCase):
    def test_unresolved_is_counted_and_not_summed(self):
        recs = scan_files({"internal/integration/n_test.go":
                           "package i\nfunc f(d D) { time.Sleep(d); time.Sleep(3 * time.Second) }\n"})
        out = wi.summarize(recs, 5)
        self.assertIn("| native-scenarios | test | 2 | 0 | 0 | 0 | 0 | 1 |", out)
        self.assertIn("| sleep | 0 | 2 |", out)
        self.assertIn("| sleep | 2 | 1 | 3s | 3s | 3s |", out)

    def test_empty_inventory(self):
        out = wi.summarize([], 5)
        self.assertIn("Call sites: 0 total, 0 in native scenarios.", out)


if __name__ == "__main__":
    unittest.main()
