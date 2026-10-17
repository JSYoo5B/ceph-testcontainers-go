"""Test the offline replay of the declaration selection without git or Go."""

import unittest
from unittest.mock import patch

import replay_declaration_selection as replay


class ReplayTests(unittest.TestCase):
    def test_merges_take_the_first_parent_as_base_and_the_second_as_head(self):
        log = ("m1\tb1 h1\tMerge pull request #119 from x/y\n"
               "m2\tb2\tsquashed commit (#98)\n"
               "m3\tb3 h3\tMerge pull request #117 from x/z\n").encode()
        with patch.object(replay, "git", return_value=log) as git:
            self.assertEqual(replay.merges("origin/main", 3), [
                ("m1", "b1", "h1", "Merge pull request #119 from x/y"),
                ("m3", "b3", "h3", "Merge pull request #117 from x/z")])
        self.assertEqual(git.call_args.args[:5], ("log", "--merges", "--first-parent", "-n", "3"))

    def test_rows_and_table(self):
        graph = {"fallback": [], "changed_declarations": ["a", "b"], "selected": {"short-x": "r"},
                 "selected_profiles": 1, "total_profiles": 86}
        fallback = {"fallback": ["go.mod changed", "x"], "changed_declarations": [], "selected": {"short-x": "r"},
                    "selected_profiles": 86, "total_profiles": 86}
        failed = {"error": "the head's planner failed", "fallback": [], "changed_declarations": [], "selected": {},
                  "selected_profiles": 0, "total_profiles": 0}
        rows = [replay.row("0123456789", "Merge pull request #12 from a/b", graph),
                replay.row("abcdef0123", "Merge branch 'x'", fallback),
                replay.row("fedcba9876", "Merge pull request #13", failed)]
        self.assertEqual(rows[0], {"merge": "0123456", "pull_request": "#12", "selected": 1, "total": 86,
                                   "why": "2 changed declarations", "selected_batches": ["short-x"]})
        self.assertEqual((rows[1]["pull_request"], rows[1]["why"], rows[1]["selected_batches"]),
                         ("", "go.mod changed", []))
        self.assertEqual(replay.table(rows).splitlines()[2:], [
            "| 0123456 | #12 | 1 of 86 | 2 changed declarations: short-x |",
            "| abcdef0 |  | 86 of 86 | go.mod changed |",
            "| fedcba9 | #13 | 0 of 0 | the head's planner failed |"])


if __name__ == "__main__":
    unittest.main()
