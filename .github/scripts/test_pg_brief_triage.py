import json
import unittest

import pg_brief_triage as t


def row(pgid, state="active+clean", up=(0, 1, 2), acting=(0, 1, 2)):
    return {"pgid": pgid, "state": state, "up": list(up), "acting": list(acting),
            "up_primary": up[0] if up else -1, "acting_primary": acting[0] if acting else -1}


def doc(rows):
    return json.dumps({"pg_stats": rows})


class Triage(unittest.TestCase):
    def test_all_settled(self):
        r = t.triage(doc([row("1.0"), row("1.1", "active+clean+scrubbing+deep")]), False, expected_pgs=2)
        self.assertEqual((r["status"], r["settled"], r["pg_count"], r["targets"]), (t.COMPLETE, True, 2, []))

    def test_unsettled_targets_are_sorted_capped_and_counted(self):
        rows = [row("2.a", "active+undersized+degraded"), row("1.10", "peering"), row("1.2"),
                row("1.9", "active+clean+remapped"), row("3.0", "stale+active+clean")]
        r = t.triage(doc(rows), False, max_targets=2)
        self.assertEqual(r["settled"], False)
        self.assertEqual(r["targets"], ["1.9", "1.10"])
        self.assertEqual(r["targets_omitted"], 2)
        self.assertEqual(r["unsettled_by_pool"], {"1": 2, "2": 1, "3": 1})

    def test_clipped_output_is_never_decoded(self):
        text = doc([row("1.0"), row("1.1")])
        r = t.triage(text[:40], True)
        self.assertEqual((r["status"], r["settled"], r["pg_count"]), (t.TRUNCATED, None, None))
        r = t.triage(text[:40], False)
        self.assertEqual((r["status"], r["settled"]), (t.UNPARSEABLE, None))

    def test_parseable_but_flagged_truncated_is_not_complete(self):
        r = t.triage(doc([row("1.0")]), True)
        self.assertEqual((r["status"], r["settled"]), (t.TRUNCATED, None))

    def test_missing_null_and_wrong_types_stay_inconclusive(self):
        for text in ("{}", '{"pg_stats": null}', '{"pg_stats": {}}', '{"pg_stats": []}', "[]", "null"):
            r = t.triage(text, False)
            self.assertEqual((r["status"], r["settled"]), (t.INCONCLUSIVE, None), text)
        for value in (None, "false", 0):
            self.assertEqual(t.triage(doc([row("1.0")]), value)["status"], t.INCONCLUSIVE)
        for value in (None, b"{}", 3):
            self.assertEqual(t.triage(value, False)["status"], t.INCONCLUSIVE)

    def test_malformed_rows_stay_inconclusive(self):
        good = row("1.0")
        for change in ({"pgid": None}, {"pgid": "1"}, {"pgid": "x.1"}, {"state": ""}, {"state": None},
                       {"up": None}, {"acting": [True]}, {"up_primary": "0"}, {"acting_primary": None},
                       {"up_primary": True}):
            r = t.triage(doc([dict(good, **change)]), False)
            self.assertEqual((r["status"], r["settled"]), (t.INCONCLUSIVE, None), change)
        self.assertEqual(t.triage(doc(["1.0"]), False)["status"], t.INCONCLUSIVE)
        missing = dict(good)
        del missing["acting"]
        self.assertEqual(t.triage(doc([missing]), False)["status"], t.INCONCLUSIVE)

    def test_duplicate_and_count_mismatch(self):
        self.assertEqual(t.triage(doc([row("1.0"), row("1.0")]), False)["status"], t.INCONCLUSIVE)
        r = t.triage(doc([row("1.0")]), False, expected_pgs=2)
        self.assertEqual((r["status"], r["settled"]), (t.INCONCLUSIVE, None))

    def test_row_budget_is_arithmetic_on_the_sample(self):
        text = doc([row("1.%x" % i) for i in range(32)])
        r = t.triage(text, False, limit=len(text.encode()) * 2)
        self.assertEqual(r["rows_fitting_limit"], 64)

    def test_brief_cannot_answer_stat_questions(self):
        r = t.triage(doc([row("1.0")]), False)
        self.assertIn("reported_epoch", r["not_answerable_from_brief"])
        self.assertIn("scrub_errors", r["not_answerable_from_brief"])

    def test_bad_arguments(self):
        for kwargs in ({"limit": 0}, {"limit": True}, {"max_targets": -1}, {"expected_pgs": -1}):
            with self.assertRaises(ValueError):
                t.triage(doc([row("1.0")]), False, **kwargs)


if __name__ == "__main__":
    unittest.main()
