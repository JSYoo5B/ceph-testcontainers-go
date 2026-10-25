"""Test the dispatch-inventory join on synthetic documents, without GitHub.

The realistic case takes its dispatch document from the producer itself
(tag_scenarios.py, through the producer's own test fixtures). Jobs have the
shape of the Actions jobs API: a called workflow's jobs are named
"<caller> / <name>".
"""

import copy
import io
import json
from pathlib import Path
import random
import shutil
import tempfile
import unittest
from unittest.mock import patch

import dispatch_join as join
import scenario_history
import tag_scenarios
import test_tag_scenarios as producer

REPOSITORY = "owner/repository"
RUN, ATTEMPT = 700, 2
CALLER = "Tentacle short"


def entry(name, identifier=None, repetition="1", network="", **extra):
    return {"id": identifier or name + "-1a2b3c4d", "name": name, "category": "short", "batch": name.split(" ")[0],
            "package": "./internal/integration", "network": network, "tests": "", "repetition": repetition,
            "artifact_suffix": "", "job_timeout": 35, **extra}


def dispatch(*entries, **context):
    return {"schema": join.DISPATCH_SCHEMA, "networks": "default", "repetitions": 1, "selected_profiles": len(entries),
            "context": {"repository": REPOSITORY, "sha": "c" * 40, "run_id": str(RUN), "run_attempt": str(ATTEMPT),
                        "category": "short", "release": "tentacle", **context},
            "count": len(entries), "entries": list(entries)}


def job(identifier, name, caller=CALLER, **extra):
    return {"id": identifier, "run_id": RUN, "run_attempt": ATTEMPT, "status": "completed", "conclusion": "success",
            "name": (caller + " / " + name) if caller else name,
            "url": "https://api.example.test/repos/" + REPOSITORY + "/actions/jobs/" + str(identifier), **extra}


def jobs_page(*jobs, total=None):
    return {"total_count": len(jobs) if total is None else total, "jobs": list(jobs)}


def receipt_row(job_id, profile, repetition="1", network="", commit="d" * 40, tree="e" * 40, run=RUN, attempt=ATTEMPT):
    row = {"schema": scenario_history.SCHEMA,
           "execution_key": {"repository": REPOSITORY, "run_id": str(run), "run_attempt": str(attempt),
                             "job_id": str(job_id), "profile_id": profile, "repetition_id": repetition,
                             "network": network},
           "source": {"checkout_commit": commit if commit is not None else scenario_history.MISSING,
                      "checkout_tree": tree}}
    row["row_sha256"] = scenario_history.row_hash(row)
    return row


def command(directory, document, jobs, *extra):
    root = Path(directory)
    (root / "dispatch.json").write_text(json.dumps(document))
    (root / "jobs.json").write_text(json.dumps(jobs))
    with patch("sys.stdout", new=io.StringIO()), patch("sys.stderr", new=io.StringIO()):
        return join.main(["--dispatch", str(root / "dispatch.json"), "--jobs", str(root / "jobs.json"),
                          "--output", str(root / "report.json"), *extra])


def ids(records, key="entry"):
    return sorted(record[key]["id"] for record in records)


class ProducerContractTests(unittest.TestCase):
    def test_constants_follow_the_producer(self):
        self.assertEqual(join.DISPATCH_SCHEMA, tag_scenarios.DISPATCH_SCHEMA)
        self.assertEqual(set(join.ENTRY_STRINGS) | set(join.ENTRY_INTEGERS), set(tag_scenarios.DISPATCH_FIELDS))

    def test_a_real_inventory_joins_to_its_own_job_names(self):
        """A campaign over all networks with repetitions, as the producer emits
        it: every entry matches exactly one job; the Plan job is an unmatched
        actual job, not a dispatch member."""
        environment = {"GITHUB_RUN_ID": str(RUN), "GITHUB_RUN_ATTEMPT": str(ATTEMPT), "SCENARIO_RELEASE": "tentacle",
                       "SCENARIO_CATEGORY": "short", "GITHUB_REPOSITORY": REPOSITORY, "GITHUB_SHA": "c" * 40}
        case = producer.DispatchInventoryTests("test_workflow_metadata_binds_the_inventory")
        _, _, _, document = case.plan("all", ("--representative", "--repetitions", "2"), environment, themes=True)
        self.assertGreater(document["count"], 4)
        jobs = [job(1, "Plan")] + [job(10 + index, item["name"]) for index, item in enumerate(document["entries"])]
        report = join.join(document, [jobs_page(*jobs)])
        self.assertEqual(len(report["matched"]), document["count"])
        self.assertEqual([item["name"] for item in report["unmatched_actual"]], [CALLER + " / Plan"])
        for key in ("conflicts", "ambiguous", "unmatched_expected", "invalid_expected", "invalid_actual", "out_of_scope"):
            self.assertEqual(report[key], [], key)
        self.assertTrue(report["conservation"]["ok"])
        self.assertEqual(report["scope"]["caller"], CALLER)
        # Repetition suffixes and the network suffix are part of the name.
        self.assertTrue(any(" #2" in item["entry"]["name"] for item in report["matched"]))
        self.assertTrue(any("(bridge)" in item["entry"]["name"] for item in report["matched"]))


class BucketTests(unittest.TestCase):
    def setUp(self):
        self.entries = [entry("alpha"), entry("beta"), entry("gamma")]
        self.jobs = [job(1, "alpha"), job(2, "beta"), job(3, "gamma")]

    def run_join(self, entries=None, jobs=None, **keywords):
        return join.join(dispatch(*(self.entries if entries is None else entries)),
                         [jobs_page(*(self.jobs if jobs is None else jobs))], **keywords)

    def test_complete_one_to_one_join(self):
        report = self.run_join()
        self.assertEqual(len(report["matched"]), 3)
        self.assertTrue(report["conservation"]["ok"])
        self.assertEqual([item["receipt"]["state"] for item in report["matched"]], ["not_provided"] * 3)

    def test_missing_job_is_unmatched_expected_and_extra_job_is_unmatched_actual(self):
        report = self.run_join(jobs=[job(1, "alpha"), job(2, "beta"), job(9, "stray")])
        self.assertEqual([item["name"] for item in report["unmatched_expected"]], ["gamma"])
        self.assertEqual([item["id"] for item in report["unmatched_actual"]], [9])
        self.assertEqual(len(report["matched"]), 2)
        self.assertTrue(report["conservation"]["ok"])

    def test_empty_inventory_leaves_every_job_unmatched(self):
        report = self.run_join(entries=[])
        self.assertEqual(len(report["unmatched_actual"]), 3)
        self.assertEqual(report["matched"], [])
        self.assertTrue(report["conservation"]["ok"])

    def test_empty_job_list_leaves_every_entry_unmatched(self):
        report = self.run_join(jobs=[])
        self.assertEqual(len(report["unmatched_expected"]), 3)
        self.assertTrue(report["conservation"]["ok"])

    def test_shared_names_are_ambiguous_and_keep_every_member(self):
        for entries, jobs, shape in (
                ([entry("alpha")], [job(1, "alpha"), job(5, "alpha")], (1, 2)),
                ([entry("alpha"), entry("alpha", "other-id")], [job(1, "alpha")], (2, 1)),
                ([entry("alpha"), entry("alpha", "other-id")], [job(1, "alpha"), job(5, "alpha")], (2, 2))):
            with self.subTest(shape=shape):
                report = self.run_join(entries, jobs)
                self.assertEqual(report["matched"], [])
                self.assertEqual(len(report["ambiguous"]), 1)
                self.assertEqual((len(report["ambiguous"][0]["entries"]), len(report["ambiguous"][0]["jobs"])), shape)
                self.assertTrue(report["conservation"]["ok"])

    def test_names_must_match_exactly(self):
        report = self.run_join([entry("alpha")], [job(1, "alpha (bridge)"), job(2, "alpha #2"), job(3, "Alpha")])
        self.assertEqual(report["matched"], [])
        self.assertEqual(len(report["unmatched_actual"]), 3)
        self.assertEqual(len(report["unmatched_expected"]), 1)

    def test_order_of_either_input_does_not_change_the_buckets(self):
        entries = [entry("alpha"), entry("beta"), entry("beta", "second"), entry("delta")]
        jobs = [job(1, "alpha"), job(2, "beta"), job(3, "beta"), job(4, "extra"), job(5, "Plan", caller="Other")]

        def shape(report):
            return {"matched": sorted((item["entry"]["name"], item["job"]["id"]) for item in report["matched"]),
                    "ambiguous": sorted((item["name"], tuple(sorted(j["id"] for j in item["jobs"])))
                                        for item in report["ambiguous"]),
                    "expected": sorted(item["name"] for item in report["unmatched_expected"]),
                    "actual": sorted(item["id"] for item in report["unmatched_actual"]),
                    "scope": sorted(item["id"] for item in report["out_of_scope"])}
        reference = shape(self.run_join(entries, jobs))
        self.assertEqual(reference["matched"], [("alpha", 1)])
        random.seed(7)
        for _ in range(12):
            shuffled_entries, shuffled_jobs = random.sample(entries, len(entries)), random.sample(jobs, len(jobs))
            self.assertEqual(shape(self.run_join(shuffled_entries, shuffled_jobs)), reference)


class InconclusiveFieldTests(unittest.TestCase):
    def test_each_entry_field_missing_null_or_wrong_typed_is_invalid_never_matched(self):
        for field in join.ENTRY_STRINGS + join.ENTRY_INTEGERS:
            for label, change in (("missing", None), ("null", ""), ("wrong type", 5.5 if field in join.ENTRY_INTEGERS else 7)):
                with self.subTest(field=field, case=label):
                    broken = entry("alpha")
                    if label == "missing":
                        del broken[field]
                    else:
                        broken[field] = None if label == "null" else change
                    report = join.join(dispatch(broken), [jobs_page(job(1, "alpha"))])
                    self.assertEqual(report["matched"], [])
                    self.assertEqual(len(report["invalid_expected"]), 1)
                    self.assertEqual(len(report["unmatched_actual"]), 1)
                    self.assertTrue(report["conservation"]["ok"])

    def test_empty_text_is_invalid_only_for_fields_that_may_not_be_empty(self):
        for field in join.ENTRY_STRINGS:
            with self.subTest(field=field):
                changed = entry("alpha")
                changed[field] = ""
                report = join.join(dispatch(changed), [jobs_page(job(1, "alpha" if field != "name" else ""))])
                if field in join.MAY_BE_EMPTY:
                    self.assertEqual(len(report["matched"]), 1)
                else:
                    self.assertEqual(report["matched"], [])
                    self.assertEqual(len(report["invalid_expected"]), 1)

    def test_a_boolean_is_not_an_integer_timeout(self):
        report = join.join(dispatch(entry("alpha", job_timeout=True)), [jobs_page(job(1, "alpha"))])
        self.assertEqual((len(report["invalid_expected"]), report["matched"]), (1, []))

    def test_non_object_entries_and_jobs_are_invalid(self):
        report = join.join(dispatch("alpha", None), [jobs_page("alpha", None)])
        self.assertEqual((len(report["invalid_expected"]), len(report["invalid_actual"])), (2, 2))
        self.assertTrue(report["conservation"]["ok"])

    def test_each_job_field_missing_null_or_wrong_typed_is_invalid(self):
        for field in ("id", "name", "run_id", "run_attempt"):
            for label, value in (("missing", join), ("null", None), ("wrong type", [1])):
                with self.subTest(field=field, case=label):
                    broken = job(1, "alpha")
                    if label == "missing":
                        del broken[field]
                    else:
                        broken[field] = value
                    report = join.join(dispatch(entry("alpha")), [jobs_page(broken)])
                    self.assertEqual(report["matched"], [])
                    self.assertEqual(len(report["invalid_actual"]), 1)
                    self.assertEqual(len(report["unmatched_expected"]), 1)
                    self.assertTrue(report["conservation"]["ok"])

    def test_identifiers_may_be_integers_or_decimal_text(self):
        text = job("1", "alpha", run_id=str(RUN), run_attempt=str(ATTEMPT))
        self.assertEqual(len(join.join(dispatch(entry("alpha")), [jobs_page(text)])["matched"]), 1)
        word = job(1, "alpha", run_attempt="latest")
        self.assertEqual(len(join.join(dispatch(entry("alpha")), [jobs_page(word)])["invalid_actual"]), 1)


class DocumentTests(unittest.TestCase):
    def inconclusive(self, document, pages):
        with self.assertRaises(join.Inconclusive):
            join.join(document, pages)

    def test_dispatch_envelope_failures_are_inconclusive_as_a_whole(self):
        good = dispatch(entry("alpha"))
        pages = [jobs_page(job(1, "alpha"))]
        self.assertEqual(len(join.join(good, pages)["matched"]), 1)
        mutations = {
            "schema": lambda d: d.update(schema="ceph-tag-dispatch/v0"),
            "entries": lambda d: d.update(entries={}),
            "count missing": lambda d: d.pop("count"),
            "count differs": lambda d: d.update(count=2),
            "context": lambda d: d.update(context=None),
        }
        for field in join.REQUIRED_CONTEXT:
            for value in (None, "", 7):
                mutations[field + "=" + repr(value)] = (lambda f, v: lambda d: d["context"].update({f: v}))(field, value)
            mutations[field + " absent"] = (lambda f: lambda d: d["context"].pop(f))(field)
        mutations["run_id text"] = lambda d: d["context"].update(run_id="abc")
        for label, mutate in mutations.items():
            with self.subTest(label):
                changed = copy.deepcopy(good)
                mutate(changed)
                self.inconclusive(changed, pages)
        self.inconclusive([], pages)

    def test_unrelated_context_fields_may_be_null(self):
        document = dispatch(entry("alpha"), sha=None, category=None, release=None)
        report = join.join(document, [jobs_page(job(1, "alpha"))])
        self.assertEqual(report["scope"]["declared_workflow_sha"], None)

    def test_truncated_or_malformed_job_lists_are_inconclusive(self):
        document = dispatch(entry("alpha"))
        self.inconclusive(document, [jobs_page(job(1, "alpha"), total=2)])
        self.inconclusive(document, [{"jobs": "x"}])
        self.inconclusive(document, [{"jobs": [], "total_count": "0"}])
        self.inconclusive(document, [jobs_page(job(1, "alpha"), total=2), jobs_page(job(2, "x"), total=3)])
        self.inconclusive(document, [5])

    def test_pages_merge_and_a_bare_list_is_accepted(self):
        document = dispatch(entry("alpha"), entry("beta"))
        pages = [jobs_page(job(1, "alpha"), total=2), jobs_page(job(2, "beta"), total=2)]
        self.assertEqual(len(join.join(document, pages)["matched"]), 2)
        self.assertEqual(len(join.join(document, [[job(1, "alpha"), job(2, "beta")]])["matched"]), 2)

    def test_a_job_page_repeated_makes_duplicate_job_ids_conflict(self):
        page = jobs_page(job(1, "alpha"), total=2)
        report = join.join(dispatch(entry("alpha")), [page, page])
        self.assertEqual(report["matched"], [])
        self.assertEqual(len(report["conflicts"]), 2)
        self.assertEqual(len(report["unmatched_expected"]), 1)
        self.assertTrue(report["conservation"]["ok"])


class RunIdentityTests(unittest.TestCase):
    def test_jobs_of_another_run_attempt_or_repository_conflict_and_never_match(self):
        document = dispatch(entry("alpha"))
        cases = {"run": job(1, "alpha", run_id=RUN + 1),
                 "attempt": job(1, "alpha", run_attempt=ATTEMPT - 1),
                 "repository": job(1, "alpha", url="https://api.example.test/repos/other/repository/actions/jobs/1")}
        for label, other in cases.items():
            with self.subTest(label):
                report = join.join(document, [jobs_page(other)])
                self.assertEqual((report["matched"], len(report["conflicts"])), ([], 1))
                self.assertEqual(len(report["unmatched_expected"]), 1)
                self.assertEqual(report["conflicts"][0]["kind"], "job")
                self.assertTrue(report["conservation"]["ok"])

    def test_a_job_without_a_url_is_not_a_repository_conflict(self):
        bare = job(1, "alpha")
        del bare["url"]
        self.assertEqual(len(join.join(dispatch(entry("alpha")), [jobs_page(bare)])["matched"]), 1)

    def test_a_rerun_attempt_does_not_borrow_the_first_attempt(self):
        report = join.join(dispatch(entry("alpha")),
                           [jobs_page(job(1, "alpha", run_attempt=1), job(2, "alpha"))])
        self.assertEqual([item["job"]["id"] for item in report["matched"]], [2])
        self.assertEqual(len(report["conflicts"]), 1)


class CallerScopeTests(unittest.TestCase):
    def test_two_callers_with_the_expected_names_need_an_explicit_caller(self):
        document = dispatch(entry("alpha"))
        pages = [jobs_page(job(1, "alpha"), job(2, "alpha", caller="Squid short"))]
        with self.assertRaises(join.Inconclusive):
            join.join(document, pages)
        report = join.join(document, pages, caller="Squid short")
        self.assertEqual([item["job"]["id"] for item in report["matched"]], [2])
        self.assertEqual([item["id"] for item in report["out_of_scope"]], [1])
        self.assertTrue(report["conservation"]["ok"])

    def test_the_only_caller_is_derived_and_other_jobs_are_out_of_scope(self):
        report = join.join(dispatch(entry("alpha")),
                           [jobs_page(job(1, "alpha"), job(2, "Plan", caller="Docker checks"))])
        self.assertEqual(report["scope"]["caller"], CALLER)
        self.assertEqual([item["id"] for item in report["out_of_scope"]], [2])

    def test_a_caller_that_has_none_of_the_jobs_matches_nothing(self):
        report = join.join(dispatch(entry("alpha")), [jobs_page(job(1, "alpha"))], caller="Squid short")
        self.assertEqual(report["matched"], [])
        self.assertEqual((len(report["unmatched_expected"]), len(report["out_of_scope"])), (1, 1))


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.document = dispatch(entry("alpha", "alpha-id"), entry("beta", "beta-id", network="bridge"))
        self.pages = [jobs_page(job(1, "alpha"), job(2, "beta"))]

    def run_join(self, rows, invalid=()):
        return join.join(self.document, self.pages, receipts=([("receipt/%d" % i, row) for i, row in enumerate(rows)],
                                                              list(invalid)))

    def states(self, report):
        return {item["job"]["id"]: item["receipt"] for item in report["matched"]}

    def test_checkout_identity_comes_from_receipts_never_from_the_declared_sha(self):
        without = join.join(self.document, self.pages)
        for item in without["matched"]:
            self.assertEqual((item["receipt"]["state"], item["receipt"]["checkout_commit"]), ("not_provided", None))
        report = self.run_join([receipt_row(1, "alpha-id"), receipt_row(2, "beta-id", network="bridge", commit=None)])
        states = self.states(report)
        self.assertEqual((states[1]["state"], states[1]["checkout_commit"]), ("consistent", "d" * 40))
        self.assertEqual((states[2]["state"], states[2]["checkout_commit"]), ("inconclusive", None))
        self.assertNotIn("c" * 40, json.dumps({"receipts": states}))
        self.assertEqual(report["scope"]["declared_workflow_sha"], "c" * 40)

    def test_a_job_without_any_receipt_is_inconclusive_when_receipts_were_supplied(self):
        report = self.run_join([receipt_row(1, "alpha-id")])
        self.assertEqual(self.states(report)[2]["state"], "inconclusive")

    def test_a_different_profile_repetition_or_requested_network_is_a_conflict(self):
        for label, row in (("profile", receipt_row(1, "other-id")), ("repetition", receipt_row(1, "alpha-id", "2"))):
            with self.subTest(label):
                report = self.run_join([row, receipt_row(2, "beta-id", network="bridge")])
                self.assertEqual([c["kind"] for c in report["conflicts"]], ["receipt"])
                self.assertEqual([item["job"]["id"] for item in report["matched"]], [2])
                self.assertTrue(report["conservation"]["ok"])
        report = self.run_join([receipt_row(1, "alpha-id"), receipt_row(2, "beta-id", network="host")])
        self.assertEqual(report["conflicts"][0]["job"]["id"], 2)

    def test_an_empty_requested_network_accepts_the_mode_the_batch_ran(self):
        report = self.run_join([receipt_row(1, "alpha-id", network="host"), receipt_row(2, "beta-id", network="bridge")])
        self.assertEqual(report["conflicts"], [])

    def test_missing_wrong_typed_receipt_fields_are_inconclusive_not_agreement(self):
        for mutate in (lambda r: r["execution_key"].pop("profile_id"),
                       lambda r: r["execution_key"].update(repetition_id=1),
                       lambda r: r["execution_key"].update(network=None)):
            row = receipt_row(1, "alpha-id")
            mutate(row)
            row["row_sha256"] = scenario_history.row_hash(row)
            # Without a profile the execution key itself is rejected as a row.
            if "profile_id" not in row["execution_key"]:
                with self.assertRaises(ValueError):
                    scenario_history.validate_row(row)
                continue
            report = self.run_join([row, receipt_row(2, "beta-id", network="bridge")])
            self.assertEqual(self.states(report)[1]["state"], "inconclusive")

    def test_receipts_that_disagree_on_the_checkout_conflict(self):
        report = self.run_join([receipt_row(1, "alpha-id"), receipt_row(1, "alpha-id", commit="f" * 40),
                                receipt_row(2, "beta-id", network="bridge")])
        self.assertEqual(len(report["conflicts"]), 1)
        self.assertTrue(report["conservation"]["ok"])
        agree = self.run_join([receipt_row(1, "alpha-id"), receipt_row(1, "alpha-id"),
                               receipt_row(2, "beta-id", network="bridge")])
        self.assertEqual(agree["conflicts"], [])

    def test_receipts_of_unmatched_jobs_or_other_attempts_never_attach(self):
        report = self.run_join([receipt_row(1, "alpha-id"), receipt_row(2, "beta-id", network="bridge"),
                                receipt_row(99, "x"), receipt_row(1, "alpha-id", attempt=ATTEMPT - 1)])
        self.assertEqual([item["job_id"] for item in report["receipts_without_a_matched_job"]], ["99"])
        self.assertEqual(self.states(report)[1]["paths"], ["receipt/0"])

    def test_loading_a_directory_keeps_invalid_receipts_visible_with_relative_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a").mkdir()
            (root / "a" / "history-row.json").write_text(json.dumps(receipt_row(1, "alpha-id")))
            (root / "b").mkdir()
            (root / "b" / "history-row.json").write_text("{not json")
            tampered = receipt_row(2, "beta-id", network="bridge")
            tampered["source"]["checkout_commit"] = "0" * 40
            (root / "c").mkdir()
            (root / "c" / "history-row.json").write_text(json.dumps(tampered))
            rows, invalid = join.load_receipts(root)
        self.assertEqual([path for path, _ in rows], ["a/history-row.json"])
        self.assertEqual(sorted(item["path"] for item in invalid), ["b/history-row.json", "c/history-row.json"])
        self.assertNotIn(directory, json.dumps(invalid))


class RealReceiptTests(unittest.TestCase):
    """Receipts built by scenario_history.normalize, stored as artifacts and
    read back by the loader, not assembled by hand."""

    def setUp(self):
        import test_scenario_history as rows
        self.rows = rows
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.count = 0

    def store(self, row):
        self.count += 1
        target = self.root / "receipts" / ("job-%d" % self.count) / "history-row.json"
        target.parent.mkdir(parents=True)
        target.write_text(json.dumps(row))

    def reset(self):
        shutil.rmtree(self.root / "receipts", ignore_errors=True)

    def real(self, **metadata):
        self.count += 1
        return self.rows.Job(self.root / ("build-%d" % self.count)).passing().row(**metadata)

    @staticmethod
    def sentinel(row, field, value=scenario_history.MISSING):
        """A hash-valid row whose execution key states a field as unknown."""
        row = copy.deepcopy(row)
        row["execution_key"][field] = value
        row["row_sha256"] = scenario_history.row_hash(row)
        return row

    def join(self, network="", rows=()):
        profile = self.rows.PROFILE["id"]
        document = dispatch(entry("alpha", profile, network=network), repository="owner/repo", run_id="100",
                            run_attempt="1")
        document["context"]["repository"] = "owner/repo"
        document["context"]["run_id"], document["context"]["run_attempt"] = "100", "1"
        page = jobs_page(job(200, "alpha", run_id=100, run_attempt=1,
                             url="https://api.example.test/repos/owner/repo/actions/jobs/200"))
        for row in rows:
            self.store(row)
        return join.join(document, [page], join.load_receipts(self.root / "receipts"))

    def test_a_real_receipt_is_consistent_and_names_the_checkout(self):
        report = self.join(rows=[self.real()])
        receipt = report["matched"][0]["receipt"]
        self.assertEqual((receipt["state"], receipt["checkout_commit"]), ("consistent", "b" * 40))
        self.assertTrue(report["conservation"]["ok"])

    def test_the_unknown_network_marker_is_a_gap_for_default_and_explicit_networks(self):
        base = self.real()
        self.assertEqual(base["execution_key"]["network"], "")
        for requested in ("", "host"):
            with self.subTest(requested=requested):
                self.reset()
                report = self.join(requested, [self.sentinel(base, "network")])
                self.assertEqual(report["conflicts"], [])
                self.assertEqual(report["matched"][0]["receipt"]["state"], "inconclusive")
                self.assertIn("network", " ".join(report["matched"][0]["receipt"]["reasons"]))

    def test_a_known_network_is_still_consistent_or_a_conflict(self):
        host = self.real()
        host = self.sentinel(host, "network", "host")
        report = self.join("host", [host])
        self.assertEqual(report["matched"][0]["receipt"]["state"], "consistent")

    def test_other_unknown_markers_in_compared_fields_are_gaps(self):
        for field in ("profile_id", "repetition_id"):
            with self.subTest(field=field):
                self.reset()
                row = self.sentinel(self.real(), field)
                try:
                    scenario_history.validate_row(row)
                except ValueError:
                    continue  # a row that cannot be told apart never loads
                report = self.join(rows=[row])
                self.assertEqual((report["conflicts"], report["matched"][0]["receipt"]["state"]),
                                 ([], "inconclusive"))

    def test_a_receipt_without_a_job_id_stays_visible_and_is_never_guessed(self):
        row = self.real(job_id="")
        self.assertEqual(row["execution_key"]["job_id"], scenario_history.MISSING)
        report = self.join(rows=[row])
        loaded, invalid = join.load_receipts(self.root / "receipts")
        self.assertEqual((len(loaded), invalid), (1, []))
        path = loaded[0][0]
        self.assertEqual([item["path"] for item in report["invalid_receipts"]], [path])
        self.assertEqual(report["matched"][0]["receipt"]["state"], "inconclusive")
        self.assertEqual(report["matched"][0]["receipt"]["paths"], [])
        self.assertEqual(report["receipts_without_a_matched_job"], [])
        self.assertEqual(report["conservation"]["receipts_read"], 1)
        self.assertTrue(report["conservation"]["ok"])

    def test_every_loaded_receipt_ends_in_exactly_one_bucket(self):
        rows = [self.real(), self.real(job_id=""), self.real(job_id="999"), self.real(run_id="101", job_id="200"),
                self.real(job_id="200", run_attempt="2")]
        (self.root / "receipts" / "broken").mkdir(parents=True)
        (self.root / "receipts" / "broken" / "history-row.json").write_text("{")
        report = self.join(rows=rows)
        conservation = report["conservation"]
        self.assertEqual((conservation["receipts_read"], conservation["bucketed_receipts"]), (6, 6))
        self.assertTrue(conservation["ok"])
        self.assertEqual(len(report["receipts_other_run"]), 2)
        self.assertEqual(len(report["receipts_without_a_matched_job"]), 1)
        self.assertEqual(len(report["invalid_receipts"]), 2)
        for bucket in ("receipts_other_run", "receipts_without_a_matched_job", "invalid_receipts"):
            with self.subTest(bucket=bucket):
                dropped = copy.deepcopy(report)
                dropped[bucket].pop()
                paths = [p for r in dropped["matched"] for p in r["receipt"]["paths"]] + sum(
                    ([r["path"]] if "path" in r else r["paths"] for k in
                     ("receipts_other_run", "receipts_without_a_matched_job", "invalid_receipts")
                     for r in report[k]), [])
                self.assertFalse(join.conserve(dropped, 1, 1, paths)["ok"])


class ConservationTests(unittest.TestCase):
    def report(self):
        return join.join(dispatch(entry("alpha"), entry("beta"), entry("beta", "second"), entry("gone")),
                         [jobs_page(job(1, "alpha"), job(2, "beta"), job(3, "extra"), job(4, "x", run_id=1))])

    def test_every_bucket_is_counted_and_a_dropped_or_doubled_record_is_detected(self):
        report = self.report()
        self.assertTrue(report["conservation"]["ok"])
        self.assertEqual(report["conservation"]["bucketed_entries"], 4)
        self.assertEqual(report["conservation"]["bucketed_jobs"], 4)
        for bucket in ("matched", "ambiguous", "unmatched_expected", "unmatched_actual", "conflicts"):
            with self.subTest(bucket=bucket):
                dropped = copy.deepcopy(report)
                dropped[bucket].pop()
                self.assertFalse(join.conserve(dropped, 4, 4)["ok"])
                doubled = copy.deepcopy(report)
                doubled[bucket].append(copy.deepcopy(doubled[bucket][0]))
                self.assertFalse(join.conserve(doubled, 4, 4)["ok"])

    def test_the_command_fails_when_the_report_does_not_conserve(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(join, "conserve") as conserve:
            conserve.return_value = {"ok": False}
            code = command(directory, dispatch(entry("alpha")), jobs_page(job(1, "alpha")))
        self.assertEqual(code, 3)


class CommandTests(unittest.TestCase):
    command = staticmethod(command)

    def test_report_file_exit_codes_and_no_local_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.command(directory, dispatch(entry("alpha")), jobs_page(job(1, "alpha"))), 0)
            text = (Path(directory) / "report.json").read_text()
            self.assertNotIn(directory, text)
            report = json.loads(text)
            self.assertEqual(report["schema"], join.REPORT_SCHEMA)
            self.assertEqual(len(report["matched"]), 1)
            # A finding is not a failure of the report.
            self.assertEqual(self.command(directory, dispatch(entry("alpha")), jobs_page()), 0)
            self.assertEqual(self.command(directory, dispatch(entry("alpha")), jobs_page(total=3)), 2)
            self.assertEqual(self.command(directory, {"schema": "x"}, jobs_page()), 2)

    def test_unreadable_input_is_inconclusive(self):
        with tempfile.TemporaryDirectory() as directory, patch("sys.stderr", new=io.StringIO()) as error:
            self.assertEqual(join.main(["--dispatch", str(Path(directory) / "none.json"),
                                        "--jobs", str(Path(directory) / "none.json")]), 2)
            self.assertNotIn(directory, error.getvalue())

    def test_the_report_states_what_it_does_not_establish(self):
        report = join.join(dispatch(entry("alpha")), [jobs_page(job(1, "alpha"))])
        text = " ".join(report["not_established"])
        for phrase in ("allocation", "native execution", "billing", "checkout"):
            self.assertIn(phrase, text)


if __name__ == "__main__":
    unittest.main()
