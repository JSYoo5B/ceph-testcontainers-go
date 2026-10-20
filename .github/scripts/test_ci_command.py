"""Test the pull request comment command without GitHub.

The event and pull request fixtures under testdata/ci_command have the shape
of an issue_comment payload and of GET /repos/{owner}/{repo}/pulls/{number};
each test changes only the field it is about.
"""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import ci_command as command
import tag_scenarios as engine

TESTDATA = Path(__file__).resolve().parent / "testdata" / "ci_command"
HEAD = "1" * 40
COMMENT = 4000000001


def load(name):
    return json.loads((TESTDATA / name).read_text())


def event(body=None, association=None, **changes):
    payload = load("comment-pull-request.json")
    if body is not None:
        payload["comment"]["body"] = body
    if association is not None:
        payload["comment"]["author_association"] = association
    payload.update(changes)
    return payload


def no_command_line_starts_a_reply(test, reply):
    for line in reply.splitlines():
        test.assertFalse(line.startswith("/ci"), reply)


class AuthorizeTests(unittest.TestCase):
    def setUp(self):
        self.pull = load("pull-same-repository.json")

    def refused(self, payload, pull=None):
        decision = command.authorize(payload, self.pull if pull is None else pull)
        self.assertEqual((decision["command"], decision["accepted"]), (True, False), decision)
        self.assertNotIn("request", decision)
        no_command_line_starts_a_reply(self, decision["reply"])
        return decision["reply"]

    def ignored(self, payload, pull=None):
        self.assertEqual(command.authorize(payload, self.pull if pull is None else pull),
                         {"command": False, "accepted": False})

    def test_authorized_associations_are_accepted_with_the_recorded_head(self):
        for association in command.AUTHORIZED:
            with self.subTest(association=association):
                decision = command.authorize(event(association=association), self.pull)
                self.assertTrue(decision["accepted"], decision)
                self.assertEqual(decision["sha"], HEAD)
                self.assertEqual(decision["request"], {
                    "names": ["rbd_mirror_bandwidth", "short/network_delay"], "selectors": ["squid"],
                    "duplicates": [], "sha": HEAD, "pull": 182, "comment_id": COMMENT})

    def test_a_comment_without_an_id_is_refused(self):
        for value in (None, 0, -1, "4000000001", True):
            with self.subTest(value=value):
                payload = event()
                payload["comment"]["id"] = value
                self.assertIn("comment could not be read", self.refused(payload))

    def test_other_associations_are_refused_before_anything_runs(self):
        for association in ("CONTRIBUTOR", "FIRST_TIME_CONTRIBUTOR", "FIRST_TIMER", "MANNEQUIN", "NONE", ""):
            with self.subTest(association=association):
                reply = self.refused(event(association=association))
                self.assertIn("owner, member or collaborator", reply)
                self.assertIn(association or "unknown", reply)
        # Authorization comes first: a stranger learns nothing about forks
        # or names.
        self.assertIn("collaborator", self.refused(event(association="NONE"), load("pull-fork.json")))

    def test_pull_requests_from_forks_are_refused(self):
        reply = self.refused(event(), load("pull-fork.json"))
        self.assertIn("forks", reply)
        self.assertIn("contributor/ceph-testcontainers-go", reply)
        deleted = load("pull-fork.json")
        deleted["head"]["repo"] = None
        self.assertIn("deleted", self.refused(event(), deleted))

    def test_closed_or_unreadable_pull_requests_are_refused(self):
        closed = copy.deepcopy(self.pull)
        closed["state"] = "closed"
        self.assertIn("open pull requests", self.refused(event(), closed))
        self.assertIn("could not be read", self.refused(event(), {}))
        other = copy.deepcopy(self.pull)
        other["number"] = 181
        self.assertIn("could not be read", self.refused(event(), other))
        headless = copy.deepcopy(self.pull)
        headless["head"]["sha"] = "main"
        self.assertIn("head commit", self.refused(event(), headless))

    def test_comments_on_issues_edits_bots_and_plain_comments_are_ignored(self):
        self.ignored(load("comment-issue.json"))
        self.ignored(event(action="edited"))
        self.ignored(event(action="deleted"))
        bot = event()
        bot["comment"]["user"]["type"] = "Bot"
        self.ignored(bot)
        self.ignored(event(body="Looks good to me."))
        self.ignored(event(body=""))
        self.ignored(event(body=None) | {"comment": {"author_association": "OWNER", "user": {"type": "User"}}})

    def test_a_command_must_start_its_line(self):
        for body in ("Please /ci run rbd_mirror_bandwidth", "> /ci run rbd_mirror_bandwidth",
                     "  /ci run rbd_mirror_bandwidth", "`/ci run rbd_mirror_bandwidth`",
                     "/cinema run rbd_mirror_bandwidth", "/CI run rbd_mirror_bandwidth",
                     "```\n/ci run rbd_mirror_bandwidth\n```", "~~~text\n/ci run x\n```\n/ci run y\n~~~"):
            with self.subTest(body=body):
                self.ignored(event(body=body))
        # Mixed text: the command line is found among prose, and a command
        # after a closed fence counts.
        for body in ("Rerun the mirror batch.\n/ci run rbd_mirror_bandwidth\nThanks",
                     "```\nexample\n```\n/ci run rbd_mirror_bandwidth"):
            with self.subTest(body=body):
                decision = command.authorize(event(body=body), self.pull)
                self.assertTrue(decision["accepted"], decision)
                self.assertEqual(decision["request"]["names"], ["rbd_mirror_bandwidth"])

    def test_more_than_one_command_line_is_refused(self):
        reply = self.refused(event(body="/ci run network_delay\n/ci run rbd_mirror_bandwidth"))
        self.assertIn("only one command", reply)

    def test_empty_and_malformed_commands_are_refused_with_usage(self):
        cases = {
            "/ci": "Unknown command",
            "/ci   ": "Unknown command",
            "/ci rerun network_delay": "Unknown command",
            "/ci run": "names no batch",
            "/ci run on squid": "names no batch",
            "/ci run network_delay on": "needs a release",
            "/ci run network_delay on squid on tentacle": "only once",
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                reply = self.refused(event(body=body))
                self.assertIn(expected, reply)
                self.assertIn(command.USAGE, reply)

    def test_names_outside_the_identifier_alphabet_are_refused_without_escaping_the_code_span(self):
        reply = self.refused(event(body="/ci run network_delay x`@someone $(id)"))
        self.assertIn("Invalid name(s)", reply)
        self.assertIn("`x'@someone`", reply)
        self.assertIn("`$(id)`", reply)
        self.assertNotIn("`@", reply)

    def test_duplicate_names_and_selectors_are_kept_once_and_reported(self):
        decision = command.authorize(event(
            body="/ci run network_delay, network_delay rbd_mirror_bandwidth on squid squid"), self.pull)
        self.assertEqual(decision["request"]["names"], ["network_delay", "rbd_mirror_bandwidth"])
        self.assertEqual(decision["request"]["selectors"], ["squid"])
        self.assertEqual(decision["request"]["duplicates"], ["network_delay", "squid"])


class SelectTests(unittest.TestCase):
    def setUp(self):
        self.vocabulary = load("vocabulary.json")

    def request(self, names, selectors=(), duplicates=(), comment=COMMENT):
        return {"names": list(names), "selectors": list(selectors), "duplicates": list(duplicates),
                "sha": HEAD, "pull": 182, "comment_id": comment}

    def test_batches_category_batches_profiles_and_ids_resolve_to_runtime_profiles(self):
        selected = command.select(self.request([
            "network_delay", "multicluster/rbd_mirror_bandwidth",
            "optional/native_rgw_translation/internal-integration", "environment-sdk_network-2956058c"]),
            self.vocabulary)
        self.assertTrue(selected["accepted"], selected)
        self.assertEqual(selected["profiles"].split(","), sorted([
            "short-network_delay-0a1b2c3d", "multicluster-rbd_mirror_bandwidth-4e5f6a7b",
            "optional-native_rgw_translation-8c9d0e1f", "environment-sdk_network-2956058c"]))
        self.assertEqual(selected["batches"], ["environment/sdk_network", "multicluster/rbd_mirror_bandwidth",
                                               "optional/native_rgw_translation", "short/network_delay"])
        # Without "on", the default release runs.
        self.assertEqual(selected["releases"], ["20.2.4"])

    def test_one_unknown_name_refuses_the_whole_command_and_lists_every_unknown_name(self):
        selected = command.select(self.request(["network_delay", "netwrok_delay", "pure", "short/pools"],
                                               ["squid", "reef"]), self.vocabulary)
        self.assertEqual(selected["accepted"], False)
        self.assertNotIn("profiles", selected)
        reply = selected["reply"]
        self.assertTrue(reply.startswith("Nothing runs."))
        for name in ("`netwrok_delay`", "`pure` (a code batch", "`short/pools`", "`reef` (supported:"):
            self.assertIn(name, reply)
        self.assertNotIn("`network_delay`", reply)
        self.assertNotIn("`squid`", reply)
        self.assertIn(HEAD[:12], reply)
        no_command_line_starts_a_reply(self, reply)

    def test_release_selectors_take_versions_codenames_and_all(self):
        cases = {("squid",): ["19.2.5"], ("20.2.4",): ["20.2.4"], ("all",): ["20.2.4", "19.2.5"],
                 ("squid", "tentacle"): ["20.2.4", "19.2.5"], ("all", "squid"): ["20.2.4", "19.2.5"]}
        for selectors, releases in cases.items():
            with self.subTest(selectors=selectors):
                selected = command.select(self.request(["network_delay"], selectors), self.vocabulary)
                self.assertEqual(selected["releases"], releases)
        for selector in ("20.2.5", "Squid", "latest"):
            with self.subTest(selector=selector):
                self.assertFalse(command.select(self.request(["network_delay"], [selector]), self.vocabulary)["accepted"])

    def test_the_concurrency_key_names_the_head_and_the_selection_not_its_spelling(self):
        first = command.select(self.request(["network_delay", "rbd_mirror_bandwidth"], ["squid"]), self.vocabulary)
        second = command.select(self.request(["multicluster-rbd_mirror_bandwidth-4e5f6a7b", "short/network_delay"],
                                             ["19.2.5"]), self.vocabulary)
        self.assertEqual(first["key"], second["key"])
        self.assertTrue(first["key"].startswith("182-" + HEAD + "-"))
        other = command.select(self.request(["network_delay"], ["squid"]), self.vocabulary)
        self.assertNotEqual(first["key"], other["key"])
        released = command.select(self.request(["network_delay", "rbd_mirror_bandwidth"], ["all"]), self.vocabulary)
        self.assertNotEqual(first["key"], released["key"])

    def test_the_status_context_names_the_comment_and_the_selection(self):
        selected = command.select(self.request(["network_delay"], ["squid"]), self.vocabulary)
        self.assertEqual(selected["context"], "CI command 4000000001: network_delay on 19.2.5")
        # The context depends only on the request, so Acknowledge and Report,
        # which both read Validate's output, set the same one.
        self.assertEqual(command.select(self.request(["network_delay"], ["squid"]), self.vocabulary), selected)

    def test_repeated_commands_have_their_own_status_and_share_the_concurrency_key(self):
        first = command.select(self.request(["network_delay"], ["squid"]), self.vocabulary)
        second = command.select(self.request(["network_delay"], ["squid"], comment=4000000002), self.vocabulary)
        self.assertNotEqual(first["context"], second["context"])
        self.assertEqual(second["context"], "CI command 4000000002: network_delay on 19.2.5")
        self.assertEqual(first["key"], second["key"])
        self.assertEqual({key: value for key, value in first.items() if key != "context"},
                         {key: value for key, value in second.items() if key != "context"})

    def test_a_long_selection_is_cut_within_the_bound_and_keeps_the_comment(self):
        names = ["short/network_delay", "multicluster/rbd_mirror_bandwidth", "optional/native_rgw_translation",
                 "environment/sdk_network"]
        contexts = set()
        for comment in (COMMENT, 4000000002, 2**63 - 1):
            with self.subTest(comment=comment):
                long = command.select(self.request(names, ["all"], comment=comment), self.vocabulary)
                self.assertLessEqual(len(long["context"]), command.MAX_CONTEXT)
                self.assertTrue(long["context"].startswith("CI command " + str(comment) + ": short/network_delay "))
                self.assertTrue(long["context"].endswith("... " + long["key"].rsplit("-", 1)[1]))
                contexts.add(long["context"])
        self.assertEqual(len(contexts), 3)
        # A context that fits is not cut.
        exact = command.status_context(COMMENT, ["x" * 67], ["20.2.4"], "0" * 12)
        self.assertEqual(len(exact), command.MAX_CONTEXT)
        self.assertTrue(exact.endswith(" on 20.2.4"))
        cut = command.status_context(COMMENT, ["x" * 68], ["20.2.4"], "0" * 12)
        self.assertEqual(len(cut), command.MAX_CONTEXT)
        self.assertTrue(cut.endswith("... " + "0" * 12))


class ReportTests(unittest.TestCase):
    RUN = "https://github.com/JSYoo5B/ceph-testcontainers-go/actions/runs/1"

    def accepted(self, duplicates=()):
        request = {"names": ["network_delay"], "selectors": [], "duplicates": list(duplicates), "sha": HEAD, "pull": 182}
        return ({"command": "true", "accepted": "true", "sha": HEAD, "reply": '""', "request": json.dumps(request)},
                {"accepted": "true", "reply": '""', "batches": '["short/network_delay"]', "releases": '["20.2.4"]',
                 "profiles": "short-network_delay-0a1b2c3d",
                 "context": "CI command 4000000001: network_delay on 20.2.4"})

    def test_refusals_are_replied_without_a_status(self):
        refused = {"command": "true", "accepted": "false", "reply": json.dumps("Only an owner may run it.")}
        result = command.report(refused, {}, "skipped", "skipped", self.RUN)
        self.assertEqual(result["state"], "")
        self.assertTrue(result["reply"].startswith("Only an owner may run it."))
        self.assertIn(self.RUN, result["reply"])
        accepted, _ = self.accepted()
        for validate_result in ("failure", "cancelled", ""):
            with self.subTest(validate_result=validate_result):
                result = command.report(accepted, {}, validate_result, "skipped", self.RUN)
                self.assertEqual(result["state"], "")
                self.assertIn("could not be validated", result["reply"])
        unknown = {"accepted": "false", "reply": json.dumps("Nothing runs. Unknown name(s): `x`.")}
        result = command.report(accepted, unknown, "success", "skipped", self.RUN)
        self.assertEqual(result["state"], "")
        self.assertTrue(result["reply"].startswith("Nothing runs. Unknown name(s)"))

    def test_finished_runs_set_the_status_and_summarise_the_selection(self):
        accepted, validated = self.accepted(duplicates=["network_delay"])
        for runtime_result, state, outcome in (("success", "success", "passed"), ("failure", "failure", "failed"),
                                               ("cancelled", "error", "was cancelled")):
            with self.subTest(runtime_result=runtime_result):
                result = command.report(accepted, validated, "success", runtime_result, self.RUN)
                self.assertEqual(result["state"], state)
                self.assertIn("CI command " + outcome + " on " + HEAD, result["reply"])
                self.assertIn("`short/network_delay`", result["reply"])
                self.assertIn("20.2.4", result["reply"])
                self.assertIn("run once: `network_delay`", result["reply"])
                self.assertEqual("not re-run automatically" in result["reply"], runtime_result == "cancelled")
                self.assertLessEqual(len(result["description"]), 140)
                no_command_line_starts_a_reply(self, result["reply"])


class MainTests(unittest.TestCase):
    def run_main(self, arguments):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            arguments = [str(value).replace("DIR", directory) for value in arguments]
            with mock.patch("sys.stdout"):
                self.assertEqual(command.main(arguments + ["--github-output", str(output)]), 0)
            lines = output.read_text().splitlines()
            values = dict(line.split("=", 1) for line in lines)
            self.assertEqual(len(values), len(lines))
            body = Path(directory) / "reply.md"
            return values, body.read_text() if body.exists() else ""

    def test_the_jobs_pass_single_line_outputs_from_one_to_the_next(self):
        authorized, _ = self.run_main(["authorize", "--event", TESTDATA / "comment-pull-request.json",
                                       "--pull", TESTDATA / "pull-same-repository.json"])
        self.assertEqual((authorized["command"], authorized["accepted"], authorized["sha"]), ("true", "true", HEAD))
        selected, _ = self.run_main(["select", "--request", authorized["request"],
                                     "--vocabulary", TESTDATA / "vocabulary.json"])
        self.assertEqual((selected["accepted"], selected["releases"]), ("true", '["19.2.5"]'))
        self.assertEqual(json.loads(selected["batches"]), ["multicluster/rbd_mirror_bandwidth", "short/network_delay"])
        self.assertEqual(selected["context"], "CI command 4000000001: rbd_mirror_bandwidth short/network_delay on 19.2.5")
        reported, reply = self.run_main(["report", "--command-outputs", json.dumps(authorized),
                                         "--validate-outputs", json.dumps(selected), "--validate-result", "success",
                                         "--runtime-result", "success", "--run-url", "https://example.invalid/run",
                                         "--body", "DIR/reply.md"])
        self.assertEqual(reported["state"], "success")
        self.assertIn("passed on " + HEAD, reply)

    def test_a_missing_pull_request_or_vocabulary_refuses(self):
        authorized, _ = self.run_main(["authorize", "--event", TESTDATA / "comment-pull-request.json",
                                       "--pull", "DIR/missing.json"])
        self.assertEqual((authorized["command"], authorized["accepted"]), ("true", "false"))
        self.assertIn("could not be read", json.loads(authorized["reply"]))
        ignored, _ = self.run_main(["authorize", "--event", TESTDATA / "comment-issue.json",
                                    "--pull", "DIR/missing.json"])
        self.assertEqual((ignored["command"], ignored["accepted"], ignored["sha"]), ("false", "false", ""))
        request = json.dumps({"names": ["network_delay"], "selectors": [], "duplicates": [], "sha": HEAD, "pull": 182,
                              "comment_id": COMMENT})
        selected, _ = self.run_main(["select", "--request", request, "--vocabulary", "DIR/missing.json"])
        self.assertEqual((selected["accepted"], selected["profiles"], selected["releases"]), ("false", "", "[]"))
        self.assertIn("Rebase", json.loads(selected["reply"]))


class ProfilePlanTests(unittest.TestCase):
    """tag_scenarios.py plan --profiles, the planner side of a command."""
    PLAN = {"schema": engine.SCHEMA, "coverage_scope": "required", "include": [
        {"id": "code-pure-1", "category": "code", "batch": "pure"},
        {"id": "short-a-1", "category": "short", "batch": "a"},
        {"id": "recovery-b-1", "category": "recovery", "batch": "b"}]}
    OPTIONAL = {"schema": engine.SCHEMA, "coverage_scope": "optional", "include": [
        {"id": "optional-c-1", "category": "optional", "batch": "c"}]}

    def test_named_profiles_of_any_category_and_optional_are_selected_without_claiming_coverage(self):
        plan = engine.select_profiles_plan(self.PLAN, self.OPTIONAL, "optional-c-1, short-a-1")
        self.assertEqual([profile["id"] for profile in plan["include"]], ["short-a-1", "optional-c-1"])
        self.assertEqual((plan["coverage_scope"], plan["selected_scope"], plan["selected_profiles"]),
                         ("selected", "profiles", 2))
        self.assertEqual(plan["required_include"], self.PLAN["include"])

    def test_unknown_duplicate_code_and_empty_profile_ids_refuse_the_plan(self):
        for identifiers, message in (("short-a-1,short-x-1", "short-x-1"), ("code-pure-1", "code-pure-1"),
                                     ("short-a-1,short-a-1", "twice"), (" , ", "at least one")):
            with self.subTest(identifiers=identifiers), self.assertRaisesRegex(ValueError, message):
                engine.select_profiles_plan(self.PLAN, self.OPTIONAL, identifiers)

    def test_profiles_exclude_every_other_selection(self):
        for extra in (["--category", "short"], ["--optional", "--batch", "c"], ["--representative"], ["--verify"],
                      ["--changed-since", "HEAD"], ["--select-declarations", "HEAD"]):
            with self.subTest(extra=extra), mock.patch("sys.stderr"), \
                    mock.patch.object(engine, "load_catalog", side_effect=AssertionError("not reached")):
                self.assertEqual(engine.main(["plan", "--profiles", "short-a-1", *extra]), 1)

    def test_the_vocabulary_lists_every_profile_and_the_supported_releases(self):
        vocabulary = engine.vocabulary({**self.PLAN, "include": [{**profile, "profile": profile["id"], "package": "./p"}
                                                                   for profile in self.PLAN["include"]]},
                                       {**self.OPTIONAL, "include": [{**profile, "profile": profile["id"],
                                                                      "package": "./p", "tags": "x"}
                                                                     for profile in self.OPTIONAL["include"]]})
        self.assertEqual([profile["id"] for profile in vocabulary["profiles"]],
                         ["code-pure-1", "short-a-1", "recovery-b-1", "optional-c-1"])
        self.assertEqual(set(vocabulary["profiles"][3]), {"id", "profile", "category", "batch", "package"})
        self.assertEqual([entry["release"] for entry in vocabulary["releases"]],
                         [release for release, _, _ in engine.images.SUPPORTED_RELEASES])
        self.assertEqual(vocabulary["default_release"], engine.images.RELEASE)


if __name__ == "__main__":
    unittest.main()
