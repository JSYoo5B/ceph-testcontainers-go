"""Test the required requested CI ledger and status without GitHub.

Repository stands in for the REST calls of required_ci.GitHub and keeps
pull requests, comments and commit statuses in memory. Flow plays the jobs
of ci-command.yml on it with the real ci_command.report and
required_ci.register, so a test reads like the comments of a review.
"""

import copy
import json
import subprocess
import tempfile
from pathlib import Path
import unittest
from unittest import mock

import ci_command
import required_ci as gate

SERVER, NAME = "https://github.com", "JSYoo5B/ceph-testcontainers-go"
A, B, C = "a" * 40, "b" * 40, "c" * 40
BOT = {"login": gate.BOT, "type": "Bot"}
HUMAN = {"login": "JSYoo5B", "type": "User"}


class Repository:
    def __init__(self, pulls):
        self.heads, self.thread, self.statuses, self.calls = dict(pulls), {number: [] for number in pulls}, {}, []
        self.next, self.failing = 5000000000, set()

    def push(self, number, sha):
        self.heads[number] = sha

    def comment(self, number, body, user=HUMAN):
        self.next += 1
        comment = {"id": self.next, "body": body, "user": dict(user), "created_at": "2026-01-01T00:00:00Z",
                   "updated_at": "2026-01-01T00:00:00Z",
                   "html_url": gate.comment_url(SERVER, NAME, number, self.next)}
        self.thread[number].append(comment)
        return comment

    def fail(self, call):
        if call in self.failing:
            raise subprocess.CalledProcessError(1, ["gh", "api", call])

    def open_pulls(self):
        self.fail("pulls")
        return [{"number": number, "head": {"sha": sha}} for number, sha in self.heads.items()]

    def pull(self, number):
        return {"number": number, "head": {"sha": self.heads[number]}}

    def comments(self, number):
        self.fail("comments-" + str(number))
        return copy.deepcopy(self.thread[number])

    def status(self, sha):
        self.fail("status-read")
        return copy.deepcopy(self.statuses[sha][-1]) if self.statuses.get(sha) else None

    def set_status(self, sha, status):
        self.fail("status")
        self.fail("status-" + sha)
        self.calls.append(("status", sha, status["state"]))
        self.statuses.setdefault(sha, []).append(dict(status))

    def create_comment(self, number, body):
        self.fail("comment")
        self.fail("summary" if body.startswith(gate.SUMMARY) else "reply")
        self.calls.append(("comment", number, body.splitlines()[0]))
        return self.comment(number, body, BOT)

    def update_comment(self, identifier, body):
        self.fail("summary")
        self.calls.append(("update", identifier))
        for thread in self.thread.values():
            for comment in thread:
                if comment["id"] == identifier:
                    comment["body"] = body

    def state(self, sha):
        return self.statuses[sha][-1]["state"] if self.statuses.get(sha) else None

    def description(self, sha):
        return self.statuses[sha][-1]["description"]

    def records(self, number):
        return gate.ledger(self.thread[number], number)[0]

    def reply(self, number):
        """The last comment of the workflow token besides the summary."""
        return [comment["body"] for comment in self.thread[number]
                if comment["user"] == BOT and not comment["body"].startswith(gate.SUMMARY)][-1]

    def summary(self, number):
        found = [comment["body"] for comment in self.thread[number] if comment["body"].startswith(gate.SUMMARY)]
        assert len(found) <= 1, found
        return found[0] if found else None


class Flow:
    """The jobs of ci-command.yml for one comment, each a separate call so a
    test can interleave two commands."""

    def __init__(self, repository):
        self.repository = repository

    def evaluate(self):
        return gate.evaluate_all(self.repository, SERVER, NAME, log=lambda line: None)

    def request(self, number, verb, names, selectors=()):
        """Command: the reviewer's comment and the head it read."""
        comment = self.repository.comment(number, "/ci " + verb + " " + ci_command.selection_text(names, selectors))
        return {**ci_command.parse(verb + " " + ci_command.selection_text(names, selectors)),
                "sha": self.repository.heads[number], "pull": number, "comment_id": comment["id"], "by": "JSYoo5B"}

    def register(self, request, resolved=True):
        decision = gate.register(self.repository, request, resolved, self.run_url(request), SERVER, NAME,
                                 log=lambda line: None)
        self.evaluate()
        return decision

    def command(self, number, verb, names, selectors=(), resolved=True):
        request = self.request(number, verb, names, selectors)
        return request, self.register(request, resolved)

    @staticmethod
    def run_url(request):
        return SERVER + "/" + NAME + "/actions/runs/" + str(request["comment_id"])

    def report(self, request, outcome, register=None):
        """Report: the reply of the finished run, without Gate."""
        command = {"accepted": "true", "sha": request["sha"], "request": json.dumps(request)}
        validate = {"accepted": "true", "batches": '["short/' + request["names"][0] + '"]', "releases": '["20.2.4"]'}
        reply = ci_command.report(command, validate, "success", outcome, self.run_url(request),
                                  {"attempt": "true", "run": "true"} if register is None else register, "success")
        if reply["reply"]:
            self.repository.comment(request["pull"], reply["reply"], BOT)
        return reply

    def finish(self, request, outcome):
        self.report(request, outcome)
        self.evaluate()


class Case(unittest.TestCase):
    def setUp(self):
        self.repository = Repository({10: A})
        self.flow = Flow(self.repository)

    def assertGate(self, sha, state, description=None):
        self.assertEqual(self.repository.state(sha), state, self.repository.statuses.get(sha))
        if description is not None:
            self.assertIn(description, self.repository.description(sha))


class GateTests(Case):
    def test_a_pull_request_without_requirements_passes_and_gets_no_comment(self):
        self.flow.evaluate()
        self.assertGate(A, "success", "No requested CI is required")
        self.assertEqual(self.repository.thread[10], [])
        self.assertEqual(self.repository.statuses[A][0]["target_url"], SERVER + "/" + NAME + "/pull/10")

    def test_an_unchanged_status_is_not_set_again(self):
        # GitHub bounds the statuses of one commit and context, and every
        # pull request event of the repository evaluates every head.
        for _ in range(3):
            self.flow.evaluate()
        self.assertEqual(len(self.repository.statuses[A]), 1)
        self.flow.command(10, "require", ["network_delay"])
        count, updates = len(self.repository.statuses[A]), self.repository.calls.count(("update", mock.ANY))
        for _ in range(3):
            self.flow.evaluate()
        self.assertEqual(len(self.repository.statuses[A]), count)
        self.assertEqual(self.repository.calls.count(("update", mock.ANY)), updates)
        # Nor is a summary that GitHub returns with other line endings.
        for comment in self.repository.thread[10]:
            if comment["body"].startswith(gate.SUMMARY):
                comment["body"] = comment["body"].replace("\n", "\r\n") + "\r\n"
        self.flow.evaluate()
        self.assertEqual(self.repository.calls.count(("update", mock.ANY)), updates)

    def test_an_advisory_run_never_changes_the_status(self):
        request = self.flow.request(10, "run", ["network_delay"])
        for outcome in ("failure", "cancelled", "success"):
            reply = self.flow.report(request, outcome, register={})
            self.assertFalse(reply["reply"].startswith("<!--"), reply["reply"])
            self.flow.evaluate()
            self.assertGate(A, "success", "No requested CI is required")
        self.assertEqual(self.repository.records(10), [])
        self.assertIsNone(self.repository.summary(10))

    def test_a_required_selection_blocks_until_it_passes_on_the_head(self):
        request, decision = self.flow.command(10, "require", ["network_delay"], ["squid"])
        self.assertEqual(decision, {"attempt": True, "run": True})
        self.assertGate(A, "pending", "0 of 1 required selection(s) passed on " + A[:12] + "; 1 running")
        self.flow.finish(request, "failure")
        self.assertGate(A, "failure", "1 failed")
        retry, decision = self.flow.command(10, "retry", ["network_delay"], ["squid"])
        self.assertEqual(decision, {"attempt": True, "run": True})
        self.assertGate(A, "pending", "1 running")
        self.flow.finish(retry, "success")
        self.assertGate(A, "success", "1 of 1 required selection(s) passed on " + A[:12])

    def test_requirements_are_cumulative_and_every_one_must_pass(self):
        first, _ = self.flow.command(10, "require", ["network_delay"])
        second, _ = self.flow.command(10, "require", ["network_delay"], ["squid"])
        third, _ = self.flow.command(10, "require", ["rbd_mirror_bandwidth"])
        self.assertEqual(len(gate.fold(self.repository.records(10))), 3)
        self.flow.finish(first, "success")
        self.flow.finish(third, "success")
        self.assertGate(A, "pending", "2 of 3 required selection(s) passed")
        self.flow.finish(second, "success")
        self.assertGate(A, "success", "3 of 3")

    def test_a_new_push_does_not_inherit_the_success_of_the_old_head(self):
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "success")
        self.assertGate(A, "success")
        self.repository.push(10, B)
        self.assertIsNone(self.repository.state(B))
        self.flow.evaluate()
        self.assertGate(B, "pending", "0 of 1 required selection(s) passed on " + B[:12]
                        + "; 1 without a run on this head")
        self.assertIn("no run on this head", self.repository.summary(10))
        # The requirement itself survived the push, and only a run of the
        # new head satisfies it.
        retry, _ = self.flow.command(10, "retry", ["network_delay"])
        self.assertEqual(retry["sha"], B)
        self.flow.finish(retry, "success")
        self.assertGate(B, "success")

    def test_a_run_that_started_before_a_push_does_not_pass_the_new_head(self):
        request = self.flow.request(10, "require", ["network_delay"])
        self.repository.push(10, B)
        self.flow.register(request)
        self.assertGate(B, "pending", "1 without a run on this head")
        self.assertIn("The head is now " + B[:12], self.repository.reply(10))
        self.flow.finish(request, "success")
        self.assertGate(B, "pending", "1 without a run on this head")

    def test_pull_requests_that_share_a_head_share_the_worst_status(self):
        self.repository = Repository({10: A, 11: A, 12: B})
        self.flow = Flow(self.repository)
        self.flow.evaluate()
        self.assertGate(A, "success")
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.assertGate(A, "pending", "#10: 0 of 1 required selection(s) passed")
        self.assertIn("head shared by #10, #11", self.repository.description(A))
        self.assertGate(B, "success", "No requested CI is required")
        self.assertIsNone(self.repository.summary(11))
        self.flow.finish(request, "success")
        self.assertGate(A, "success")

    def test_the_result_of_another_pull_request_at_the_same_head_satisfies_nothing(self):
        self.repository = Repository({10: A, 11: A})
        self.flow = Flow(self.repository)
        first, _ = self.flow.command(10, "require", ["network_delay"])
        second, _ = self.flow.command(11, "require", ["network_delay"])
        self.assertEqual(first["id"], second["id"])
        self.flow.finish(first, "success")
        self.assertGate(A, "pending", "#11: 0 of 1 required selection(s) passed")
        # Not even when the workflow token repeats the first pull request's
        # result on the second one.
        result = [comment for comment in self.repository.thread[10] if '"op":"result"' in comment["body"]][-1]
        self.repository.comment(11, result["body"], BOT)
        self.flow.evaluate()
        self.assertGate(A, "error", "#11: 1 ledger comment(s) of #11 cannot be trusted")
        self.assertIn("written for pull request 10", self.repository.summary(11))

    def test_two_requirements_that_finish_in_reverse_order_pass_only_together(self):
        first, _ = self.flow.command(10, "require", ["network_delay"])
        second, _ = self.flow.command(10, "require", ["rbd_mirror_bandwidth"])
        self.flow.finish(second, "success")
        self.assertGate(A, "pending", "1 of 2 required selection(s) passed on " + A[:12] + "; 1 running")
        self.flow.finish(first, "success")
        self.assertGate(A, "success", "2 of 2")

    def test_the_late_result_of_a_replaced_attempt_changes_nothing(self):
        for late, current, state in (("failure", "success", "success"), ("success", "failure", "failure")):
            with self.subTest(late=late):
                self.setUp()
                first, _ = self.flow.command(10, "require", ["network_delay"])
                second, _ = self.flow.command(10, "retry", ["network_delay"])
                self.flow.finish(second, current)
                self.assertGate(A, state)
                self.flow.finish(first, late)
                self.assertGate(A, state)
        # The replaced attempt's success does not stand in for a result of
        # the current one either.
        self.setUp()
        first, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.command(10, "retry", ["network_delay"])
        self.flow.finish(first, "success")
        self.assertGate(A, "pending", "1 running")

    def test_a_result_from_before_a_requirement_changed_is_stale(self):
        first, _ = self.flow.command(10, "require", ["network_delay"])
        _, decision = self.flow.command(10, "drop", ["network_delay"])
        self.assertEqual(decision, {"attempt": False, "run": False})
        self.assertGate(A, "success", "No requested CI is required")
        second, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(first, "success")
        self.assertGate(A, "pending", "1 running")
        self.flow.finish(second, "success")
        self.assertGate(A, "success")
        # A result with no requirement at all is ignored.
        self.flow.command(10, "drop", ["network_delay"])
        self.flow.finish(second, "failure")
        self.assertGate(A, "success", "No requested CI is required")

    def test_a_cancelled_skipped_or_unknown_run_fails_the_requirement(self):
        for outcome in ("cancelled", "skipped", "", "neutral"):
            with self.subTest(outcome=outcome):
                self.setUp()
                request, _ = self.flow.command(10, "require", ["network_delay"])
                self.flow.finish(request, outcome)
                self.assertGate(A, "failure", "1 did not complete")
                self.assertIn("run did not complete", self.repository.summary(10))

    def test_a_result_that_was_never_recorded_never_passes(self):
        # Report failed or was cancelled before it replied: the publisher's
        # failure leaves the selection running, however often it is evaluated.
        self.flow.command(10, "require", ["network_delay"])
        for _ in range(2):
            self.flow.evaluate()
            self.assertGate(A, "pending", "1 running")
        self.assertIn("no result was recorded", self.repository.summary(10))

    def test_a_rerun_of_the_current_attempt_replaces_its_result(self):
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "failure")
        self.assertGate(A, "failure")
        self.flow.finish(request, "success")
        self.assertGate(A, "success")

    def test_a_retry_whose_names_no_longer_resolve_blocks_until_a_reviewer_decides(self):
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "success")
        self.repository.push(10, B)
        _, decision = self.flow.command(10, "retry", ["network_delay"], resolved=False)
        self.assertEqual(decision, {"attempt": False, "run": False})
        self.assertGate(B, "failure", "1 no longer resolve")
        self.assertIn("names no longer resolve", self.repository.summary(10))
        # Evaluating again does not clear it; a head that resolves the
        # names again, or dropping the selection, does.
        self.flow.evaluate()
        self.assertGate(B, "failure")
        self.repository.push(10, C)
        retry, _ = self.flow.command(10, "retry", ["network_delay"])
        self.flow.finish(retry, "success")
        self.assertGate(C, "success")
        self.repository.push(10, B)
        self.flow.command(10, "retry", ["network_delay"], resolved=False)
        self.flow.command(10, "drop", ["network_delay"])
        self.assertGate(B, "success", "No requested CI is required")

    def main(self, *arguments):
        """main with the repository in place of GitHub: its exit code, or the
        exception it ended with, and the outputs it wrote."""
        with tempfile.TemporaryDirectory() as directory, mock.patch("sys.stdout"), \
                mock.patch.object(gate, "GitHub", return_value=self.repository), \
                mock.patch.dict("os.environ", {"GITHUB_REPOSITORY": NAME}):
            output = Path(directory) / "output"
            try:
                code = gate.main([*arguments, "--github-output", str(output)])
            except Exception as error:  # noqa: BLE001 - the test reads it
                code = error
            return code, output.read_text() if output.exists() else ""

    def test_an_unreadable_pull_request_withdraws_the_green_status_of_its_head(self):
        # #10 passed its requirement on A and is pushed to B, a commit that
        # is already green for #11. Its ledger cannot be read: B must not
        # stay green, because #10 has no evidence there.
        self.repository = Repository({10: A, 11: B})
        self.flow = Flow(self.repository)
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "success")
        self.assertGate(A, "success")
        self.assertGate(B, "success", "No requested CI is required")
        self.repository.push(10, B)
        self.repository.failing.add("comments-10")
        result = self.flow.evaluate()
        self.assertEqual((result["unread"], result["unpublished"]), ([10], []))
        self.assertGate(B, "error", "#10: The requirements of #10 could not be read; head shared by #10, #11")
        self.assertEqual(self.main("evaluate"), (1, "settled=true\n"))
        # Readable again, the same state is what the ledger says.
        self.repository.failing.clear()
        self.assertEqual(self.main("evaluate"), (0, "settled=true\n"))
        self.assertGate(B, "pending", "#10: 0 of 1 required selection(s) passed on " + B[:12])

    def test_an_unreadable_pull_request_is_an_error_whatever_its_head_showed(self):
        self.repository = Repository({10: A, 11: B, 12: A})
        self.flow = Flow(self.repository)
        self.repository.failing.add("comments-10")
        # A head without any status, one that was green and one that failed.
        self.flow.evaluate()
        self.assertGate(A, "error", "#10: The requirements of #10 could not be read")
        self.assertGate(B, "success")
        self.repository.failing.clear()
        request, _ = self.flow.command(12, "require", ["network_delay"])
        self.flow.finish(request, "failure")
        self.assertGate(A, "failure")
        self.repository.failing.add("comments-12")
        del self.repository.calls[:]
        self.flow.evaluate()
        self.assertGate(A, "error", "#12: The requirements of #12 could not be read")
        # No summary is written for a pull request that was not read.
        self.assertEqual(self.repository.calls, [("status", A, "error")])

    def test_a_pull_request_that_cannot_be_evaluated_is_an_error(self):
        # Comments that are not what the API returns, not only a failed call.
        self.flow.evaluate()
        self.assertGate(A, "success")
        self.repository.thread[10].append({"body": "no id"})
        result = self.flow.evaluate()
        self.assertEqual(result["unread"], [10])
        self.assertGate(A, "error", "The requirements of #10 could not be read")

    def test_a_status_that_cannot_be_read_is_set(self):
        self.flow.evaluate()
        self.repository.failing.add("status-read")
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.assertEqual(self.repository.statuses[A][-1]["state"], "pending")
        self.assertIn("1 running", self.repository.statuses[A][-1]["description"])
        self.assertEqual(self.flow.evaluate()["unpublished"], [])

    def test_a_status_that_cannot_be_set_is_reported_and_stops_neither_the_others_nor_the_job_failure(self):
        self.repository = Repository({10: A, 11: B})
        self.flow = Flow(self.repository)
        self.flow.evaluate()
        request = self.flow.request(10, "require", ["network_delay"])
        gate.register(self.repository, request, True, Flow.run_url(request), SERVER, NAME, log=lambda line: None)
        self.repository.push(11, C)
        self.repository.failing.add("status-" + A)
        result = self.flow.evaluate()
        self.assertEqual(result["unpublished"], [A])
        self.assertGate(C, "success")
        # A keeps the pending that Register set before the acknowledgment,
        # and the summary does not say more than the status.
        self.assertGate(A, "pending", "Recording a change")
        self.assertIsNone(self.repository.summary(10))
        # The job fails without settled, so the workflow withdraws the
        # status of the commit that started it.
        self.assertEqual(self.main("evaluate"), (1, ""))
        self.repository.failing.clear()
        self.assertEqual(self.main("evaluate"), (0, "settled=true\n"))
        self.assertIsNotNone(self.repository.summary(10))

    def test_a_summary_that_cannot_be_written_fails_the_job_after_the_statuses(self):
        self.flow.evaluate()
        self.repository.failing.add("summary")
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.assertGate(A, "pending", "1 running")
        self.assertEqual(self.flow.evaluate()["unsummarized"], [10])
        self.assertEqual(self.main("evaluate"), (1, "settled=true\n"))

    def test_an_evaluation_that_cannot_list_the_pull_requests_is_not_settled(self):
        self.flow.evaluate()
        self.repository.failing.add("pulls")
        code, output = self.main("evaluate")
        self.assertIsInstance(code, subprocess.CalledProcessError)
        self.assertEqual(output, "")

    def test_a_pull_request_without_a_head_is_reported_and_the_others_are_published(self):
        self.repository = Repository({10: A, 11: B})
        self.flow = Flow(self.repository)
        pulls = self.repository.open_pulls()
        for broken in ({"number": 10, "head": None}, {"number": 10, "head": {"sha": "main"}}, {"head": {"sha": A}}):
            with self.subTest(broken=broken), mock.patch.object(self.repository, "open_pulls",
                                                                return_value=[broken, pulls[1]]):
                result = self.flow.evaluate()
                self.assertEqual(len(result["unpublished"]), 1)
                self.assertGate(B, "success")
                self.assertEqual(self.main("evaluate"), (1, ""))


class RegisterTests(Case):
    def test_identical_requests_are_registered_once_whatever_their_spelling(self):
        first, _ = self.flow.command(10, "require", ["network_delay", "rbd_mirror_bandwidth"], ["squid"])
        records = len(self.repository.records(10))
        second, decision = self.flow.command(10, "require", ["rbd_mirror_bandwidth", "network_delay"], ["squid"])
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(decision, {"attempt": False, "run": False})
        self.assertEqual(len(self.repository.records(10)), records)
        self.assertEqual(len(gate.fold(self.repository.records(10))), 1)
        reply = self.repository.reply(10)
        self.assertTrue(reply.startswith("Already required: `rbd_mirror_bandwidth network_delay on squid` is running"))
        # The first command stays the attempt, so the repeated comment's
        # run, had it started, could not pass the requirement.
        self.flow.finish(second, "success")
        self.assertGate(A, "pending", "1 running")
        self.flow.finish(first, "success")
        self.assertGate(A, "success")

    def test_the_identity_is_the_selection_as_typed(self):
        identity = ci_command.selection_id
        self.assertEqual(identity(["a", "b"], ["squid"]), identity(["b", "a", "a"], ["squid"]))
        self.assertNotEqual(identity(["a"], []), identity(["a"], ["tentacle"]))
        self.assertNotEqual(identity(["a", "squid"], []), identity(["a"], ["squid"]))
        self.assertNotEqual(identity(["a"], ["squid"]), identity(["a"], ["squid", "tentacle"]))

    def test_a_selection_is_removed_only_by_an_explicit_drop_of_it(self):
        request, _ = self.flow.command(10, "require", ["network_delay"], ["squid"])
        self.flow.finish(request, "failure")
        for names, selectors in ((["network_delay"], []), (["network_faults"], ["squid"])):
            before = len(self.repository.records(10))
            _, decision = self.flow.command(10, "drop", names, selectors)
            self.assertEqual(decision, {"attempt": False, "run": False})
            self.assertEqual(len(self.repository.records(10)), before)
            self.assertIn("is not a required selection", self.repository.reply(10))
            self.assertIn("Required: `network_delay on squid`.", self.repository.reply(10))
            self.assertGate(A, "failure")
        # A drop needs no resolution: the names may be gone from the head.
        self.flow.command(10, "drop", ["network_delay"], ["squid"], resolved=False)
        self.assertGate(A, "success", "No requested CI is required")
        record = self.repository.records(10)[-1]
        self.assertEqual((record["op"], record["by"]), ("drop", "JSYoo5B"))
        self.assertIn("**success**", self.repository.summary(10))

    def test_retry_and_drop_of_a_selection_that_is_not_required_change_nothing(self):
        for verb in ("retry", "drop"):
            with self.subTest(verb=verb):
                _, decision = self.flow.command(10, verb, ["network_delay"])
                self.assertEqual(decision, {"attempt": False, "run": False})
                self.assertEqual(self.repository.records(10), [])
                self.assertIn("It requires none.", self.repository.reply(10))
                self.assertGate(A, "success")

    def test_a_require_with_unresolved_names_changes_nothing(self):
        _, decision = self.flow.command(10, "require", ["netwrok_delay"], resolved=False)
        self.assertEqual(decision, {"attempt": False, "run": False})
        self.assertEqual(self.repository.records(10), [])
        self.assertGate(A, "success", "No requested CI is required")

    def test_the_green_status_is_withdrawn_before_the_acknowledgment(self):
        self.flow.evaluate()
        self.assertGate(A, "success")
        for verb, prepare in (("require", None), ("retry", "success"), ("retry", "unresolved")):
            with self.subTest(verb=verb, prepare=prepare):
                del self.repository.calls[:]
                request = self.flow.request(10, verb, ["network_delay"])
                gate.register(self.repository, request, prepare != "unresolved", Flow.run_url(request), SERVER, NAME,
                              log=lambda line: None)
                # No evaluation has run yet: these are register's own calls.
                self.assertEqual([call[:2] for call in self.repository.calls], [("status", A), ("comment", 10)])
                self.assertEqual(self.repository.calls[0][2], "pending")
                self.assertTrue(self.repository.calls[1][2].startswith("<!-- required-requested-ci {"))
                self.flow.evaluate()
                self.flow.finish(request, "success")
                self.assertEqual(self.repository.state(A), "failure" if prepare == "unresolved" else "success")

    def test_a_command_that_is_not_yet_processed_is_not_a_requirement(self):
        # The window before Register runs: the comment exists, the status is
        # still green, and nothing acknowledged it.
        self.flow.request(10, "require", ["network_delay"])
        self.flow.evaluate()
        self.assertGate(A, "success", "No requested CI is required")
        self.assertEqual(self.repository.records(10), [])
        self.assertFalse([comment for comment in self.repository.thread[10] if comment["user"] == BOT])

    def test_an_acknowledgment_that_cannot_be_written_leaves_the_status_withdrawn(self):
        self.flow.evaluate()
        request = self.flow.request(10, "require", ["network_delay"])
        self.repository.failing.add("comment")
        with self.assertRaises(subprocess.CalledProcessError):
            gate.register(self.repository, request, True, Flow.run_url(request), SERVER, NAME, log=lambda line: None)
        self.assertGate(A, "pending", "Recording a change")
        self.assertEqual(self.repository.records(10), [])
        # Nothing was accepted, so the next evaluation passes again.
        self.repository.failing.clear()
        self.flow.evaluate()
        self.assertGate(A, "success", "No requested CI is required")

    def test_a_status_that_cannot_be_withdrawn_records_nothing(self):
        self.flow.evaluate()
        request = self.flow.request(10, "require", ["network_delay"])
        self.repository.failing.add("status")
        with self.assertRaises(subprocess.CalledProcessError):
            gate.register(self.repository, request, True, Flow.run_url(request), SERVER, NAME, log=lambda line: None)
        self.assertEqual(self.repository.records(10), [])

    def test_a_drop_is_recorded_before_any_status_changes(self):
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "failure")
        del self.repository.calls[:]
        drop = self.flow.request(10, "drop", ["network_delay"])
        gate.register(self.repository, drop, False, Flow.run_url(drop), SERVER, NAME, log=lambda line: None)
        self.assertEqual([call[:2] for call in self.repository.calls], [("comment", 10)])
        self.assertGate(A, "failure")

    def test_main_writes_the_outputs_before_it_evaluates(self):
        request = self.flow.request(10, "require", ["network_delay"])
        self.repository.failing.add("comments-10")
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(gate, "GitHub", return_value=self.repository), \
                mock.patch("sys.stdout"), mock.patch.dict("os.environ", {"GITHUB_REPOSITORY": NAME}):
            arguments = ["register", "--request", json.dumps(request), "--run-url", Flow.run_url(request),
                         "--github-output", str(Path(directory) / "output")]
            # Register cannot read the ledger: nothing is recorded or announced.
            with self.assertRaises(subprocess.CalledProcessError):
                gate.main(arguments + ["--resolved", "true"])
            self.assertFalse((Path(directory) / "output").exists())
            self.repository.failing.clear()
            self.assertEqual(gate.main(arguments + ["--resolved", "true"]), 0)
            self.assertEqual((Path(directory) / "output").read_text(), "attempt=true\nrun=true\nsettled=true\n")
            # The record is written and the evaluation after it fails: the
            # run it announced still starts, and nothing is settled.
            retry = self.flow.request(10, "retry", ["network_delay"])
            (Path(directory) / "output").unlink()
            with mock.patch.object(gate, "evaluate_all", side_effect=RuntimeError("no API")), \
                    self.assertRaises(RuntimeError):
                gate.main(["register", "--request", json.dumps(retry), "--run-url", Flow.run_url(retry),
                           "--github-output", str(Path(directory) / "output"), "--resolved", "true"])
            self.assertEqual((Path(directory) / "output").read_text(), "attempt=true\nrun=true\n")
            self.assertGate(A, "pending", "Recording a change")
            with mock.patch("sys.stderr"), self.assertRaises(SystemExit):
                gate.main(["register", "--request", json.dumps({**request, "verb": "run"}), "--resolved", "true",
                           "--run-url", Flow.run_url(request)])


class LedgerTests(Case):
    def record(self, **changes):
        record = {"v": 1, "op": "require", "pull": 10, "id": ci_command.selection_id(["network_delay"], []),
                  "comment": 1, "sha": A, "names": ["network_delay"], "selectors": [], "by": "JSYoo5B",
                  "run": "https://github.com/o/r/actions/runs/1"}
        record.update(changes)
        return {key: value for key, value in record.items() if value is not None}

    def test_only_the_workflow_token_writes_records(self):
        line = ci_command.ledger_line(self.record())
        for user in (HUMAN, {"login": "JSYoo5B", "type": "Bot"}, {"login": gate.BOT, "type": "User"},
                     {"login": "dependabot[bot]", "type": "Bot"}):
            with self.subTest(user=user):
                self.repository.comment(10, line + "\nRequired.", user)
                self.assertEqual(gate.ledger(self.repository.thread[10], 10), ([], []))
        # A record is the first line: a reply that quotes one is not one.
        self.repository.comment(10, "Quoting:\n" + line, BOT)
        self.repository.comment(10, " " + line, BOT)
        self.assertEqual(gate.ledger(self.repository.thread[10], 10), ([], []))
        self.repository.comment(10, line + "\r\nRequired.", BOT)
        self.assertEqual(len(gate.ledger(self.repository.thread[10], 10)[0]), 1)

    def test_an_edited_record_makes_the_status_an_error_until_it_is_deleted(self):
        request, _ = self.flow.command(10, "require", ["network_delay"])
        self.flow.finish(request, "failure")
        result = self.repository.thread[10][-1]
        result["body"] = result["body"].replace('"outcome":"failure"', '"outcome":"success"')
        result["updated_at"] = "2026-01-02T00:00:00Z"
        self.flow.evaluate()
        self.assertGate(A, "error", "1 ledger comment(s) of #10 cannot be trusted")
        self.assertIn("edited after it was written. Delete it", self.repository.summary(10))
        self.repository.thread[10].remove(result)
        self.flow.evaluate()
        self.assertGate(A, "pending", "1 running")

    def test_records_that_the_workflow_cannot_have_written_are_refused(self):
        bad = {
            "unknown record": [self.record(v=2), self.record(op="approve"), {}],
            "unexpected fields": [self.record(extra=1), self.record(run=None), self.record(op="drop"),
                                  self.record(op="result")],
            "written for pull request": [self.record(pull=11)],
            "malformed identity": [self.record(comment="1"), self.record(comment=True), self.record(comment=0),
                                   self.record(sha="main"), self.record(sha=A + "0"), self.record(id="short"),
                                   self.record(id=7)],
            "malformed author": [self.record(by="x y"), self.record(by="@a")],
            "malformed by": [self.record(by=1)],
            "malformed run link": [self.record(run="http://example.invalid"), self.record(run="https://a b"),
                                   self.record(run="https://a)[x](https://b")],
            "selection does not match": [self.record(names=["other"]), self.record(names=[]),
                                         self.record(names=["a b"]), self.record(names="network_delay"),
                                         self.record(selectors=["x`"]), self.record(names=["a", "a"])],
            "unknown outcome": [{**self.record(op="result", outcome="passed"), "names": None, "selectors": None,
                                 "by": None}],
        }
        for reason, records in bad.items():
            for record in records:
                with self.subTest(reason=reason, record=record):
                    record = ({key: value for key, value in record.items() if value is not None}
                              if isinstance(record, dict) else record)
                    self.repository.thread[10] = []
                    self.repository.comment(10, ci_command.ledger_line(record), BOT)
                    records, problems = gate.ledger(self.repository.thread[10], 10)
                    self.assertEqual(records, [])
                    self.assertIn(reason, problems[0]["reason"])
                    self.assertEqual(gate.evaluate(10, A, records, problems)["state"], "error")
        self.repository.thread[10] = []
        self.repository.comment(10, "<!-- required-requested-ci {not json} -->", BOT)
        self.assertEqual(gate.ledger(self.repository.thread[10], 10)[1][0]["reason"], "not JSON")

    def test_a_second_require_record_of_a_selection_never_replaces_its_attempt(self):
        # Register writes no second record, and the ledger does not depend
        # on that: the first command stays the one whose result counts.
        first, second = self.record(comment=1, url="u1"), self.record(comment=2, url="u2")
        result = {"v": 1, "op": "result", "pull": 10, "id": first["id"], "sha": A, "run": first["run"],
                  "outcome": "success", "url": "u3"}
        late = gate.fold([first, second, {**result, "comment": 2}])[first["id"]]
        self.assertEqual((late["request"], late["attempt"]["comment"], late["result"]), (1, 1, None))
        self.assertEqual(gate.evaluate(10, A, [first, second, {**result, "comment": 2}], [])["state"], "pending")
        self.assertEqual(gate.evaluate(10, A, [first, second, {**result, "comment": 1}], [])["state"], "success")

    def test_the_records_of_the_flow_are_exactly_the_checked_ones(self):
        request, _ = self.flow.command(10, "require", ["network_delay"], ["squid"])
        self.flow.finish(request, "failure")
        self.flow.command(10, "retry", ["network_delay"], ["squid"], resolved=False)
        self.flow.command(10, "retry", ["network_delay"], ["squid"])
        self.flow.command(10, "drop", ["network_delay"], ["squid"])
        records, problems = gate.ledger(self.repository.thread[10], 10)
        self.assertEqual(problems, [])
        self.assertEqual([record["op"] for record in records], ["require", "result", "blocked", "retry", "drop"])
        for record in records:
            self.assertEqual(set(record), gate.FIELDS[record["op"]] | {"v", "op", "url"})

    def test_the_summary_lists_each_selection_with_its_evidence_and_commands(self):
        first, _ = self.flow.command(10, "require", ["network_delay"], ["squid"])
        self.flow.command(10, "require", ["rbd_mirror_bandwidth"])
        self.flow.finish(first, "success")
        summary = self.repository.summary(10)
        self.assertIn("Head `" + A + "`: **pending**. 1 of 2 required selection(s) passed", summary)
        passed = [line for line in summary.splitlines() if line.startswith("| `network_delay on squid`")][0]
        for cell in ("| passed |", "| `" + A[:12] + "` |", "[run](" + Flow.run_url(first) + ")",
                     "[command](" + gate.comment_url(SERVER, NAME, 10, first["comment_id"]) + ")", "[result](",
                     "`JSYoo5B` in [comment](", "`/ci retry network_delay on squid` or `/ci drop network_delay on squid`"):
            self.assertIn(cell, passed)
        self.assertIn("| `rbd_mirror_bandwidth` | running |", summary)
        self.assertIn("Ledger: 3 record(s)", summary)
        # A summary line is never a command, and the status links to it.
        for comment in self.repository.thread[10]:
            for line in comment["body"].splitlines()[1 if comment["user"] == BOT else 0:]:
                self.assertFalse(comment["user"] == BOT and line.startswith("/ci"), line)
        self.assertEqual(self.repository.statuses[A][-1]["target_url"],
                         [comment for comment in self.repository.thread[10]
                          if comment["body"].startswith(gate.SUMMARY)][0]["html_url"])

    def test_a_long_description_is_cut_to_the_status_limit(self):
        views = [gate.evaluate(number, A, [], [{"url": "", "reason": "x"}]) for number in range(100000, 100030)]
        _, status = gate.combine(views)
        self.assertGreater(len(status["description"]), gate.MAX_DESCRIPTION - 4)
        self.assertLessEqual(len(status["description"]), gate.MAX_DESCRIPTION)
        self.assertTrue(status["description"].endswith("..."))
        self.assertEqual(status["state"], "error")


class GitHubTests(unittest.TestCase):
    def test_calls_go_through_gh_with_the_repository_path(self):
        calls = []

        def run(args, input, **options):
            calls.append((args, input))
            page = int(args[-1].rsplit("page=", 1)[1]) if "page=" in args[-1] else 0
            if "status?" in args[-1]:
                rows = [{"context": "CI command 1: x on 20.2.4", "state": "failure"}] * (100 if page == 1 else 1)
                if page == 2:
                    rows = [{"context": gate.CONTEXT, "state": "pending", "description": "d", "target_url": None,
                             "creator": {"login": gate.BOT}}]
                return mock.Mock(stdout=json.dumps({"statuses": rows}).encode())
            return mock.Mock(stdout=json.dumps([{"number": page}] * (100 if page == 1 else 2) if page else {}).encode())

        github = gate.GitHub("o/r", run, sleep=lambda seconds: self.fail("no retry expected"))
        self.assertEqual(len(github.open_pulls()), 102)
        self.assertEqual([call[0][-1] for call in calls], ["repos/o/r/pulls?state=open&per_page=100&page=1",
                                                           "repos/o/r/pulls?state=open&per_page=100&page=2"])
        self.assertEqual(github.status(A), {"state": "pending", "description": "d", "target_url": ""})
        del calls[:]
        github.set_status(A, {"state": "success", "description": "d", "target_url": "u"})
        github.create_comment(10, "body")
        github.update_comment(7, "body")
        self.assertEqual([call[0][2:5] for call in calls], [
            ["--method", "POST", "repos/o/r/statuses/" + A], ["--method", "POST", "repos/o/r/issues/10/comments"],
            ["--method", "PATCH", "repos/o/r/issues/comments/7"]])
        self.assertEqual(json.loads(calls[0][1]), {"state": "success", "description": "d", "target_url": "u",
                                                   "context": gate.CONTEXT})


    def test_a_read_is_repeated_and_a_write_is_not(self):
        calls, pauses = [], []

        def run(args, **options):
            calls.append(args[3])
            if len(calls) < 3:
                raise subprocess.CalledProcessError(1, args)
            return mock.Mock(stdout=b"{}" if len(calls) > 3 else b"not json")

        github = gate.GitHub("o/r", run, sleep=pauses.append)
        with self.assertRaises(ValueError):
            github.pull(10)
        self.assertEqual((calls, pauses), (["GET"] * 3, list(gate.GitHub.RETRY)))
        self.assertEqual(github.pull(10), {})
        del calls[:], pauses[:]

        def refuse(args, **options):
            calls.append(args[3])
            raise subprocess.CalledProcessError(1, args)

        github = gate.GitHub("o/r", refuse, sleep=pauses.append)
        for write in (lambda: github.set_status(A, {}), lambda: github.create_comment(10, "b"),
                      lambda: github.update_comment(7, "b")):
            with self.assertRaises(subprocess.CalledProcessError):
                write()
        self.assertEqual((calls, pauses), (["POST", "POST", "PATCH"], []))


if __name__ == "__main__":
    unittest.main()
