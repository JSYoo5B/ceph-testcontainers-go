"""Exercise the cutover against an in-memory repository without GitHub writes."""
import base64
from contextlib import redirect_stdout
from copy import deepcopy
from fnmatch import fnmatchcase
from io import StringIO
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import unquote

import git_flow as flow


class Repository:
    def __init__(self):
        self.policies = {}
        self.branches = {"main": "a" * 40}
        self.writes = []
        self.default_branch = "main"
        self.admin = True
        self.extra = []
        self.releases = [{"tag_name": "v1.0.0", "draft": False, "prerelease": False}]
        self.pulls = []
        self.statuses = {}

    def api(self, path, *, method="GET", body=None, pages=False):
        path = path.split("?")[0]
        if method != "GET":
            self.writes.append((path, method, deepcopy(body)))
            if path == "git/refs":
                name = body["ref"].removeprefix("refs/heads/")
                if name in self.branches:
                    raise AssertionError("duplicate seed")
                self.branches[name] = body["sha"]
                return {"object": {"sha": body["sha"]}}
            key = len(self.policies) + 1 if method == "POST" else int(path.split("/")[1])
            self.policies[key] = dict(body, id=key, source=flow.REPO, source_type="Repository")
            return self.policies[key]
        if not path:
            return {"default_branch": self.default_branch, "allow_merge_commit": True, "permissions": {"admin": self.admin}}
        if path == "rulesets":
            return list(self.policies.values())
        if path.startswith("rulesets/"):
            return self.policies[int(path.split("/")[1])]
        if path.startswith("rules/branches/"):
            ref = "refs/heads/" + unquote(path.split("/")[-1])
            result = list(self.extra)
            for key, policy in self.policies.items():
                names = policy["conditions"]["ref_name"]
                if (any(fnmatchcase(ref, pattern.replace("**/", "")) for pattern in names["include"])
                        and ref not in names["exclude"]):
                    result.extend(dict(rule, ruleset_id=key) for rule in policy["rules"])
            return result
        if path == "branches":
            return [{"name": name, "commit": {"sha": sha}} for name, sha in self.branches.items()]
        if path == "releases":
            return self.releases
        if path == "pulls":
            return self.pulls
        if path.startswith("commits/") and path.endswith("/status"):
            return [{"context": context} for context in self.statuses.get(path.split("/")[1], [])]
        if path.startswith("releases/tags/"):
            return {"draft": False, "prerelease": False}
        if path.startswith("git/ref/tags/"):
            return {"object": {"type": "commit", "sha": flow.BASE_SHA}}
        raise AssertionError(path)


class CutoverTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository()
        for cm in (redirect_stdout(StringIO()), patch.object(flow, "api", self.repo.api),
                   patch.object(flow, "cutover_ready", return_value=self.repo.branches["main"])):
            cm.__enter__()
            self.addCleanup(cm.__exit__, None, None, None)

    def cutover(self):
        flow.main(["apply", "--policy-pr", "42"])

    def test_plan_and_idempotent_cutover(self):
        flow.main(["plan"])
        self.assertFalse(self.repo.writes)
        self.cutover()
        count = len(self.repo.writes)
        flow.main(["verify"])
        self.cutover()
        self.assertEqual(len(self.repo.writes), count)
        self.assertEqual(self.repo.branches["main"], self.repo.branches["develop"])
        core, main, tags = flow.baseline()
        self.assertEqual(core["rules"][2]["parameters"]["required_approving_review_count"], 0)
        self.assertTrue(main["rules"][0]["parameters"]["strict_required_status_checks_policy"])
        self.assertEqual(core["rules"][3]["parameters"]["required_status_checks"],
                         [{"context": name, "integration_id": flow.ACTIONS} for name in flow.CODE])
        self.assertEqual(tags["conditions"]["ref_name"]["include"], ["refs/tags/v*"])
        self.assertEqual(tags["rules"], [{"type": "update"}, {"type": "deletion"}])
        self.assertTrue(all(not p["bypass_actors"] for p in self.repo.policies.values()))

    def test_develop_default_after_bootstrap(self):
        self.cutover()
        self.repo.default_branch = "develop"
        self.repo.writes.clear()
        flow.main(["verify"])
        args = ["--line", "release/1.1", "--from", "develop"]
        flow.main(["plan", *args])
        with self.assertRaisesRegex(ValueError, "initial cutover"):
            self.cutover()
        self.assertFalse(self.repo.writes)
        flow.main(["apply", *args, "--at", self.repo.branches["develop"]])
        flow.main(["verify"])
        self.repo.default_branch = "topic"
        with self.assertRaisesRegex(ValueError, "default branch"):
            flow.main(["verify"])

    def test_registration_installs_gate_before_releasing_name_and_seeding(self):
        self.cutover()
        for line, source, sha in (("release/1.1", "develop", self.repo.branches["develop"]),
                                  ("support/1.0", "v1.0.0", flow.BASE_SHA)):
            args = ["--line", line, "--from", source]
            self.repo.writes.clear()
            flow.main(["plan", *args])
            with self.assertRaisesRegex(ValueError, "reviewed seed"):
                flow.main(["apply", *args, "--at", "f" * 40])
            self.assertFalse(self.repo.writes)
            flow.main(["apply", *args, "--at", sha])
            install, allow, seed = self.repo.writes
            self.assertEqual(install[2], flow.candidate(line))
            self.assertIn("refs/heads/" + line, allow[2]["conditions"]["ref_name"]["exclude"])
            self.assertEqual(seed[2], {"ref": "refs/heads/" + line, "sha": sha})
            flow.main(["apply", *args, "--at", sha])
            self.assertEqual(len(self.repo.writes), 3)
            flow.main(["verify"])

    def test_registration_can_resume_after_gate_install(self):
        self.cutover()
        self.repo.api("rulesets", method="POST", body=flow.candidate("release/1.1"))
        self.repo.writes.clear()
        flow.main(["apply", "--line", "release/1.1", "--from", "develop", "--at", self.repo.branches["develop"]])
        self.assertEqual([op[1] for op in self.repo.writes], ["PUT", "POST"])

    def test_retry_after_guard_update_preserves_a_conflicting_branch(self):
        self.cutover()
        line = "support/1.0"
        args = ["apply", "--line", line, "--from", "v1.0.0", "--at", flow.BASE_SHA]
        with patch.object(flow, "seed", side_effect=RuntimeError("interrupted")):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                flow.main(args)
        self.repo.writes.clear()
        self.repo.branches[line] = "b" * 40
        with self.assertRaisesRegex(ValueError, "another SHA"):
            flow.main(args)
        self.assertEqual(self.repo.branches[line], "b" * 40)
        self.assertFalse(self.repo.writes)
        del self.repo.branches[line]
        flow.main(args)
        self.assertEqual(self.repo.branches[line], flow.BASE_SHA)
        self.assertEqual([op[0] for op in self.repo.writes], ["git/refs"])

    def test_support_seed_tracks_latest_stable_patch_on_its_own_line(self):
        self.cutover()
        line = ["--line", "support/1.0", "--from"]
        self.repo.releases.extend([
            {"tag_name": "v2.0.0", "draft": False, "prerelease": False},
            {"tag_name": "v1.0.11", "draft": True, "prerelease": False},
            {"tag_name": "v1.0.12", "draft": False, "prerelease": True},
            {"tag_name": "v1.0.13-rc.1", "draft": False, "prerelease": False},
        ])
        self.repo.writes.clear()
        flow.main(["plan", *line, "v1.0.0"])
        for tag in ["v1.0.10", "v1.0.2"]:
            self.repo.releases.append({"tag_name": tag, "draft": False, "prerelease": False})
        for mode in ["plan", "apply"]:
            with self.assertRaisesRegex(ValueError, "v1.0.10"):
                flow.main([mode, *line, "v1.0.0"] + (["--at", flow.BASE_SHA] if mode == "apply" else []))
        self.assertFalse(self.repo.writes)
        flow.main(["plan", *line, "v1.0.10"])

    def test_guard_drift_and_conflicting_inherited_rules(self):
        self.cutover()
        self.repo.extra.append(flow.rule("pull_request", required_approving_review_count=1))
        with self.assertRaisesRegex(ValueError, "separate approver"):
            flow.main(["verify"])
        self.repo.extra[:] = [flow.checks(["Unexpected"])]
        with self.assertRaisesRegex(ValueError, "unexpected effective"):
            flow.main(["verify"])
        self.repo.extra.clear()
        guard = next(p for p in self.repo.policies.values() if p["name"] == flow.GUARD)
        guard["bypass_actors"] = [{"actor_type": "RepositoryRole"}]
        with self.assertRaisesRegex(ValueError, "drift"):
            self.cutover()

    def test_nonadmin_apply_refused_without_writes(self):
        self.repo.admin = False
        with self.assertRaisesRegex(ValueError, "administration"):
            self.cutover()
        self.assertFalse(self.repo.writes)


class RequestedCITests(unittest.TestCase):
    ARGS = ["--requested-ci"]

    def setUp(self):
        self.repo = Repository()
        for cm in (redirect_stdout(StringIO()), patch.object(flow, "api", self.repo.api),
                   patch.object(flow, "cutover_ready", return_value=self.repo.branches["main"])):
            cm.__enter__()
            self.addCleanup(cm.__exit__, None, None, None)
        flow.main(["apply", "--policy-pr", "42"])
        self.repo.default_branch = "develop"
        flow.main(["apply", "--line", "release/1.1", "--from", "develop", "--at", self.repo.branches["develop"]])
        self.repo.writes.clear()
        self.before = deepcopy(self.repo.policies)

    def pull(self, number, base, sha, *contexts):
        self.repo.pulls.append({"number": number, "base": {"ref": base}, "head": {"sha": sha}})
        self.repo.statuses[sha] = list(contexts)

    def installed(self):
        return [policy for policy in self.repo.policies.values() if policy["name"] == flow.REQUESTED_RULE]

    def test_the_rule_requires_the_gate_status_from_actions_on_the_maintained_branches(self):
        policy = flow.requested()
        self.assertEqual(policy["name"], "Git Flow / requested CI")
        self.assertEqual(policy["conditions"]["ref_name"], flow.baseline()[0]["conditions"]["ref_name"])
        self.assertEqual((policy["enforcement"], policy["bypass_actors"]), ("active", []))
        self.assertEqual(policy["rules"], [{"type": "required_status_checks", "parameters": {
            "required_status_checks": [{"context": "Required requested CI", "integration_id": 15368}],
            "strict_required_status_checks_policy": False, "do_not_enforce_on_create": True}}])
        # The context is the one the gate publishes, and the branches are
        # those whose pull requests start Code checks, which triggers it.
        root = Path(flow.__file__).resolve().parents[2]
        self.assertIn('\nCONTEXT = "' + flow.REQUESTED + '"\n', (root / ".github/scripts/required_ci.py").read_text())
        code = (root / ".github/workflows/code.yml").read_text()
        self.assertIn("  pull_request:\n    branches: [develop, main, 'release/**', 'support/**']\n", code)
        self.assertEqual(sorted(ref.removeprefix("refs/heads/").removesuffix("/*") for ref in flow.MAINTAINED),
                         sorted(["develop", "main", "release/**", "support/**"]))
        self.assertIn("workflows: [Code checks]", (root / ".github/workflows/requested-ci-gate.yml").read_text())

    def test_required_short_rule_checks_only_develop(self):
        policy = flow.required_short()
        self.assertEqual(policy["conditions"]["ref_name"]["include"], ["refs/heads/develop"])
        self.assertEqual(policy["bypass_actors"], [])
        self.repo.api("rulesets", method="POST", body=policy)
        flow.main(["verify"])
        self.assertEqual(flow.optional({flow.REQUIRED_SHORT_RULE: policy}, "main"), [])
        self.assertEqual(flow.optional({flow.REQUIRED_SHORT_RULE: policy}, "develop"), [policy])

    def test_without_the_option_nothing_installs_or_expects_the_rule(self):
        flow.main(["verify"])
        flow.main(["plan", "--line", "release/1.2", "--from", "develop"])
        flow.main(["apply", "--line", "release/1.2", "--from", "develop", "--at", self.repo.branches["develop"]])
        flow.main(["verify"])
        self.assertFalse(self.installed())
        self.assertNotIn(flow.REQUESTED_RULE, [write[2].get("name") for write in self.repo.writes])

    def test_plan_writes_nothing_and_apply_adds_one_ruleset_beside_the_others(self):
        self.pull(7, "develop", "1" * 40, flow.REQUESTED)
        out = StringIO()
        with redirect_stdout(out):
            flow.main(["plan", *self.ARGS])
        self.assertIn("Create ruleset", out.getvalue())
        self.assertIn(flow.REQUESTED_RULE, out.getvalue())
        self.assertFalse(self.repo.writes)
        flow.main(["apply", *self.ARGS])
        self.assertEqual(self.repo.writes, [("rulesets", "POST", flow.requested())])
        # Every ruleset that existed is what it was.
        self.assertEqual({key: self.repo.policies[key] for key in self.before}, self.before)
        self.assertEqual(len(self.repo.policies), len(self.before) + 1)
        flow.main(["verify"])
        # Applying or planning again changes nothing.
        flow.main(["apply", *self.ARGS])
        flow.main(["plan", *self.ARGS])
        self.assertEqual(len(self.repo.writes), 1)

    def test_verify_checks_the_rule_only_once_it_is_installed(self):
        # Not installed: the context is as unexpected as any other.
        self.repo.extra[:] = [flow.checks([flow.REQUESTED])]
        with self.assertRaisesRegex(ValueError, "unexpected effective"):
            flow.main(["verify"])
        self.repo.extra.clear()
        flow.main(["apply", *self.ARGS])
        flow.main(["verify"])
        rule = self.installed()[0]
        # Installed: every maintained branch must have it in effect.
        for include in (["refs/heads/develop"], ["refs/heads/main", "refs/heads/release/**/*", "refs/heads/support/**/*"]):
            with self.subTest(include=include):
                kept = rule["conditions"]
                rule["conditions"] = {"ref_name": {"include": include, "exclude": []}}
                with self.assertRaisesRegex(ValueError, "drift"):
                    flow.main(["verify"])
                rule["conditions"] = kept
        for change, value in (("enforcement", "disabled"), ("bypass_actors", [{"actor_type": "RepositoryRole"}])):
            with self.subTest(change=change):
                kept, rule[change] = rule[change], value
                with self.assertRaisesRegex(ValueError, "drift"):
                    flow.main(["verify"])
                with self.assertRaisesRegex(ValueError, "drift"):
                    flow.main(["apply", *self.ARGS])
                rule[change] = kept
        kept = deepcopy(rule["rules"])
        rule["rules"][0]["parameters"]["required_status_checks"][0]["integration_id"] = None
        with self.assertRaisesRegex(ValueError, "drift"):
            flow.main(["verify"])
        rule["rules"][0]["parameters"]["required_status_checks"].append({"context": "Other", "integration_id": 15368})
        with self.assertRaisesRegex(ValueError, "drift"):
            flow.main(["verify"])
        rule["rules"] = kept
        flow.main(["verify"])
        self.assertEqual(len(self.repo.writes), 1)

    def test_an_installed_rule_does_not_stop_a_line_registration(self):
        flow.main(["apply", *self.ARGS])
        args = ["--line", "release/1.2", "--from", "develop"]
        flow.main(["plan", *args])
        flow.main(["apply", *args, "--at", self.repo.branches["develop"]])
        flow.main(["verify"])
        self.assertEqual(len(self.installed()), 1)
        active = self.repo.api("rules/branches/release%2F1.2")
        self.assertIn(self.installed()[0]["id"], [rule["ruleset_id"] for rule in active])

    def test_apply_refuses_while_an_open_pull_request_has_no_status(self):
        self.pull(7, "develop", "1" * 40, flow.REQUESTED, "Unit tests")
        self.pull(8, "release/1.1", "2" * 40, "Unit tests")
        self.pull(9, "main", "3" * 40)
        self.pull(10, "scratch/base", "4" * 40)
        self.pull(11, "release/next", "5" * 40)
        out = StringIO()
        with redirect_stdout(out):
            flow.main(["plan", *self.ARGS])
        lines = [line for line in out.getvalue().splitlines() if line.startswith("No ")]
        self.assertEqual(lines, ["No Required requested CI status: #8 (release/1.1, 222222222222)",
                                 "No Required requested CI status: #9 (main, 333333333333)"])
        with self.assertRaisesRegex(ValueError, "2 open pull request.*Requested CI gate"):
            flow.main(["apply", *self.ARGS])
        self.assertFalse(self.repo.writes)
        self.repo.statuses["2" * 40].append(flow.REQUESTED)
        self.repo.statuses["3" * 40].append(flow.REQUESTED)
        flow.main(["apply", *self.ARGS])
        self.assertEqual(len(self.repo.writes), 1)
        # Installed: a later pull request without a status is the gate's to
        # serve, not a reason to refuse an idempotent apply.
        self.pull(12, "develop", "6" * 40)
        flow.main(["apply", *self.ARGS])
        self.assertEqual(len(self.repo.writes), 1)

    def test_the_option_is_its_own_operation_on_sound_rules(self):
        for arguments in (["verify", *self.ARGS], ["evidence", *self.ARGS, "--branch", "main", "--sha", "a" * 40],
                          ["apply", *self.ARGS, "--policy-pr", "42"],
                          ["plan", *self.ARGS, "--line", "release/1.2", "--from", "develop"]):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                flow.main(arguments)
        self.repo.admin = False
        with self.assertRaisesRegex(ValueError, "administration"):
            flow.main(["apply", *self.ARGS])
        self.repo.admin = True
        guard = next(p for p in self.repo.policies.values() if p["name"] == flow.GUARD)
        guard["bypass_actors"] = [{"actor_type": "RepositoryRole"}]
        with self.assertRaisesRegex(ValueError, "drift"):
            flow.main(["apply", *self.ARGS])
        guard["bypass_actors"] = []
        self.repo.extra[:] = [flow.checks(["Unexpected"])]
        with self.assertRaisesRegex(ValueError, "unexpected effective"):
            flow.main(["apply", *self.ARGS])
        self.assertFalse(self.repo.writes)

    def test_before_the_cutover_the_rule_cannot_be_planned(self):
        self.repo.policies.clear()
        del self.repo.branches["develop"], self.repo.branches["release/1.1"]
        with self.assertRaisesRegex(ValueError, "cutover first"):
            flow.main(["plan", *self.ARGS])


class BootstrapTests(unittest.TestCase):
    def test_requires_policy_merge_at_main_and_exact_delivered_tool(self):
        sha = "a" * 40
        path = "scripts/release/git_flow.py"
        pr = {"merged": True, "base": {"ref": "main"}, "merge_commit_sha": sha}
        content = base64.b64encode(Path(flow.__file__).read_bytes()).decode()
        replies = {"pulls/7": pr, "pulls/7/files?per_page=100": [{"filename": path}],
                   f"contents/{path}?ref={sha}": {"content": content},
                   f"compare/{flow.BASE_SHA}...{sha}": {"status": "ahead"},
                   "releases/latest": {"tag_name": flow.BASE_TAG}}

        def get(key, **kwargs):
            return replies[key]

        with patch.object(flow, "api", side_effect=get), patch.object(flow, "candidate_evidence") as evidence:
            with self.assertRaisesRegex(ValueError, "current main tip"):
                flow.cutover_ready(7, {"main": "d" * 40})
            with self.assertRaisesRegex(ValueError, "after merge"):
                flow.cutover_ready(None, {"main": sha})
            self.assertEqual(flow.cutover_ready(7, {"main": sha}), sha)
            evidence.assert_called_once_with("main", sha)
            replies[f"contents/{path}?ref={sha}"]["content"] = "YQ=="
            with self.assertRaisesRegex(ValueError, "merged policy SHA"):
                flow.cutover_ready(7, {"main": sha})


class EvidenceTests(unittest.TestCase):
    def collect(self, *, change_run=None, change_job=None):
        branch, sha = "release/1.1", "d" * 40

        def request(path, *, pages):
            if pages == "workflow_runs":
                filename = path.split("/")[2]
                self.assertIn("event=push&branch=release%2F1.1&head_sha=" + sha, path)
                run = dict(event="push", head_branch=branch, head_sha=sha, path=".github/workflows/" + filename,
                           run_number=9, run_attempt=2, status="completed", conclusion="success",
                           id=1 if filename == "code.yml" else 2, html_url="https://example.org/run")
                return change_run(run) if change_run else [run]
            self.assertIn("/attempts/2/jobs?", path)
            run_id = int(path.split("/")[2])
            names = flow.CODE if run_id == 1 else [flow.gate(branch)]
            jobs = [dict(name=n, head_sha=sha, run_id=run_id, run_attempt=2, status="completed",
                         conclusion="success") for n in names]
            return change_job(jobs) if change_job else jobs

        with patch.object(flow, "api", side_effect=request), redirect_stdout(StringIO()):
            flow.candidate_evidence(branch, sha)

    def test_matching_push_and_jobs(self):
        self.collect()

    def test_wrong_push_and_latest_failure(self):
        for fields in ({"event": "pull_request"}, {"event": "workflow_dispatch"}, {"head_branch": "main"},
                       {"head_sha": "e" * 40}, {"path": ".github/workflows/other.yml"}):
            with self.subTest(fields=fields), self.assertRaisesRegex(ValueError, "no push run"):
                self.collect(change_run=lambda r: [dict(r, **fields)])
        with self.assertRaisesRegex(ValueError, "has not passed"):
            self.collect(change_run=lambda r: [r, dict(r, run_number=10, conclusion="cancelled")])

    def test_wrong_jobs(self):
        for change in (lambda j: j[1:], lambda j: [*j, j[0]],
                       lambda j: [dict(i, run_attempt=1) for i in j],
                       lambda j: [dict(i, head_sha="e" * 40) for i in j],
                       lambda j: [dict(i, run_id=100) for i in j],
                       lambda j: [dict(i, conclusion="skipped") for i in j]):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "missing or unsuccessful"):
                self.collect(change_job=change)


if __name__ == "__main__":
    unittest.main()
