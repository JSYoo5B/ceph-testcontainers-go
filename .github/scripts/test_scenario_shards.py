"""Check source-owned CI scheduling and workflow phases without Docker.

The Go AST catalog reads source only. Compiled profile verification belongs to
`tag_scenarios.py plan --verify`; this suite does not compile every batch again.
"""

import contextlib
import io
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest
from unittest import mock

import periodic_gate
import report_test_failures as failure_reporter
import run_image_matrix as runner
import tag_scenarios as engine


ROOT = Path(__file__).resolve().parents[2]


def blocks(text, pattern):
    matches = list(re.finditer(pattern, text, re.MULTILINE))
    return [(match[1], text[match.start():matches[index + 1].start()
             if index + 1 < len(matches) else len(text)]) for index, match in enumerate(matches)]


def steps(job):
    return [text for _, text in blocks(job, r"^      - (.+)$")]


def scalar(text, key):
    values = re.findall(r"^\s+(?:-\s+)?" + re.escape(key) + r":\s*(.*?)\s*$", text, re.MULTILINE)
    if len(values) != 1:
        raise AssertionError("expected exactly one " + key + " field")
    return values[0]


def run_command(step):
    match = re.search(r"^( +)(?:-\s+)?run:\s*(.*?)\s*$", step, re.MULTILINE)
    if not match:
        return ""
    if match[2] not in ("|", "|-", "|+", ">", ">-", ">+"):
        return match[2]
    indent = len(match[1])
    lines = []
    for line in step[match.end():].splitlines():
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        if line.strip():
            lines.append(line.strip())
    return "\n".join(lines)


def engine_step(job_steps, operation):
    matches = [step for step in job_steps if re.search(
        r"\btag_scenarios\.py " + re.escape(operation) + r"(?:\s|$)", run_command(step))]
    if len(matches) != 1:
        raise AssertionError("expected exactly one tag runner operation: " + operation)
    return matches[0]


def engine_command(job_steps, operation, option=None):
    commands = [shlex.split(line) for line in run_command(engine_step(job_steps, operation)).splitlines()
                if re.search(r"\btag_scenarios\.py " + re.escape(operation) + r"(?:\s|$)", line)]
    if option:
        commands = [command for command in commands if option in command]
    if len(commands) != 1:
        raise AssertionError("expected one engine command for selected operation/option")
    return commands[0]


class SourceDrivenWorkflowTests(unittest.TestCase):
    # Workflow categories are public check types, never a list of Test names.
    ENTRY_POINTS = {
        "docker.yml": ("Docker checks", "environment"),
        "ceph-short.yml": ("Ceph short", "short"),
        "ceph-topology.yml": ("Ceph topology", "topology"),
        "ceph-multicluster.yml": ("Ceph multicluster", "multicluster"),
        "ceph-recovery.yml": ("Ceph recovery", "recovery"),
    }
    # Runtime batches never read these paths, so a change to them alone runs
    # only Code checks, on pull requests and on pushes to main alike.
    RUNTIME_PATHS_IGNORE = ("'docs/**'", "'*.md'", "'LICENSE'", "'internal/integration/goceph/**'")
    # These categories gate every merge on the current release.
    GATING = {"docker.yml", "ceph-short.yml"}
    # The other categories run on a pull request only for the batches whose
    # own test files changed, unless labelled ci:full or in a release phase,
    # and on pushes to main only in a release phase.
    SCOPED = {"ceph-topology.yml", "ceph-multicluster.yml", "ceph-recovery.yml"}
    # A pull request of this gating category may opt in to declaration
    # selection with a label; pushes to main always run every batch.
    OPT_IN = {"ceph-short.yml": "ci:selected-batches"}
    # The releases and check names that the per-release caller jobs had
    # before SUPPORTED_RELEASES generated them. They must stay covered.
    HISTORICAL_RELEASES = {"20.2.4": "tentacle", "19.2.5": "squid"}
    PERIODIC, RELEASE_MATRIX = "ceph-periodic.yml", "ceph-release-matrix.yml"
    # The pull request comment command: issue_comment only.
    COMMAND = "ci-command.yml"
    # The scratch-device research probe (#7): its own paths only, no scenarios.
    BLOCK_FAULT_PROBE = "block-fault-probe.yml"
    PERIODIC_SCHEDULE = "41 */3 * * *"
    CAMPAIGN_SCHEDULE = "23 4 * * *"

    @staticmethod
    def github_glob(pattern):
        """GitHub's path filter glob: * stays within one path segment, ** may
        cross them."""
        regex, index = "", 0
        while index < len(pattern):
            if pattern.startswith("**/", index):
                regex, index = regex + "(?:.*/)?", index + 3
            elif pattern.startswith("**", index):
                regex, index = regex + ".*", index + 2
            elif pattern[index] == "*":
                regex, index = regex + "[^/]*", index + 1
            else:
                regex, index = regex + re.escape(pattern[index]), index + 1
        return re.compile(regex + r"\Z")

    def test_runtime_paths_ignore_only_documentation(self):
        # A Markdown file in a code directory can be a test's embedded or
        # run-time input, so changing it alone must still run the batches.
        patterns = [self.github_glob(path.strip("'")) for path in self.RUNTIME_PATHS_IGNORE]
        ignored = lambda path: any(pattern.match(path) for pattern in patterns)
        for path in ("README.md", "CHANGELOG.md", "docs/ceph.md", "docs/guides/messenger-client-fixtures.md",
                     "LICENSE", "internal/integration/goceph/README.md"):
            with self.subTest(path=path):
                self.assertTrue(ignored(path))
        for path in ("internal/integration/testdata/expected.md", "internal/integration/notes.md",
                     "ceph/notes.md", "rgw/testdata/policy.md", "internal/tools/batchselect/testdata/sel/README.md",
                     ".github/scripts/fixtures/receipt.md"):
            with self.subTest(path=path):
                self.assertFalse(ignored(path))


    @classmethod
    def setUpClass(cls):
        cls.workflows = {path.name: path.read_text()
                         for path in (ROOT / ".github/workflows").glob("*.yml")}
        cls.jobs = {}
        for filename, workflow in cls.workflows.items():
            cls.jobs[filename] = dict(blocks(workflow.split("\njobs:\n", 1)[1],
                                             r"^  ([\w-]+):$"))
        cls.catalog = engine.load_catalog()
        cls.plan = engine.discover(cls.catalog)
        cls.optional = engine.discover_optional(cls.catalog)

    def matrix_jobs(self, prefix):
        """The expected caller jobs of a matrix workflow: every runtime
        category on each release, with Docker checks once in Ceph periodic."""
        jobs = {}
        for release, codename in self.HISTORICAL_RELEASES.items():
            for category in engine.RUNTIME_CATEGORIES:
                if category != "environment":
                    jobs[codename + "-" + category] = (category, release, prefix + " " + codename + " " + category)
        return jobs

    def assert_release_caller(self, jobs, scope, prefix):
        """A release-matrix workflow expands SUPPORTED_RELEASES in its
        releases job and calls tagged-runtime once per entry of that matrix."""
        releases, runtime = jobs["releases"], jobs["runtime"]
        command = shlex.split(run_command(engine_step(steps(releases), "releases")))
        self.assertEqual(command[command.index("--scope") + 1], scope)
        self.assertEqual(command[command.index("--check-prefix") + 1], prefix)
        self.assertEqual(command[command.index("--github-output") + 1], "$GITHUB_OUTPUT")
        self.assertIn("matrix: ${{ steps.releases.outputs.matrix }}", releases)
        self.assertIn("uses: ./.github/workflows/tagged-runtime.yml", runtime)
        self.assertEqual(scalar(runtime, "needs"), "releases")
        self.assertEqual(scalar(runtime, "matrix"), "${{ fromJSON(needs.releases.outputs.matrix) }}")
        self.assertEqual(scalar(runtime, "fail-fast"), "false")
        self.assertEqual(scalar(runtime.split("    strategy:", 1)[0], "name"), "${{ matrix.title }}")
        for key in ("category", "check_name", "release"):
            self.assertEqual(scalar(runtime, key), "${{ matrix." + key + " }}")
        self.assertEqual(scalar(runtime, "networks"), "all")
        self.assertNotIn("changed_since", runtime)
        # The generated callers keep each historical job's check name.
        entries = engine.release_matrix(scope, prefix)
        expected = {value[2]: (value[0], value[1]) for value in self.matrix_jobs(prefix).values()
                    if scope == "all" or value[1] != runner.RELEASE}
        names = {entry["check_name"]: (entry["category"], entry["release"]) for entry in entries}
        self.assertLessEqual(expected.items(), names.items())
        self.assertEqual(len(names), len(entries))

    def test_release_callers_keep_every_required_cell_of_the_historical_jobs(self):
        """Required coverage before and after the generated release matrix:
        the cells of the per-release caller jobs that the workflows listed
        before must all remain, and the callers must run each required
        profile on every supported release exactly once."""
        bound = [category for category in engine.RUNTIME_CATEGORIES if category != "environment"]
        before = engine.runtime_cells(self.plan, [(release, category) for release in self.HISTORICAL_RELEASES
                                                  for category in bound] + [(None, "environment")])
        callers = [(entry["release"], entry["category"]) for entry in engine.release_matrix("all", "Ceph periodic")]
        after = engine.runtime_cells(self.plan, callers + [(None, "environment")])
        self.assertEqual(engine.coverage_difference(before, after)["missing"], [])
        self.assertEqual(after, engine.required_cells(self.plan, runner.RELEASES))
        self.assertEqual(max(after.values()), 1)
        for scope in engine.RELEASE_SCOPES:
            with self.subTest(scope=scope):
                report = engine.releases_report(self.plan, scope, "Ceph periodic")
                self.assertTrue(report["passed"], report)
                self.assertEqual((report["missing"], report["extra"]), ([], []))
        others = {entry["release"] for entry in engine.release_matrix("others", "Ceph release matrix")}
        self.assertEqual(others, set(runner.RELEASES) - {runner.RELEASE})
        # The pull request entry points run the current release only, with
        # every category once.
        current = engine.runtime_cells(self.plan, [(runner.RELEASE, category)
                                                   for _, category in self.ENTRY_POINTS.values()])
        self.assertEqual(current, engine.required_cells(self.plan, [runner.RELEASE]))

    def test_coverage_difference_names_missing_duplicate_and_unknown_release_cells(self):
        required = engine.required_cells(self.plan, runner.RELEASES)
        callers = [(entry["release"], entry["category"]) for entry in engine.release_matrix("all", "Ceph periodic")]
        environment = [(None, "environment")]
        short = [profile["id"] for profile in engine.select_required_plan(self.plan, "short")["include"]]
        dropped = engine.coverage_difference(required, engine.runtime_cells(
            self.plan, [caller for caller in callers if caller != ("19.2.5", "short")] + environment))
        self.assertEqual(sorted(cell[1] for cell in dropped["missing"]), sorted(short))
        self.assertEqual({cell[0] for cell in dropped["missing"]}, {"19.2.5"})
        self.assertEqual(dropped["extra"], [])
        duplicated = engine.coverage_difference(required, engine.runtime_cells(
            self.plan, callers + [("20.2.4", "recovery")] + environment))
        self.assertEqual(duplicated["missing"], [])
        self.assertEqual(len(duplicated["extra"]), len(engine.select_required_plan(self.plan, "recovery")["include"]))
        moved = engine.coverage_difference(required, engine.runtime_cells(
            self.plan, [("20.2.4" if caller == ("19.2.5", "topology") else caller[0], caller[1]) for caller in callers]
            + environment))
        self.assertTrue(moved["missing"] and moved["extra"])
        # An unlisted patch or release is refused, never mapped to a row.
        for release in ("20.2.5", "21.2.0", ""):
            with self.subTest(release=release), self.assertRaises(ValueError):
                engine.runtime_cells(self.plan, [(release, "short")])
        with self.assertRaises(ValueError):
            engine.release_matrix("all", "Ceph periodic", current="20.2.5")
        with self.assertRaises(ValueError):
            engine.release_matrix("some", "Ceph periodic")
        self.assertEqual(set(runner.CODENAMES), set(runner.RELEASES))
        self.assertEqual(len(set(runner.CODENAMES.values())), len(runner.CODENAMES))

    def test_runtime_profiles_consume_dependency_roles_without_legacy_core_fallback(self):
        profiles = self.plan["include"]
        for profile in profiles:
            if not profile["requires_ceph"]:
                self.assertEqual(profile["image_roles"], "")
                continue
            roles = profile["image_roles"].split(",")
            self.assertEqual(roles[0], "essential", profile["id"])
            self.assertTrue(set(roles) <= {"essential", "client", "cephfs", "rbd-aux", "rgw"}, profile["id"])
        scoped = {profile["batch"]: profile["image_roles"] for profile in profiles}
        self.assertEqual(scoped["daemon_addresses"], "essential,client")
        self.assertEqual(scoped["supervised_command"], "essential")
        self.assertEqual(scoped["native_input"], "essential,client")
        self.assertEqual(scoped["rbd_namespaces"], "essential,client,rbd-aux")
        self.assertEqual(scoped["rados_fixtures"], "essential,client,cephfs")

    def test_pr_checks_are_separate_workflows_with_independent_entry_points(self):
        expected = {"code.yml", "tagged-runtime.yml", "native-regressions.yml", self.PERIODIC,
                    self.RELEASE_MATRIX, self.COMMAND, self.BLOCK_FAULT_PROBE, *self.ENTRY_POINTS}
        self.assertEqual(set(self.workflows), expected)
        probe = self.workflows[self.BLOCK_FAULT_PROBE]
        self.assertIn("\non:\n  pull_request:\n    paths:\n", probe)
        self.assertNotIn("tagged-runtime.yml", probe)
        self.assertEqual(set(self.ENTRY_POINTS), self.GATING | self.SCOPED)
        self.assertNotIn("test.yml", self.workflows)
        ignored = "".join("      - " + path + "\n" for path in self.RUNTIME_PATHS_IGNORE)
        for filename, (name, category) in self.ENTRY_POINTS.items():
            with self.subTest(workflow=filename):
                workflow = self.workflows[filename]
                self.assertRegex(workflow, r"(?m)^name: " + re.escape(name) + r"$")
                types = ("    types: [opened, synchronize, reopened, labeled]\n" if filename in self.SCOPED
                         else "    types: [opened, synchronize, reopened, labeled, unlabeled]\n" if filename in self.OPT_IN
                         else "")
                self.assertIn("\non:\n  push:\n    branches: [main]\n    paths-ignore:\n" + ignored +
                              "  pull_request:\n" + types + "    paths-ignore:\n" + ignored + "  workflow_dispatch:\n", workflow)
                group = "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || (github.event_name == 'push' && github.ref) || github.run_id }}"
                if filename in self.SCOPED:
                    group += "${{ github.event.action == 'labeled' && github.event.label.name != 'ci:full' && format('-{0}', github.run_id) || '' }}"
                elif filename in self.OPT_IN:
                    group += ("${{ (github.event.action == 'labeled' || github.event.action == 'unlabeled') && "
                              "github.event.label.name != '" + self.OPT_IN[filename] + "' && format('-{0}', github.run_id) || '' }}")
                self.assertEqual(scalar(workflow, "group"), group)
                self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
                self.assertNotRegex(workflow, r"(?m)^    needs:")
                callers = [job for job in self.jobs[filename].values()
                           if "uses: ./.github/workflows/tagged-runtime.yml" in job]
                self.assertEqual(len(callers), 1)
                self.assertEqual(scalar(callers[0].split("    with:", 1)[0], "name"), "Scenarios")
                self.assertEqual(scalar(callers[0], "category"), category)
                self.assertEqual(scalar(callers[0], "check_name"), name)
                # Pull requests and pushes to main run the current release.
                self.assertNotRegex(callers[0], r"(?m)^      release:")
                if filename in self.SCOPED:
                    self.assertIn("    if: >-\n      (github.event_name != 'push' || vars.CI_RELEASE_PHASE == 'true') &&\n"
                                  "      (github.event.action != 'labeled' || github.event.label.name == 'ci:full')\n", callers[0])
                    self.assertIn("      changed_since: >-\n        ${{ github.event_name == 'pull_request' &&\n"
                                  "        !contains(github.event.pull_request.labels.*.name, 'ci:full') &&\n"
                                  "        vars.CI_RELEASE_PHASE != 'true' &&\n"
                                  "        github.event.pull_request.base.sha || '' }}\n", callers[0])
                elif filename in self.OPT_IN:
                    # Evaluated per event in OptInDeclarationSelectionTests.
                    self.assertNotIn("changed_since", callers[0])
                    self.assertIn("select_declarations", callers[0])
                else:
                    self.assertNotRegex(callers[0], r"(?m)^    if:")
                    self.assertNotIn("changed_since", callers[0])
                    self.assertNotIn("select_declarations", callers[0])
                self.assertNotIn("steps:", callers[0])
                self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
        with self.subTest(workflow=self.PERIODIC):
            workflow = self.workflows[self.PERIODIC]
            self.assertRegex(workflow, r"(?m)^name: Ceph periodic$")
            self.assertIn("\non:\n  schedule:\n    - cron: '" + self.PERIODIC_SCHEDULE + "'\n    - cron: '"
                          + self.CAMPAIGN_SCHEDULE + "'\n  workflow_dispatch:\n", workflow)
            self.assertNotRegex(workflow, r"(?m)^  (?:push|pull_request):")
            self.assertEqual(scalar(workflow, "group"),
                             "${{ github.workflow }}-${{ github.ref }}${{ (github.event.schedule == '"
                             + self.CAMPAIGN_SCHEDULE + "' || inputs.mode == 'representative') && '-campaign' || '' }}")
            self.assertEqual(scalar(workflow, "cancel-in-progress"), "false")
            jobs = dict(self.jobs[self.PERIODIC])
            gate = jobs.pop("gate")
            self.assertIn("python3 .github/scripts/periodic_gate.py --workflow " + self.PERIODIC
                          + " --campaign-schedule '" + self.CAMPAIGN_SCHEDULE + "'", gate)
            self.assertEqual(scalar(gate, "CI_RELEASE_PHASE"), "${{ vars.CI_RELEASE_PHASE }}")
            self.assertEqual(scalar(gate, "PERIODIC_SCHEDULE"), "${{ github.event.schedule }}")
            self.assertEqual(scalar(gate, "PERIODIC_MODE"), "${{ inputs.mode }}")
            # The campaign: one pinned commit and pinned digests for every
            # attempt of the source-owned representative batches.
            pin, representative, history = jobs.pop("pin"), jobs.pop("representative"), jobs.pop("history")
            self.assertEqual(scalar(pin, "if"), "needs.gate.outputs.campaign == 'true'")
            self.assertIn("python3 .github/scripts/periodic_campaign.py pin --release", pin)
            self.assertEqual(scalar(representative, "needs"), "pin")
            self.assertEqual(scalar(representative, "category"), "representative")
            self.assertEqual(scalar(representative, "ref"), "${{ needs.pin.outputs.sha }}")
            self.assertEqual(scalar(representative, "role_digests"), "${{ needs.pin.outputs.digests }}")
            self.assertEqual(scalar(representative, "release"), "${{ needs.pin.outputs.release }}")
            self.assertEqual(scalar(representative, "repetitions"), "${{ fromJSON(inputs.repetitions || 3) }}")
            self.assertEqual(scalar(representative.split("    with:", 1)[0], "name"), "Representative")
            # The gate tells campaign jobs from matrix batches by this name.
            self.assertEqual(scalar(representative.split("    with:", 1)[0], "name"), periodic_gate.CAMPAIGN_CALLER)
            # Only the History job writes, and only the history branch.
            self.assertEqual(scalar(history, "contents"), "write")
            self.assertEqual(scalar(workflow.split("\njobs:\n", 1)[0], "contents"), "read")
            self.assertEqual(scalar(history, "if"),
                             "always() && (needs.gate.outputs.run == 'true' || needs.gate.outputs.campaign == 'true')")
            self.assertEqual(scalar(history, "pattern"), "tag-scenario-*")
            self.assertIn("scenario_history.py collect --history-dir . --rows ../artifacts/rows", history)
            self.assertIn("push -q origin HEAD:refs/heads/scenario-history", history)
            self.assertNotIn("git push origin main", history)
            needs = re.search(r"needs: \[(.*?)\]", history, re.S)[1]
            self.assertEqual(sorted(name.strip() for name in needs.split(",")),
                             sorted(name for name in self.jobs[self.PERIODIC] if name != "history"))
            # The gate counts only jobs of the runtime callers as matrix
            # batches: no job that this workflow defines itself (Gate, the
            # Releases planner, Pin, History) and no called Plan job, but
            # every batch of every generated release caller.
            for job_id, body in self.jobs[self.PERIODIC].items():
                if "uses: ./.github/workflows/tagged-runtime.yml" not in body:
                    name = scalar(body.split("    steps:", 1)[0], "name")
                    self.assertNotIn(periodic_gate.CALLED_JOB, name, job_id)
                    self.assertFalse(periodic_gate.runtime_job({"name": name}), job_id)
            for caller in ["Environment", *(entry["title"] for entry in engine.release_matrix("all", "Ceph periodic"))]:
                self.assertTrue(periodic_gate.runtime_job({"name": caller + " / rbd_fixtures_locks"}), caller)
                self.assertFalse(periodic_gate.runtime_job({"name": caller + " / Plan"}), caller)
            self.assertFalse(periodic_gate.runtime_job({"name": "Representative / pools #1"}))
            self.assertEqual(set(jobs), {"environment", "releases", "runtime"})
            environment = jobs["environment"]
            self.assertIn("uses: ./.github/workflows/tagged-runtime.yml", environment)
            self.assertEqual(scalar(environment, "needs"), "gate")
            self.assertEqual(scalar(environment, "if"), "needs.gate.outputs.run == 'true'")
            self.assertEqual(scalar(environment, "category"), "environment")
            self.assertEqual(scalar(environment, "check_name"), "Ceph periodic environment")
            self.assertNotRegex(environment, r"(?m)^      release:")
            self.assertEqual(scalar(jobs["releases"], "needs"), "gate")
            self.assertEqual(scalar(jobs["releases"], "if"), "needs.gate.outputs.run == 'true'")
            self.assert_release_caller(jobs, "all", "Ceph periodic")
            self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
        with self.subTest(workflow=self.RELEASE_MATRIX):
            workflow = self.workflows[self.RELEASE_MATRIX]
            self.assertRegex(workflow, r"(?m)^name: Ceph release matrix$")
            self.assertIn("\non:\n  push:\n    branches: [main]\n    paths-ignore:\n" + ignored +
                          "  pull_request:\n    paths-ignore:\n" + ignored + "  workflow_dispatch:\n", workflow)
            self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
            jobs = self.jobs[self.RELEASE_MATRIX]
            self.assertEqual(set(jobs), {"releases", "runtime"})
            self.assertEqual(scalar(jobs["releases"], "if"),
                             "vars.CI_RELEASE_PHASE == 'true' || github.event_name == 'workflow_dispatch'")
            self.assertEqual(scalar(jobs["runtime"], "if"), "needs.releases.outputs.count != '0'")
            self.assert_release_caller(jobs, "others", "Ceph release matrix")
        reusable = self.workflows["tagged-runtime.yml"]
        self.assertRegex(reusable, r"(?m)^  workflow_call:")
        self.assertNotRegex(reusable, r"(?m)^  (?:push|pull_request|workflow_dispatch):")
        self.assertNotIn("concurrency:", reusable)
        optional = self.workflows["native-regressions.yml"]
        self.assertRegex(optional, r"(?m)^  workflow_dispatch:")
        self.assertNotRegex(optional, r"(?m)^  (?:push|pull_request|workflow_call):")
        self.assertNotIn("concurrency:", optional)

    def test_comment_command_runs_named_profiles_with_minimal_permissions_per_job(self):
        workflow, jobs = self.workflows[self.COMMAND], self.jobs[self.COMMAND]
        self.assertRegex(workflow, r"(?m)^name: CI command$")
        self.assertIn("\non:\n  issue_comment:\n    types: [created]\n\npermissions: {}\n", workflow)
        self.assertNotRegex(workflow, r"(?m)^  (?:push|pull_request|pull_request_target|workflow_dispatch):")
        # No workflow-level group: a comment that is not a command must never
        # cancel or queue behind a command run.
        self.assertNotRegex(workflow, r"(?m)^concurrency:")
        self.assertEqual(set(jobs), {"command", "validate", "acknowledge", "runtime", "report"})
        permissions = {name: dict(re.findall(r"(?m)^      ([a-z-]+): (read|write)$", re.search(
                           r"(?m)^    permissions:\n((?:      .*\n)+)", job)[1])) for name, job in jobs.items()}
        self.assertEqual(permissions, {
            "command": {"contents": "read", "pull-requests": "read"},
            "validate": {"contents": "read"},
            "acknowledge": {"statuses": "write"},
            "runtime": {"contents": "read"},
            "report": {"contents": "read", "pull-requests": "write", "statuses": "write"}})
        self.assertEqual(scalar(jobs["command"], "if"),
                         "github.event.issue.pull_request && contains(github.event.comment.body, '/ci')")
        # Only Validate checks out the head commit, beside the default
        # branch's scripts; no checkout keeps credentials.
        self.assertEqual(workflow.count("uses: actions/checkout@v7"), workflow.count("persist-credentials: false"))
        for name in ("command", "report"):
            self.assertNotIn("ref:", jobs[name])
            self.assertNotIn("head/", jobs[name])
        self.assertEqual(scalar(jobs["validate"], "ref"), "${{ needs.command.outputs.sha }}")
        self.assertIn("python3 trusted/.github/scripts/ci_command.py select", jobs["validate"])
        self.assertIn("working-directory: head\n        run: python3 .github/scripts/tag_scenarios.py vocabulary",
                      jobs["validate"])
        runtime = jobs["runtime"]
        self.assertEqual(scalar(runtime, "uses"), "./.github/workflows/tagged-runtime.yml")
        self.assertEqual(scalar(runtime, "if"), "needs.validate.outputs.accepted == 'true'")
        self.assertEqual(scalar(runtime, "category"), "selected")
        self.assertEqual(scalar(runtime, "profiles"), "${{ needs.validate.outputs.profiles }}")
        self.assertEqual(scalar(runtime, "ref"), "${{ needs.command.outputs.sha }}")
        self.assertIn("        release: ${{ fromJSON(needs.validate.outputs.releases) }}\n", runtime)
        self.assertIn("    with:\n", runtime)
        self.assertIn("      release: ${{ matrix.release }}\n", runtime.split("    with:\n", 1)[1])
        self.assertEqual(scalar(runtime, "group"), "ci-command-${{ needs.validate.outputs.key }}-${{ matrix.release }}")
        self.assertEqual(scalar(runtime, "cancel-in-progress"), "false")
        # Acknowledge marks pending and Report finalizes the same per-comment
        # context computed once by Validate; the concurrency key above stays
        # the selection's, so repeated commands queue with separate statuses.
        self.assertIn("      context: ${{ steps.select.outputs.context }}\n", jobs["validate"])
        self.assertNotIn("comment", scalar(runtime, "group"))
        for name, state in (("acknowledge", "pending"), ("report", "$STATUS_STATE")):
            with self.subTest(job=name):
                self.assertEqual(jobs[name].count("STATUS_CONTEXT: "), 1)
                self.assertEqual(scalar(jobs[name], "STATUS_CONTEXT"), "${{ needs.validate.outputs.context }}")
                self.assertIn('-f context="$STATUS_CONTEXT"', jobs[name])
                self.assertIn("-f state=" + ('"' + state + '"' if state.startswith("$") else state), jobs[name])
        self.assertIn("if: steps.reply.outputs.state != ''\n", jobs["report"])
        self.assertEqual(scalar(jobs["report"].split("    steps:", 1)[0], "if"),
                         "always() && needs.command.outputs.command == 'true'")
        self.assertNotIn("rerun", workflow)
        # A comment reaches scripts only through the event file and env.
        self.assertNotIn("github.event.comment.body }}", workflow)
        plan = self.jobs["tagged-runtime.yml"]["plan"]
        command = engine_command(steps(plan), "plan", "--profiles")
        self.assertEqual(command[command.index("--profiles") + 1], "$SCENARIO_PROFILES")
        self.assertEqual(scalar(engine_step(steps(plan), "plan"), "SCENARIO_PROFILES"), "${{ inputs.profiles }}")
        self.assertIn('elif [ "$SCENARIO_CATEGORY" = selected ]; then', plan)

    def test_every_runtime_profile_is_a_comment_command_name(self):
        vocabulary = engine.vocabulary(self.plan, self.optional)
        runtime = [profile["id"] for profile in vocabulary["profiles"] if profile["category"] != "code"]
        self.assertEqual(len(runtime), len(set(runtime)))
        selected = engine.select_profiles_plan(self.plan, self.optional, ",".join(runtime))
        self.assertEqual(sorted(profile["id"] for profile in selected["include"]), sorted(runtime))

    def test_entry_categories_partition_source_profiles_without_ci_name_inventories(self):
        categories = [category for _, category in self.ENTRY_POINTS.values()]
        self.assertEqual(set(categories), set(engine.RUNTIME_CATEGORIES))
        self.assertEqual(len(categories), len(set(categories)))
        selected = []
        for category in categories:
            plan = engine.select_required_plan(self.plan, category)
            self.assertTrue(plan["include"])
            self.assertTrue(all(profile["category"] == category for profile in plan["include"]))
            selected.extend(profile["id"] for profile in plan["include"])
        expected = [profile["id"] for profile in self.plan["include"]
                    if profile["category"] != "code"]
        self.assertEqual(set(selected), set(expected))
        self.assertEqual(len(selected), len(set(selected)))
        self.assertTrue(any(profile["category"] == "code" for profile in self.plan["include"]))
        for workflow in self.workflows.values():
            self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
            self.assertNotRegex(workflow, r"(?:^|\s)-run(?:=|\s)")
            self.assertNotRegex(workflow, r"(?:^|\s)-short(?:=|\s)")

    def test_reusable_plan_filters_category_and_keeps_source_owned_job_budgets(self):
        plan, runtime = self.jobs["tagged-runtime.yml"]["plan"], self.jobs["tagged-runtime.yml"]["runtime"]
        command = engine_command(steps(plan), "plan", "--changed-since")
        self.assertNotIn("--verify", command)
        self.assertEqual(command[command.index("--category") + 1], "$SCENARIO_CATEGORY")
        self.assertEqual(scalar(engine_step(steps(plan), "plan"), "SCENARIO_CATEGORY"), "${{ inputs.category }}")
        self.assertIn("--github-output", command)
        self.assertEqual(command[command.index("--github-output") + 1], "$GITHUB_OUTPUT")
        self.assertIn("--output", command)
        self.assertIn("matrix: ${{ steps.plan.outputs.matrix }}", plan)
        # A changed-file selection diffs against the pull request base, so the
        # plan job needs full history then. "x && 0 || 1" would always be 1.
        self.assertEqual(scalar(plan, "fetch-depth"),
                         "${{ inputs.changed_since == '' && inputs.select_declarations == '' && 1 || 0 }}")
        command = engine_command(steps(plan), "plan", "--changed-since")
        self.assertIn("--changed-since", command)
        self.assertEqual(command[command.index("--changed-since") + 1], "$SCENARIO_CHANGED_SINCE")
        self.assertIn("\n    if: needs.plan.outputs.count != '0'\n", runtime)
        self.assertEqual(scalar(runtime, "needs"), "plan")
        self.assertEqual(scalar(runtime, "matrix"), "${{ fromJSON(needs.plan.outputs.matrix) }}")
        self.assertEqual(scalar(runtime, "timeout-minutes"), "${{ matrix.job_timeout }}")
        self.assertEqual(scalar(runtime, "fail-fast"), "false")
        self.assertEqual(scalar(plan.split("    steps:", 1)[0], "name"), "Plan")
        # The batch name, with the network mode for a batch's non-default mode.
        self.assertEqual(scalar(runtime.split("    steps:", 1)[0], "name"), "${{ matrix.name }}")
        self.assertIn("--networks", command)
        self.assertEqual(command[command.index("--networks") + 1], "$SCENARIO_NETWORKS")
        self.assertNotRegex(runtime, r"(?m)^        (?:case|group|include|exclude):")

    def test_campaign_attempts_are_separate_jobs_on_the_pinned_commit_and_images(self):
        reusable = self.workflows["tagged-runtime.yml"]
        plan, runtime = self.jobs["tagged-runtime.yml"]["plan"], self.jobs["tagged-runtime.yml"]["runtime"]
        command = engine_command(steps(plan), "plan", "--representative")
        self.assertEqual(command[command.index("--repetitions") + 1], "$SCENARIO_REPETITIONS")
        self.assertEqual(scalar(plan, "SCENARIO_REPETITIONS"), "${{ inputs.repetitions }}")
        self.assertIn('elif [ "$SCENARIO_CATEGORY" = representative ]; then', plan)
        # Every checkout takes the pinned commit; empty keeps the caller's.
        self.assertEqual(reusable.count("uses: actions/checkout@v7"), 2)
        self.assertEqual(reusable.count("          ref: ${{ inputs.ref }}\n"), 2)
        runtime_steps = steps(runtime)
        prepared = next(step for step in runtime_steps if "uses: ./.github/actions/scenario-images" in step)
        self.assertEqual(scalar(prepared, "digests"), "${{ inputs.role_digests }}")
        # Each attempt has its own artifacts but keeps the profile ID.
        names = re.findall(r"(?:artifact_name|          name): ((?:runtime-cleanup|scenario-images|tag-scenario)-.*)$",
                           runtime, re.M)
        self.assertEqual(len(names), 4)
        self.assertTrue(all(name.endswith("-${{ inputs.release }}${{ matrix.artifact_suffix }}") for name in names))
        normalize = next(step for step in runtime_steps if "scenario_history.py normalize" in run_command(step))
        self.assertIn('--repetition-id "$SCENARIO_REPETITION"', run_command(normalize))
        self.assertEqual(scalar(normalize, "SCENARIO_REPETITION"), "${{ matrix.repetition }}")
        self.assertEqual(scalar(normalize, "SCENARIO_REQUESTED_HEAD"),
                         "${{ inputs.ref || github.event.pull_request.head.sha }}")
        # A failing job reads the history summary and reports beside it.
        summary = next(step for step in runtime_steps if "ref=scenario-history" in run_command(step))
        reporter = next(step for step in runtime_steps if "report_test_failures.py" in run_command(step))
        self.assertEqual(runtime_steps.index(summary) + 1, runtime_steps.index(reporter))
        self.assertEqual((scalar(summary, "if"), scalar(summary, "continue-on-error")), ("failure()", "true"))
        command = shlex.split(run_command(reporter))
        self.assertEqual(command[command.index("--row") + 1], "artifacts/scenario/history-row.json")
        self.assertEqual(command[command.index("--history-summary") + 1], "artifacts/history-summary.json")
        self.assertIn("> artifacts/history-summary.json", run_command(summary))

    def test_runtime_compiles_before_environment_then_runs_and_cleans_on_same_runner(self):
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        runtime_steps = steps(runtime)
        compiled = engine_step(runtime_steps, "compile")
        native = engine_step(runtime_steps, "run")
        baseline = next(step for step in runtime_steps if "phase: snapshot" in step)
        prepared = next(step for step in runtime_steps if "uses: ./.github/actions/scenario-images" in step)
        cleanup = next(step for step in runtime_steps if "phase: check" in step)
        self.assertLess(runtime_steps.index(compiled), runtime_steps.index(baseline))
        self.assertLess(runtime_steps.index(baseline), runtime_steps.index(prepared))
        self.assertLess(runtime_steps.index(prepared), runtime_steps.index(native))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(cleanup))
        self.assertEqual(scalar(prepared, "if"), "matrix.requires_ceph")
        self.assertEqual(scalar(cleanup, "if"),
                         "${{ always() && steps.docker_baseline.outcome == 'success' }}")
        self.assertEqual(scalar(baseline, "artifact_name"), scalar(cleanup, "artifact_name"))
        self.assertIn("${{ matrix.id }}", scalar(cleanup, "artifact_name"))
        for step in (compiled, baseline, native):
            self.assertNotRegex(step, r"(?m)^\s+(?:if|continue-on-error):")
        self.assertNotRegex(runtime, r"(?m)^    continue-on-error:")
        self.assertNotIn("actions/download-artifact", runtime)

    def test_selected_source_profile_is_recomputed_and_no_name_selector_reaches_native(self):
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])
        for operation in ("compile", "run"):
            step = engine_step(runtime_steps, operation)
            command = engine_command(runtime_steps, operation)
            self.assertEqual(command[:3], ["python3", ".github/scripts/tag_scenarios.py", operation])
            for name in ("category", "batch", "package"):
                variable = "SCENARIO_" + name.upper()
                self.assertEqual(command[command.index("--" + name) + 1], "$" + variable)
                self.assertEqual(scalar(step, variable), "${{ matrix." + name + " }}")
            self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast", "--timeout", "--tags"))
                                 for argument in command))
            self.assertNotIn("make", command)
        compiled = engine_command(runtime_steps, "compile")
        native = engine_command(runtime_steps, "run")
        self.assertNotEqual(compiled[compiled.index("--directory") + 1],
                            native[native.index("--directory") + 1])

    def test_native_step_runs_the_binary_of_this_jobs_compile_step(self):
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        runtime_steps = steps(runtime)
        compiled = engine_command(runtime_steps, "compile")
        directory = compiled[compiled.index("--directory") + 1]
        for operation in ("run", "optional"):
            command = engine_command(runtime_steps, operation)
            self.assertEqual(command[command.index("--binary") + 1], directory)
        upload = next(step for step in runtime_steps if "uses: actions/upload-artifact" in step)
        self.assertIn("!" + directory + "/*.test", upload)

    def test_go_modules_fall_back_to_their_origin(self):
        # With "|", the go command tries the origin after any proxy error; with
        # ",", only after a 404 or 410, so one reset stream failed a whole job.
        for name, workflow in self.workflows.items():
            if "actions/setup-go" in workflow:
                self.assertRegex(workflow, r"(?m)^env:\n(?:  .*\n)*  GOPROXY: 'https://proxy\.golang\.org\|direct'$", name)

    def test_every_go_setup_installs_the_latest_patch(self):
        # A cached older patch, such as go1.26.8, fails the vulnerability check
        # on standard-library advisories that the latest patch fixes.
        for name, workflow in self.workflows.items():
            for match in re.finditer(r"(?m)^( +)go-version: .+\n", workflow):
                following = workflow[match.end():].splitlines()[0] if workflow[match.end():] else ""
                self.assertEqual(following, match.group(1) + "check-latest: true", name)
        toolchain = re.search(r"(?m)^toolchain go(\d+)\.(\d+)\.(\d+)$", (ROOT / "go.mod").read_text())
        self.assertIsNotNone(toolchain)
        self.assertGreaterEqual(tuple(map(int, toolchain.groups())), (1, 26, 9))

    def test_code_workflow_has_visible_unit_race_static_and_coverage_jobs(self):
        workflow = self.workflows["code.yml"]
        jobs = self.jobs["code.yml"]
        self.assertRegex(workflow, r"(?m)^name: Code checks$")
        self.assertEqual(set(jobs), {"unit", "race", "static", "tag-coverage", "vulncheck"})
        self.assertEqual({scalar(job.split("    steps:", 1)[0], "name") for job in jobs.values()},
                         {"Unit tests", "Race detector", "Vet, compile and CI tooling", "Compiled tag coverage and helpers",
                          "Vulnerability check (Go ${{ matrix.go }})"})
        for event in ("push", "pull_request", "workflow_dispatch"):
            self.assertRegex(workflow, r"(?m)^  " + event + r":")
        self.assertIn("  push:\n    branches: [main]\n  pull_request:\n  workflow_dispatch:", workflow)
        self.assertEqual(scalar(workflow, "group"),
                         "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || (github.event_name == 'push' && github.ref) || github.run_id }}")
        self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
        for block in jobs.values():
            self.assertNotIn("./.github/actions/scenario-images", block)
            self.assertNotIn("./.github/actions/runtime-cleanup", block)
            self.assertNotRegex(block, r"\bdocker\s|docker://|docker/setup-")
            self.assertNotRegex(block, r"(?m)^    needs:")
        # The weekly schedule runs only the vulnerability check.
        self.assertRegex(workflow, r"(?m)^  schedule:\n    - cron: '[^']+'$")
        for name in ("unit", "race", "static", "tag-coverage"):
            self.assertIn("    if: github.event_name != 'schedule'\n", jobs[name])
        self.assertNotIn("if:", jobs["vulncheck"].split("    steps:", 1)[0])
        self.assertIn("make vulncheck", jobs["vulncheck"])
        self.assertIn("make test", jobs["unit"])
        self.assertIn("make race", jobs["race"])
        self.assertIn("make vet", jobs["static"])
        self.assertIn("make tag-compile", jobs["static"])
        self.assertIn("python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v", jobs["static"])
        self.assertTrue(list((ROOT / ".github/scripts").glob("test_*.py")))
        coverage_steps = steps(jobs["tag-coverage"])
        coverage = shlex.split(run_command(engine_step(coverage_steps, "plan")))
        self.assertIn("--verify", coverage)
        self.assertNotIn("--category", coverage)
        code_step = engine_step(coverage_steps, "code")
        self.assertNotIn("--batch", run_command(code_step))
        self.assertNotIn("--category", run_command(code_step))

    def test_source_profiles_have_positive_budgets_safe_ids_and_exact_checker_scopes(self):
        profiles = self.plan["include"]
        identifiers = [profile["id"] for profile in profiles]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        for profile in profiles:
            with self.subTest(profile=profile["profile"]):
                self.assertRegex(profile["id"], r"\A[a-z][a-z0-9_-]{0,79}\Z")
                self.assertGreater(profile["job_timeout"], 0)
                self.assertIsNotNone(engine.DURATION.fullmatch(profile["timeout"]))
                self.assertTrue(engine.selected_tests(self.catalog, profile["package"], profile["tags"]))
                self.assertNotIn("all", profile["tags"].split(","))
                self.assertIn("ci_batch_" + profile["batch"], profile["tags"].split(","))
                self.assertNotIn("native_regression", profile["tags"].split(","))
        for checker, (module_name, _) in engine.CHECKERS.items():
            module = __import__(module_name)
            expected = set(module.NETWORKS if checker == "receivers" else module.CASES)
            actual = [engine.checker_case(checker, network, profile["case"])
                      for profile in profiles if profile["checker"] == checker for network in profile["networks"]]
            self.assertEqual(set(actual), expected)
            self.assertEqual(len(actual), len(set(actual)))

    def test_docker_environment_checks_are_source_owned_and_do_not_require_ceph_images(self):
        profiles = engine.select_required_plan(self.plan, "environment")["include"]
        self.assertTrue(profiles)
        self.assertTrue(all(not profile["requires_ceph"] for profile in profiles))
        # Docker behaviour without Ceph: bridge endpoints and packet capture.
        self.assertTrue(all(profile["package"] in ("./internal/dockerbridge", "./capture") for profile in profiles))
        for category in ("short", "topology", "multicluster", "recovery"):
            self.assertTrue(all(profile["requires_ceph"] for profile in
                                engine.select_required_plan(self.plan, category)["include"]))

    def test_optional_native_regressions_have_explicit_tag_runner_and_source_budget(self):
        jobs = self.jobs["native-regressions.yml"]
        self.assertEqual(len(jobs), 1)
        caller = next(iter(jobs.values()))
        self.assertEqual(scalar(caller.split("    with:", 1)[0], "name"), "Scenarios")
        self.assertNotRegex(caller, r"(?m)^    needs:")
        self.assertEqual(scalar(caller, "uses"), "./.github/workflows/tagged-runtime.yml")
        self.assertEqual(scalar(caller, "category"), "optional")
        batch = scalar(caller, "optional_batch")
        profiles = [profile for profile in self.optional["include"] if profile["batch"] == batch]
        self.assertEqual(len(profiles), 1)
        selected = engine.select_optional_plan(self.optional, batch)
        self.assertEqual(selected["include"], profiles)
        self.assertIn("ci_optional", profiles[0]["tags"].split(","))
        self.assertIn("native_regression", profiles[0]["tags"].split(","))
        self.assertFalse(any(profile["batch"] == batch for profile in self.plan["include"]))
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        self.assertEqual(scalar(runtime, "timeout-minutes"), "${{ matrix.job_timeout }}")
        optional = engine_command(steps(runtime), "optional")
        self.assertEqual(optional[optional.index("--batch") + 1], "$SCENARIO_BATCH")
        self.assertFalse(any(argument.startswith(("-run", "-short", "-failfast")) for argument in optional))
        self.assertIn("CEPH_TEST_RGW_IMAGE: ${{ inputs.rgw_image || env.CEPH_TEST_RGW_IMAGE }}", runtime)
        self.assertNotRegex(runtime, r"(?m)^    continue-on-error:")
        plan = self.jobs["tagged-runtime.yml"]["plan"]
        plan_command = engine_command(steps(plan), "plan", "--optional")
        self.assertEqual(plan_command[plan_command.index("--batch") + 1], "$SCENARIO_OPTIONAL_BATCH")
        self.assertNotIn("--verify", plan_command)

    def test_result_uploads_and_failure_reports_preserve_native_failure(self):
        for filename, jobs in self.jobs.items():
            for name, block in jobs.items():
                uploads = [step for step in steps(block) if "uses: actions/upload-artifact" in step]
                for upload in uploads:
                    with self.subTest(workflow=filename, job=name):
                        self.assertEqual(scalar(upload, "if"), "always()")
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])
        native = engine_step(runtime_steps, "run")
        reporter = next(step for step in runtime_steps if "report_test_failures.py" in run_command(step))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(reporter))
        self.assertEqual(scalar(reporter, "if"), "failure()")
        self.assertEqual(scalar(reporter, "continue-on-error"), "true")
        command = shlex.split(run_command(reporter))
        directory = engine_command(runtime_steps, "run")[-1]
        self.assertEqual(command[2], directory + "/native.log")
        self.assertEqual(command[command.index("--profile") + 1], "${{ matrix.id }}")
        for profile in self.plan["include"]:
            self.assertIsNotNone(failure_reporter.PROFILE.fullmatch(profile["id"]))
        phases = next(step for step in runtime_steps if "report_workflow_phases.py" in run_command(step))
        self.assertEqual(scalar(phases, "if"), "always()")
        self.assertNotIn("continue-on-error", phases)
        self.assertLess(runtime_steps.index(native), runtime_steps.index(phases))
        cleanup = next(step for step in runtime_steps if "phase: check" in step)
        self.assertLess(runtime_steps.index(cleanup), runtime_steps.index(phases))
        for name, step_id in (("COMPILE", "compile"), ("DOCKER", "docker_baseline"),
                              ("IMAGES", "role_images"), ("NATIVE", "native"), ("CLEANUP", "cleanup")):
            self.assertEqual(scalar(phases, "SCENARIO_" + name + "_OUTCOME"),
                             "${{ steps." + step_id + ".outcome }}")
        self.assertEqual(scalar(phases, "SCENARIO_CHECK_NAME"), "${{ inputs.check_name }}")
        self.assertEqual(scalar(phases, "SCENARIO_PROFILE"), "${{ matrix.id }}")
        self.assertEqual(scalar(phases, "SCENARIO_REQUIRES_CEPH"), "${{ matrix.requires_ceph }}")

    def test_all_target_selects_source_union_and_tag_compile_covers_explicit_opt_ins(self):
        for target in ("test-all", "tag-compile"):
            result = subprocess.run(["make", "--no-print-directory", "-n", target], cwd=ROOT,
                                    text=True, capture_output=True, timeout=10, check=True)
            commands = [shlex.split(line) for line in result.stdout.splitlines() if " go test " in line]
            self.assertTrue(commands)
            for command in commands:
                self.assertIn("CGO_ENABLED=0", command)
                self.assertIn("-mod=readonly", command)
                self.assertIn("./...", command)
            if target == "test-all":
                self.assertEqual(len(commands), 1)
                self.assertIn("-tags=all", commands[0])
                self.assertIn("-count=1", commands[0])
                self.assertIn("-v", commands[0])
                self.assertFalse(any(token in ("-run", "-short", "-failfast") for token in commands[0]))
            else:
                tag_sets = [{tag for tag in token.removeprefix("-tags=").split(",")}
                            for command in commands for token in command if token.startswith("-tags=")]
                self.assertIn({"integration"}, tag_sets)
                self.assertIn({"all"}, tag_sets)
                self.assertTrue(any({"all", "goceph", "native_regression", "role_images", "ci_optional"} <= tags for tags in tag_sets))

    def test_release_callers_bound_their_runtime_jobs_within_the_runner_budget(self):
        """Each release caller passes its category's max-parallel, and the
        callers of each scope together leave PULL_REQUEST_RUNNERS free."""
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        self.assertEqual(scalar(runtime, "max-parallel"), "${{ inputs.max_parallel }}")
        self.assertIn("fail-fast: false", runtime)
        for scope, prefix in (("all", "Ceph periodic"), ("others", "Ceph release matrix")):
            with self.subTest(scope=scope):
                entries = engine.release_matrix(scope, prefix)
                self.assertEqual({entry["category"]: entry["max_parallel"] for entry in entries},
                                 engine.RELEASE_MAX_PARALLEL)
                report = engine.releases_report(self.plan, scope, prefix)
                self.assertTrue(report["runner_budget"]["passed"], report["runner_budget"])
        self.assertLessEqual(engine.runner_budget(engine.release_matrix("all", "Ceph periodic"))["runners"],
                             engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS)
        periodic_budget = engine.periodic_runner_budget(engine.release_matrix("all", "Ceph periodic"))
        self.assertTrue(periodic_budget["passed"], periodic_budget)
        for workflow in (self.PERIODIC, self.RELEASE_MATRIX):
            self.assertEqual(scalar(self.jobs[workflow]["runtime"], "max_parallel"), "${{ matrix.max_parallel }}")
        periodic = self.jobs[self.PERIODIC]
        self.assertEqual(scalar(periodic["environment"], "max_parallel"), str(engine.ENVIRONMENT_MAX_PARALLEL))
        self.assertEqual(scalar(periodic["representative"], "max_parallel"), str(engine.CAMPAIGN_MAX_PARALLEL))
        # The pull request entry points stay unbounded: they use the runners
        # that the periodic budget leaves.
        for workflow in self.ENTRY_POINTS:
            self.assertNotIn("max_parallel", self.workflows[workflow])

    def periodic_group_keys(self):
        """The concurrency group key of a full and of a representative run of
        Ceph periodic, from the workflow's group expression."""
        expression = scalar(self.workflows[self.PERIODIC], "group")
        match = re.fullmatch(r"\$\{\{ github\.workflow \}\}-\$\{\{ github\.ref \}\}"
                             r"\$\{\{ \(github\.event\.schedule == '([^']+)' \|\| inputs\.mode == 'representative'\)"
                             r" && '([^']*)' \|\| '([^']*)' \}\}", expression)
        self.assertIsNotNone(match, expression)
        self.assertEqual(match[1], self.CAMPAIGN_SCHEDULE)
        return {"full": "Ceph periodic-refs/heads/main" + match[3],
                "representative": "Ceph periodic-refs/heads/main" + match[2]}

    def test_full_and_campaign_runs_together_leave_pull_request_runners(self):
        """A full run and a campaign run in different concurrency groups can
        be in progress at once, so the periodic budget adds both up."""
        keys = self.periodic_group_keys()
        self.assertEqual({mode: key.removeprefix("Ceph periodic-refs/heads/main") for mode, key in keys.items()},
                         engine.PERIODIC_GROUP_SUFFIX)
        self.assertNotEqual(keys["full"], keys["representative"])
        entries = engine.release_matrix("all", "Ceph periodic")
        full = engine.runner_budget(entries)["runners"]
        campaign = int(scalar(self.jobs[self.PERIODIC]["representative"], "max_parallel"))
        budget = engine.periodic_runner_budget(entries, campaign)
        self.assertEqual(budget["modes"], {"full": full, "representative": campaign})
        self.assertEqual(budget["runners"], full + campaign)
        self.assertLessEqual(budget["runners"], engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS)
        self.assertTrue(budget["passed"], budget)
        # A campaign bounded by the whole periodic share, beside a full run,
        # takes pull request runners.
        overlapping = engine.periodic_runner_budget(entries, engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS)
        self.assertEqual(overlapping["runners"], full + engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS)
        self.assertFalse(overlapping["passed"])
        with mock.patch.object(engine, "CAMPAIGN_MAX_PARALLEL", engine.CAMPAIGN_MAX_PARALLEL + 1):
            report = engine.releases_report(self.plan, "all", "Ceph periodic")
        self.assertFalse(report["passed"])
        self.assertEqual((report["missing"], report["extra"]), ([], []))
        # One shared group would let the runs wait for each other: the
        # budget is then the larger mode's, not the sum.
        shared = engine.periodic_runner_budget(entries, engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS,
                                               {"full": "", "representative": ""})
        self.assertEqual(shared["runners"], engine.RUNNER_LIMIT - engine.PULL_REQUEST_RUNNERS)
        self.assertTrue(shared["passed"])

    def test_runner_budget_refuses_callers_that_take_pull_request_runners(self):
        entries = engine.release_matrix("all", "Ceph periodic")
        crowded = [dict(entry, max_parallel=entry["max_parallel"] + 1) for entry in entries]
        self.assertFalse(engine.runner_budget(crowded)["passed"])
        with mock.patch.dict(engine.RELEASE_MAX_PARALLEL, short=engine.RELEASE_MAX_PARALLEL["short"] + 1):
            report = engine.releases_report(self.plan, "all", "Ceph periodic")
        self.assertFalse(report["passed"])
        self.assertEqual((report["missing"], report["extra"]), ([], []))

    def test_plan_fills_one_build_cache_that_every_runtime_job_restores_before_compiling(self):
        plan_steps = steps(self.jobs["tagged-runtime.yml"]["plan"])
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])

        def named(job_steps, name):
            [step] = [step for step in job_steps if step.splitlines()[0].strip() == "- name: " + name]
            return step

        restore = named(plan_steps, "Restore the Go build cache")
        warm = engine_step(plan_steps, "buildcache")
        save = named(plan_steps, "Save the Go build cache")
        key = scalar(restore, "key")
        self.assertIn("${{ steps.go.outputs.go-version }}", key)
        self.assertIn("${{ hashFiles('go.sum') }}", key)
        self.assertEqual(scalar(save, "key"), key)
        self.assertLess(plan_steps.index(restore), plan_steps.index(warm))
        self.assertLess(plan_steps.index(warm), plan_steps.index(save))
        # Only a successful warm compile on a miss is saved, and its failure
        # never fails the plan: the cache is only an acceleration.
        self.assertEqual(scalar(warm, "if"),
                         "steps.build_cache.outputs.cache-hit != 'true' && steps.plan.outputs.count != '0'")
        self.assertEqual(scalar(warm, "continue-on-error"), "true")
        self.assertEqual(scalar(warm, "GOPROXY"), "'off'")
        self.assertEqual(scalar(save, "if"), "steps.warm_build.outcome == 'success'")
        restored = named(runtime_steps, "Restore the Go build cache")
        self.assertEqual(scalar(restored, "key"), key)
        self.assertIn("actions/cache/restore", restored)
        self.assertNotIn("actions/cache/save", self.jobs["tagged-runtime.yml"]["runtime"])
        self.assertLess(runtime_steps.index(restored), runtime_steps.index(engine_step(runtime_steps, "compile")))
        for job_steps in (plan_steps, runtime_steps):
            [setup] = [step for step in job_steps if "actions/setup-go" in step]
            self.assertEqual(scalar(setup, "id"), "go")



class Expression:
    """The subset of the GitHub Actions expression language that the opt-in
    selection uses: literals, property paths with the * filter, !, ==, !=,
    && and || with their value-returning semantics, contains() and
    format(). A missing property is null."""

    TOKEN = re.compile(r"\s*(?:(?P<string>'(?:[^']|'')*')|(?P<number>\d+)|(?P<name>[A-Za-z_][\w-]*)"
                       r"|(?P<op>&&|\|\||==|!=|[!().,*]))")

    def __init__(self, text, context):
        self.tokens, self.context, position = [], context, 0
        text = text.strip()
        while position < len(text):
            match = self.TOKEN.match(text, position)
            if not match:
                raise AssertionError("unsupported expression at: " + text[position:])
            kind = match.lastgroup
            self.tokens.append((kind, match[kind]))
            position = match.end()
        self.index = 0

    @staticmethod
    def truthy(value):
        return value not in (None, False, 0, "")

    @staticmethod
    def text(value):
        return "" if value is None else str(value).lower() if isinstance(value, bool) else str(value)

    def peek(self):
        return self.tokens[self.index] if self.index < len(self.tokens) else (None, None)

    def take(self, value=None):
        token = self.peek()
        if value is not None and token[1] != value:
            raise AssertionError("expected " + value + ", got " + str(token[1]))
        self.index += 1
        return token

    def evaluate(self):
        value = self.disjunction()
        if self.index != len(self.tokens):
            raise AssertionError("trailing tokens in expression")
        return value

    def disjunction(self):
        value = self.conjunction()
        while self.peek()[1] == "||":
            self.take()
            right = self.conjunction()
            value = value if self.truthy(value) else right
        return value

    def conjunction(self):
        value = self.negation()
        while self.peek()[1] == "&&":
            self.take()
            right = self.negation()
            value = right if self.truthy(value) else value
        return value

    def negation(self):
        if self.peek()[1] == "!":
            self.take()
            return not self.truthy(self.negation())
        value = self.primary()
        if self.peek()[1] in ("==", "!="):
            operator = self.take()[1]
            right = self.primary()
            if isinstance(value, str) or isinstance(right, str):
                equal = self.text(value).lower() == self.text(right).lower()
            else:
                equal = value == right
            value = equal if operator == "==" else not equal
        return value

    def primary(self):
        kind, value = self.take()
        if value == "(":
            result = self.disjunction()
            self.take(")")
            return result
        if kind == "string":
            return value[1:-1].replace("''", "'")
        if kind == "number":
            return int(value)
        if kind != "name":
            raise AssertionError("unexpected token " + str(value))
        if value in ("true", "false", "null"):
            return {"true": True, "false": False, "null": None}[value]
        if self.peek()[1] == "(":
            self.take("(")
            arguments = [self.disjunction()]
            while self.peek()[1] == ",":
                self.take()
                arguments.append(self.disjunction())
            self.take(")")
            if value == "contains":
                haystack, needle = arguments
                if isinstance(haystack, list):
                    return any(self.text(item).lower() == self.text(needle).lower() for item in haystack)
                return self.text(needle).lower() in self.text(haystack).lower()
            if value == "format":
                return re.sub(r"\{(\d+)\}", lambda match: self.text(arguments[1 + int(match[1])]), arguments[0])
            raise AssertionError("unsupported function " + value)
        current = self.context.get(value)
        while self.peek()[1] == ".":
            self.take()
            name = self.take()[1]
            if name == "*":
                current = current if isinstance(current, list) else []
            elif isinstance(current, list):
                current = [item.get(name) for item in current if isinstance(item, dict)]
            else:
                current = current.get(name) if isinstance(current, dict) else None
        return current


def interpolate(text, context):
    """A workflow value: literal text with ${{ }} expressions."""
    return re.sub(r"\$\{\{(.*?)\}\}", lambda match: Expression.text(Expression(match[1], context).evaluate()),
                  text, flags=re.S)


def folded(text, key):
    """The value of a key, as a plain scalar or a folded block scalar."""
    match = re.search(r"^( +)" + re.escape(key) + r":[ ]*(.*?)[ ]*$", text, re.M)
    if not match:
        raise AssertionError("no " + key + " field")
    if match[2] not in (">", ">-"):
        return match[2]
    lines = []
    for line in text[match.end():].splitlines()[1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= len(match[1]):
            break
        lines.append(line.strip())
    return " ".join(line for line in lines if line)


class OptInDeclarationSelectionTests(unittest.TestCase):
    """Ceph short narrows a pull request to the declaration selection only
    when the pull request carries the opt-in label. Every other event,
    and every push to main, runs every batch."""

    LABEL = SourceDrivenWorkflowTests.OPT_IN["ceph-short.yml"]
    BASE = "0123456789abcdef0123456789abcdef01234567"

    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/ceph-short.yml").read_text()
        cls.caller = dict(blocks(cls.workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["scenarios"]
        reusable = (ROOT / ".github/workflows/tagged-runtime.yml").read_text()
        plan = dict(blocks(reusable.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["plan"]
        cls.plan_step = engine_step(steps(plan), "plan")
        cls.reusable_inputs = reusable.split("\npermissions:", 1)[0]

    def context(self, event, action=None, labels=(), label=None):
        github = {"workflow": "Ceph short", "event_name": event, "run_id": 99,
                  "ref": "refs/heads/main" if event == "push" else "refs/pull/7/merge", "event": {}}
        if event == "pull_request":
            github["event"] = {"action": action, "pull_request": {
                "number": 7, "base": {"sha": self.BASE}, "labels": [{"name": name} for name in labels]}}
            if label is not None:
                github["event"]["label"] = {"name": label}
        return {"github": github}

    def outcome(self, context):
        """(runs, select_declarations, concurrency group) for one event."""
        runs = Expression(folded(self.caller, "if"), context).evaluate()
        selection = interpolate(folded(self.caller, "select_declarations"), context)
        group = interpolate(scalar(self.workflow, "group"), context)
        return Expression.truthy(runs), selection, group

    def test_pushes_to_main_and_manual_runs_run_every_batch(self):
        self.assertEqual(self.outcome(self.context("push")), (True, "", "Ceph short-push-refs/heads/main"))
        self.assertEqual(self.outcome(self.context("workflow_dispatch")), (True, "", "Ceph short-workflow_dispatch-99"))

    def test_a_pull_request_without_the_label_runs_every_batch(self):
        for action in ("opened", "synchronize", "reopened"):
            with self.subTest(action=action):
                self.assertEqual(self.outcome(self.context("pull_request", action, labels=("ci:full", "docs"))),
                                 (True, "", "Ceph short-pull_request-7"))

    def test_a_labelled_pull_request_selects_from_its_base(self):
        for action in ("opened", "synchronize", "reopened"):
            with self.subTest(action=action):
                self.assertEqual(self.outcome(self.context("pull_request", action, labels=(self.LABEL,))),
                                 (True, self.BASE, "Ceph short-pull_request-7"))
        # Adding the label replaces the full run in progress with a selected
        # one; removing it replaces a selected run with a full one.
        self.assertEqual(self.outcome(self.context("pull_request", "labeled", labels=(self.LABEL,), label=self.LABEL)),
                         (True, self.BASE, "Ceph short-pull_request-7"))
        self.assertEqual(self.outcome(self.context("pull_request", "unlabeled", labels=(), label=self.LABEL)),
                         (True, "", "Ceph short-pull_request-7"))

    def test_another_label_starts_nothing_and_cancels_nothing(self):
        for action, labels in (("labeled", (self.LABEL, "docs")), ("unlabeled", (self.LABEL,)), ("labeled", ("docs",))):
            with self.subTest(action=action, labels=labels):
                runs, _, group = self.outcome(self.context("pull_request", action, labels=labels, label="docs"))
                self.assertFalse(runs)
                self.assertEqual(group, "Ceph short-pull_request-7-99")

    def test_the_expression_subset_follows_github_semantics(self):
        context = {"a": {"list": [{"n": "X"}, {"n": "y"}]}, "e": ""}
        self.assertEqual(Expression("e && 0 || 1", context).evaluate(), 1)
        self.assertEqual(Expression("'x' && 0 || 1", context).evaluate(), 1)
        self.assertEqual(Expression("'x' && 'b' || 'c'", context).evaluate(), "b")
        self.assertTrue(Expression("contains(a.list.*.n, 'x')", context).evaluate())
        self.assertFalse(Expression("contains(a.list.*.n, 'z')", context).evaluate())
        self.assertIsNone(Expression("missing.path", context).evaluate())
        self.assertEqual(interpolate("p${{ format('-{0}', 'it''s') }}", context), "p-it's")

    def run_plan_step(self, selection):
        """Run the plan step's shell with python3 replaced by a recorder."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "bin").mkdir()
            recorder = directory / "bin" / "python3"
            recorder.write_text("#!/bin/sh\nfor argument in \"$@\"; do printf '%s\\n' \"$argument\"; done "
                                "> \"$RECORD\"\n")
            recorder.chmod(0o755)
            environment = {"PATH": str(directory / "bin") + ":/usr/bin:/bin", "RECORD": str(directory / "record"),
                           "SCENARIO_CATEGORY": "short", "SCENARIO_OPTIONAL_BATCH": "", "SCENARIO_CHANGED_SINCE": "",
                           "SCENARIO_SELECT_DECLARATIONS": selection, "SCENARIO_NETWORKS": "default",
                           "SCENARIO_REPETITIONS": "1", "GITHUB_OUTPUT": str(directory / "output"),
                           "GITHUB_STEP_SUMMARY": str(directory / "summary")}
            subprocess.run(["sh", "-e", "-c", run_command(self.plan_step)], cwd=directory, env=environment,
                           check=True, timeout=30)
            return (directory / "record").read_text().splitlines(), str(directory / "summary")

    def test_the_plan_applies_the_selection_and_writes_the_summary_only_when_opted_in(self):
        self.assertEqual(scalar(self.plan_step, "SCENARIO_SELECT_DECLARATIONS"), "${{ inputs.select_declarations }}")
        self.assertRegex(self.reusable_inputs, r"(?m)^      select_declarations:\n(?:        .*\n)*?        default: ''\n")
        command, summary = self.run_plan_step(self.BASE)
        self.assertEqual(command[:4], [".github/scripts/tag_scenarios.py", "plan", "--category", "short"])
        self.assertEqual(command[command.index("--select-declarations") + 1], self.BASE)
        self.assertIn("--apply-declarations", command)
        self.assertEqual(command[command.index("--summary") + 1], summary)
        self.assertNotIn("--changed-since", command)
        # The default: the full plan, with no declaration analysis at all.
        command, _ = self.run_plan_step("")
        self.assertEqual(command[:4], [".github/scripts/tag_scenarios.py", "plan", "--category", "short"])
        self.assertEqual(command[command.index("--changed-since") + 1], "")
        for option in ("--select-declarations", "--apply-declarations", "--summary"):
            self.assertNotIn(option, command)
        # The other workflows never pass the input, so they keep every batch.
        for path in (ROOT / ".github/workflows").glob("*.yml"):
            if path.name not in ("ceph-short.yml", "tagged-runtime.yml"):
                self.assertNotIn("select_declarations", path.read_text(), path.name)


class BugCompatibilityWorkflowTests(unittest.TestCase):
    """One explicit bug compatibility input reaches every planner command of
    a tagged runtime run as an argument; callers keep the empty default."""

    # The planner commands that recompute a plan, so take the selection.
    SELECTING = ("plan", "buildcache", "compile", "run", "optional")
    # The commands of a runtime job that also name their matrix entry's tags.
    ENTRY = ("compile", "run", "optional")
    ARGUMENT = '--bug-compatibility "$SCENARIO_BUG_COMPATIBILITY"'
    VARIABLE = "${{ inputs.bug_compatibility }}"

    @classmethod
    def setUpClass(cls):
        cls.workflows = {path.name: path.read_text() for path in (ROOT / ".github/workflows").glob("*.yml")}
        cls.reusable = cls.workflows["tagged-runtime.yml"]
        jobs = dict(blocks(cls.reusable.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))
        cls.steps = [step for job in jobs.values() for step in steps(job)]

    def engine_lines(self):
        """(operation, command line, step) of every tag_scenarios.py command."""
        for step in self.steps:
            for line in run_command(step).splitlines():
                match = re.search(r"\btag_scenarios\.py (\w+)", line)
                if match:
                    yield match[1], line, step

    def test_the_input_is_declared_once_with_an_empty_default(self):
        inputs = self.reusable.split("\npermissions:", 1)[0]
        self.assertRegex(inputs, r"(?m)^      bug_compatibility:\n(?:        .*\n)*?        default: ''\n"
                                 r"        type: string\n")
        self.assertEqual(inputs.count("bug_compatibility:"), 1)

    def test_every_selecting_command_takes_the_same_input_as_an_argument(self):
        seen = set()
        for operation, line, step in self.engine_lines():
            with self.subTest(operation=operation, line=line):
                if operation not in self.SELECTING:
                    self.assertNotIn("--bug-compatibility", line)
                    continue
                seen.add(operation)
                self.assertEqual(shlex.split(line).count("--bug-compatibility"), 1)
                self.assertIn(self.ARGUMENT, line)
                self.assertEqual(scalar(step, "SCENARIO_BUG_COMPATIBILITY"), self.VARIABLE)
                if operation in self.ENTRY:
                    self.assertIn('--profile-tags "$SCENARIO_PROFILE_TAGS"', line)
                    self.assertEqual(scalar(step, "SCENARIO_PROFILE_TAGS"), "${{ matrix.tags }}")
        self.assertEqual(seen, set(self.SELECTING))

    def test_history_row_checks_the_selection_and_writes_the_job_summary(self):
        step = next(step for step in self.steps if "scenario_history.py normalize" in run_command(step))
        command = shlex.split(run_command(step).replace("\\\n", " "))
        self.assertEqual(command[command.index("--bug-compatibility") + 1], "$SCENARIO_BUG_COMPATIBILITY")
        self.assertEqual(command[command.index("--summary") + 1], "$GITHUB_STEP_SUMMARY")
        self.assertEqual(scalar(step, "SCENARIO_BUG_COMPATIBILITY"), self.VARIABLE)

    def test_no_expression_or_go_variable_carries_the_selection(self):
        # The input reaches a command only through a quoted step variable,
        # and no workflow sets a Go variable that could select tags.
        for step in self.steps:
            self.assertNotIn("inputs.bug_compatibility", run_command(step))
        for name, text in self.workflows.items():
            with self.subTest(workflow=name):
                self.assertNotRegex(text, r"(?m)^\s+(?:GOFLAGS|GOENV|GOWORK|CGO_CFLAGS):")
                self.assertNotRegex(text, r"(?<![-\w])-tags[= ]")

    def test_callers_keep_the_default_selection(self):
        # Pull requests, ci:full, the comment command and the periodic
        # campaigns select every ordinary path until a caller opts in.
        for name, text in self.workflows.items():
            if name != "tagged-runtime.yml":
                self.assertNotIn("bug_compatibility", text, name)

    def run_plan_step(self, category, selection):
        """Run the plan step's shell with python3 replaced by a recorder."""
        plan_step = next(step for step in self.steps if re.search(r"\btag_scenarios\.py plan\b", run_command(step)))
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "bin").mkdir()
            recorder = directory / "bin" / "python3"
            recorder.write_text("#!/bin/sh\nfor argument in \"$@\"; do printf '%s\\n' \"$argument\"; done "
                                "> \"$RECORD\"\n")
            recorder.chmod(0o755)
            environment = {"PATH": str(directory / "bin") + ":/usr/bin:/bin", "RECORD": str(directory / "record"),
                           "SCENARIO_CATEGORY": category, "SCENARIO_OPTIONAL_BATCH": "native_rgw_translation",
                           "SCENARIO_PROFILES": "x", "SCENARIO_CHANGED_SINCE": "",
                           "SCENARIO_SELECT_DECLARATIONS": "", "SCENARIO_NETWORKS": "default",
                           "SCENARIO_REPETITIONS": "1", "SCENARIO_BUG_COMPATIBILITY": selection,
                           "GITHUB_OUTPUT": str(directory / "output"), "GITHUB_STEP_SUMMARY": str(directory / "s")}
            subprocess.run(["sh", "-e", "-c", run_command(plan_step)], cwd=directory, env=environment,
                           check=True, timeout=30)
            return (directory / "record").read_text().splitlines()

    def test_every_plan_branch_validates_the_selection_before_any_runtime_job(self):
        for category in ("short", "optional", "selected", "representative"):
            for selection in ("", "ceph_bug_synthetic_a, ceph_bug_synthetic_b"):
                with self.subTest(category=category, selection=selection):
                    command = self.run_plan_step(category, selection)
                    self.assertEqual(command[1], "plan")
                    self.assertEqual(command[command.index("--bug-compatibility") + 1], selection)
        jobs = dict(blocks(self.reusable.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))
        self.assertEqual(scalar(jobs["runtime"], "needs"), "plan")

    def test_the_checked_in_registry_allows_only_the_empty_selection(self):
        registry = engine.load_bug_registry()
        self.assertEqual(registry["tags"], {})
        self.assertEqual(engine.bug_selection(registry, "")["selected"], ())
        with contextlib.redirect_stderr(io.StringIO()) as error, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(engine.main(["plan", "--bug-compatibility", "ceph_bug_synthetic_a"]), 1)
        self.assertIn("unregistered bug compatibility tag", error.getvalue())


class FailureReporterSafetyTests(unittest.TestCase):
    def report(self, text=None, profile="tag-scenario", extra=()):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "native.log"
            if text is not None:
                path.write_text(text)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                status = failure_reporter.main([str(path), "--profile", profile, *extra])
            return status, output.getvalue()

    def test_only_completed_failure_names_are_published_once(self):
        log = """=== RUN   TestExample/leaf
secret diagnostic token=private-key
    --- FAIL: TestExample/leaf (1.23s)
    --- FAIL: TestExample/leaf (1.23s)
--- FAIL: TestExample (1.23s)
FAIL example/internal/integration 1.23s
"""
        status, output = self.report(log)
        self.assertEqual(status, 0)
        self.assertEqual(output.count("TestExample/leaf"), 1)
        self.assertEqual(output.count("::error "), 2)
        self.assertNotIn("private-key", output)
        self.assertNotIn("secret diagnostic", output)

    def test_missing_or_incomplete_log_is_notice_only_and_preserves_exit_status(self):
        for log in (None, "=== RUN   TestIncomplete\npanic: secret\n", ""):
            status, output = self.report(log)
            self.assertEqual(status, 0)
            self.assertIn("::notice ", output)
            self.assertNotIn("::error ", output)
            self.assertNotIn("secret", output)

    def test_completed_go_test_or_test_binary_log_needs_no_notice(self):
        for log in ("=== RUN   TestDone\n--- PASS: TestDone (1.00s)\nPASS\nok  \texample/internal/integration\t1.002s\n",
                    "=== RUN   TestDone\n--- PASS: TestDone (1.00s)\nPASS\n"):
            status, output = self.report(log)
            self.assertEqual((status, output), (0, ""))
        status, output = self.report("PASS\nlater output\n")
        self.assertIn("::notice ", output)

    def test_malformed_failure_names_and_profile_arguments_are_not_exposed(self):
        bad_log = "--- FAIL: TestExample/::error-secret (1.00s)\n--- FAIL: TestExample (1.00s) secret\n"
        status, output = self.report(bad_log)
        self.assertEqual(status, 0)
        self.assertNotIn("::error ", output)
        self.assertNotIn("secret", output)
        for profile in ("secret\n::error::private-key", "bad%0Avalue", ""):
            status, output = self.report(bad_log, profile)
            self.assertEqual(status, 0)
            self.assertNotIn("private-key", output)
            self.assertNotIn("secret", output)
        status, output = self.report(bad_log, extra=("--unknown=private-key",))
        self.assertEqual(status, 0)
        self.assertNotIn("private-key", output)

    def test_successful_package_does_not_emit_false_failure_annotations(self):
        status, output = self.report("PASS\nok example/internal/integration 2.01s\n")
        self.assertEqual(status, 0)
        self.assertEqual(output, "")


if __name__ == "__main__":
    unittest.main()
