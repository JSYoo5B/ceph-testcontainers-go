"""Check source-owned CI scheduling and workflow phases without Docker.

The Go AST catalog reads source only. Compiled profile verification belongs to
`tag_scenarios.py plan --verify`; this suite does not compile every batch again.
"""

from collections import Counter
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
import required_ci as command_gate
import run_image_matrix as runner
import tag_scenarios as engine


ROOT = Path(__file__).resolve().parents[2]


# The runtime job's budget: a release candidate keeps a second one for its
# retry of failed tests.
RUNTIME_TIMEOUT = "${{ inputs.require_profiles && matrix.retry_job_timeout || matrix.job_timeout }}"
# The release candidate runs whose first native failure a retry judges.
RELEASE_RETRY = "${{ inputs.require_profiles && matrix.category != 'optional' }}"
# The failure reports run for a failed job and for a retried first failure.
FAILED_NATIVE = "failure() || steps.native.outcome == 'failure'"

class PublicBlockIOQualificationTests(unittest.TestCase):
    def test_disposable_qualification_is_explicit_and_uses_source_owned_batch(self):
        workflow = (ROOT / '.github/workflows/block-fault-probe.yml').read_text()
        jobs = dict(blocks(workflow.split('\njobs:\n', 1)[1], r'^  ([\w-]+):$'))
        self.assertEqual(re.search(r'^    if: (.+)$', jobs['probe'], re.MULTILINE)[1], '${{ !inputs.public_api }}')
        public = jobs['public-api']
        self.assertEqual(re.search(r'^    if: (.+)$', public, re.MULTILINE)[1],
                         "github.event_name == 'workflow_dispatch' && inputs.public_api")
        kernel = next(step for step in steps(public) if 'modprobe' in step)
        self.assertEqual(scalar(kernel, 'if'), 'inputs.prepare_kernel')
        self.assertIn('test "$RUNNER_ENVIRONMENT" = github-hosted', kernel)
        self.assertIn("CEPH_TEST_BLOCKIO_REQUIRE_DELAY: '1'", public)
        self.assertIn('--category optional', public)
        self.assertIn('--batch block_io --package ./internal/integration', public)
        self.assertIn('--binary artifacts/blockio/compile', public)
        self.assertIn('steps.baseline.outcome ==', public)
        self.assertNotIn('docker push', public)
        self.assertNotIn('packages: write', workflow)
        revision = re.search(r'HELPER_REVISION: ([0-9a-f]{40})$', public, re.MULTILINE)[1]
        self.assertIn('ref: ' + revision, public)
        source = (ROOT / 'internal/integration/public_block_io_integration_test.go').read_text()
        self.assertIn('integration && ci_optional && ci_batch_block_io', source)


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
    # The runner allocation that each release-matrix workflow asks for.
    ALLOCATION = {"Periodic": "periodic", "Release matrix": "release"}
    ENTRY_POINTS = {
        "docker.yml": ("Docker checks", "environment"),
        "short.yml": ("Short", "short"),
        "topology.yml": ("Topology", "topology"),
        "multicluster.yml": ("Multicluster", "multicluster"),
        "recovery.yml": ("Recovery", "recovery"),
    }
    # Runtime batches never read these paths, so a change to them alone runs
    # only Code checks, on develop pull requests and pushes alike.
    RUNTIME_PATHS_IGNORE = ("'docs/**'", "'*.md'", "'LICENSE'", "'internal/integration/goceph/**'")
    # These categories gate every merge on the current release.
    GATING = {"docker.yml", "short.yml"}
    # The other categories run on a pull request only for the batches whose
    # own test files changed, unless labelled ci:full.
    SCOPED = {"topology.yml", "multicluster.yml", "recovery.yml"}
    # A pull request of this gating category may opt in to declaration
    # selection with a label; pushes to develop always run every batch.
    OPT_IN = {"short.yml": "ci:selected-batches"}
    # The releases and check names that the per-release caller jobs had
    # before SUPPORTED_RELEASES generated them. They must stay covered.
    HISTORICAL_RELEASES = {"20.2.4": "tentacle", "19.2.5": "squid"}
    PERIODIC, RELEASE_MATRIX = "periodic.yml", "release-matrix.yml"
    # The pull request comment command: issue_comment only.
    COMMAND = "ci-command.yml"
    # The fixed status of the requested CI that a reviewer required.
    GATE = "requested-ci-gate.yml"
    # The scratch-device research probe (#7): its own paths only, no scenarios.
    BLOCK_FAULT_PROBE = "block-fault-probe.yml"
    # The native ARM64 qualification of the source-owned arm64 cohort.
    ARM64 = "arm64.yml"
    PERIODIC_SCHEDULE = "41 4 * * *"
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
        for path in ("README.md", "NOTES.md", "docs/ceph.md", "docs/guides/messenger-client-fixtures.md",
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
        category on each release, with Docker checks once in Periodic."""
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
        self.assertEqual(command[command.index("--allocation") + 1], self.ALLOCATION[prefix])
        self.assertEqual(command[command.index("--check-prefix") + 1], prefix)
        self.assertEqual(command[command.index("--github-output") + 1], "$GITHUB_OUTPUT")
        self.assertIn("matrix: ${{ steps.releases.outputs.matrix }}", releases)
        self.assertIn("uses: ./.github/workflows/tagged-runtime.yml", runtime)
        self.assertEqual(scalar(runtime, "needs"), {"Periodic": "releases", "Release matrix": "[releases, reuse]"}[prefix])
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
        callers = [(entry["release"], entry["category"]) for entry in engine.release_matrix("all", "Periodic")]
        after = engine.runtime_cells(self.plan, callers + [(None, "environment")])
        self.assertEqual(engine.coverage_difference(before, after)["missing"], [])
        self.assertEqual(after, engine.required_cells(self.plan, runner.RELEASES))
        self.assertEqual(max(after.values()), 1)
        for scope in engine.RELEASE_SCOPES:
            with self.subTest(scope=scope):
                report = engine.releases_report(self.plan, scope, "Periodic")
                self.assertTrue(report["passed"], report)
                self.assertEqual((report["missing"], report["extra"]), ([], []))
        others = {entry["release"] for entry in engine.release_matrix("others", "Release matrix")}
        self.assertEqual(others, set(runner.RELEASES) - {runner.RELEASE})
        # The pull request entry points run the current release only, with
        # every category once.
        current = engine.runtime_cells(self.plan, [(runner.RELEASE, category)
                                                   for _, category in self.ENTRY_POINTS.values()])
        self.assertEqual(current, engine.required_cells(self.plan, [runner.RELEASE]))

    def test_umbrella_runs_every_release_bound_category_in_both_scopes(self):
        self.assertEqual(runner.CODENAMES["21.2.0"], "umbrella")
        self.assertEqual(runner.RELEASE, "20.2.4")
        categories = set(engine.RUNTIME_CATEGORIES) - {"environment"}
        for scope in engine.RELEASE_SCOPES:
            entries = [entry for entry in engine.release_matrix(scope, "Periodic")
                       if entry["release"] == "21.2.0"]
            self.assertEqual({entry["category"] for entry in entries}, categories)
            self.assertEqual(len(entries), len(categories))
            self.assertTrue(all(entry["codename"] == "umbrella" for entry in entries))

    def test_coverage_difference_names_missing_duplicate_and_unknown_release_cells(self):
        required = engine.required_cells(self.plan, runner.RELEASES)
        callers = [(entry["release"], entry["category"]) for entry in engine.release_matrix("all", "Periodic")]
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
        for release in ("20.2.5", "21.2.1", ""):
            with self.subTest(release=release), self.assertRaises(ValueError):
                engine.runtime_cells(self.plan, [(release, "short")])
        with self.assertRaises(ValueError):
            engine.release_matrix("all", "Periodic", current="20.2.5")
        with self.assertRaises(ValueError):
            engine.release_matrix("some", "Periodic")
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
            self.assertTrue(set(roles) <= {"essential", "client", "cephfs", "rbd-aux", "rgw", "mgr-modules"}, profile["id"])
        scoped = {profile["batch"]: profile["image_roles"] for profile in profiles}
        # The optional mgr-modules role reaches only the batch that declares
        # it: the MGR image batch, whose prometheus tests need its modules.
        self.assertEqual(scoped["mgr_image"], "essential,client,mgr-modules")
        self.assertEqual([batch for batch, roles in scoped.items() if "mgr-modules" in roles.split(",")], ["mgr_image"])
        self.assertEqual(scoped["daemon_basics"], "essential,client,rbd-aux")
        self.assertEqual(scoped["network_faults"], "essential,client")
        self.assertEqual(scoped["cephfs_clients"], "essential,client,cephfs,rbd-aux,rgw")
        self.assertEqual(scoped["rbd_namespaces"], "essential,client,rbd-aux")
        self.assertEqual(scoped["rados_fixtures"], "essential,client,cephfs")

    def test_pr_checks_are_separate_workflows_with_independent_entry_points(self):
        expected = {"code.yml", "tagged-runtime.yml", "native-regressions.yml", "experiments.yml", self.PERIODIC,
                    self.RELEASE_MATRIX, self.COMMAND, self.GATE, self.BLOCK_FAULT_PROBE, self.ARM64,
                    "structural-review.yml",
                    "structural-collect.yml", *self.ENTRY_POINTS}
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
                types = ("    types: [opened, synchronize, reopened, edited, labeled]\n" if filename in self.SCOPED
                         else "    types: [opened, synchronize, reopened, edited, labeled, unlabeled]\n" if filename in self.OPT_IN
                         else "    types: [opened, synchronize, reopened, edited]\n")
                self.assertIn("\non:\n  push:\n    branches: [develop]\n    paths-ignore:\n" + ignored +
                              "  pull_request:\n    branches: [develop]\n" + types + "    paths-ignore:\n" + ignored + "  workflow_dispatch:\n", workflow)
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
                # Development pull requests and pushes run the current release.
                self.assertNotRegex(callers[0], r"(?m)^      release:")
                if filename in self.SCOPED:
                    self.assertIn("    if: >-\n      github.event_name != 'push' &&\n"
                                  "      (github.event.action != 'labeled' || github.event.label.name == 'ci:full')\n", callers[0])
                    self.assertIn("      changed_since: >-\n        ${{ github.event_name == 'pull_request' &&\n"
                                  "        !contains(github.event.pull_request.labels.*.name, 'ci:full') &&\n"
                                  "        github.event.pull_request.base.sha || '' }}\n", callers[0])
                    self.assertIn("      changed_head: >-\n        ${{ github.event_name == 'pull_request' &&\n"
                                  "        !contains(github.event.pull_request.labels.*.name, 'ci:full') &&\n"
                                  "        github.event.pull_request.head.sha || '' }}\n", callers[0])
                elif filename in self.OPT_IN:
                    # Evaluated per event in OptInDeclarationSelectionTests.
                    self.assertNotIn("changed_since", callers[0])
                    self.assertNotIn("changed_head", callers[0])
                    self.assertIn("select_declarations", callers[0])
                else:
                    self.assertNotRegex(callers[0], r"(?m)^    if:")
                    self.assertNotIn("changed_since", callers[0])
                    self.assertNotIn("changed_head", callers[0])
                    self.assertNotIn("select_declarations", callers[0])
                self.assertNotIn("steps:", callers[0])
                self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
        with self.subTest(workflow=self.PERIODIC):
            workflow = self.workflows[self.PERIODIC]
            self.assertRegex(workflow, r"(?m)^name: Periodic$")
            self.assertIn("\non:\n  schedule:\n    - cron: '" + self.PERIODIC_SCHEDULE + "'\n  workflow_dispatch:\n", workflow)
            self.assertNotRegex(workflow, r"(?m)^  (?:push|pull_request):")
            self.assertEqual(scalar(workflow, "group"),
                             "${{ github.workflow }}-${{ github.ref }}${{ inputs.mode == 'representative' && '-campaign' || '' }}")
            self.assertEqual(scalar(workflow, "cancel-in-progress"), "false")
            jobs = dict(self.jobs[self.PERIODIC])
            sample_plan, sample = jobs.pop("sample-plan"), jobs.pop("sample")
            self.assertIn("github.event_name == 'schedule'", scalar(sample_plan.split("    steps:", 1)[0], "if"))
            # No mode starts native work beside a release run: the yield job
            # decides first, and every job that starts a mode needs it.
            yielding = jobs.pop("yield")
            self.assertIn("python3 .github/scripts/periodic_gate.py --yield-to " + self.RELEASE_MATRIX, yielding)
            self.assertEqual(scalar(yielding.split("    steps:", 1)[0], "clear"), "${{ steps.yield.outputs.clear }}")
            self.assertNotIn("needs:", yielding)
            for starter in ("sample-plan", "gate", "storage"):
                body = self.jobs[self.PERIODIC][starter].split("    steps:", 1)[0].split("    with:", 1)[0]
                self.assertEqual(scalar(body, "needs"), "yield", starter)
                self.assertTrue(scalar(body, "if").startswith("needs.yield.outputs.clear == 'true' && "), starter)
            for job_id, body in self.jobs[self.PERIODIC].items():
                if job_id not in ("yield", "history"):
                    self.assertIn("needs:", body.split("    steps:", 1)[0].split("    with:", 1)[0], job_id)
            self.assertEqual(scalar(sample, "needs"), "sample-plan")
            self.assertEqual(scalar(sample, "timeout-minutes"), "120")
            self.assertIn('--source "$sha"', sample_plan)
            self.assertIn("periodic_execute.py execute", sample)
            self.assertIn("ref: ${{ needs.sample-plan.outputs.sha }}", sample)
            gate = jobs.pop("gate")
            self.assertIn("python3 .github/scripts/periodic_gate.py --workflow " + self.PERIODIC
                          + " --campaign-schedule '" + self.CAMPAIGN_SCHEDULE + "'", gate)
            self.assertEqual(scalar(gate, "PERIODIC_SCHEDULE"), "${{ github.event.schedule }}")
            self.assertEqual(scalar(gate, "PERIODIC_MODE"), "${{ inputs.mode }}")
            qualification = jobs.pop("storage")
            self.assertEqual(scalar(qualification, "if"), "needs.yield.outputs.clear == 'true' && github.event_name == 'workflow_dispatch' && inputs.mode == 'storage'")
            self.assertEqual(scalar(qualification, "category"), "selected")
            self.assertEqual(scalar(qualification, "profiles"), "${{ inputs.storage_profiles }}")
            storage_input = next(block for key, block in blocks(workflow.split("\npermissions:", 1)[0], r"^      ([\w-]+):$") if key == "storage_profiles")
            self.assertEqual(scalar(storage_input, "default"), "short-rados_fixtures_watchers-52f45dc3")
            self.assertEqual(scalar(qualification, "max_parallel"), str(engine.STORAGE_MAX_PARALLEL))
            self.assertEqual(scalar(qualification, "ref"), "${{ inputs.ref || github.sha }}")
            self.assertEqual(scalar(qualification, "storage_policy"), "${{ inputs.storage_policy }}")
            self.assertEqual(scalar(gate, "if"), "needs.yield.outputs.clear == 'true' && github.event_name == 'workflow_dispatch' && (inputs.mode == 'full' || inputs.mode == 'representative')")
            self.assertFalse(periodic_gate.runtime_job({"name": "Storage qualification / rados_fixtures_watchers"}))
            # The campaign: one pinned commit for every attempt of the
            # source-owned representative batches, which pull the role
            # tags of the release as any other run does.
            pin, representative, history = jobs.pop("pin"), jobs.pop("representative"), jobs.pop("history")
            self.assertEqual(scalar(pin, "if"), "needs.gate.outputs.campaign == 'true'")
            self.assertEqual(scalar(pin.split("    outputs:\n", 1)[1].split("    steps:\n", 1)[0], "sha"),
                             "${{ steps.commit.outputs.sha }}")
            self.assertEqual(len(steps(pin)), 2)
            self.assertNotRegex(pin, r"(?i)digest|imagetools|docker")
            self.assertEqual(scalar(representative, "needs"), "pin")
            self.assertEqual(scalar(representative, "category"), "representative")
            self.assertEqual(scalar(representative, "ref"), "${{ needs.pin.outputs.sha }}")
            self.assertNotIn("role_digests", representative)
            self.assertEqual(scalar(representative, "release"), "${{ inputs.release || '" + runner.RELEASE + "' }}")
            self.assertEqual(scalar(representative, "repetitions"), "${{ fromJSON(inputs.repetitions || 1) }}")
            self.assertEqual(scalar(representative.split("    with:", 1)[0], "name"), "Representative")
            # The gate tells campaign jobs from matrix batches by this name.
            self.assertEqual(scalar(representative.split("    with:", 1)[0], "name"), periodic_gate.CAMPAIGN_CALLER)
            # Only the History job writes, and only the history branch.
            self.assertEqual(scalar(history, "contents"), "write")
            self.assertEqual(scalar(workflow.split("\njobs:\n", 1)[0], "contents"), "read")
            self.assertEqual(scalar(history.split("    steps:", 1)[0], "if"),
                             "always() && (needs.gate.outputs.run == 'true' || needs.gate.outputs.campaign == 'true' || needs.sample-plan.result == 'success')")
            self.assertIn("pattern: tag-scenario-*", history)
            self.assertIn("pattern: periodic-sample-*", history)
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
            for caller in ["Environment", *(entry["title"] for entry in engine.release_matrix("all", "Periodic"))]:
                self.assertTrue(periodic_gate.runtime_job({"name": caller + " / rbd_fixtures_locks"}), caller)
                self.assertFalse(periodic_gate.runtime_job({"name": caller + " / Plan"}), caller)
            self.assertFalse(periodic_gate.runtime_job({"name": "Representative / pools #1"}))
            self.assertEqual(set(jobs), {"environment", "releases", "runtime"})
            environment = jobs["environment"]
            self.assertIn("uses: ./.github/workflows/tagged-runtime.yml", environment)
            self.assertEqual(scalar(environment, "needs"), "gate")
            self.assertEqual(scalar(environment, "if"), "needs.gate.outputs.run == 'true'")
            self.assertEqual(scalar(environment, "category"), "environment")
            self.assertEqual(scalar(environment, "check_name"), "Periodic environment")
            self.assertNotRegex(environment, r"(?m)^      release:")
            self.assertEqual(scalar(jobs["releases"], "needs"), "gate")
            self.assertEqual(scalar(jobs["releases"], "if"), "needs.gate.outputs.run == 'true'")
            self.assert_release_caller(jobs, "all", "Periodic")
            self.assertNotRegex(workflow, r"\bTest[A-Z][A-Za-z0-9_]*")
        with self.subTest(workflow=self.RELEASE_MATRIX):
            workflow = self.workflows[self.RELEASE_MATRIX]
            self.assertRegex(workflow, r"(?m)^name: Release matrix$")
            branches = "    branches: [main, 'release/**', 'support/**', rehearsal/promotion]\n"
            # A release line's push runs no second matrix of its candidate.
            pushed = "    branches: [main, 'support/**', rehearsal/promotion]\n"
            self.assertIn("\non:\n  push:\n" + pushed + "  pull_request:\n" + branches +
                          "    types: [opened, synchronize, reopened, edited]\n  workflow_dispatch:\n", workflow)
            self.assertNotIn("paths", workflow)
            self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
            jobs = self.jobs[self.RELEASE_MATRIX]
            self.assertEqual(set(jobs), {"releases", "reuse", "runtime", "environment", "validated", "receipt"})
            for caller in ("runtime", "environment"):
                self.assertNotIn("      ref:", jobs[caller])
                self.assertEqual(scalar(jobs[caller], "networks"), "all")
                self.assertEqual(scalar(jobs[caller], "require_profiles"), "true")
            self.assertEqual(scalar(jobs["environment"], "category"), "environment")
            self.assertEqual(scalar(jobs["releases"], "ref"), "${{ github.sha }}")
            self.assert_release_caller(jobs, "all", "Release matrix")
            gate = jobs["validated"]
            self.assertEqual(scalar(gate, "needs"), "[releases, reuse, runtime, environment]")
            self.assertEqual(scalar(gate.split("    steps:", 1)[0], "if"), "always()")
            name = scalar(gate.split("    steps:", 1)[0], "name")
            for event, base, ref, want in (
                ("pull_request", "release/1.0", "7/merge", "Release candidate validated (release/1.0)"),
                ("pull_request", "support/1.0", "7/merge", "Release candidate validated (support/1.0)"),
                ("push", "", "main", "Release candidate validated (main)"),
                ("workflow_dispatch", "", "main", "Manual release matrix (main)"),
            ):
                self.assertEqual(interpolate(name, {"github": {"event_name": event, "base_ref": base, "ref_name": ref}}), want)
            check = run_command(steps(gate)[0])
            success = dict(PLANNER="success", RUNTIME="success", ENVIRONMENT="success", COUNT="1",
                           REUSE="skipped", REUSED="")
            for overrides in ({}, {"PLANNER": "failure"}, {"RUNTIME": "skipped"}, {"ENVIRONMENT": "cancelled"}, {"COUNT": "0"}):
                result = subprocess.run(["bash", "-e", "-c", check], env={**success, **overrides})
                self.assertEqual(result.returncode == 0, not overrides)
            # A verified promotion reuse passes the gate without the
            # matrix; nothing short of it does, and it never replaces the
            # planner.
            reused = dict(success, REUSE="success", REUSED="true", RUNTIME="skipped", ENVIRONMENT="skipped")
            for overrides, passes in (({}, True), ({"REUSED": "false"}, False), ({"REUSED": ""}, False),
                                      ({"REUSE": "failure"}, False), ({"REUSE": "skipped"}, False),
                                      ({"REUSE": "cancelled"}, False), ({"PLANNER": "failure"}, False),
                                      ({"COUNT": "0"}, False),
                                      ({"REUSED": "false", "RUNTIME": "success", "ENVIRONMENT": "success"}, True),
                                      ({"REUSE": "failure", "REUSED": "", "RUNTIME": "success",
                                        "ENVIRONMENT": "success"}, True)):
                with self.subTest(overrides=overrides):
                    result = subprocess.run(["bash", "-e", "-c", check], env={**reused, **overrides},
                                            capture_output=True)
                    self.assertEqual(result.returncode == 0, passes)
            # Only a push to main asks for a reuse, after the planner; the
            # matrix runs unless the reuse was verified, also when the reuse
            # job failed or was skipped.
            reuse = jobs["reuse"]
            header = reuse.split("    steps:", 1)[0]
            self.assertEqual(scalar(header, "needs"), "releases")
            self.assertEqual(scalar(header, "if"), "github.event_name == 'push' && (github.ref == 'refs/heads/main' || "
                             "github.ref == 'refs/heads/rehearsal/promotion')")
            self.assertEqual(scalar(header, "reused"), "${{ steps.verify.outputs.reused }}")
            self.assertIn("python3 scripts/release/promotion_reuse.py verify --coverage artifacts/release-coverage.json "
                          "--output artifacts/promotion-reuse\n", reuse)
            self.assertIn("name: promotion-reuse-receipt-${{ github.run_attempt }}", reuse)
            self.assertEqual(scalar(steps(reuse)[-1], "path"), "artifacts/promotion-reuse/")
            condition = ("${{ !cancelled() && needs.releases.result == 'success' && "
                         "needs.reuse.outputs.reused != 'true' }}")
            for caller in ("runtime", "environment"):
                self.assertEqual(scalar(jobs[caller].split("    with:", 1)[0].split("    strategy:", 1)[0], "if"), condition)
                self.assertEqual(scalar(jobs[caller].split("    with:", 1)[0].split("    strategy:", 1)[0], "needs"),
                                 "[releases, reuse]")
            # The gate only decides. The candidate receipt is a job of its
            # own after a passed gate of a promotion to main, so that
            # collecting the records can neither delay nor fail the gate.
            self.assertEqual(len(steps(gate)), 1)
            receipt = jobs["receipt"]
            header = receipt.split("    steps:", 1)[0]
            self.assertEqual(scalar(header, "needs"), "[releases, validated]")
            self.assertEqual(scalar(header, "if"), "${{ !cancelled() && github.event_name == 'pull_request' && "
                                                   "(github.base_ref == 'main' || "
                                                   "github.base_ref == 'rehearsal/promotion') "
                                                   "&& needs.validated.result == 'success' }}")
            self.assertEqual(scalar(header, "name"), "Candidate receipt")
            # It keeps the history row of every runtime job with the
            # receipt, for as long as the receipt. The script fetches the
            # rows itself: a release run has more artifacts than
            # actions/download-artifact lists, so no step downloads them
            # by pattern.
            self.assertNotIn("pattern:", receipt)
            self.assertIn("python3 scripts/release/promotion_reuse.py record --coverage artifacts/coverage/release-coverage.json "
                          "--output artifacts/release-candidate\n", receipt)
            upload = steps(receipt)[-1]
            self.assertEqual(scalar(upload, "name"), "release-candidate-receipt-${{ github.run_attempt }}")
            self.assertEqual(scalar(upload, "path"), "artifacts/release-candidate/")
            for kept in (upload, steps(reuse)[-1]):
                self.assertEqual(scalar(kept, "retention-days"), "90")
            self.assertEqual(scalar(workflow.split("\nconcurrency:", 1)[0], "actions"), "read")
            self.assertIn("name: release-coverage-${{ github.run_attempt }}", jobs["releases"])
            self.assertIn("set -o pipefail", jobs["releases"])
        for source in self.workflows.values():
            self.assertNotIn("vars.CI_RELEASE_PHASE", source)
        reusable = self.workflows["tagged-runtime.yml"]
        self.assertEqual(reusable.count('test "$(git rev-parse HEAD)" = "$GITHUB_SHA"'), 2)
        self.assertIn('test "$COUNT" -gt 0', reusable)
        self.assertRegex(reusable, r"(?m)^  workflow_call:")
        self.assertNotRegex(reusable, r"(?m)^  (?:push|pull_request|workflow_dispatch):")
        self.assertNotIn("concurrency:", reusable)
        optional = self.workflows["native-regressions.yml"]
        self.assertRegex(optional, r"(?m)^  workflow_dispatch:")
        self.assertNotRegex(optional, r"(?m)^  (?:push|pull_request|workflow_call):")
        self.assertNotIn("concurrency:", optional)

    def test_arm64_cohort_runs_natively_outside_the_ordinary_matrix(self):
        workflow = self.workflows[self.ARM64]
        self.assertRegex(workflow, r"(?m)^name: ARM64$")
        triggers = workflow.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
        self.assertEqual(re.findall(r"(?m)^  ([a-z_]+):", triggers), ["schedule", "pull_request", "workflow_dispatch"])
        self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
        callers = [job for job in self.jobs[self.ARM64].values() if "uses: ./.github/workflows/tagged-runtime.yml" in job]
        self.assertEqual(len(callers), 1)
        self.assertEqual(len(self.jobs[self.ARM64]), 1)
        caller = callers[0]
        self.assertEqual(scalar(caller.split("    with:", 1)[0], "name"), "Scenarios")
        self.assertEqual((scalar(caller, "category"), scalar(caller, "goarch"), scalar(caller, "check_name")),
                         ("architecture", "arm64", "ARM64"))
        # Its cohort comes from the source, never from the workflow.
        for key in ("profiles", "optional_batch", "release", "storage_policy", "networks"):
            self.assertNotRegex(caller, r"(?m)^      " + key + ":")
        # Every pull request path is a file or tree that exists, so that a
        # rename cannot silently stop the trial on changes that matter.
        paths = re.findall(r"(?m)^      - '([^']+)'$", triggers.split("  pull_request:\n", 1)[1].split("  workflow_dispatch:", 1)[0])
        self.assertIn(".github/workflows/" + self.ARM64, paths)
        self.assertIn(".github/workflows/tagged-runtime.yml", paths)
        tracked = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
        for path in paths:
            with self.subTest(path=path):
                pattern = self.github_glob(path)
                self.assertTrue(any(pattern.match(name) for name in tracked + [".github/workflows/" + self.ARM64]))
        # No other caller runs a non-default architecture.
        for name, body in self.workflows.items():
            if name not in (self.ARM64, "tagged-runtime.yml"):
                self.assertNotRegex(body, r"(?m)^      goarch:", name)

    def test_runtime_template_propagates_one_target_architecture(self):
        plan, runtime = self.jobs["tagged-runtime.yml"]["plan"], self.jobs["tagged-runtime.yml"]["runtime"]
        inputs = dict(blocks(self.workflows["tagged-runtime.yml"].split("\npermissions:", 1)[0], r"^      ([\w-]+):$"))
        self.assertEqual(scalar(inputs["goarch"], "default"), "amd64")
        runner = "'ubuntu-24.04-arm' || 'ubuntu-24.04' }}"
        self.assertEqual(scalar(plan, "runs-on"), "${{ inputs.goarch == 'arm64' && " + runner)
        self.assertEqual(scalar(runtime, "runs-on"), "${{ matrix.goarch == 'arm64' && " + runner)
        planning = engine_step(steps(plan), "plan")
        self.assertEqual(scalar(planning, "SCENARIO_GOARCH"), "${{ inputs.goarch }}")
        commands = [shlex.split(line) for line in run_command(planning).splitlines() if "tag_scenarios.py plan" in line]
        self.assertEqual(len(commands), 6)
        for command in commands:
            self.assertEqual(command[command.index("--goarch") + 1], "$SCENARIO_GOARCH")
        cohort = engine_command(steps(plan), "plan", "--architecture-cohort")
        self.assertNotIn("--category", cohort)
        self.assertIn('if [ "$SCENARIO_CATEGORY" = architecture ]; then', planning)
        runtime_steps = steps(runtime)
        compiled = engine_step(runtime_steps, "compile")
        command = engine_command(runtime_steps, "compile")
        self.assertEqual(command[command.index("--goarch") + 1], "$SCENARIO_GOARCH")
        self.assertEqual(scalar(compiled, "SCENARIO_GOARCH"), "${{ matrix.goarch }}")
        prepared = next(step for step in runtime_steps if "uses: ./.github/actions/scenario-images" in step)
        self.assertEqual(scalar(prepared, "platform"), "linux/${{ matrix.goarch }}")
        # The amd64 build cache key stays the one Code checks shares; another
        # architecture's objects get their own key.
        workflow = self.workflows["tagged-runtime.yml"]
        keys = re.findall(r"key: (go-build-.*)$", workflow, re.M)
        self.assertEqual(len(keys), 3)
        self.assertEqual(set(keys), {"go-build-${{ runner.os }}${{ runner.arch != 'X64' && format('-{0}', runner.arch) "
                                     "|| '' }}-${{ steps.go.outputs.go-version }}-${{ hashFiles('go.sum') }}"})

    def test_short_storage_control_only_changes_explicit_dispatches(self):
        caller = self.jobs["short.yml"]["scenarios"]
        for event in ("pull_request", "push", "workflow_dispatch"):
            context = {"github": {"event_name": event}, "inputs": {"storage_policy": "always", "ref": "a" * 40}}
            self.assertEqual(interpolate(scalar(caller, "storage_policy"), context), "always" if event == "workflow_dispatch" else "auto")
            self.assertEqual(interpolate(scalar(caller, "ref"), context), "a" * 40 if event == "workflow_dispatch" else "")
        self.assertEqual(scalar(caller, "max_parallel"), str(engine.PULL_REQUEST_MAX_PARALLEL["short"]))
        self.assertEqual(scalar(caller, "category"), "short")

    def test_storage_pressure_is_explicit_owned_and_never_uploaded(self):
        runtime = dict(blocks(self.workflows["tagged-runtime.yml"].split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["runtime"]
        release = next(step for step in steps(runtime) if "Release the qualification pressure file" in step)
        self.assertEqual(scalar(release, "if"), "always() && startsWith(inputs.storage_policy, 'pressure')")
        self.assertLess(runtime.index("Release the qualification pressure file"), runtime.index("Save compile, native and phase results"))
        self.assertIn("!artifacts/scenario/runner-storage/qualification-pressure.bin", runtime)
        self.assertIn("Verify post-image runner headroom", runtime)
        self.assertLess(runtime.index("Verify post-image runner headroom"), runtime.index("Native assertions and completion proof"))

    def test_only_trusted_jobs_can_write_the_required_status_or_its_ledger(self):
        # A commit status and a pull request comment are what the required
        # status and its ledger are made of. The jobs that can write either
        # run default-branch code only: none checks out a pull request head,
        # builds Go or calls the runtime template.
        writers = set()
        for filename, jobs in self.jobs.items():
            for name, job in jobs.items():
                if re.search(r"(?m)^      (?:statuses|pull-requests|issues): write$", job):
                    writers.add((filename, name))
                    with self.subTest(workflow=filename, job=name):
                        self.assertNotIn("uses: ./", job)
                        self.assertNotIn("setup-go", job)
                        self.assertNotIn("go test", job)
                        self.assertNotIn("head.sha", job)
                        self.assertNotIn("outputs.sha }}\n          path:", job)
                        for ref in re.findall(r"(?m)^          ref: (.*)$", job):
                            self.assertIn(ref, ("${{ github.event.repository.default_branch }}", "${{ github.sha }}"))
        self.assertEqual(writers, {(self.COMMAND, "register"), (self.COMMAND, "acknowledge"), (self.COMMAND, "report"),
                                   (self.COMMAND, "gate"), (self.COMMAND, "structural-report"),
                                   (self.GATE, "evaluate"), ("structural-review.yml", "publish")})
        for filename, workflow in self.workflows.items():
            with self.subTest(workflow=filename):
                # No workflow grants a write scope to all its jobs, and none
                # runs pull request code with the base repository's token.
                top = workflow.split("\njobs:\n", 1)[0]
                self.assertNotRegex(top, r"(?m)^  [a-z-]+: write$")
                self.assertNotIn("write-all", workflow)
                self.assertNotIn("pull_request_target", workflow)

    def test_every_publication_of_the_required_status_is_in_one_concurrency_group(self):
        publishers = {(self.COMMAND, "register"), (self.COMMAND, "gate"), (self.GATE, "evaluate")}
        for filename, jobs in self.jobs.items():
            for name, job in jobs.items():
                with self.subTest(workflow=filename, job=name):
                    self.assertEqual("required_ci.py" in job, (filename, name) in publishers)
                    if (filename, name) in publishers:
                        self.assertIn("    concurrency:\n      group: required-requested-ci\n"
                                      "      cancel-in-progress: false\n", job)
                        self.assertEqual(set(re.findall(r"GH_TOKEN: (.*)", job)), {"${{ github.token }}"})
                    else:
                        self.assertNotIn("required-requested-ci\n", job)
        # A failed job does not fail a commit status, so each publisher
        # ends with a step that sets the error itself unless the script
        # reported every head published. It runs after a failed checkout or
        # a cancelled step as well, and it is the only place a workflow
        # names the context: no step sets it to anything but error.
        commits = {(self.COMMAND, "register"): ("register", "needs.command.outputs.sha"),
                   (self.COMMAND, "gate"): ("evaluate", "needs.command.outputs.sha"),
                   (self.GATE, "evaluate"): ("evaluate", "github.event.workflow_run.head_sha")}
        for (filename, name), (step, commit) in commits.items():
            with self.subTest(workflow=filename, job=name):
                job = self.jobs[filename][name]
                script, withdraw = job.split("      - name: Withdraw the status when the evaluation did not finish\n")
                self.assertIn("        id: " + step + "\n", script)
                self.assertIn('--github-output "$GITHUB_OUTPUT"\n', script)
                self.assertTrue(withdraw.startswith(
                    "        if: always() && steps." + step + ".outputs.settled != 'true' && " + commit + " != ''\n"))
                self.assertEqual(scalar(withdraw, "HEAD_SHA"), "${{ " + commit + " }}")
                self.assertIn('gh api "repos/$GITHUB_REPOSITORY/statuses/$HEAD_SHA"\n'
                              "          -f state=error -f context='" + command_gate.CONTEXT + "' ", withdraw)
                self.assertNotIn("- name:", withdraw)
                if name != "evaluate" or filename == self.COMMAND:
                    self.assertIn("command", scalar(job, "needs"))
        for filename, workflow in self.workflows.items():
            self.assertEqual(workflow.count(command_gate.CONTEXT + "'"),
                             sum(key[0] == filename for key in commits), filename)
            self.assertNotRegex(workflow, r"state=(?!error)\S+ -f context='" + command_gate.CONTEXT)

    def test_the_gate_evaluates_from_the_default_branch_on_every_pull_request_event(self):
        workflow, jobs = self.workflows[self.GATE], self.jobs[self.GATE]
        self.assertRegex(workflow, r"(?m)^name: Requested CI gate$")
        self.assertIn("\non:\n  workflow_run:\n    workflows: [Code checks]\n    types: [requested]\n"
                      "  workflow_dispatch:\n\npermissions: {}\n", workflow)
        self.assertNotRegex(workflow, r"(?m)^  (?:push|pull_request|pull_request_target|issue_comment):")
        # Code checks is the workflow that starts for every pull request
        # event of every maintained branch, documentation-only ones included.
        code = self.workflows["code.yml"]
        self.assertRegex(code, r"(?m)^name: Code checks$")
        self.assertIn("  pull_request:\n    branches: [develop, main, 'release/**', 'support/**']\n"
                      "    types: [opened, synchronize, reopened, edited]\n", code)
        self.assertNotIn("paths", code.split("\njobs:\n", 1)[0])
        self.assertEqual(set(jobs), {"evaluate"})
        job = jobs["evaluate"]
        self.assertEqual(scalar(job.split("    steps:", 1)[0], "if"),
                         "github.event_name == 'workflow_dispatch' || github.event.workflow_run.event == 'pull_request'")
        self.assertEqual(scalar(job, "ref"), "${{ github.event.repository.default_branch }}")
        self.assertEqual(re.findall(r"(?m)^ +(?:- )?uses: (.*)$", job), ["actions/checkout@v7"])
        self.assertIn("persist-credentials: false", job)
        self.assertIn('run: python3 .github/scripts/required_ci.py evaluate --github-output "$GITHUB_OUTPUT"\n', job)
        # The job's own check is not the status: the status is the script's.
        self.assertNotIn("name: " + command_gate.CONTEXT, workflow)

    def test_comment_command_runs_named_profiles_with_minimal_permissions_per_job(self):
        workflow, jobs = self.workflows[self.COMMAND], self.jobs[self.COMMAND]
        self.assertRegex(workflow, r"(?m)^name: CI command$")
        self.assertIn("\non:\n  issue_comment:\n    types: [created]\n  workflow_dispatch:\n", workflow)
        self.assertNotRegex(workflow, r"(?m)^  (?:push|pull_request|pull_request_target):")
        # No workflow-level group: a comment that is not a command must never
        # cancel or queue behind a command run.
        self.assertNotRegex(workflow, r"(?m)^concurrency:")
        self.assertEqual(set(jobs), {"command", "validate", "register", "acknowledge", "runtime", "report", "gate",
                                     "structural-source", "structural-report"})
        permissions = {name: dict(re.findall(r"(?m)^      ([a-z-]+): (read|write)$", re.search(
                           r"(?m)^    permissions:\n((?:      .*\n)+)", job)[1])) for name, job in jobs.items()}
        self.assertEqual(permissions, {
            "command": {"contents": "read", "pull-requests": "read"},
            "validate": {"contents": "read"},
            "register": {"contents": "read", "pull-requests": "write", "statuses": "write"},
            "acknowledge": {"statuses": "write"},
            "runtime": {"contents": "read"},
            "report": {"contents": "read", "pull-requests": "write", "statuses": "write"},
            "gate": {"contents": "read", "pull-requests": "write", "statuses": "write"},
            "structural-source": {"actions": "read"},
            "structural-report": {"contents": "read", "actions": "read", "pull-requests": "write"}})
        self.assertEqual(scalar(jobs["command"], "if"),
                         "github.event_name == 'issue_comment' && github.event.issue.pull_request && contains(github.event.comment.body, '/ci')")
        # Only Validate checks out the head commit, beside the default
        # branch's scripts; no checkout keeps credentials.
        self.assertEqual(workflow.count("uses: actions/checkout@v7"), workflow.count("persist-credentials: false"))
        for name in ("command", "register", "report", "gate"):
            self.assertNotIn("ref:", jobs[name])
            self.assertNotIn("head/", jobs[name])
        self.assertEqual(scalar(jobs["validate"], "ref"), "${{ needs.command.outputs.sha }}")
        self.assertIn("python3 trusted/.github/scripts/ci_command.py select", jobs["validate"])
        self.assertIn("working-directory: head\n        run: python3 .github/scripts/tag_scenarios.py vocabulary",
                      jobs["validate"])
        runtime = jobs["runtime"]
        self.assertEqual(scalar(runtime, "uses"), "./.github/workflows/tagged-runtime.yml")
        # An advisory run starts once its names resolve. A command that
        # changes the requirements starts only as the attempt that Register
        # recorded, and Acknowledge marks exactly the commands that start.
        starts = ("    if: >-\n      !cancelled() && needs.validate.outputs.accepted == 'true'\n"
                  "      && (needs.command.outputs.verb == 'run' || needs.register.outputs.run == 'true')\n")
        for name in ("acknowledge", "runtime"):
            self.assertIn(starts, jobs[name])
            self.assertIn("    needs: [command, validate, register]\n", jobs[name])
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
        self.assertIn("        if: steps.reply.outputs.post == 'true'\n", jobs["report"])
        self.assertIn("    needs: [command, validate, register, acknowledge, runtime]\n", jobs["report"])
        # drop is not validated: its names may be gone from the head.
        self.assertEqual(scalar(jobs["validate"], "if"),
                         "needs.command.outputs.accepted == 'true' && needs.command.outputs.verb != 'drop'")
        # Register records drop always, require only with resolved names,
        # and retry whenever Validate decided, resolved or not.
        self.assertIn("    if: >-\n      !cancelled() && needs.command.outputs.accepted == 'true'\n"
                      "      && (needs.command.outputs.verb == 'drop'\n"
                      "      || (needs.command.outputs.verb == 'require' && needs.validate.outputs.accepted == 'true')\n"
                      "      || (needs.command.outputs.verb == 'retry' && needs.validate.result == 'success'))\n",
                      jobs["register"])
        self.assertIn("python3 .github/scripts/required_ci.py register\n", jobs["register"])
        self.assertEqual(scalar(jobs["register"], "RESOLVED"), "${{ needs.validate.outputs.accepted == 'true' }}")
        # Gate evaluates after the result is recorded, also after a failed
        # or cancelled run.
        self.assertIn("    needs: [command, register, report]\n", jobs["gate"])
        self.assertEqual(scalar(jobs["gate"].split("    steps:", 1)[0], "if"),
                         "always() && needs.register.outputs.run == 'true'")
        self.assertIn('run: python3 .github/scripts/required_ci.py evaluate --github-output "$GITHUB_OUTPUT"\n',
                      jobs["gate"])
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
        self.assertEqual(command[command.index("--changed-head") + 1], "$SCENARIO_CHANGED_HEAD")
        self.assertEqual(scalar(engine_step(steps(plan), "plan"), "SCENARIO_CHANGED_HEAD"), "${{ inputs.changed_head }}")
        self.assertIn("\n    if: needs.plan.outputs.count != '0'\n", runtime)
        self.assertEqual(scalar(runtime, "needs"), "plan")
        self.assertEqual(scalar(runtime, "matrix"), "${{ fromJSON(needs.plan.outputs.matrix) }}")
        self.assertEqual(scalar(runtime, "timeout-minutes"), RUNTIME_TIMEOUT)
        self.assertEqual(scalar(runtime, "fail-fast"), "false")
        self.assertEqual(scalar(plan.split("    steps:", 1)[0], "name"), "Plan")
        # The batch name, with the network mode for a batch's non-default mode.
        self.assertEqual(scalar(runtime.split("    steps:", 1)[0], "name"), "${{ matrix.name }}")
        self.assertIn("--networks", command)
        self.assertEqual(command[command.index("--networks") + 1], "$SCENARIO_NETWORKS")
        self.assertNotRegex(runtime, r"(?m)^        (?:case|group|include|exclude):")

    def test_campaign_attempts_are_separate_jobs_on_the_pinned_commit(self):
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
        # Every runtime job prepares the role tags of the release that its
        # batch declares. Neither Plan nor a caller resolves a tag to a
        # digest, and the template has no input or output that carries one.
        self.assertEqual(set(re.findall(r"(?m)^          (\w+): ", prepared.split("        with:\n", 1)[1])),
                         {"artifact_name", "release", "roles", "platform"})
        self.assertEqual(scalar(prepared, "release"), "${{ inputs.release }}")
        self.assertEqual(scalar(prepared, "roles"), "${{ matrix.image_roles }}")
        self.assertNotRegex(reusable, r"(?i)digest|imagetools")
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
        self.assertEqual((scalar(summary, "if"), scalar(summary, "continue-on-error")), (FAILED_NATIVE, "true"))
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
        for step in (compiled, baseline):
            self.assertNotRegex(step, r"(?m)^\s+(?:if|continue-on-error):")
        # Only a release candidate's first native run may fail without
        # failing the job, and then only for the retry that follows it.
        self.assertNotRegex(native, r"(?m)^\s+if:")
        self.assertEqual(scalar(native, "continue-on-error"), RELEASE_RETRY)
        retry = engine_step(runtime_steps, "retry")
        self.assertEqual(runtime_steps.index(native) + 1, runtime_steps.index(retry))
        self.assertEqual(scalar(retry, "if"), "always() && steps.native.outcome == 'failure' && " + RELEASE_RETRY[4:-3])
        self.assertNotRegex(retry, r"(?m)^\s+continue-on-error:")
        command = engine_command(runtime_steps, "retry")
        self.assertEqual(command[command.index("--first") + 1], engine_command(runtime_steps, "run")[-1])
        self.assertLess(runtime_steps.index(retry), runtime_steps.index(cleanup))
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
        # setup-go's own cache stays off: the explicit module and build
        # cache steps are the only ones (see docs/contributing/ci.md).
        setups = 0
        for name, workflow in self.workflows.items():
            lines = workflow.splitlines()
            for index, line in enumerate(lines):
                found = re.match(r"^( *)(- )?uses: actions/setup-go@", line)
                if not found:
                    continue
                setups += 1
                # The column of the step's "- ", whether "uses" opens the
                # step or follows its "name".
                step = len(found.group(1)) if found.group(2) else len(found.group(1)) - 2
                block = []
                for following in lines[index + 1:]:
                    indent = len(following) - len(following.lstrip(" "))
                    if following.strip() and indent <= step:
                        break
                    block.append(following.strip())
                with self.subTest(workflow=name, line=index + 1):
                    self.assertIn("check-latest: true", block)
                    self.assertIn("cache: false", block)
                    self.assertTrue(any(entry.startswith("go-version: ") for entry in block), block)
        self.assertGreater(setups, 10)
        toolchain = re.search(r"(?m)^toolchain go(\d+)\.(\d+)\.(\d+)$", (ROOT / "go.mod").read_text())
        self.assertIsNotNone(toolchain)
        self.assertGreaterEqual(tuple(map(int, toolchain.groups())), (1, 26, 9))

    def test_workflow_release_literals_follow_the_supported_releases(self):
        # A workflow input or matrix cannot import run_image_matrix, so each
        # literal copy of the release table is checked against it.
        supported = "[" + ", ".join("'" + release + "'" for release, _, _ in runner.SUPPORTED_RELEASES) + "]"
        default = "'" + runner.RELEASE + "'"
        periodic = self.workflows["periodic.yml"]
        release_input = periodic.split("      release:\n", 1)[1].split("\n      ref:\n", 1)[0]
        self.assertEqual(scalar(release_input, "options"), supported)
        self.assertEqual(scalar(release_input, "default"), default)
        self.assertIn("      release: ${{ inputs.release || " + default + " }}\n", periodic)
        self.assertRegex(self.workflows["block-fault-probe.yml"], r"(?m)^        release: " + re.escape(supported) + "$")
        runtime_input = self.workflows["tagged-runtime.yml"].split("\n      release:\n", 1)[1].split("\n      changed_since:", 1)[0]
        self.assertEqual(scalar(runtime_input, "default"), default)
        literals = re.findall(r"'(\d+\.\d+\.\d+)'", "\n".join(self.workflows.values()))
        self.assertLessEqual(set(literals), set(runner.RELEASES))

    def test_every_runtime_cleanup_call_names_its_artifact(self):
        # The default name is shared, so two jobs of one run, such as the
        # releases of one matrix, would collide on their upload.
        calls = 0
        for name, workflow in self.workflows.items():
            for block in re.split(r"(?m)^ *- ", workflow):
                if "uses: ./.github/actions/runtime-cleanup" in block:
                    calls += 1
                    with self.subTest(workflow=name):
                        self.assertRegex(block, r"(?m)^ +artifact_name: \S*\$\{\{")
        self.assertGreater(calls, 1)

    def test_code_workflow_has_visible_unit_race_static_and_coverage_jobs(self):
        workflow = self.workflows["code.yml"]
        jobs = self.jobs["code.yml"]
        self.assertRegex(workflow, r"(?m)^name: Code checks$")
        self.assertEqual(set(jobs), {"changes", "unit", "race", "static", "tag-coverage", "vulncheck"})
        self.assertEqual({scalar(job.split("    steps:", 1)[0], "name") for job in jobs.values()},
                         {"Detect documentation-only changes", "Unit tests", "Race detector", "Vet, compile and CI tooling",
                          "Compiled tag coverage and helpers", "Vulnerability check (Go ${{ matrix.go }})"})
        for event in ("push", "pull_request", "workflow_dispatch"):
            self.assertRegex(workflow, r"(?m)^  " + event + r":")
        self.assertIn("  push:\n    branches: [develop, main, 'release/**', 'support/**']\n  pull_request:\n    branches: [develop, main, 'release/**', 'support/**']\n    types: [opened, synchronize, reopened, edited]\n  workflow_dispatch:", workflow)
        self.assertEqual(scalar(workflow, "group"),
                         "${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || (github.event_name == 'push' && github.ref) || github.run_id }}")
        self.assertEqual(scalar(workflow, "cancel-in-progress"), "true")
        for block in jobs.values():
            self.assertNotIn("./.github/actions/scenario-images", block)
            self.assertNotIn("./.github/actions/runtime-cleanup", block)
            self.assertNotRegex(block, r"\bdocker\s|docker://|docker/setup-")
        # Every check job waits for the documentation-only detection and nothing else.
        self.assertNotRegex(jobs["changes"], r"(?m)^    needs:")
        for name in ("unit", "race", "static", "tag-coverage", "vulncheck"):
            self.assertIn("    needs: changes\n", jobs[name])
        # The weekly schedule runs only the vulnerability check.
        self.assertRegex(workflow, r"(?m)^  schedule:\n    - cron: '[^']+'$")
        for name in ("unit", "race", "static", "tag-coverage"):
            self.assertIn("    if: ${{ github.event_name != 'schedule' && !cancelled() && "
                          "needs.changes.outputs.docs_only != 'true' }}\n", jobs[name])
        # The matrix job always runs (so its expanded check name is always reported)
        # and skips its steps instead.
        self.assertIn("    if: ${{ !cancelled() }}\n", jobs["vulncheck"].split("    steps:", 1)[0])
        self.assertIn("make vulncheck", jobs["vulncheck"])
        self.assertIn("make test", jobs["unit"])
        self.assertIn("make race", jobs["race"])
        self.assertIn("make vet", jobs["static"])
        self.assertIn("make tag-compile", jobs["static"])
        self.assertIn("python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v", jobs["static"])
        self.assertIn("python3 -m unittest discover -s scripts/release -p 'test_*.py' -v", jobs["static"])
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
        self.assertEqual(scalar(caller, "optional_batch"), "${{ inputs.batch }}")
        workflow = self.workflows["native-regressions.yml"]
        self.assertIn("      batch:\n", workflow)
        self.assertIn("        type: choice\n        default: native_rgw_translation\n", workflow)
        choices = re.findall(r"(?m)^          - ([a-z0-9_]+)$", workflow)
        self.assertEqual(sorted(choices), ["cephfs_dangling_symlink_create",
                                           "cephfs_mds_requests_json",
                                           "cephfs_mirror_blockdiff_empty",
                                           "cephfs_readdir_after_snapdiff",
                                           "cephfs_reclaim_closing_open",
                                           "cephfs_retained_snapshot_full_pool",
                                           "cephfs_snapshot_readdir_eviction", "cephfs_statfs_mixed_quota",
                                           "failure_archive", "mgr_pg_merge_primary_override",
                                           "mon_log_ack", "native_rgw_translation",
                                           "native_shuffle", "rbd_demote_restart",
                                           "rbd_group_image_namespace",
                                           "rbd_group_snap_default_quiesce",
                                           "rbd_group_snap_late_complete",
                                           "rbd_perf_data_pool",
                                           "rbd_resume_checkpoint",
                                           "rbd_trash_purge_timezone",
                                           "rbd_unlink_trashed_snapshot",
                                           "rgw_compressed_copy", "rgw_conditional_writes",
                                           "rgw_empty_sse_header", "rgw_encrypted_multipart_transition",
                                           "rgw_expiration_header",
                                           "rgw_legacy_replication_destination",
                                           "rgw_multipart_completion_replay_etag",
                                           "rgw_multipart_part_class", "rgw_object_lock_sync",
                                           "rgw_post_usage_bytes_received",
                                           "rgw_slo_head_segment_reads", "rgw_ssec_completion",
                                           "rgw_sync_lease_lapse", "rgw_zonegroup_redirect_endpoint"])
        # Every choice is an optional profile that no required plan selects;
        # the reproducers among them compile only with native_regression.
        for choice in choices:
            selected = engine.select_optional_plan(self.optional, choice)["include"]
            self.assertTrue(selected, choice)
            self.assertFalse(any(profile["batch"] == choice for profile in self.plan["include"]), choice)
            if choice != "failure_archive":
                self.assertTrue(all("native_regression" in profile["tags"].split(",") for profile in selected), choice)
        batch = "native_rgw_translation"
        profiles = [profile for profile in self.optional["include"] if profile["batch"] == batch]
        self.assertEqual(len(profiles), 1)
        selected = engine.select_optional_plan(self.optional, batch)
        self.assertEqual(selected["include"], profiles)
        self.assertIn("ci_optional", profiles[0]["tags"].split(","))
        self.assertIn("native_regression", profiles[0]["tags"].split(","))
        self.assertFalse(any(profile["batch"] == batch for profile in self.plan["include"]))
        proof = engine.select_optional_plan(self.optional, "failure_archive")["include"]
        self.assertEqual(len(proof), 1)
        self.assertEqual(proof[0]["image_roles"], "essential")
        self.assertFalse(any(profile["batch"] == "failure_archive" for profile in self.plan["include"]))
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        self.assertEqual(scalar(runtime, "timeout-minutes"), RUNTIME_TIMEOUT)
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
                        expected = ("${{ always() && needs.changes.outputs.docs_only != 'true' }}"
                                    if (filename, name) == ("code.yml", "vulncheck") else "always()")
                        if (filename, name) == ("structural-collect.yml", "collect"):
                            # Report-only evidence: nothing to upload when the
                            # change was not eligible and collection was skipped.
                            expected = "${{ always() && steps.structural.outcome != 'skipped' }}"
                            self.assertEqual(scalar(upload, "continue-on-error"), "true")
                        self.assertEqual(scalar(upload, "if"), expected)
        runtime_steps = steps(self.jobs["tagged-runtime.yml"]["runtime"])
        native = engine_step(runtime_steps, "run")
        reporter = next(step for step in runtime_steps if "report_test_failures.py" in run_command(step))
        self.assertLess(runtime_steps.index(native), runtime_steps.index(reporter))
        self.assertEqual(scalar(reporter, "if"), FAILED_NATIVE)
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

    def test_release_matrix_callers_take_every_runner_but_the_other_repositories(self):
        """Each Release matrix caller passes its category's share, and the
        callers with the environment caller leave OTHER_REPOSITORY_RUNNERS."""
        runtime = self.jobs["tagged-runtime.yml"]["runtime"]
        self.assertEqual(scalar(runtime, "max-parallel"), "${{ inputs.max_parallel }}")
        self.assertIn("fail-fast: false", runtime)
        caps = engine.release_max_parallel()
        share = engine.RUNNER_LIMIT - engine.OTHER_REPOSITORY_RUNNERS
        for scope in engine.RELEASE_SCOPES:
            with self.subTest(scope=scope):
                entries = engine.release_matrix(scope, "Release matrix")
                self.assertEqual({entry["category"]: entry["max_parallel"] for entry in entries}, caps)
                report = engine.releases_report(self.plan, scope, "Release matrix")
                self.assertEqual(report["allocation"], "release")
                self.assertEqual(report["runner_budget"]["available"], share)
                self.assertTrue(report["runner_budget"]["passed"], report["runner_budget"])
        used = engine.runner_budget(engine.release_matrix("all", "Release matrix"))["runners"]
        releases = len(runner.SUPPORTED_RELEASES)
        self.assertEqual(used, releases * sum(caps.values()) + engine.ENVIRONMENT_MAX_PARALLEL)
        # The share is used: one more runner for each release would not fit.
        self.assertGreater(used + releases, share)
        release = self.jobs[self.RELEASE_MATRIX]
        self.assertEqual(scalar(release["runtime"], "max_parallel"), "${{ matrix.max_parallel }}")
        self.assertNotIn("max-parallel", release["runtime"])
        self.assertEqual(scalar(release["environment"], "max_parallel"), str(engine.ENVIRONMENT_MAX_PARALLEL))

    def test_release_report_digests_its_cells_and_images(self):
        """The coverage digest changes with a required cell, with a cell's
        multiplicity and with an image reference, and with nothing else."""
        report = engine.releases_report(self.plan, "all", "Release matrix")
        self.assertEqual(report["coverage"]["cells"], report["cells"])
        self.assertEqual(report["coverage"]["callers"], len(report["include"]) + 1)
        self.assertRegex(report["coverage"]["digest"], r"^[a-f0-9]{64}$")
        self.assertEqual(set(report["images"]), {release for release, _, _ in runner.SUPPORTED_RELEASES})
        for release, references in report["images"].items():
            self.assertEqual(references["official"], runner.RELEASES[release])
            self.assertEqual(references["roles"], runner.image_references("official", "roles", release))
        # The allocation bounds jobs and is no part of what was covered.
        self.assertEqual(engine.releases_report(self.plan, "all", "Periodic", "periodic")["coverage"],
                         report["coverage"])
        releases = [release for release, _, _ in runner.SUPPORTED_RELEASES]
        cells, references = engine.required_cells(self.plan, releases), engine.release_images(releases)
        digest = engine.coverage_digest(cells, references)
        self.assertEqual(digest, report["coverage"]["digest"])
        cell = next(iter(cells))
        fewer = Counter(cells)
        del fewer[cell]
        doubled = Counter(cells)
        doubled[cell] += 1
        renamed = Counter(fewer)
        renamed[(cell[0], cell[1] + "x")] = cells[cell]
        moved = {release: dict(value, official=value["official"] + "-r1") for release, value in references.items()}
        variants = [engine.coverage_digest(fewer, references), engine.coverage_digest(doubled, references),
                    engine.coverage_digest(renamed, references), engine.coverage_digest(cells, moved),
                    engine.coverage_digest(cells, engine.release_images(releases[:-1]))]
        self.assertEqual(len({digest, *variants}), len(variants) + 1)
        self.assertEqual(engine.coverage_digest(Counter(dict(reversed(list(cells.items())))), references), digest)

    def test_release_share_follows_each_callers_runner_time(self):
        """A caller's share is proportional to its category's runner time, so
        the callers of a run end at about the same time."""
        minutes = engine.RELEASE_CALLER_RUNNER_MINUTES
        caps = engine.release_max_parallel()
        ends = {category: minutes[category] / caps[category] for category in caps}
        # No caller could give a runner to the slowest one and end sooner
        # than it does now.
        slowest = max(ends, key=ends.get)
        for category in caps:
            if category != slowest and caps[category] > 1:
                self.assertGreaterEqual(minutes[category] / (caps[category] - 1), ends[slowest] - 1e-9, category)
        for releases, share, expected in (
                (1, 32, {"short": 9, "topology": 10, "multicluster": 6, "recovery": 6}),
                (2, 32, {"short": 4, "topology": 5, "multicluster": 3, "recovery": 3}),
                (3, 32, {"short": 3, "topology": 3, "multicluster": 2, "recovery": 2}),
                (4, 32, {"short": 2, "topology": 2, "multicluster": 1, "recovery": 2}),
                (7, 32, {"short": 1, "topology": 1, "multicluster": 1, "recovery": 1}),
                (3, 13, {"short": 1, "topology": 1, "multicluster": 1, "recovery": 1})):
            with self.subTest(releases=releases, share=share):
                got = engine.release_max_parallel(releases, share)
                self.assertEqual(got, expected)
                self.assertLessEqual(releases * sum(got.values()) + engine.ENVIRONMENT_MAX_PARALLEL, share)
        # A category that needs almost nothing still keeps one runner, and
        # the others give it up.
        skewed = engine.release_max_parallel(1, 9, {"short": 1000, "topology": 1000, "multicluster": 1, "recovery": 1})
        self.assertEqual(skewed, {"short": 3, "topology": 3, "multicluster": 1, "recovery": 1})
        for releases, share in ((8, 32), (3, 12), (0, 32)):
            with self.subTest(releases=releases, share=share), self.assertRaises(ValueError):
                engine.release_max_parallel(releases, share)
        for minutes in ({"short": 1}, dict(engine.RELEASE_CALLER_RUNNER_MINUTES, short=0),
                        dict(engine.RELEASE_CALLER_RUNNER_MINUTES, environment=1)):
            with self.subTest(minutes=minutes), self.assertRaises(ValueError):
                engine.release_max_parallel(minutes=minutes)

    def test_release_budget_refuses_callers_that_take_the_other_repositories_runners(self):
        entries = engine.release_matrix("all", "Release matrix")
        crowded = [dict(entry, max_parallel=entry["max_parallel"] + 1) for entry in entries]
        self.assertFalse(engine.runner_budget(crowded)["passed"])
        # A larger floor for the other repositories with the callers left as
        # they were: the report refuses what release_max_parallel would not
        # have allocated.
        caps = engine.release_max_parallel()
        with mock.patch.object(engine, "OTHER_REPOSITORY_RUNNERS", engine.OTHER_REPOSITORY_RUNNERS + 2), \
                mock.patch.object(engine, "release_max_parallel", return_value=caps):
            report = engine.releases_report(self.plan, "all", "Release matrix")
        self.assertFalse(report["passed"])
        self.assertEqual((report["missing"], report["extra"]), ([], []))
        with self.assertRaisesRegex(ValueError, "unknown runner allocation"):
            engine.release_matrix("all", "Release matrix", allocation="pull_request")

    def periodic_group_keys(self):
        """The concurrency group key of a full and of a representative run of
        Periodic, from the workflow's group expression."""
        expression = scalar(self.workflows[self.PERIODIC], "group")
        match = re.fullmatch(r"\$\{\{ github\.workflow \}\}-\$\{\{ github\.ref \}\}"
                             r"\$\{\{ inputs\.mode == 'representative' && '([^']*)' \|\| '([^']*)' \}\}", expression)
        self.assertIsNotNone(match, expression)
        return {"full": "Periodic-refs/heads/main" + match[2],
                "representative": "Periodic-refs/heads/main" + match[1]}

    def test_periodic_stays_within_its_small_fixed_share(self):
        """Periodic's callers pass PERIODIC_MAX_PARALLEL, the caller matrix
        runs PERIODIC_CALLERS of them at once, and every mode together holds
        at most PERIODIC_RUNNERS."""
        periodic = self.jobs[self.PERIODIC]
        entries = engine.release_matrix("all", "Periodic", allocation="periodic")
        self.assertEqual({entry["max_parallel"] for entry in entries}, {engine.PERIODIC_MAX_PARALLEL})
        self.assertEqual(scalar(periodic["runtime"], "max_parallel"), "${{ matrix.max_parallel }}")
        self.assertEqual(scalar(periodic["runtime"], "max-parallel"), str(engine.PERIODIC_CALLERS))
        self.assertEqual(scalar(periodic["environment"], "max_parallel"), str(engine.ENVIRONMENT_MAX_PARALLEL))
        self.assertEqual(scalar(periodic["representative"], "max_parallel"), str(engine.CAMPAIGN_MAX_PARALLEL))
        self.assertNotIn("matrix:", periodic["sample"])
        report = engine.releases_report(self.plan, "all", "Periodic", "periodic")
        self.assertEqual(report["allocation"], "periodic")
        budget = report["runner_budget"]
        self.assertTrue(budget["passed"], budget)
        self.assertEqual(budget["available"], engine.PERIODIC_RUNNERS)
        full = engine.PERIODIC_CALLERS * engine.PERIODIC_MAX_PARALLEL + engine.ENVIRONMENT_MAX_PARALLEL
        self.assertEqual(budget["modes"], {"full": full, "representative": engine.CAMPAIGN_MAX_PARALLEL,
                                           "storage": engine.STORAGE_MAX_PARALLEL,
                                           "sample": engine.SAMPLE_MAX_PARALLEL})
        # Periodic and a pull request beside it leave the other
        # repositories their runners.
        self.assertLessEqual(engine.PERIODIC_RUNNERS + engine.pull_request_budget()["runners"],
                             engine.RUNNER_LIMIT - engine.OTHER_REPOSITORY_RUNNERS)
        # Without the bound on the caller matrix every caller runs at once.
        unbounded = engine.periodic_runner_budget(entries, callers=len(entries))
        self.assertEqual(unbounded["modes"]["full"], len(entries) + engine.ENVIRONMENT_MAX_PARALLEL)
        self.assertFalse(unbounded["passed"])
        # The release allocation is not Periodic's.
        self.assertFalse(engine.periodic_runner_budget(engine.release_matrix("all", "Periodic"))["passed"])
        with mock.patch.object(engine, "PERIODIC_MAX_PARALLEL", engine.PERIODIC_MAX_PARALLEL + 1):
            crowded = engine.releases_report(self.plan, "all", "Periodic", "periodic")
        self.assertFalse(crowded["passed"])
        self.assertEqual((crowded["missing"], crowded["extra"]), ([], []))

    def test_full_and_campaign_runs_together_stay_within_the_periodic_share(self):
        """A full run and a campaign run in different concurrency groups can
        be in progress at once, so the periodic budget adds both up."""
        keys = self.periodic_group_keys()
        self.assertEqual({mode: key.removeprefix("Periodic-refs/heads/main") for mode, key in keys.items()},
                         {mode: engine.PERIODIC_GROUP_SUFFIX[mode] for mode in keys})
        self.assertNotEqual(keys["full"], keys["representative"])
        entries = engine.release_matrix("all", "Periodic", allocation="periodic")
        campaign = int(scalar(self.jobs[self.PERIODIC]["representative"], "max_parallel"))
        budget = engine.periodic_runner_budget(entries, campaign)
        full = budget["modes"]["full"]
        self.assertEqual(budget["runners"], full + campaign)
        self.assertLessEqual(budget["runners"], engine.PERIODIC_RUNNERS)
        self.assertTrue(budget["passed"], budget)
        # A campaign bounded by the whole periodic share, beside a full run,
        # exceeds it.
        overlapping = engine.periodic_runner_budget(entries, engine.PERIODIC_RUNNERS)
        self.assertEqual(overlapping["runners"], full + engine.PERIODIC_RUNNERS)
        self.assertFalse(overlapping["passed"])
        with mock.patch.object(engine, "CAMPAIGN_MAX_PARALLEL", engine.PERIODIC_RUNNERS - full + 1):
            report = engine.releases_report(self.plan, "all", "Periodic", "periodic")
        self.assertFalse(report["passed"])
        self.assertEqual((report["missing"], report["extra"]), ([], []))
        # One shared group would let the runs wait for each other: the
        # budget is then the larger mode's, not the sum.
        shared = engine.periodic_runner_budget(entries, engine.PERIODIC_RUNNERS,
                                               dict.fromkeys(engine.PERIODIC_GROUP_SUFFIX, ""))
        self.assertEqual(shared["runners"], engine.PERIODIC_RUNNERS)
        self.assertTrue(shared["passed"])

    def test_storage_and_sample_runs_wait_in_the_full_matrix_group(self):
        """A storage dispatch and the daily sample are neither the campaign
        schedule nor the representative mode, so the group expression gives
        them the full matrix's key. A group of their own would add up."""
        entries = engine.release_matrix("all", "Periodic", allocation="periodic")
        self.assertTrue(engine.periodic_runner_budget(entries)["passed"])
        for mode, runners in (("storage", engine.STORAGE_MAX_PARALLEL), ("sample", engine.SAMPLE_MAX_PARALLEL)):
            with self.subTest(mode=mode):
                self.assertEqual(engine.PERIODIC_GROUP_SUFFIX[mode], engine.PERIODIC_GROUP_SUFFIX["full"])
                separate = engine.periodic_runner_budget(entries, groups=dict(engine.PERIODIC_GROUP_SUFFIX, **{mode: "-" + mode}))
                self.assertEqual(separate["runners"], engine.periodic_runner_budget(entries)["runners"] + runners)
                self.assertFalse(separate["passed"])

    def test_pull_request_callers_bound_one_pull_request(self):
        """Every pull request entry point passes its category's bound, and a
        pull request that runs them all leaves room for another beside it."""
        for workflow, (_, category) in self.ENTRY_POINTS.items():
            with self.subTest(workflow=workflow):
                self.assertEqual(scalar(self.jobs[workflow]["scenarios"], "max_parallel"),
                                 str(engine.PULL_REQUEST_MAX_PARALLEL[category]))
        self.assertEqual(sorted(category for _, category in self.ENTRY_POINTS.values()),
                         sorted(engine.PULL_REQUEST_MAX_PARALLEL))
        budget = engine.pull_request_budget()
        self.assertTrue(budget["passed"], budget)
        self.assertLessEqual(engine.PULL_REQUESTS_AT_ONCE * budget["runners"] + engine.OTHER_REPOSITORY_RUNNERS,
                             engine.RUNNER_LIMIT)
        crowded = dict(engine.PULL_REQUEST_MAX_PARALLEL, short=engine.PULL_REQUEST_MAX_PARALLEL["short"] + 1)
        self.assertFalse(engine.pull_request_budget(crowded)["passed"])
        self.assertFalse(engine.pull_request_budget(dict(engine.PULL_REQUEST_MAX_PARALLEL, short=0))["passed"])
        unbound = {key: value for key, value in engine.PULL_REQUEST_MAX_PARALLEL.items() if key != "recovery"}
        self.assertFalse(engine.pull_request_budget(unbound)["passed"])

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
        # never fails the plan: the cache is only an acceleration. A caller
        # that may not save caches skips the warm compile and both saves.
        self.assertEqual(scalar(warm, "if"), "inputs.save_caches && "
                         "steps.build_cache.outputs.cache-hit != 'true' && steps.plan.outputs.count != '0'")
        self.assertEqual(scalar(warm, "continue-on-error"), "true")
        self.assertEqual(scalar(warm, "GOPROXY"), "'off'")
        self.assertEqual(scalar(save, "if"), "inputs.save_caches && steps.warm_build.outcome == 'success'")
        self.assertEqual(scalar(named(plan_steps, "Save the Go module cache"), "if"),
                         "inputs.save_caches && steps.modules_cache.outputs.cache-hit != 'true'")
        save_input = self.workflows["tagged-runtime.yml"].split("\n      save_caches:\n", 1)[1].split("\n\n", 1)[0]
        self.assertEqual((scalar(save_input, "default"), scalar(save_input, "type")), ("true", "boolean"))
        # An issue_comment run belongs to the default branch, so its caches
        # would reach every later run although it checks out an unmerged head.
        for name, workflow in self.workflows.items():
            calls = workflow.count("uses: ./.github/workflows/tagged-runtime.yml")
            refusals = workflow.count("      save_caches: false\n")
            with self.subTest(workflow=name):
                if re.search(r"(?m)^  issue_comment:", workflow):
                    self.assertGreater(calls, 0)
                    self.assertEqual(refusals, calls)
                else:
                    self.assertEqual(refusals, 0)
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


class ChangedSelectionIdentityWorkflowTests(unittest.TestCase):
    """Base and requested head are one selection input pair, never a policy toggle."""

    def test_scoped_callers_pair_base_and_head_under_the_same_conditions(self):
        base, head = "1" * 40, "2" * 40
        for category in ("topology", "recovery", "multicluster"):
            workflow = (ROOT / (".github/workflows/" + category + ".yml")).read_text()
            caller = dict(blocks(workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["scenarios"]
            for event, labels, selected in (
                ("pull_request", [], True),
                ("pull_request", ["ci:full"], False),
                ("push", [], False),
                ("workflow_dispatch", [], False),
            ):
                with self.subTest(category=category, event=event, labels=labels):
                    context = {"github": {"event_name": event, "event": {"pull_request": {
                        "base": {"sha": base}, "head": {"sha": head},
                        "labels": [{"name": name} for name in labels]}}}}
                    self.assertEqual(interpolate(folded(caller, "changed_since"), context), base if selected else "")
                    self.assertEqual(interpolate(folded(caller, "changed_head"), context), head if selected else "")
        for filename in ("short.yml", "periodic.yml", "release-matrix.yml"):
            self.assertNotIn("changed_head", (ROOT / (".github/workflows/" + filename)).read_text())


class OptInDeclarationSelectionTests(unittest.TestCase):
    """Short narrows a pull request to the declaration selection only
    when the pull request to develop carries the opt-in label.
    Every other event, and every push to develop, runs every batch."""

    LABEL = SourceDrivenWorkflowTests.OPT_IN["short.yml"]
    BASE = "0123456789abcdef0123456789abcdef01234567"

    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / ".github/workflows/short.yml").read_text()
        cls.caller = dict(blocks(cls.workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["scenarios"]
        reusable = (ROOT / ".github/workflows/tagged-runtime.yml").read_text()
        plan = dict(blocks(reusable.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))["plan"]
        cls.plan_step = engine_step(steps(plan), "plan")
        cls.reusable_inputs = reusable.split("\npermissions:", 1)[0]

    def context(self, event, action=None, labels=(), label=None):
        github = {"workflow": "Short", "event_name": event, "run_id": 99,
                  "ref": "refs/heads/develop" if event == "push" else "refs/pull/7/merge", "event": {}}
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

    def test_pushes_to_develop_and_manual_runs_run_every_batch(self):
        self.assertEqual(self.outcome(self.context("push")), (True, "", "Short-push-refs/heads/develop"))
        self.assertEqual(self.outcome(self.context("workflow_dispatch")), (True, "", "Short-workflow_dispatch-99"))

    def test_a_pull_request_without_the_label_runs_every_batch(self):
        for action in ("opened", "synchronize", "reopened"):
            with self.subTest(action=action):
                self.assertEqual(self.outcome(self.context("pull_request", action, labels=("ci:full", "docs"))),
                                 (True, "", "Short-pull_request-7"))

    def test_a_labelled_pull_request_selects_from_its_base(self):
        for action in ("opened", "synchronize", "reopened"):
            with self.subTest(action=action):
                self.assertEqual(self.outcome(self.context("pull_request", action, labels=(self.LABEL,))),
                                 (True, self.BASE, "Short-pull_request-7"))
        # Adding the label replaces the full run in progress with a selected
        # one; removing it replaces a selected run with a full one.
        self.assertEqual(self.outcome(self.context("pull_request", "labeled", labels=(self.LABEL,), label=self.LABEL)),
                         (True, self.BASE, "Short-pull_request-7"))
        self.assertEqual(self.outcome(self.context("pull_request", "unlabeled", labels=(), label=self.LABEL)),
                         (True, "", "Short-pull_request-7"))

    def test_another_label_starts_nothing_and_cancels_nothing(self):
        for action, labels in (("labeled", (self.LABEL, "docs")), ("unlabeled", (self.LABEL,)), ("labeled", ("docs",))):
            with self.subTest(action=action, labels=labels):
                runs, _, group = self.outcome(self.context("pull_request", action, labels=labels, label="docs"))
                self.assertFalse(runs)
                self.assertEqual(group, "Short-pull_request-7-99")

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
                           "SCENARIO_CHANGED_HEAD": "",
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
            if path.name not in ("short.yml", "tagged-runtime.yml"):
                self.assertNotIn("select_declarations", path.read_text(), path.name)


class BugCompatibilityWorkflowTests(unittest.TestCase):
    """One explicit bug compatibility input reaches every planner command of
    a tagged runtime run as an argument; callers keep the empty default."""

    # The planner commands that recompute a plan, so take the selection.
    SELECTING = ("plan", "buildcache", "compile", "run", "optional", "retry")
    # The commands of a runtime job that also name their matrix entry's tags.
    ENTRY = ("compile", "run", "optional", "retry")
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
