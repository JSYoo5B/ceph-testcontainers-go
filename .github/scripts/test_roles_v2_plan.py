"""Regression coverage for campaign selection, PR diffs and workflow wiring."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import roles_v2_plan as planner


ROOT = Path(__file__).resolve().parents[2]


class RolesV2PlanTests(unittest.TestCase):
    def test_runtime_pr_is_one_representative_cell(self):
        for path in ("ceph/pool.go", "cephfs/filesystem.go", "rbd/mirror.go", "rgw/zone.go",
                     "fault/monitor.go", "internal/supervised/exec.go", "internal/integration/new_test.go"):
            with self.subTest(path=path):
                plan = planner.plan("pull_request", [path])
                self.assertEqual(plan["scope"], "smoke")
                self.assertEqual(plan["matrix"]["include"], [{"release": "20.2.4", "architecture": "amd64",
                                  "runner": "ubuntu-24.04", "scope": "smoke"}])

    def test_non_runtime_changes_do_not_build_images(self):
        for path in ("docs/contributing/ci.md", "README.md", "LICENSE", "internal/tools/roleoverhead/main.go",
                     "internal/integration/goceph/main.go", ".github/scripts/scenario_history.py"):
            with self.subTest(path=path):
                plan = planner.plan("pull_request", [path])
                self.assertEqual((plan["scope"], plan["count"], plan["matrix"]),
                                 ("none", 0, {"include": []}))

    def test_contract_changes_retain_every_release_and_architecture(self):
        paths = ("ceph/images.go", "ceph/images_test.go", "Makefile", "go.mod", "go.sum",
                 ".github/workflows/roles-v2.yml", ".github/scripts/roles_v2_plan.py",
                 ".github/scripts/test_roles_v2_plan.py", ".github/scripts/check_runtime_cleanup.py",
                 ".github/scripts/run_image_matrix.py",
                 "internal/integration/integration_images_fixture_test.go",
                 "internal/integration/role_images_v2_integration_test.go",
                 "rbd/mirror_role_images_v2_integration_test.go")
        expected = {(release, arch) for release in ("20.2.4", "19.2.5") for arch in ("amd64", "arm64")}
        for path in paths:
            with self.subTest(path=path):
                plan = planner.plan("pull_request", [path])
                self.assertEqual(plan["scope"], "full")
                self.assertEqual(plan["count"], 4)
                self.assertEqual({(cell["release"], cell["architecture"]) for cell in plan["matrix"]["include"]}, expected)
                self.assertTrue(all(cell["scope"] == "full" for cell in plan["matrix"]["include"]))

    def test_full_wins_over_smoke_or_excluded_files_in_mixed_change(self):
        plan = planner.plan("pull_request", ["internal/tools/roleoverhead/main.go", "ceph/pool.go", "ceph/images.go"])
        self.assertEqual(plan["scope"], "full")
        self.assertEqual(planner.plan("pull_request", ["docs/a.md", "ceph/pool.go"])["scope"], "smoke")

    def test_manual_weekly_and_explicit_overrides(self):
        self.assertEqual(planner.plan("workflow_dispatch")["count"], 4)
        self.assertEqual(planner.plan("workflow_dispatch", mode="smoke")["count"], 1)
        self.assertEqual(planner.plan("schedule")["count"], 4)
        self.assertEqual(planner.plan("pull_request", [], labels=["ci:full"])["count"], 4)
        self.assertEqual(planner.plan("pull_request", [], release_phase=True)["count"], 4)
        self.assertEqual(planner.plan("pull_request", [], labels=["enhancement"])["count"], 0)
        with self.assertRaisesRegex(ValueError, "manual scope"):
            planner.plan("workflow_dispatch", mode="unknown")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            planner.plan("push")

    def test_pr_diff_retains_deletions_and_both_sides_of_rename(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()
            git("init", "-q")
            git("config", "user.email", "ci@example.invalid")
            git("config", "user.name", "CI fixture")
            (root / "ceph").mkdir()
            (root / "ceph/images.go").write_text("contract\n")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            (root / "ceph/images.go").rename(root / "moved.txt")
            git("add", "-A")
            git("commit", "-qm", "move contract")
            head = git("rev-parse", "HEAD")
            event = {"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}
            previous = os.getcwd()
            try:
                os.chdir(root)
                changed = planner.changed_paths(event)
            finally:
                os.chdir(previous)
            self.assertEqual(set(changed), {"ceph/images.go", "moved.txt"})
            self.assertEqual(planner.plan("pull_request", changed)["scope"], "full")
            event["pull_request"]["head"]["sha"] = "--help"
            with self.assertRaisesRegex(ValueError, "complete commit"):
                planner.changed_paths(event)

    def test_main_records_selection_and_github_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = root / "event.json"
            event.write_text(json.dumps({"inputs": {"scope": "smoke"}}))
            output, summary, plan_file = root / "output", root / "summary", root / "plan.json"
            with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_EVENT_PATH": str(event),
                            "GITHUB_OUTPUT": str(output), "GITHUB_STEP_SUMMARY": str(summary)}, clear=True), \
                    patch("sys.argv", ["roles_v2_plan.py", "--output", str(plan_file)]):
                planner.main()
            result = json.loads(plan_file.read_text())
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            self.assertEqual(values["scope"], "smoke")
            self.assertEqual(values["count"], "1")
            self.assertEqual(json.loads(values["matrix"]), result["matrix"])
            self.assertIn("1 native jobs", summary.read_text())

    def test_workflow_keeps_full_suites_and_bounds_only_smoke(self):
        workflow = (ROOT / ".github/workflows/roles-v2.yml").read_text()
        self.assertIn("types: [opened, synchronize, reopened, labeled, unlabeled]", workflow)
        self.assertNotIn("    paths:", workflow)
        self.assertIn("cron: '41 3 * * 1'", workflow)
        self.assertIn("cancel-in-progress: ${{ github.event_name == 'pull_request' }}", workflow)
        self.assertIn("github.event.label.name != 'ci:full'", workflow)
        self.assertIn("github.event.pull_request.number || github.run_id", workflow)
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}", workflow)
        self.assertIn("if: needs.plan.outputs.count != '0'", workflow)
        self.assertIn("if: matrix.scope == 'smoke'\n        run: make roles-v2-smoke", workflow)
        self.assertEqual(workflow.count("if: matrix.scope == 'full'"), 5)
        for target in ("image-compatibility", "roles-v2-routing", "roles-v2-runtime", "roles-v2-oracles"):
            self.assertIn("run: make " + target, workflow)
        self.assertIn('CEPH_TEST_RGW_IMAGE="$id" make image-compatibility', workflow)
        self.assertIn("check_runtime_cleanup.py snapshot", workflow)
        self.assertIn("if: always() && steps.cleanup.outcome == 'success'", workflow)
        self.assertIn("check_runtime_cleanup.py check", workflow)
        makefile = (ROOT / "Makefile").read_text()
        self.assertIn("roles-v2-smoke: roles-v2-routing", makefile)
        self.assertIn("^Test(ClusterLifecycle|RBDLifecycle|CephFSFilesystem|RGWS3|ManagerLifecycle)$$", makefile)


if __name__ == "__main__":
    unittest.main()
