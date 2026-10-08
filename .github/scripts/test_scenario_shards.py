"""Verify actual compiled scenario selection without starting Docker."""

import os
import importlib.util
from pathlib import Path
import re
import shlex
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
GROUPS = ("infra", "rbd", "cephfs", "rgw")
CASES = ("peer-drain", "directory-release", "process-quiescence",
         "process-recovery", "directory-intent")
RGW_SYNC_GROUPS = ("policy", "account", "translation")


class ScenarioShardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["go", "test", "-mod=readonly", "-tags=integration,auth,features,topology,hostnetwork,multicluster",
             "-list", "^Test", "./internal/integration"], cwd=ROOT,
            env=os.environ | {"CGO_ENABLED": "0"}, capture_output=True,
            text=True, timeout=180, check=True)
        cls.compiled = set(re.findall(r"^Test\w+$", result.stdout, re.M))
        if not cls.compiled:
            raise AssertionError("Go did not list compiled scenario parents")

    def commands(self, target, option=None, value=None, extra=()):
        args = ["make", "--no-print-directory", "-n", target]
        if option:
            args.append(option + "=" + value)
        args.extend(extra)
        result = subprocess.run(args, cwd=ROOT, capture_output=True,
                                text=True, timeout=10, check=True)
        commands = [shlex.split(line) for line in result.stdout.splitlines()
                    if " go test " in line]
        self.assertTrue(commands, result.stdout)
        return commands

    def command(self, target, option=None, value=None, extra=()):
        commands = self.commands(target, option, value, extra)
        self.assertEqual(len(commands), 1, commands)
        return commands[0]

    def selection(self, target, option=None, value=None):
        selected = set()
        for command in self.commands(target, option, value):
            expression = command[command.index("-run") + 1]
            parent_expression = expression.split("/", 1)[0]
            matched = {name for name in self.compiled if re.search(parent_expression, name)}
            self.assertTrue(matched, (target, value, expression))
            self.assertFalse(selected & matched, (target, value, selected & matched))
            selected |= matched
        return selected

    def assert_partition(self, target, option, values, counts):
        aggregate = self.selection(target)
        combined = set()
        for value, count in zip(values, counts):
            selected = self.selection(target, option, value)
            self.assertEqual(len(selected), count, value)
            self.assertFalse(combined & selected, (value, combined & selected))
            combined |= selected
        self.assertEqual(combined, aggregate)

    def test_multicluster_shards_preserve_compiled_aggregate(self):
        self.assert_partition("scenario-multicluster-topology",
                              "SCENARIO_MULTICLUSTER_GROUP", GROUPS, (4, 6, 4, 6))

    def test_removal_cases_preserve_compiled_aggregate(self):
        self.assert_partition("scenario-cephfs-removal",
                              "SCENARIO_CEPHFS_REMOVAL_CASE", CASES, (1,) * 5)

    def test_rgw_sync_shards_preserve_compiled_aggregate(self):
        self.assert_partition("scenario-rgw-sync-fixtures",
                              "SCENARIO_RGW_SYNC_GROUP", RGW_SYNC_GROUPS, (3, 2, 2))

    def test_rgw_sync_local_all_preserves_two_original_commands(self):
        commands = self.commands("scenario-rgw-sync-fixtures")
        self.assertEqual(commands, self.commands("scenario-rgw-sync-fixtures",
                                                 "SCENARIO_RGW_SYNC_GROUP", "all"))
        self.assertEqual(len(commands), 2)
        expressions = [command[command.index("-run") + 1] for command in commands]
        self.assertEqual(expressions, [
            r"^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGW(OwnedSyncPolicy|AccountRootSync))$",
            r"^Test(HostNetwork)?MultiClusterRGWSyncTranslationFiltering$/(tag_owner_class|tenant_system_user_isolation)$"])
        for command in commands:
            self.assertIn("-timeout=60m", command)

    def test_rgw_sync_translation_preserves_required_children(self):
        command = self.command("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP", "translation")
        expression = command[command.index("-run") + 1]
        parent_expression, child_expression = expression.split("/", 1)
        parents = ("TestMultiClusterRGWSyncTranslationFiltering",
                   "TestHostNetworkMultiClusterRGWSyncTranslationFiltering")
        for parent in parents:
            self.assertRegex(parent, parent_expression)
            for child in ("tag_owner_class", "tenant_system_user_isolation"):
                self.assertRegex(child, child_expression)
            for child in ("priority_tags_owner_class", "ordinary_user_denial_grant"):
                self.assertNotRegex(child, child_expression)
        for group in ("policy", "account"):
            command = self.command("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP", group)
            self.assertNotIn("/", command[command.index("-run") + 1])

    def test_rgw_sync_shards_keep_fixture_image_contract_and_ci_budget(self):
        for group in RGW_SYNC_GROUPS:
            with self.subTest(group=group):
                command = self.command("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP",
                                       group, ("MULTICLUSTER_TIMEOUT=40m",))
                for image in ("CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE", "CEPH_TEST_RGW_IMAGE",
                              "CEPH_TEST_MDS_IMAGE", "CEPH_TEST_RBD_CLIENT_IMAGE"):
                    self.assertIn(image, command)
                    self.assertEqual(command[command.index(image) - 1], "-u")
                for flag in ("-count=1", "-mod=readonly", "CGO_ENABLED=0", "-timeout=40m",
                             "-tags=integration,auth,features,topology,hostnetwork,multicluster"):
                    self.assertIn(flag, command)

    def test_unknown_or_empty_shards_fail_before_go(self):
        for target, option in [("scenario-multicluster-topology", "SCENARIO_MULTICLUSTER_GROUP"),
                               ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE"),
                               ("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP")]:
            for value in ("unknown", ""):
                with self.subTest(target=target, value=value):
                    result = subprocess.run(["make", "--no-print-directory", target,
                                             option + "=" + value], cwd=ROOT,
                                            capture_output=True, text=True, timeout=10)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Unknown " + option, result.stderr)
                    self.assertNotIn("go test", result.stdout)

    def test_shards_keep_original_image_and_all_child_selection(self):
        for target, option, values in [
                ("scenario-multicluster-topology", "SCENARIO_MULTICLUSTER_GROUP", GROUPS),
                ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE", CASES)]:
            for value in values:
                with self.subTest(target=target, value=value):
                    command = self.command(target, option, value)
                    for image in ("CEPH_TEST_IMAGE", "CEPH_TEST_OSD_IMAGE",
                                  "CEPH_TEST_RGW_IMAGE", "CEPH_TEST_MDS_IMAGE"):
                        self.assertIn(image, command)
                        self.assertEqual(command[command.index(image) - 1], "-u")
                    self.assertIn("-count=1", command)
                    self.assertIn("-mod=readonly", command)
                    self.assertIn("CGO_ENABLED=0", command)
                    self.assertNotIn("/", command[command.index("-run") + 1])

    def test_workflow_selects_every_supported_shard(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        for job, key, values, option in [
                ("scenario-multicluster-topology", "group", GROUPS, "SCENARIO_MULTICLUSTER_GROUP"),
                ("scenario-cephfs-removal", "case", CASES, "SCENARIO_CEPHFS_REMOVAL_CASE"),
                ("scenario-rgw-sync-fixtures", "group", RGW_SYNC_GROUPS, "SCENARIO_RGW_SYNC_GROUP")]:
            block = re.search(r"^  " + job + r":\n.*?(?=^  \w[\w-]*:\n|\Z)",
                              workflow, re.M | re.S).group()
            actual = re.search(r"^        " + key + r": \[(.*?)\]$", block, re.M)
            self.assertEqual(tuple(x.strip() for x in actual.group(1).split(",")), values)
            self.assertIn("fail-fast: false", block)
            self.assertIn(option + "='${{ matrix." + key + " }}'", block)
            self.assertIn("set -o pipefail", block)
            self.assertIn("phase: snapshot", block)
            self.assertIn("phase: check", block)
            if job == "scenario-rgw-sync-fixtures":
                self.assertIn("timeout-minutes: 50", block)
                self.assertIn("MULTICLUSTER_TIMEOUT=40m", block)
                self.assertIn("name: rgw sync / ${{ matrix.group }}", block)
                cleanup_name = "artifact_name: runtime-cleanup-rgw-sync-${{ matrix.group }}"
                self.assertEqual(block.count(cleanup_name), 2)
                self.assertIn("always() && steps.runtime_cleanup_baseline.outcome == 'success'", block)
                self.assertIn("name: scenario-rgw-sync-${{ matrix.group }}", block)
                self.assertIn("uses: actions/upload-artifact@v7", block)
                profile = re.search(r"--profile '([^']+)'", block).group(1)
                spec = importlib.util.spec_from_file_location(
                    "report_test_failures", ROOT / ".github/scripts/report_test_failures.py")
                reporter = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(reporter)
                for group in RGW_SYNC_GROUPS:
                    self.assertIsNotNone(reporter.PROFILE.fullmatch(
                        profile.replace("${{ matrix.group }}", group)))


if __name__ == "__main__":
    unittest.main()
