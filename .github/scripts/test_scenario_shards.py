"""Verify actual compiled scenario selection without starting Docker."""

import os
from pathlib import Path
import re
import shlex
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]
GROUPS = ("infra", "rbd", "cephfs", "rgw")
CASES = ("peer-drain", "directory-release", "process-quiescence",
         "process-recovery", "directory-intent")


class ScenarioShardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["go", "test", "-mod=readonly", "-tags=integration,topology,hostnetwork,multicluster",
             "-list", "^Test", "./internal/integration"], cwd=ROOT,
            env=os.environ | {"CGO_ENABLED": "0"}, capture_output=True,
            text=True, timeout=180, check=True)
        cls.compiled = set(re.findall(r"^Test\w+$", result.stdout, re.M))
        if not cls.compiled:
            raise AssertionError("Go did not list compiled scenario parents")

    def command(self, target, option=None, value=None):
        args = ["make", "--no-print-directory", "-n", target]
        if option:
            args.append(option + "=" + value)
        result = subprocess.run(args, cwd=ROOT, capture_output=True,
                                text=True, timeout=10, check=True)
        commands = [shlex.split(line) for line in result.stdout.splitlines()
                    if " go test " in line]
        self.assertEqual(len(commands), 1, result.stdout)
        return commands[0]

    def selection(self, target, option=None, value=None):
        command = self.command(target, option, value)
        expression = command[command.index("-run") + 1]
        selected = {name for name in self.compiled if re.search(expression, name)}
        self.assertTrue(selected, (target, value, expression))
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

    def test_unknown_or_empty_shards_fail_before_go(self):
        for target, option in [("scenario-multicluster-topology", "SCENARIO_MULTICLUSTER_GROUP"),
                               ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE")]:
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
                ("scenario-cephfs-removal", "case", CASES, "SCENARIO_CEPHFS_REMOVAL_CASE")]:
            block = re.search(r"^  " + job + r":\n.*?(?=^  \w[\w-]*:\n|\Z)",
                              workflow, re.M | re.S).group()
            actual = re.search(r"^        " + key + r": \[(.*?)\]$", block, re.M)
            self.assertEqual(tuple(x.strip() for x in actual.group(1).split(",")), values)
            self.assertIn("fail-fast: false", block)
            self.assertIn(option + "='${{ matrix." + key + " }}'", block)
            self.assertIn("set -o pipefail", block)
            self.assertIn("phase: snapshot", block)
            self.assertIn("phase: check", block)


if __name__ == "__main__":
    unittest.main()
