"""Verify actual compiled scenario selection without starting Docker."""

import os
import importlib.util
from collections import Counter
from pathlib import Path
import re
import shlex
import subprocess
import unittest

import check_scenario_receivers as receiver_checker
import test_scenario_images as image_helpers

ROOT = Path(__file__).resolve().parents[2]
# Native live-shuffle diagnostics require CEPH_TEST_CEPHFS_NATIVE_SHUFFLE=1.
# Required CI retains the existing explicit-rebalance variants instead.
OPTIONAL_NATIVE_SHUFFLE_PARENTS = frozenset((
    "TestMultiClusterCephFSMirrorDaemonTopology",
    "TestHostNetworkCephFSMirrorDaemonTopology",
))
RGW_TOPOLOGY_CASES = {
    "rgw-endpoints-host": "TestHostNetworkRGWEndpoints",
    "rgw-multisite-bridge": "TestMultiClusterRGWMultisite",
    "rgw-multisite-host": "TestHostNetworkRGWMultisite",
    "rgw-three-zone-bridge": "TestMultiClusterRGWThreeZoneTopology",
    "rgw-three-zone-host": "TestHostNetworkRGWThreeZoneTopology",
    "rgw-master-failover": "TestMultiClusterRGWMetadataMasterFailover",
}
GROUPS = ("infra", "rbd", "cephfs", *RGW_TOPOLOGY_CASES)
CASES = ("peer-drain", "directory-release", "process-quiescence",
         "process-recovery", "directory-intent")
RGW_SYNC_POLICY_CASES = {
    "policy-selective": "TestMultiClusterRGWSelectivePolicy",
    "policy-owned-bridge": "TestMultiClusterRGWOwnedSyncPolicy",
    "policy-owned-host": "TestHostNetworkMultiClusterRGWOwnedSyncPolicy",
}
RGW_SYNC_GROUPS = (*RGW_SYNC_POLICY_CASES, "account", "translation")
CEPHFS_FIXTURE_CASES = {
    "data-pools": "TestCephFSDynamicDataPools",
    "clone-cancellation": "TestCephFSCloneCancellationAndPartialCleanup",
    "quiesce": "TestCephFSQuiesceCheckpoints",
    "authorization": "TestCephFSSubvolumeClientAuthorization",
    "pins": "TestCephFSPins",
    "retained-snapshot": "TestCephFSRetainedSnapshotAndMetadataRecipe",
    "ec-data-pool": "TestCephFSAdditionalErasureCodedDataPool",
    "host-filesystem": "TestHostNetworkCephFSFilesystem",
}
TOPOLOGY_EXTENSION_CASES = {
    "network-interruption": "TestSeparateClusterNetworksAndInterruptions",
    "five-monitors": "TestFiveMonitorQuorumAndNetworkRecovery",
    "rbd-mirror-bridge": "TestMultiClusterRBDMirrorDaemonTopology",
    "rbd-mirror-host": "TestHostNetworkRBDMirrorDaemonTopology",
    "cephfs-mirror-bridge": "TestMultiClusterCephFSMirrorDaemonRebalanceTopology",
    "cephfs-mirror-host": "TestHostNetworkCephFSMirrorDaemonRebalanceTopology",
    "rgw-initial-bridge": "TestMultiClusterRGWInitialZonegroupsTopology",
    "rgw-initial-host": "TestHostNetworkRGWInitialZonegroupsTopology",
    "rgw-removal-bridge": "TestMultiClusterRGWZonegroupsAndRemovalTopology",
    "rgw-removal-host": "TestHostNetworkRGWZonegroupsAndRemovalTopology",
    "rbd-peer-network": "TestMultiClusterRBDPeerNetworkInterruption",
    "rgw-peer-network": "TestMultiClusterRGWPeerNetworkTopology",
}
PARENT_CASE_TARGETS = (
    ("scenario-multicluster-topology", "SCENARIO_MULTICLUSTER_GROUP", RGW_TOPOLOGY_CASES),
    ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", CEPHFS_FIXTURE_CASES),
    ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", TOPOLOGY_EXTENSION_CASES),
)


class ScenarioShardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        def compiled(tags):
            result = subprocess.run(
                ["go", "test", "-mod=readonly", "-tags=" + tags,
                 "-list", "^Test", "./internal/integration"], cwd=ROOT,
                env=os.environ | {"CGO_ENABLED": "0"}, capture_output=True,
                text=True, timeout=180, check=True)
            names = set(re.findall(r"^Test\w+$", result.stdout, re.M))
            if not names:
                raise AssertionError("Go did not list compiled scenario parents")
            return names
        cls.compiled = compiled("integration,auth,features,topology,hostnetwork,multicluster")
        cls.default_compiled = compiled("integration")

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
                              "SCENARIO_MULTICLUSTER_GROUP", GROUPS, (4, 6, 4, *((1,) * 6)))

    def test_parent_cases_select_exact_compiled_name_and_all_children(self):
        for target, option, cases in (*PARENT_CASE_TARGETS,
                ("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP", RGW_SYNC_POLICY_CASES)):
            for case, parent in cases.items():
                with self.subTest(target=target, case=case):
                    self.assertEqual(self.selection(target, option, case), {parent})
                    command = self.command(target, option, case)
                    expression = command[command.index("-run") + 1]
                    self.assertNotIn("/", expression)
                    self.assertEqual(expression, "^" + parent + "$")

    def test_cephfs_and_extension_cases_preserve_compiled_aggregates(self):
        for target, option, cases in PARENT_CASE_TARGETS[1:]:
            with self.subTest(target=target):
                self.assert_partition(target, option, cases, (1,) * len(cases))

    def test_legacy_multicluster_rgw_aggregate_preserves_all_six_cases(self):
        aggregate = self.selection("scenario-multicluster-topology",
                                   "SCENARIO_MULTICLUSTER_GROUP", "rgw")
        self.assertEqual(aggregate, set(RGW_TOPOLOGY_CASES.values()))

    def test_new_local_all_defaults_preserve_original_process_budgets(self):
        for target, option, timeout in (
                ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", "120m"),
                ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", "90m"),
                ("scenario-rbd-receivers", "SCENARIO_RBD_RECEIVERS_CASE", "90m")):
            with self.subTest(target=target):
                command = self.command(target)
                self.assertEqual(command, self.command(target, option, "all"))
                self.assertIn("-timeout=" + timeout, command)

    def test_receiver_network_cases_preserve_original_compiled_parent_and_ten_scope_leaves(self):
        target, option = "scenario-rbd-receivers", "SCENARIO_RBD_RECEIVERS_CASE"
        self.assertEqual(self.selection(target), {receiver_checker.PARENT})
        aggregate_command = self.command(target)
        self.assertEqual(aggregate_command[aggregate_command.index("-run") + 1],
                         "^" + receiver_checker.PARENT + "$")
        inventory = {leaf for network in receiver_checker.NETWORKS
                     for leaf in receiver_checker.required_leaves(network)}
        selected_leaves = set()
        for network in receiver_checker.NETWORKS:
            command = self.command(target, option, network)
            expression = command[command.index("-run") + 1]
            self.assertEqual(expression, receiver_checker.SELECTORS[network])
            self.assertEqual(self.selection(target, option, network), {receiver_checker.PARENT})
            components = expression.split("/")
            self.assertEqual(len(components), 2)  # no scope filter: preserve same-pair phase history
            selected = {leaf for leaf in inventory
                        if all(re.search(part, name) for part, name
                               in zip(components, leaf.split("/")))}
            self.assertEqual(selected, set(receiver_checker.required_leaves(network)))
            self.assertFalse(selected_leaves & selected)
            selected_leaves |= selected
            for foreign in (network + "-other", "other-" + network, "", "scope-0"):
                self.assertNotRegex(foreign, components[1])
            self.assertNotRegex(receiver_checker.PARENT + "Other", components[0])
            for flag in ("-tags=integration,multicluster", "-timeout=90m", "-count=1",
                         "-mod=readonly", "CGO_ENABLED=0"):
                self.assertIn(flag, command)
        self.assertEqual(selected_leaves, inventory)
        self.assertEqual(len(selected_leaves), 10)

    def test_primary_jobs_cover_every_compiled_parent_with_only_the_exact_receiver_partition(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        jobs = dict(image_helpers.blocks(workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))
        options = {
            "scenario-multicluster-topology": "SCENARIO_MULTICLUSTER_GROUP",
            "scenario-cephfs-removal": "SCENARIO_CEPHFS_REMOVAL_CASE",
            "scenario-rgw-sync-fixtures": "SCENARIO_RGW_SYNC_GROUP",
            "scenario-cephfs-fixtures": "SCENARIO_CEPHFS_FIXTURE_CASE",
            "scenario-topology-extensions": "SCENARIO_TOPOLOGY_EXTENSION_CASE",
            "scenario-rbd-receivers": "SCENARIO_RBD_RECEIVERS_CASE",
        }
        counts = Counter()
        receiver_jobs = {}
        for target in image_helpers.PRIMARY:
            matrix = re.search(r"^        (?:group|case): \[(.*?)\]$", jobs[target], re.M)
            values = [value.strip() for value in matrix.group(1).split(",")] if matrix else [None]
            for value in values:
                commands = self.commands(target, options.get(target) if matrix else None, value)
                selected = set()
                for command in commands:
                    if "-run" in command:
                        expression = command[command.index("-run") + 1].split("/", 1)[0]
                        parents = {name for name in self.compiled if re.search(expression, name)}
                    else:
                        self.assertEqual(target, "scenario-default")
                        self.assertIn("-tags=integration", command)
                        parents = self.default_compiled
                    self.assertTrue(parents, (target, value))
                    self.assertFalse(selected & parents, (target, value))
                    selected |= parents
                if target == "scenario-rbd-receivers":
                    self.assertEqual(selected, {receiver_checker.PARENT})
                    receiver_jobs[value] = self.command(target, options[target], value)
                counts.update(selected)
        self.assertTrue(OPTIONAL_NATIVE_SHUFFLE_PARENTS <= self.compiled)
        self.assertEqual(set(counts), self.compiled - OPTIONAL_NATIVE_SHUFFLE_PARENTS)
        self.assertEqual({parent: count for parent, count in counts.items() if count > 1},
                         {receiver_checker.PARENT: 2})
        self.assertEqual(sum(counts.values()), len(self.compiled - OPTIONAL_NATIVE_SHUFFLE_PARENTS) + 1)
        self.assertEqual(set(receiver_jobs), set(receiver_checker.NETWORKS))
        for network, command in receiver_jobs.items():
            self.assertEqual(command[command.index("-run") + 1], receiver_checker.SELECTORS[network])

    def test_removal_cases_preserve_compiled_aggregate(self):
        self.assert_partition("scenario-cephfs-removal",
                              "SCENARIO_CEPHFS_REMOVAL_CASE", CASES, (1,) * 5)

    def test_rgw_sync_shards_preserve_compiled_aggregate(self):
        self.assert_partition("scenario-rgw-sync-fixtures",
                              "SCENARIO_RGW_SYNC_GROUP", RGW_SYNC_GROUPS, (1, 1, 1, 2, 2))

    def test_rgw_sync_local_policy_preserves_all_three_independent_cases(self):
        target, option = "scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP"
        aggregate = self.selection(target, option, "policy")
        self.assertEqual(aggregate, set(RGW_SYNC_POLICY_CASES.values()))
        combined = set()
        for case in RGW_SYNC_POLICY_CASES:
            selected = self.selection(target, option, case)
            self.assertFalse(combined & selected, case)
            combined |= selected
        self.assertEqual(combined, aggregate)
        command = self.command(target, option, "policy")
        self.assertEqual(command[command.index("-run") + 1],
                         r"^Test(MultiClusterRGWSelectivePolicy|(HostNetwork)?MultiClusterRGWOwnedSyncPolicy)$")
        self.assertIn("-timeout=60m", command)

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
        for group in ("policy", *RGW_SYNC_POLICY_CASES, "account"):
            command = self.command("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP", group)
            self.assertNotIn("/", command[command.index("-run") + 1])

    def test_rgw_sync_shards_keep_fixture_image_contract_and_ci_budget(self):
        for group in ("policy", *RGW_SYNC_GROUPS):
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
                               ("scenario-rgw-sync-fixtures", "SCENARIO_RGW_SYNC_GROUP"),
                               ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE"),
                               ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE"),
                               ("scenario-rbd-receivers", "SCENARIO_RBD_RECEIVERS_CASE")]:
            for value in ("unknown", "", "bridge/scope-0"):
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
                ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE", CASES),
                ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", CEPHFS_FIXTURE_CASES),
                ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", TOPOLOGY_EXTENSION_CASES)]:
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
                ("scenario-rgw-sync-fixtures", "group", RGW_SYNC_GROUPS, "SCENARIO_RGW_SYNC_GROUP"),
                ("scenario-cephfs-fixtures", "case", CEPHFS_FIXTURE_CASES, "SCENARIO_CEPHFS_FIXTURE_CASE"),
                ("scenario-topology-extensions", "case", TOPOLOGY_EXTENSION_CASES, "SCENARIO_TOPOLOGY_EXTENSION_CASE"),
                ("scenario-rbd-receivers", "case", receiver_checker.NETWORKS, "SCENARIO_RBD_RECEIVERS_CASE")]:
            block = re.search(r"^  " + job + r":\n.*?(?=^  \w[\w-]*:\n|\Z)",
                              workflow, re.M | re.S).group()
            actual = re.search(r"^        " + key + r": \[(.*?)\]$", block, re.M)
            self.assertEqual(tuple(x.strip() for x in actual.group(1).split(",")), tuple(values))
            self.assertIn("fail-fast: false", block)
            self.assertIn(option + "='${{ matrix." + key + " }}'", block)
            self.assertIn("set -o pipefail", block)
            self.assertIn("phase: snapshot", block)
            self.assertIn("phase: check", block)
            if job in ("scenario-cephfs-fixtures", "scenario-topology-extensions"):
                self.assertIn("timeout-minutes: 50", block)
                timeout = "CLIENT_FIXTURES_TIMEOUT" if job == "scenario-cephfs-fixtures" else "TOPOLOGY_EXTENSIONS_TIMEOUT"
                self.assertIn(timeout + "=40m", block)
                for artifact in ("runtime-cleanup-", "scenario-images-", "scenario-"):
                    self.assertRegex(block, re.escape(artifact) + r"[^\n]*\$\{\{ matrix\.case \}\}")
                self.assertIn("uses: actions/upload-artifact@v7", block)
                self.assertIn("if: always()", block)
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
            if job == "scenario-rbd-receivers":
                self.assertIn("name: rbd receivers / ${{ matrix.case }}", block)
                self.assertIn("timeout-minutes: 100", block)
                self.assertIn("check_scenario_receivers.py artifacts/scenario/test.log --network '${{ matrix.case }}'", block)
                self.assertIn("> artifacts/scenario/coverage.json", block)
                self.assertEqual(block.count("artifact_name: runtime-cleanup-scenario-rbd-receivers-${{ matrix.case }}"), 2)
                self.assertIn("artifact_name: scenario-images-scenario-rbd-receivers-${{ matrix.case }}", block)
                self.assertIn("name: scenario-rbd-receivers-${{ matrix.case }}", block)
                self.assertIn("uses: actions/upload-artifact@v7", block)
                self.assertIn("if: always()", block)
                self.assertLess(block.index("| tee artifacts/scenario/test.log"),
                                block.index("check_scenario_receivers.py artifacts/scenario/test.log"))
                self.assertLess(block.index("check_scenario_receivers.py artifacts/scenario/test.log"),
                                block.index("phase: check"))

    def test_bridge_sdk_recovery_runs_only_once_in_network_interruption_case(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        block = re.search(r"^  scenario-topology-extensions:\n.*?(?=^  \w[\w-]*:\n|\Z)",
                          workflow, re.M | re.S).group()
        sdk_steps = re.findall(r"^      - name: [^\n]*\n(?:(?!^      - ).)*?internal/dockerbridge[^\n]*",
                               block, re.M | re.S)
        self.assertEqual(len(sdk_steps), 1)
        self.assertIn("if: ${{ matrix.case == 'network-interruption' }}", sdk_steps[0])
        self.assertIn("^TestRecoverableBridge(EndpointIdentity|PublishedPort)$", sdk_steps[0])
        self.assertEqual(workflow.count("./internal/dockerbridge"), 1)


if __name__ == "__main__":
    unittest.main()
