"""Verify actual compiled scenario selection without starting Docker."""

import os
import importlib.util
from collections import Counter
from itertools import product
from pathlib import Path
import re
import shlex
import subprocess
import unittest

import check_scenario_receivers as receiver_checker
import check_scenario_quiescence as quiescence_checker
import check_scenario_recovery as recovery_checker
import report_test_failures as failure_reporter
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
CEPHFS_CI_CASES = ("peer-drain", "directory-release", *quiescence_checker.CASES,
                   *recovery_checker.CASES, "directory-intent")
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
RBD_FIXTURE_CASES = {
    "client-features": "TestRBDClientFeatures",
    "snapshot-schedule": "TestRBDAutomaticSnapshotSchedule",
    "mirror-scope": "TestMultiClusterRBDMirrorScopeAndNamespaces",
    "failback": "TestMultiClusterRBDFailback",
    "split-brain": "TestMultiClusterRBDSplitBrainResync",
    "host-lifecycle": "TestHostNetworkRBDLifecycle",
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
# CI bundles only whole independent parents. Exact per-parent Make cases and
# the local all selectors above remain available for diagnosis.
CEPHFS_CI_FIXTURE_CASES = {
    "data-layout": tuple(CEPHFS_FIXTURE_CASES[case] for case in
                         ("data-pools", "ec-data-pool", "host-filesystem")),
    **{case: (CEPHFS_FIXTURE_CASES[case],) for case in
       ("clone-cancellation", "quiesce", "authorization", "pins", "retained-snapshot")},
}
RBD_CI_FIXTURE_CASES = {
    "client-setup": tuple(RBD_FIXTURE_CASES[case] for case in
                          ("client-features", "host-lifecycle")),
    **{case: (RBD_FIXTURE_CASES[case],) for case in
       ("snapshot-schedule", "mirror-scope", "failback", "split-brain")},
}
TOPOLOGY_EXTENSION_CI_CASES = {
    "network-recovery": tuple(TOPOLOGY_EXTENSION_CASES[case] for case in
                              ("network-interruption", "five-monitors")),
    "rbd-daemons": tuple(TOPOLOGY_EXTENSION_CASES[case] for case in
                         ("rbd-mirror-bridge", "rbd-mirror-host", "rbd-peer-network")),
    **{case: (TOPOLOGY_EXTENSION_CASES[case],) for case in
       ("cephfs-mirror-bridge", "cephfs-mirror-host", "rgw-initial-bridge", "rgw-initial-host",
        "rgw-removal-bridge", "rgw-removal-host", "rgw-peer-network")},
}
CI_GROUP_TARGETS = (
    ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", CEPHFS_CI_FIXTURE_CASES),
    ("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE", RBD_CI_FIXTURE_CASES),
    ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", TOPOLOGY_EXTENSION_CI_CASES),
)
LIFECYCLE_BUNDLES = {
    "scenario-empty-bootstrap": {
        "scenario-storage-bootstrap": ("TestNoInitialOSDTopology", "80m"),
        "scenario-manager-bootstrap": ("TestNoInitialManagerTopology", "80m"),
        "scenario-mds-bootstrap": ("TestNoInitialMDSTopology", "50m"),
    },
    "scenario-mds-replacements": {
        "scenario-mds-replacement": ("TestStoppedMDSRetirementTopology", "50m"),
        "scenario-last-mds-replacement": ("TestLastMDSReplacementTopology", "50m"),
    },
}
PARENT_CASE_TARGETS = (
    ("scenario-multicluster-topology", "SCENARIO_MULTICLUSTER_GROUP", RGW_TOPOLOGY_CASES),
    ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", CEPHFS_FIXTURE_CASES),
    ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", TOPOLOGY_EXTENSION_CASES),
    ("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE", RBD_FIXTURE_CASES),
)


class ScenarioShardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        def compiled(tags, package="./internal/integration"):
            result = subprocess.run(
                ["go", "test", "-mod=readonly", "-tags=" + tags,
                 "-list", "^Test", package], cwd=ROOT,
                env=os.environ | {"CGO_ENABLED": "0"}, capture_output=True,
                text=True, timeout=180, check=True)
            names = set(re.findall(r"^Test\w+$", result.stdout, re.M))
            if not names:
                raise AssertionError("Go did not list compiled scenario parents")
            return names
        cls.compiled = compiled("integration,auth,features,topology,hostnetwork,multicluster")
        cls.default_compiled = compiled("integration")
        cls.bridge_compiled = compiled("integration", "./internal/dockerbridge")

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

    def test_related_ci_groups_preserve_exact_whole_parent_partitions(self):
        for target, option, cases in CI_GROUP_TARGETS:
            with self.subTest(target=target):
                self.assert_partition(target, option, cases,
                                      tuple(len(parents) for parents in cases.values()))
            for case, parents in cases.items():
                with self.subTest(target=target, case=case):
                    self.assertEqual(self.selection(target, option, case), set(parents))
                    command = self.command(target, option, case)
                    expression = command[command.index("-run") + 1]
                    self.assertTrue(expression.startswith("^") and expression.endswith("$"))
                    self.assertNotIn("/", expression)  # retain all children and evidence phases
                    for parent in parents:
                        self.assertRegex(parent, expression)
                        self.assertNotRegex(parent + "Other", expression)

    def test_lifecycle_bundles_keep_manual_parents_flags_and_original_budgets(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        for target, manual_targets in LIFECYCLE_BUNDLES.items():
            with self.subTest(target=target):
                expected = {parent for parent, _ in manual_targets.values()}
                self.assertEqual(self.selection(target), expected)
                command = self.command(target)
                process_budget = "80m" if target == "scenario-empty-bootstrap" else "50m"
                job_budget = "90" if target == "scenario-empty-bootstrap" else "60"
                self.assertIn("-timeout=" + process_budget, command)
                expression = command[command.index("-run") + 1]
                self.assertTrue(expression.startswith("^") and expression.endswith("$"))
                self.assertNotIn("/", expression)
                for flag in ("-tags=integration,topology", "-count=1", "-mod=readonly",
                             "-v", "CGO_ENABLED=0"):
                    self.assertIn(flag, command)
                self.assertNotIn("-failfast", command)  # attempt later independent parents after a failure
                block = re.search(r"^  " + target + r":\n.*?(?=^  \w[\w-]*:\n|\Z)",
                                  workflow, re.M | re.S).group()
                self.assertIn("timeout-minutes: " + job_budget, block)
                self.assertRegex(block, r"(?m)^\s+make " + target +
                                 r" 2>&1 \| tee artifacts/scenario/test\.log$")
                self.assertIn("set -o pipefail", block)
                self.assertEqual(block.count("artifact_name: runtime-cleanup-" + target), 2)
                self.assertIn("artifact_name: scenario-images-" + target, block)
                self.assertIn("--profile " + target, block)
                self.assertIsNotNone(failure_reporter.PROFILE.fullmatch(target))
                self.assertIn("uses: actions/upload-artifact@v7", block)
                self.assertIn("name: " + target + "\n", block)
                self.assertIn("if: always()", block)
                self.assertLess(block.index("phase: snapshot"), block.index("make " + target))
                self.assertLess(block.index("make " + target), block.index("phase: check"))
                for manual, (parent, original_budget) in manual_targets.items():
                    self.assertEqual(self.selection(manual), {parent})
                    original = self.command(manual)
                    self.assertEqual(original[original.index("-run") + 1], "^" + parent + "$")
                    self.assertIn("-timeout=" + original_budget, original)
                    self.assertIn("-failfast", original)
                    self.assertNotRegex(parent + "Other", expression)
                    # Keep every operational flag; a union must still attempt
                    # later fresh parents when an earlier parent fails.
                    def normalize(tokens):
                        index = tokens.index("-run")
                        return [token for i, token in enumerate(tokens)
                                if i != index + 1 and not token.startswith("-timeout=")
                                and token != "-failfast"]
                    self.assertEqual(normalize(command), normalize(original))
                    self.assertNotRegex(workflow, r"(?m)^  " + manual + r":$")

    def test_rbd_cases_preserve_six_whole_native_fixtures_and_original_budget(self):
        for case in RBD_FIXTURE_CASES:
            command = self.command("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE", case)
            for flag in ("-tags=integration,auth,features,topology,hostnetwork,multicluster",
                         "-timeout=120m", "-count=1", "-mod=readonly", "CGO_ENABLED=0"):
                self.assertIn(flag, command)
            self.assertEqual(command[command.index("CEPH_TEST_RBD_CLIENT_IMAGE") - 1], "-u")

    def test_legacy_multicluster_rgw_aggregate_preserves_all_six_cases(self):
        aggregate = self.selection("scenario-multicluster-topology",
                                   "SCENARIO_MULTICLUSTER_GROUP", "rgw")
        self.assertEqual(aggregate, set(RGW_TOPOLOGY_CASES.values()))

    def test_new_local_all_defaults_preserve_original_process_budgets(self):
        for target, option, timeout in (
                ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", "120m"),
                ("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE", "120m"),
                ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", "90m"),
                ("scenario-rbd-receivers", "SCENARIO_RBD_RECEIVERS_CASE", "90m"),
                ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE", "90m")):
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

    def test_primary_jobs_cover_every_compiled_parent_with_exact_network_and_recovery_partitions(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        jobs = dict(image_helpers.blocks(workflow.split("\njobs:\n", 1)[1], r"^  ([\w-]+):$"))
        options = {
            "scenario-multicluster-topology": "SCENARIO_MULTICLUSTER_GROUP",
            "scenario-cephfs-removal": "SCENARIO_CEPHFS_REMOVAL_CASE",
            "scenario-rgw-sync-fixtures": "SCENARIO_RGW_SYNC_GROUP",
            "scenario-cephfs-fixtures": "SCENARIO_CEPHFS_FIXTURE_CASE",
            "scenario-topology-extensions": "SCENARIO_TOPOLOGY_EXTENSION_CASE",
            "scenario-rbd-receivers": "SCENARIO_RBD_RECEIVERS_CASE",
            "scenario-rbd-fixtures": "SCENARIO_RBD_FIXTURE_CASE",
        }
        counts = Counter()
        receiver_jobs, quiescence_jobs, recovery_jobs = {}, {}, {}
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
                if target == "scenario-cephfs-removal" and value in quiescence_checker.CASES:
                    self.assertEqual(selected, {quiescence_checker.PARENT})
                    quiescence_jobs[value] = self.command(target, options[target], value)
                if target == "scenario-cephfs-removal" and value in recovery_checker.CASES:
                    self.assertEqual(selected, {recovery_checker.PARENT})
                    recovery_jobs[value] = self.command(target, options[target], value)
                counts.update(selected)
        self.assertTrue(OPTIONAL_NATIVE_SHUFFLE_PARENTS <= self.compiled)
        self.assertEqual(set(counts), self.compiled - OPTIONAL_NATIVE_SHUFFLE_PARENTS)
        self.assertEqual({parent: count for parent, count in counts.items() if count > 1},
                         {receiver_checker.PARENT: 2, quiescence_checker.PARENT: 4,
                          recovery_checker.PARENT: 4})
        self.assertEqual(sum(counts.values()), len(self.compiled - OPTIONAL_NATIVE_SHUFFLE_PARENTS) + 7)
        # The workflow totals include its two bridge SDK tests in addition to
        # the Ceph integration parents and repeated exact network leaves.
        sdk_commands = [shlex.split(image_helpers.step_run(step))
                        for block in jobs.values() for step in image_helpers.steps(block)
                        if "./internal/dockerbridge" in image_helpers.step_run(step)]
        self.assertEqual(len(sdk_commands), 1)
        sdk_expression = sdk_commands[0][sdk_commands[0].index("-run") + 1]
        sdk_parents = {parent for parent in self.bridge_compiled if re.search(sdk_expression, parent)}
        self.assertEqual(sdk_parents, {"TestRecoverableBridgeEndpointIdentity",
                                      "TestRecoverableBridgePublishedPort"})
        self.assertFalse(set(counts) & sdk_parents)
        total_counts = counts + Counter(sdk_parents)
        self.assertEqual(len(total_counts), 120)
        self.assertEqual(sum(total_counts.values()), 127)
        self.assertEqual(set(quiescence_jobs), set(quiescence_checker.CASES))
        for case, command in quiescence_jobs.items():
            self.assertEqual(command[command.index("-run") + 1], quiescence_checker.SELECTORS[case])
        self.assertEqual(set(recovery_jobs), set(recovery_checker.CASES))
        for case, command in recovery_jobs.items():
            self.assertEqual(command[command.index("-run") + 1], recovery_checker.SELECTORS[case])
        self.assertEqual(set(receiver_jobs), set(receiver_checker.NETWORKS))
        for network, command in receiver_jobs.items():
            self.assertEqual(command[command.index("-run") + 1], receiver_checker.SELECTORS[network])

    def test_quiescence_cases_preserve_four_exact_fresh_pair_leaves_and_legacy_aggregate(self):
        target, option = "scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE"
        self.assert_fresh_pair_partition(target, option, "process-quiescence", quiescence_checker)

    def test_recovery_cases_preserve_four_exact_fresh_pair_leaves_and_legacy_aggregate(self):
        target, option = "scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE"
        self.assert_fresh_pair_partition(target, option, "process-recovery", recovery_checker)

    def assert_fresh_pair_partition(self, target, option, legacy_case, checker):
        legacy = self.command(target, option, legacy_case)
        self.assertEqual(legacy[legacy.index("-run") + 1], "^" + checker.PARENT + "$")
        inventory = {checker.required_leaf(case) for case in checker.CASES}
        selected_leaves = set()
        for case, (network, kind) in checker.CASES.items():
            command = self.command(target, option, case)
            expression = command[command.index("-run") + 1]
            self.assertEqual(expression, checker.SELECTORS[case])
            self.assertEqual(self.selection(target, option, case), {checker.PARENT})
            components = expression.split("/")
            self.assertEqual(len(components), 3)  # no evidence-phase filter
            selected = {leaf for leaf in inventory if all(re.fullmatch(part, name)
                        for part, name in zip(components, leaf.split("/")))}
            self.assertEqual(selected, {checker.required_leaf(case)})
            self.assertFalse(selected_leaves & selected)
            selected_leaves |= selected
            for foreign in (network + "-other", "other-" + network, "", kind):
                self.assertNotRegex(foreign, components[1])
            for foreign in (kind + "-other", "other-" + kind, "", network):
                self.assertNotRegex(foreign, components[2])
            self.assertNotRegex(checker.PARENT + "Other", components[0])
            for flag in ("-tags=integration,topology,hostnetwork,multicluster", "-timeout=90m",
                         "-count=1", "-mod=readonly", "CGO_ENABLED=0"):
                self.assertIn(flag, command)
            ci_command = self.command(target, option, case, ("SCENARIO_CEPHFS_REMOVAL_TIMEOUT=60m",))
            self.assertIn("-timeout=60m", ci_command)
        self.assertEqual(selected_leaves, inventory)
        self.assertEqual(len(selected_leaves), 4)

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
                               ("scenario-rbd-receivers", "SCENARIO_RBD_RECEIVERS_CASE"),
                               ("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE")]:
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
                ("scenario-cephfs-removal", "SCENARIO_CEPHFS_REMOVAL_CASE", CEPHFS_CI_CASES),
                ("scenario-cephfs-fixtures", "SCENARIO_CEPHFS_FIXTURE_CASE", CEPHFS_FIXTURE_CASES | CEPHFS_CI_FIXTURE_CASES),
                ("scenario-rbd-fixtures", "SCENARIO_RBD_FIXTURE_CASE", RBD_FIXTURE_CASES | RBD_CI_FIXTURE_CASES),
                ("scenario-topology-extensions", "SCENARIO_TOPOLOGY_EXTENSION_CASE", TOPOLOGY_EXTENSION_CASES | TOPOLOGY_EXTENSION_CI_CASES)]:
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
                    expression = command[command.index("-run") + 1]
                    if target == "scenario-cephfs-removal" and value in quiescence_checker.CASES:
                        self.assertEqual(expression, quiescence_checker.SELECTORS[value])
                    elif target == "scenario-cephfs-removal" and value in recovery_checker.CASES:
                        self.assertEqual(expression, recovery_checker.SELECTORS[value])
                    else:
                        self.assertNotIn("/", expression)

    def test_workflow_selects_every_supported_shard(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        self.assertIn("python3 -m unittest discover -s .github/scripts -p test_scenario_recovery.py -v", workflow)
        for job, key, values, option in [
                ("scenario-multicluster-topology", "group", GROUPS, "SCENARIO_MULTICLUSTER_GROUP"),
                ("scenario-cephfs-removal", "case", CEPHFS_CI_CASES, "SCENARIO_CEPHFS_REMOVAL_CASE"),
                ("scenario-rgw-sync-fixtures", "group", RGW_SYNC_GROUPS, "SCENARIO_RGW_SYNC_GROUP"),
                ("scenario-cephfs-fixtures", "case", CEPHFS_CI_FIXTURE_CASES, "SCENARIO_CEPHFS_FIXTURE_CASE"),
                ("scenario-topology-extensions", "case", TOPOLOGY_EXTENSION_CI_CASES, "SCENARIO_TOPOLOGY_EXTENSION_CASE"),
                ("scenario-rbd-receivers", "case", receiver_checker.NETWORKS, "SCENARIO_RBD_RECEIVERS_CASE"),
                ("scenario-rbd-fixtures", "case", RBD_CI_FIXTURE_CASES, "SCENARIO_RBD_FIXTURE_CASE")]:
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
            if job == "scenario-cephfs-removal":
                self.assertIn("timeout-minutes: 70", block)
                self.assertIn("SCENARIO_CEPHFS_REMOVAL_TIMEOUT=60m", block)
                self.assertIn("if: ${{ startsWith(matrix.case, 'process-quiescence-') }}", block)
                self.assertIn("check_scenario_quiescence.py artifacts/scenario/test.log --case '${{ matrix.case }}'", block)
                self.assertIn("if: ${{ startsWith(matrix.case, 'process-recovery-') }}", block)
                self.assertIn("check_scenario_recovery.py artifacts/scenario/test.log --case '${{ matrix.case }}'", block)
                self.assertIn("> artifacts/scenario/coverage.json", block)
                self.assertEqual(block.count("artifact_name: runtime-cleanup-cephfs-removal-${{ matrix.case }}"), 2)
                self.assertIn("artifact_name: scenario-images-cephfs-removal-${{ matrix.case }}", block)
                self.assertIn("name: scenario-cephfs-removal-${{ matrix.case }}", block)
                self.assertLess(block.index("| tee artifacts/scenario/test.log"),
                                block.index("check_scenario_quiescence.py artifacts/scenario/test.log"))
                self.assertLess(block.index("check_scenario_quiescence.py artifacts/scenario/test.log"),
                                block.index("phase: check"))
                self.assertLess(block.index("| tee artifacts/scenario/test.log"),
                                block.index("check_scenario_recovery.py artifacts/scenario/test.log"))
                self.assertLess(block.index("check_scenario_recovery.py artifacts/scenario/test.log"),
                                block.index("phase: check"))
            if job == "scenario-rbd-fixtures":
                self.assertIn("name: rbd fixture / ${{ matrix.case }}", block)
                self.assertIn("timeout-minutes: 130", block)
                self.assertNotIn("CLIENT_FIXTURES_TIMEOUT=", block)
                self.assertEqual(block.count("artifact_name: runtime-cleanup-scenario-rbd-fixtures-${{ matrix.case }}"), 2)
                self.assertIn("artifact_name: scenario-images-scenario-rbd-fixtures-${{ matrix.case }}", block)
                self.assertIn("name: scenario-rbd-fixtures-${{ matrix.case }}", block)
                self.assertIn("uses: actions/upload-artifact@v7", block)
                self.assertIn("if: always()", block)
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

    def test_bridge_sdk_recovery_runs_only_once_in_network_recovery_bundle(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        block = re.search(r"^  scenario-topology-extensions:\n.*?(?=^  \w[\w-]*:\n|\Z)",
                          workflow, re.M | re.S).group()
        sdk_steps = re.findall(r"^      - name: [^\n]*\n(?:(?!^      - ).)*?internal/dockerbridge[^\n]*",
                               block, re.M | re.S)
        self.assertEqual(len(sdk_steps), 1)
        self.assertIn("if: ${{ matrix.case == 'network-recovery' }}", sdk_steps[0])
        self.assertIn("^TestRecoverableBridge(EndpointIdentity|PublishedPort)$", sdk_steps[0])
        self.assertEqual(workflow.count("./internal/dockerbridge"), 1)
        command = shlex.split(image_helpers.step_run(sdk_steps[0]))
        expression = command[command.index("-run") + 1]
        selected = {parent for parent in self.bridge_compiled if re.search(expression, parent)}
        self.assertEqual(selected, {"TestRecoverableBridgeEndpointIdentity",
                                    "TestRecoverableBridgePublishedPort"})
        for flag in ("-mod=readonly", "-tags=integration", "-count=1", "-v", "-timeout=5m"):
            self.assertIn(flag, command)

    def test_topology_parents_attempt_after_sdk_failure_only_with_prepared_resources(self):
        workflow = (ROOT / ".github/workflows/test.yml").read_text()
        block = dict(image_helpers.blocks(workflow.split("\njobs:\n", 1)[1],
                                          r"^  ([\w-]+):$"))["scenario-topology-extensions"]
        job_steps = image_helpers.steps(block)
        baseline = next(step for step in job_steps if "phase: snapshot" in step)
        prep = next(step for step in job_steps if "uses: " + image_helpers.PREP in step)
        sdk = next(step for step in job_steps if "./internal/dockerbridge" in image_helpers.step_run(step))
        native = next(step for step in job_steps if re.search(
            r"(?m)^\s*make scenario-topology-extensions(?:\s|$)", image_helpers.step_run(step)))
        cleanup = next(step for step in job_steps if "phase: check" in step)
        self.assertEqual(image_helpers.scalar(prep, "id"), "scenario_role_images")
        self.assertNotRegex(prep, r"(?m)^\s+(?:if|continue-on-error):")
        gate = image_helpers.scalar(native, "if")
        self.assertEqual(gate, "${{ !cancelled() && steps.runtime_cleanup_baseline.outcome == 'success'"
                              " && steps.scenario_role_images.outcome == 'success' }}")
        # A status-check function overrides Actions' implicit success() gate.
        # Resolve the checked-in conjunction against all relevant outcomes:
        # SDK failure must allow both fresh parents without making the job green.
        terms = gate.removeprefix("${{ ").removesuffix(" }}").split(" && ")
        outcomes = ("success", "failure", "skipped", "cancelled")
        for cancelled, before, images, bridge_sdk in product((False, True), outcomes, outcomes, outcomes):
            with self.subTest(cancelled=cancelled, baseline=before, prep=images, sdk=bridge_sdk):
                states = {"runtime_cleanup_baseline": before, "scenario_role_images": images,
                          "bridge_sdk": bridge_sdk}
                values = []
                for term in terms:
                    if term == "!cancelled()":
                        values.append(not cancelled)
                    else:
                        match = re.fullmatch(r"steps\.(\w+)\.outcome == 'success'", term)
                        self.assertIsNotNone(match, term)
                        values.append(states[match.group(1)] == "success")
                self.assertEqual(all(values), not cancelled and before == "success" and images == "success")
        for step in (sdk, native):
            self.assertNotRegex(step, r"(?m)^\s+continue-on-error:")
        self.assertNotRegex(block, r"(?m)^    continue-on-error:")
        self.assertEqual(image_helpers.scalar(sdk, "if"), "${{ matrix.case == 'network-recovery' }}")
        self.assertEqual(image_helpers.scalar(cleanup, "if"),
                         "${{ always() && steps.runtime_cleanup_baseline.outcome == 'success' }}")
        self.assertEqual(len([step for step in job_steps if "phase: snapshot" in step]), 1)
        self.assertEqual(len([step for step in job_steps if "phase: check" in step]), 1)
        self.assertEqual(image_helpers.scalar(baseline, "artifact_name"),
                         image_helpers.scalar(cleanup, "artifact_name"))
        self.assertLess(job_steps.index(prep), job_steps.index(sdk))
        self.assertLess(job_steps.index(sdk), job_steps.index(native))
        self.assertLess(job_steps.index(native), job_steps.index(cleanup))


if __name__ == "__main__":
    unittest.main()
