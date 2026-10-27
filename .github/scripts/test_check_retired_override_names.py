import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import check_retired_override_names as guard  # noqa: E402

# Built from parts so that this file does not hold a bare retired name in
# running text; the file is also in ALLOWED.
RETIRED = (
    "cluster." + "Override" + "Config(ctx, setting)",
    "c." + "Override" + "MessengerPolicy(ctx, policy)",
    "cluster." + "Override" + "OSDFlag(ctx, flag, true)",
    "cluster." + "Override" + "MGRModule(ctx, name, true)",
    "cluster." + "Override" + "FullRatios(ctx, ratios)",
    "cluster." + "Override" + "DebugLevel(ctx, setting)",
    "cluster." + "Temporary" + "Blocklist(ctx, addr, time.Minute)",
    "var h *ceph." + "ConfigOverride" + "Handle",
    "var h *ceph." + "MessengerPolicyOverride" + "Handle",
    "var h *ceph." + "OSDFlagOverride" + "Handle",
    "var h *ceph." + "MGRModuleOverride" + "Handle",
    "var h *ceph." + "DebugLevelOverride" + "Handle",
    "var h *ceph." + "FullRatios" + "Override",
    "var h *ceph." + "Blocklist" + "Override",
    "`(*Container)." + "BlocklistEntries`",
    "`(*Tenant)." + "BlocklistEntries`",
    "`(*Container)." + "FullRatios`",
    "ceph.Container." + "HealthDetails",
    "Tenant." + "Pools lists the pools",
    "Container." + "EssentialImage",
    "`(*ceph." + "Container).BlocklistEntries`",
    "`(*ceph." + "Tenant).FullRatios`",
    "(*ceph." + "Container).Status(ctx)",
    "`( *ceph." + "Container ).HealthDetails`",
    "`(* ceph." + "Tenant).Pools`",
    "(ceph." + "Container).OSDFlags",
    "[(*ceph." + "Container).Configuration](ceph.md#x)",
)
VALID = (
    "override.SetConfig(ctx, cluster, setting)",
    "override.AddBlocklist(ctx, cluster, address, time.Minute)",
    "*override.FullRatiosHandle",
    "faultConfigOverride is a registry kind",
    "func TestOverrideConfigRefusesTheBootstrapPolicy(t *testing.T)",
    "allowPoolDeleteHeld asks the registry",
    "BlocklistEntry and Observer.BlocklistEntries stay in ceph",
    "DebugLevelReceipter has Receipt",
    "`(*Observer).BlocklistEntries` and `(*Observer).FullRatios`",
    "ceph.Observer.HealthDetails",
    "observeCluster(s.Container).ManagerStatus(ctx)",
    "Container.Pool returns one pool and Pool.Status reports it",
    "ClusterFacts.NetworkName",
    "`(*ceph.Observer).BlocklistEntries`",
    "(*ceph.Tenant).Pool and (*ceph.Container).Pool",
    "(*ceph.Container).Start",
)


class RetiredOverrideNamesTest(unittest.TestCase):
    def test_every_retired_name_is_found(self):
        for text in RETIRED:
            with self.subTest(text=text):
                self.assertTrue(guard.scan_text("x.md", text), text)

    def test_current_names_and_lookalikes_pass(self):
        for text in VALID:
            with self.subTest(text=text):
                self.assertEqual(guard.scan_text("x.md", text), [])

    def test_historical_records_are_skipped_and_everything_else_is_scanned(self):
        with tempfile.TemporaryDirectory() as root:
            subprocess.check_call(["git", "init", "-q", root])
            paths = (
                "CHANGELOG.md",
                "docs/changes/146-x.md",
                "docs/guides/upgrading-x.md",
                "docs/design/api-naming/moves/146-x.json",
                "docs/guides/other.md",
                "docs/design/other.md",
                "ceph/a.go",
                "internal/integration/b_test.go",
                ".github/scripts/fixtures/c.json",
            )
            for path in paths:
                os.makedirs(os.path.dirname(os.path.join(root, path)), exist_ok=True)
                with open(os.path.join(root, path), "w") as handle:
                    handle.write("cluster." + "Override" + "Config(ctx, setting)\n")
            subprocess.check_call(["git", "-C", root, "add", "-A"])
            found = sorted({p[0] for p in guard.scan(root)})
            self.assertEqual(found, [".github/scripts/fixtures/c.json", "ceph/a.go", "docs/design/other.md",
                                     "docs/guides/other.md", "internal/integration/b_test.go"])

    def test_the_repository_is_clean(self):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.assertEqual(guard.scan(root), [])

    def test_allowed_paths_exist(self):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        for path in guard.ALLOWED:
            self.assertTrue(os.path.isfile(os.path.join(root, path)), path)

    def test_main_reports_a_problem_and_succeeds_on_a_clean_tree(self):
        with tempfile.TemporaryDirectory() as root:
            subprocess.check_call(["git", "init", "-q", root])
            with open(os.path.join(root, "a.md"), "w") as handle:
                handle.write("override.SetConfig\n")
            subprocess.check_call(["git", "-C", root, "add", "-A"])
            self.assertEqual(guard.main(["--root", root]), 0)
            with open(os.path.join(root, "a.md"), "a") as handle:
                handle.write("cluster." + "Override" + "OSDFlag(ctx, flag, true)\n")
            self.assertEqual(guard.main(["--root", root]), 1)


if __name__ == "__main__":
    unittest.main()
