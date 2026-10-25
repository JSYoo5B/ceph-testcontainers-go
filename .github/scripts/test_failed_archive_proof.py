"""Synthetic proof-auditor controls, not native execution/upload evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import failed_archive_proof as proof
import scenario_history


class FailedArchiveProof(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.scenario = self.base / "scenario"
        self.images = self.base / "images"
        self.cleanup = self.base / "cleanup"
        self.root = self.base / "source"
        self.root.mkdir()
        (self.root / "go.mod").write_text("module fixture\n")
        self.revision, self.image = "a" * 40, "sha256:" + "d" * 64
        profile = {"id": "optional-failure_archive-control", "category": "optional", "batch": "failure_archive", "bug_compatibility": []}
        binary = {"sha256": "b" * 64, "build_key": {"goos": "linux", "goarch": "amd64", "tags": "integration,ci_optional,ci_batch_failure_archive"},
                  "tests": [proof.TEST], "sources": {"go.mod": proof.digest((self.root / "go.mod").read_bytes())}}
        self.put(self.scenario / "compile/compile-report.json", {"passed": True, "profile": profile, "binary": binary})
        completion = {"observed_runs": [proof.TEST, *(proof.TEST + "/" + m for m in proof.MODES)], "observed_passes": [], "skipped_tests": [],
                      "failed_tests": [proof.TEST, *(proof.TEST + "/" + m for m in proof.MODES), "package failure"]}
        self.put(self.scenario / "native/report.json", {"passed": False, "exit_code": 1, "classification": "native-test-failure", "profile": profile,
                                                       "binary": binary, "compiled_tests": [proof.TEST], "completion": completion})
        history = {"schema": scenario_history.SCHEMA, "execution_key": {"repository": "example/repo", "run_id": "1234", "run_attempt": "1", "profile_id": profile["id"]},
                   "source": {"checkout_commit": self.revision}, "outcome": {"status": "failed", "failed_phases": ["native"]},
                   "comparability": {"status": "complete"}, "build": {"native_binary": "matches_compile", "binary_sha256": binary["sha256"]},
                   "phases": [{"phase": p, "status": "failed" if p == "native" else "passed"} for p in scenario_history.PHASES],
                   "runtime": {"platform": "linux/amd64"}}
        history["row_sha256"] = scenario_history.row_hash(history)
        self.put(self.scenario / "history-row.json", history)
        self.put(self.images / "summary.json", {"preparation_passed": True, "release": "20.2.4", "platform": "linux/amd64", "images": {"essential": {"image_id": self.image}}})
        self.put(self.cleanup / "before.json", {"passed": True, "engine": {"id": "engine"}})
        self.put(self.cleanup / "after.json", {"passed": True, "engine": {"id": "engine"}, "new_resources": {"containers": [], "networks": [], "volumes": []}})
        ready = []
        for mode in proof.MODES:
            archive = self.archive(mode)
            d = {"complete": True, "artifacts": [{"kind": "container-inspect", "data": json.dumps({"id": "osd-" + mode, "image_id": self.image, "state": {"started_at": "2026-10-25T01:00:00Z"}})},
                                                {"kind": "ceph", "name": "status", "data": json.dumps({"fsid": "fsid-" + mode})}]}
            self.put(archive / "diagnostics.json", d)
            self.put(archive / "crash-metadata.json", {"complete": True, "containers": [{"image_id": self.image}]})
            files = [{"name": p.name, "bytes": p.stat().st_size} for p in archive.iterdir()]
            if mode == "verification":
                self.put(archive / "verification-1-absent-crash-control.json", {"outcome": "unexpected", "crashes": {"outcome": "unexpected", "expectations": [{"name": "required-but-absent", "satisfied": False}]}})
                p = archive / "verification-1-absent-crash-control.json"
                files.append({"name": p.name, "bytes": p.stat().st_size})
            receipt = {"daemon": "osd.0", "name": "debug_osd", "generation": "osd-" + mode + "@2026-10-25T01:00:00Z", "prior": "0/5", "applied": "1/1", "state": "applied-verified"}
            final = dict(receipt, state="restored-verified", restored="0/5")
            manifest = {"test": proof.TEST + "/" + mode, "scenario": "failure-archive-proof/" + mode, "phase": "before-cleanup", "final": True,
                        "archive": {"retained": True, "cluster_closed": False, "complete": True},
                        "execution": {"id": "execution-" + mode, "run_id": "1234", "run_attempt": "1", "platform": "linux/amd64"},
                        "missing_provenance": ["source revision", "bug compatibility selection"],
                        "trigger": {"test_failed": mode == "assertion", "check_failed": mode == "verification", "cleanup_failed": False, "cleanup_reported_failure": False},
                        "termination": {"name": "terminate"}, "cleanups": [{"name": "restore controlled debug window"}], "files": files,
                        "debug_windows": [{"sequence": 1, "archive": {"phase": "before-cleanup", "present": True, "receipt": receipt},
                                           "final": {"phase": "after-cleanup", "present": True, "receipt": final}}]}
            self.put(archive / "manifest.json", manifest)
            ready.append("FAILURE_ARCHIVE_PROOF_READY " + json.dumps({"mode": mode, "fsid": "fsid-" + mode, "osd_container": "osd-" + mode, "osds": [0], "ceph_version": "20.2.4"}))
        (self.scenario / "native/native.log").write_text("\n".join(ready) + "\nCONTROLLED_ARCHIVE_ASSERTION: deliberately wrong count\nFAIL\n")
        self.inventory()

    def put(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def change(self, path, edit):
        obj = json.loads(path.read_text())
        edit(obj)
        if path.name == "history-row.json":
            obj["row_sha256"] = scenario_history.row_hash(obj)
        self.put(path, obj)

    def archive(self, mode):
        return self.scenario / "native/test-artifacts" / mode / "ceph-archive"

    def inventory(self):
        root = self.scenario / "native/test-artifacts"
        self.change(self.scenario / "native/report.json", lambda n: n.update(test_artifacts={"files": [{"path": p.relative_to(root).as_posix(), "bytes": p.stat().st_size} for p in root.rglob("*") if p.is_file()]}))

    def verify(self, **overrides):
        args = dict(scenario=self.scenario, images=self.images, cleanup=self.cleanup, root=self.root, revision=self.revision, run_id="1234", attempt="1")
        return proof.verify(**(args | overrides))

    def reject(self):
        with self.assertRaises((proof.ProofError, KeyError, ValueError)):
            self.verify()

    def test_synthetic_positive_preserves_failed_native_result(self):
        result = self.verify()
        self.assertEqual(result["archive_verification"], "verified")
        self.assertEqual(result["native_outcome"], "failed")
        self.assertFalse(result["workflow_pass_claimed"])
        self.assertEqual(len(result["archives"]), 2)
        self.assertEqual(result["source_files_verified"], 1)

    def test_wrong_ci_identity(self):
        for kwargs in [{"run_id": "999"}, {"attempt": "2"}, {"revision": "c" * 40}]:
            with self.subTest(kwargs), self.assertRaises(proof.ProofError):
                self.verify(**kwargs)

    def test_stale_source(self):
        (self.root / "go.mod").write_text("module changed\n")
        self.reject()

    def test_binary_mismatch(self):
        self.change(self.scenario / "native/report.json", lambda d: d["binary"].update(sha256="c" * 64))
        self.reject()

    def test_different_build_key(self):
        self.change(self.scenario / "native/report.json", lambda d: d["binary"]["build_key"].update(goarch="arm64"))
        self.reject()

    def test_native_pass_is_not_failure_archive_acceptance(self):
        self.change(self.scenario / "native/report.json", lambda d: d.update(passed=True, exit_code=0))
        self.reject()

    def test_wrong_failure_class(self):
        self.change(self.scenario / "native/report.json", lambda d: d.update(classification="process-or-build-failure"))
        self.reject()

    def test_unreached_or_skipped_case(self):
        self.change(self.scenario / "native/report.json", lambda d: d["completion"].update(skipped_tests=[proof.TEST + "/verification"]))
        self.reject()

    def test_missing_original_failure_marker(self):
        p = self.scenario / "native/native.log"
        p.write_text(p.read_text().replace("CONTROLLED_ARCHIVE_ASSERTION:", "other:"))
        self.reject()

    def test_cleanup_remnants(self):
        self.change(self.cleanup / "after.json", lambda d: d["new_resources"].update(volumes=["remaining-volume"]))
        self.reject()

    def test_another_engine(self):
        self.change(self.cleanup / "after.json", lambda d: d.update(engine={"id": "other"}))
        self.reject()

    def test_unconfirmed_workflow_phase(self):
        self.change(self.scenario / "history-row.json", lambda d: d["phases"][0].update(status="missing"))
        self.reject()

    def test_missing_workflow_phase(self):
        self.change(self.scenario / "history-row.json", lambda d: d.update(phases=[]))
        self.reject()

    def test_history_binary_mismatch(self):
        self.change(self.scenario / "history-row.json", lambda d: d["build"].update(binary_sha256="e" * 64))
        self.reject()

    def test_missing_archive_file(self):
        (self.archive("assertion") / "diagnostics.json").unlink()
        self.reject()

    def test_changed_downloaded_file(self):
        p = self.archive("assertion") / "diagnostics.json"
        p.write_bytes(p.read_bytes() + b" ")
        self.reject()

    def test_nonfinal_archive(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d.update(final=False))
        self.inventory()
        self.reject()

    def test_cleanup_failure_visible(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d["cleanups"][0].update(error="restore failed"))
        self.inventory()
        self.reject()

    def test_trigger_must_retain_assertion_failure(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d["trigger"].update(test_failed=False))
        self.inventory()
        self.reject()

    def test_missing_source_not_silently_filled_by_workflow_sha(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d.update(missing_provenance=[]))
        self.inventory()
        self.reject()

    def test_aliased_debug_snapshot(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d["debug_windows"][0].update(archive=copy.deepcopy(d["debug_windows"][0]["final"])))
        self.inventory()
        self.reject()

    def test_debug_generation_not_bound_to_native_osd(self):
        def change(d):
            for phase in ("archive", "final"):
                d["debug_windows"][0][phase]["receipt"]["generation"] = "other-generation"
        self.change(self.archive("assertion") / "manifest.json", change)
        self.inventory()
        self.reject()

    def test_restore_cannot_claim_a_different_prior_value(self):
        self.change(self.archive("assertion") / "manifest.json", lambda d: d["debug_windows"][0]["final"]["receipt"].update(restored="1/1"))
        self.inventory()
        self.reject()

    def test_image_identity_mismatch(self):
        self.change(self.images / "summary.json", lambda d: d["images"]["essential"].update(image_id="sha256:" + "e" * 64))
        self.reject()

    def test_wrong_native_version(self):
        p = self.scenario / "native/native.log"
        p.write_text(p.read_text().replace('"ceph_version": "20.2.4"', '"ceph_version": "19.2.5"'))
        self.reject()

    def test_native_fsid_mismatch(self):
        p = self.scenario / "native/native.log"
        p.write_text(p.read_text().replace('"fsid": "fsid-assertion"', '"fsid": "another-cluster"'))
        self.reject()

    def test_forged_history_hash(self):
        p = self.scenario / "history-row.json"
        d = json.loads(p.read_text()); d["source"]["checkout_commit"] = "e" * 40
        self.put(p, d)
        self.reject()


if __name__ == "__main__":
    unittest.main()
