"""Test the role image pinning of a run without Docker or a registry."""

import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import role_image_pins as campaign
import run_image_matrix


class Registry:
    """Answers imagetools inspect with a digest per reference."""

    def __init__(self, broken=()):
        self.broken = set(broken)
        # stderr of successive failing inspections before one succeeds.
        self.transport = []
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        self.test.assertEqual(command[:4], ["docker", "buildx", "imagetools", "inspect"])
        reference = command[4]
        if reference in self.broken:
            return subprocess.CompletedProcess(command, 1, "", "ERROR: " + reference + ": not found")
        if self.transport:
            return subprocess.CompletedProcess(command, 1, "", self.transport.pop(0))
        digest = "sha256:" + format(abs(hash(reference)) % 16, "x") * 64
        return subprocess.CompletedProcess(command, 0, json.dumps({"digest": digest, "mediaType": "x"}), "")


class PinTests(unittest.TestCase):
    def registry(self, broken=()):
        registry = Registry(broken)
        registry.test = self
        return registry

    def test_every_role_tag_of_the_release_is_pinned_to_a_digest(self):
        registry = self.registry()
        digests = campaign.pin("19.2.5", registry)
        # The optional roles of the official variant are pinned too.
        self.assertEqual(list(digests), list(run_image_matrix.ROLE_ENV) + ["mgr-modules"])
        self.assertEqual([call[4] for call in registry.calls],
                         list(run_image_matrix.image_references("official", "roles", "19.2.5", tuple(digests)).values()))
        # The pins are what the image preparation accepts.
        text = ",".join(role + "=" + digest for role, digest in digests.items())
        self.assertEqual(run_image_matrix.parse_digests(text), digests)

    def test_an_unresolvable_or_malformed_tag_fails_the_pinning(self):
        reference = run_image_matrix.image_references("official", "roles", "20.2.4")["essential"]
        with self.assertRaisesRegex(RuntimeError, "cannot resolve"):
            campaign.pin("20.2.4", self.registry([reference]))
        for stdout in ("{}", "[]", "not json", json.dumps({"digest": "sha256:short"})):
            with self.subTest(stdout=stdout), self.assertRaisesRegex(RuntimeError, "no sha256 manifest digest"):
                campaign.resolve("r:t", lambda command, **kwargs: subprocess.CompletedProcess(command, 0, stdout, ""))

    def test_a_registry_transport_failure_is_retried_and_a_refusal_is_not(self):
        timeout = 'ERROR: failed to do request: Head "https://ghcr.io/v2/x/manifests/t": net/http: TLS handshake timeout'
        registry, sleeps = self.registry(), []
        registry.transport = [timeout, timeout]
        self.assertRegex(campaign.resolve("r:t", registry, sleeps.append), r"^sha256:[0-9a-f]{64}$")
        self.assertEqual((len(registry.calls), sleeps), (3, list(campaign.registry_pull.DELAYS)))
        registry, sleeps = self.registry(), []
        registry.transport = [timeout] * 5
        with self.assertRaisesRegex(RuntimeError, r"cannot resolve r:t after 3 attempt\(s\)"):
            campaign.resolve("r:t", registry, sleeps.append)
        self.assertEqual(len(registry.calls), 3)
        registry, sleeps = self.registry(["r:t"]), []
        with self.assertRaisesRegex(RuntimeError, r"cannot resolve r:t after 1 attempt"):
            campaign.resolve("r:t", registry, sleeps.append)
        self.assertEqual((len(registry.calls), sleeps), (1, []))

    def test_a_callers_pins_are_kept_and_a_plan_without_ceph_pins_nothing(self):
        refuse = mock.Mock(side_effect=AssertionError("no tag is resolved"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            given = "essential=sha256:" + "a" * 64 + ",rgw=sha256:" + "b" * 64
            docker_only, ceph = root / "docker.json", root / "ceph.json"
            docker_only.write_text(json.dumps({"include": [{"id": "docker-a", "image_roles": ""}]}))
            ceph.write_text(json.dumps({"include": [{"id": "docker-a", "image_roles": ""},
                                                    {"id": "short-b", "image_roles": "essential,client"}]}))
            for name, arguments, expected in (("given", ["--digests", given, "--plan", str(ceph)], given),
                                              ("docker", ["--plan", str(docker_only)], "")):
                output = root / name
                with mock.patch.object(campaign.subprocess, "run", refuse), mock.patch("sys.stdout", new=io.StringIO()):
                    self.assertEqual(campaign.main(["pin", "--release", "21.2.0", *arguments,
                                                    "--github-output", str(output)]), 0)
                self.assertEqual(output.read_text(), "release=21.2.0\ndigests=" + expected + "\n")
            output = root / "resolved"
            with mock.patch.object(campaign.subprocess, "run", self.registry()), mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(campaign.main(["pin", "--release", "21.2.0", "--plan", str(ceph),
                                                "--github-output", str(output)]), 0)
            self.assertEqual(sorted(run_image_matrix.parse_digests(output.read_text().splitlines()[1].split("=", 1)[1])),
                             sorted(run_image_matrix.published_roles("official")))
            # Invalid pins and an unreadable plan fail before any output.
            for arguments in (["--digests", "essential=sha256:short"], ["--plan", str(root / "missing.json")],
                              ["--plan", str(root / "given")]):
                output = root / "refused"
                with mock.patch.object(campaign.subprocess, "run", refuse), mock.patch("sys.stderr", new=io.StringIO()):
                    self.assertEqual(campaign.main(["pin", "--release", "21.2.0", *arguments,
                                                    "--github-output", str(output)]), 1)
                self.assertFalse(output.exists())

    def test_main_writes_release_and_digests_to_the_step_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with mock.patch.object(campaign.subprocess, "run", self.registry()), \
                    mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(campaign.main(["pin", "--release", "20.2.4", "--github-output", str(output)]), 0)
            lines = output.read_text().splitlines()
            self.assertEqual(lines[0], "release=20.2.4")
            self.assertEqual(sorted(run_image_matrix.parse_digests(lines[1].split("=", 1)[1])),
                             sorted(run_image_matrix.published_roles("official")))
            with mock.patch.object(campaign.subprocess, "run", self.registry(broken=["x"])), \
                    mock.patch.object(campaign, "pin", side_effect=RuntimeError("boom")), \
                    mock.patch("sys.stderr", new=io.StringIO()):
                self.assertEqual(campaign.main(["pin", "--release", "20.2.4"]), 1)


if __name__ == "__main__":
    unittest.main()
