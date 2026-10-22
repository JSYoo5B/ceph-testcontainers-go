"""Test campaign image pinning without Docker or a registry."""

import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import periodic_campaign as campaign
import run_image_matrix


class Registry:
    """Answers imagetools inspect with a digest per reference."""

    def __init__(self, broken=()):
        self.broken = set(broken)
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append(command)
        self.test.assertEqual(command[:4], ["docker", "buildx", "imagetools", "inspect"])
        reference = command[4]
        if reference in self.broken:
            return subprocess.CompletedProcess(command, 1, "", "not found")
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
        self.assertEqual(list(digests), list(run_image_matrix.ROLE_ENV))
        self.assertEqual([call[4] for call in registry.calls],
                         list(run_image_matrix.image_references("official", "roles", "19.2.5").values()))
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

    def test_main_writes_release_and_digests_to_the_step_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with mock.patch.object(campaign.subprocess, "run", self.registry()), \
                    mock.patch("sys.stdout", new=io.StringIO()):
                self.assertEqual(campaign.main(["pin", "--release", "20.2.4", "--github-output", str(output)]), 0)
            lines = output.read_text().splitlines()
            self.assertEqual(lines[0], "release=20.2.4")
            self.assertEqual(sorted(run_image_matrix.parse_digests(lines[1].split("=", 1)[1])),
                             sorted(run_image_matrix.ROLE_ENV))
            with mock.patch.object(campaign.subprocess, "run", self.registry(broken=["x"])), \
                    mock.patch.object(campaign, "pin", side_effect=RuntimeError("boom")), \
                    mock.patch("sys.stderr", new=io.StringIO()):
                self.assertEqual(campaign.main(["pin", "--release", "20.2.4"]), 1)


if __name__ == "__main__":
    unittest.main()
