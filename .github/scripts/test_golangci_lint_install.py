"""The pinned golangci-lint installer and the workflows that use it; no network."""

import hashlib
import importlib.util
import io
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import unittest
import urllib.error


SPEC = importlib.util.spec_from_file_location("golangci_lint_install",
                                              Path(__file__).with_name("golangci_lint_install.py"))
lint = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(lint)

ROOT = Path(__file__).resolve().parents[2]
VERSION = "2.15.0"


def archive_bytes(version=VERSION, member=None, body=b"#!/bin/sh\necho v2.15.0\n"):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        info = tarfile.TarInfo(member or "golangci-lint-%s-linux-amd64/golangci-lint" % version)
        info.size = len(body)
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(body))
    return buffer.getvalue()


ARCHIVE = archive_bytes()
DIGEST = hashlib.sha256(ARCHIVE).hexdigest()


class Response(io.BytesIO):
    def __init__(self, body, length=None):
        super().__init__(body)
        self.headers = {} if length is None else {"Content-Length": str(length)}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Opener:
    """Answers each request with the next scripted outcome."""

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.urls = []

    def __call__(self, url, timeout):
        self.urls.append(url)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if isinstance(outcome, tuple):
            return Response(*outcome)
        return Response(outcome)


def http_error(code):
    return urllib.error.HTTPError("https://example.invalid", code, "status", {}, None)


def version_run(reported="v2.15.0", code=0):
    def run(args, **kwargs):
        return subprocess.CompletedProcess(args, code, stdout=reported + "\n", stderr="")
    return run


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cache = Path(self.tmp.name) / "cache"
        self.bin = Path(self.tmp.name) / "bin"
        self.sleeps = []
        self.logs = []

    def install(self, opener, run=None, version="v" + VERSION, digest=DIGEST):
        return lint.install(version, digest, self.cache, self.bin, opener=opener, sleep=self.sleeps.append,
                            run=run or version_run(), log=self.logs.append)

    def test_a_verified_cached_archive_makes_no_request(self):
        self.cache.mkdir()
        (self.cache / lint.asset_name(VERSION)).write_bytes(ARCHIVE)
        opener = Opener()
        self.assertFalse(self.install(opener))
        self.assertEqual(opener.urls, [])
        self.assertTrue((self.bin / "golangci-lint").stat().st_mode & 0o111)

    def test_the_issue_647_gateway_timeouts_are_retried_until_a_verified_download(self):
        opener = Opener(http_error(504), http_error(504), http_error(504), ARCHIVE)
        self.assertTrue(self.install(opener))
        self.assertEqual(opener.urls, ["https://github.com/golangci/golangci-lint/releases/download/v2.15.0/"
                                       "golangci-lint-2.15.0-linux-amd64.tar.gz"] * 4)
        self.assertEqual(self.sleeps, [10, 20, 40])
        self.assertEqual((self.cache / lint.asset_name(VERSION)).read_bytes(), ARCHIVE)

    def test_transport_failures_are_retried(self):
        opener = Opener(urllib.error.URLError("reset"), TimeoutError("read"), ConnectionResetError(), ARCHIVE)
        self.assertTrue(self.install(opener))
        self.assertEqual(len(opener.urls), 4)

    def test_attempts_are_bounded(self):
        opener = Opener(*[http_error(503)] * 5)
        with self.assertRaisesRegex(lint.InstallError, "no verified archive after 5 attempts"):
            self.install(opener)
        self.assertEqual(self.sleeps, list(lint.DELAYS))
        self.assertEqual(list(self.cache.iterdir()), [])

    def test_a_refusal_is_not_retried(self):
        for code in (403, 404):
            with self.subTest(code=code):
                opener = Opener(http_error(code))
                with self.assertRaisesRegex(lint.InstallError, "HTTP Error %d" % code):
                    self.install(opener)
                self.assertEqual(len(opener.urls), 1)

    def test_a_download_with_another_digest_is_never_kept(self):
        altered = archive_bytes(body=b"#!/bin/sh\necho other\n")
        opener = Opener(*[altered] * 5)
        with self.assertRaises(lint.InstallError):
            self.install(opener)
        self.assertEqual(list(self.cache.iterdir()), [])
        self.assertFalse((self.bin / "golangci-lint").exists())

    def test_a_complete_download_with_another_digest_is_a_wrong_pin(self):
        altered = archive_bytes(body=b"#!/bin/sh\necho other\n")
        opener = Opener((altered, len(altered)))
        with self.assertRaisesRegex(lint.InstallError, "want %s" % DIGEST):
            self.install(opener)
        self.assertEqual(len(opener.urls), 1)
        self.assertEqual(list(self.cache.iterdir()), [])

    def test_a_short_download_is_retried(self):
        opener = Opener((ARCHIVE[:10], len(ARCHIVE)), (ARCHIVE, len(ARCHIVE)))
        self.assertTrue(self.install(opener))
        self.assertEqual(len(opener.urls), 2)

    def test_a_cached_archive_with_another_digest_is_replaced(self):
        self.cache.mkdir()
        (self.cache / lint.asset_name(VERSION)).write_bytes(archive_bytes(body=b"other"))
        opener = Opener(ARCHIVE)
        self.assertTrue(self.install(opener))
        self.assertEqual((self.cache / lint.asset_name(VERSION)).read_bytes(), ARCHIVE)
        self.assertTrue(any("discarding cached" in line for line in self.logs))

    def test_the_binary_must_report_the_pinned_version(self):
        self.cache.mkdir()
        (self.cache / lint.asset_name(VERSION)).write_bytes(ARCHIVE)
        for run in (version_run("v2.14.0"), version_run("v2.15.0", code=3)):
            with self.subTest(run=run), self.assertRaisesRegex(lint.InstallError, "want 2.15.0"):
                self.install(Opener(), run=run)

    def test_an_archive_without_the_executable_is_refused(self):
        other = archive_bytes(member="golangci-lint-2.15.0-linux-amd64/README.md")
        self.cache.mkdir()
        (self.cache / lint.asset_name(VERSION)).write_bytes(other)
        with self.assertRaisesRegex(lint.InstallError, "has no"):
            self.install(Opener(), digest=hashlib.sha256(other).hexdigest())

    def test_inexact_pins_are_refused(self):
        for version, digest in (("latest", DIGEST), ("v2.15", DIGEST), ("v2.15.0", "ABC"), ("v2.15.0", DIGEST.upper())):
            with self.subTest(version=version, digest=digest), self.assertRaises(lint.InstallError):
                self.install(Opener(), version=version, digest=digest)


class WorkflowTests(unittest.TestCase):
    """Both callers install one pin through the action, and the lint step downloads nothing."""

    def pins(self, workflow):
        text = (ROOT / ".github/workflows" / workflow).read_text()
        return text, re.findall(r"uses: \./\.github/actions/golangci-lint\n\s+with:\n\s+version: (\S+)\n"
                                r"\s+sha256: ([0-9a-f]{64})\n", text)

    def test_callers_share_one_pin(self):
        code, code_pins = self.pins("code.yml")
        structural, structural_pins = self.pins("structural-collect.yml")
        self.assertEqual(len(code_pins), 1)
        self.assertEqual(code_pins, structural_pins)
        self.assertNotIn("install-only", structural)

    def test_the_lint_step_uses_the_installed_binary(self):
        code, _ = self.pins("code.yml")
        steps = re.findall(r"uses: golangci/golangci-lint-action@\S+\n\s+with:\n((?:\s{10}.*\n)+)", code)
        self.assertEqual(len(steps), 1)
        self.assertIn("install-mode: none", steps[0])
        self.assertNotIn("version:", steps[0])
        self.assertLess(code.index("uses: ./.github/actions/golangci-lint"),
                        code.index("uses: golangci/golangci-lint-action@"))

    def test_the_makefile_names_the_pinned_version(self):
        _, pins = self.pins("code.yml")
        self.assertIn("golangci-lint %s with" % pins[0][0], (ROOT / "Makefile").read_text())


if __name__ == "__main__":
    unittest.main()
