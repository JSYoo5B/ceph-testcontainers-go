"""Registry transport classification and the bounded pull retry; no Docker."""

import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location("registry_pull", Path(__file__).with_name("registry_pull.py"))
registry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(registry)

# Daemon errors as CI jobs printed them (issue 381), one per failure family.
RYUK_TOKEN = ('create ceph network: allocate bridge subnet: reaper: new reaper: run container: Error response from '
              'daemon: Head "https://registry-1.docker.io/v2/testcontainers/ryuk/manifests/0.14.0": Get '
              '"https://auth.docker.io/token?account=githubactions&scope=repository%3Atestcontainers%2Fryuk%3Apull'
              '&service=registry.docker.io": net/http: TLS handshake timeout: failed to create network')
RYUK_MANIFEST = ('create ceph network: allocate bridge subnet: reaper: new reaper: run container: Error response from '
                 'daemon: Get "https://registry-1.docker.io/v2/testcontainers/ryuk/manifests/sha256:' + "7" * 64 +
                 '": net/http: TLS handshake timeout: failed to create network')
GHCR_EOF = ('Error response from daemon: Head "https://ghcr.io/v2/jsyoo5b/ceph-testcontainers-images/manifests/sha256:'
            + "8" * 64 + '": Get "https://ghcr.io/token?scope=repository%3Ajsyoo5b%3Apull&service=ghcr.io": EOF')
GHCR_DNS = ('Error response from daemon: Get "https://ghcr.io/v2/": dial tcp: lookup ghcr.io on 127.0.0.53:53: '
            'read udp 127.0.0.1:40000->127.0.0.53:53: i/o timeout')


class TransportTests(unittest.TestCase):
    def test_recorded_failures_name_reason_phase_and_endpoint_without_query(self):
        cases = {
            RYUK_TOKEN: {"reason": "tls-handshake-timeout", "phase": "token", "endpoint": "https://auth.docker.io/token"},
            RYUK_MANIFEST: {"reason": "tls-handshake-timeout", "phase": "manifest",
                            "endpoint": "https://registry-1.docker.io/v2/testcontainers/ryuk/manifests/sha256:" + "7" * 64},
            GHCR_EOF: {"reason": "eof", "phase": "token", "endpoint": "https://ghcr.io/token"},
            GHCR_DNS: {"reason": "io-timeout", "phase": "resolve", "endpoint": "ghcr.io"},
            GHCR_DNS.replace("read udp 127.0.0.1:40000->127.0.0.53:53: i/o timeout", "no such host"):
                {"reason": "name-resolution", "phase": "resolve", "endpoint": "ghcr.io"},
        }
        for text, expected in cases.items():
            with self.subTest(text=text[:60]):
                failure = registry.transport_failure("Pulling from x\n" + text + "\n")
                self.assertEqual(failure, expected)
                self.assertNotIn("account", str(failure))
                self.assertNotIn("scope", str(failure))

    def test_user_information_and_port_are_reduced_to_host(self):
        failure = registry.transport_failure('Error response from daemon: Get "https://user:secret@registry.example:5000'
                                             '/v2/x/blobs/sha256:' + "1" * 64 + '?token=abc": connection reset by peer')
        self.assertEqual(failure, {"reason": "connection-reset", "phase": "blob",
                                   "endpoint": "https://registry.example:5000/v2/x/blobs/sha256:" + "1" * 64})

    def test_gateway_errors_are_transport(self):
        failure = registry.transport_failure('Error response from daemon: received unexpected HTTP status: '
                                             '503 Service Unavailable')
        self.assertEqual(failure["reason"], "server-unavailable")

    def test_registry_refusals_and_unknown_errors_are_not_transport(self):
        for text in ('Error response from daemon: manifest unknown: manifest unknown',
                     'Error response from daemon: pull access denied for x, repository does not exist',
                     'Error response from daemon: Head "https://ghcr.io/v2/x/manifests/y": unauthorized',
                     'Error response from daemon: toomanyrequests: You have reached your pull rate limit',
                     'Error response from daemon: no matching manifest for linux/amd64 in the manifest list entries',
                     'Error response from daemon: Get "https://ghcr.io/v2/x/manifests/y": denied: i/o timeout',
                     'Error response from daemon: write /var/lib/docker/tmp: no space left on device',
                     'pulled\n', ''):
            with self.subTest(text=text):
                self.assertIsNone(registry.transport_failure(text))

    def test_only_the_last_error_line_decides(self):
        text = ('Error response from daemon: Get "https://ghcr.io/v2/": net/http: TLS handshake timeout\n'
                'Error response from daemon: manifest unknown\n')
        self.assertIsNone(registry.transport_failure(text))


class DaemonLineTests(unittest.TestCase):
    def test_reaper_and_image_stages(self):
        self.assertEqual(registry.daemon_transport_failure("    helpers_test.go:144: " + RYUK_TOKEN)["stage"], "reaper")
        line = ('    auth_test.go:147: create container: Error response from daemon: Get '
                '"https://quay.io/v2/ceph/ceph/manifests/v20.2.3": net/http: TLS handshake timeout')
        self.assertEqual(registry.daemon_transport_failure(line),
                         {"stage": "image", "reason": "tls-handshake-timeout", "phase": "manifest",
                          "endpoint": "https://quay.io/v2/ceph/ceph/manifests/v20.2.3"})

    def test_lines_without_a_daemon_registry_request_are_ignored(self):
        for line in ("    cephfs_test.go:10: read 3.0: context deadline exceeded",
                     "    rgw_test.go:20: Get \"http://127.0.0.1:8080/bucket\": EOF",
                     "    x_test.go:30: Error response from daemon: Get \"http://127.0.0.1:2375/containers\": EOF",
                     "    x_test.go:40: Error response from daemon: No such image: quay.io/ceph/ceph:v20.2.3",
                     "    x_test.go:50: Error response from daemon: context deadline exceeded"):
            with self.subTest(line=line):
                self.assertIsNone(registry.daemon_transport_failure(line))


class PullTests(unittest.TestCase):
    def run_pull(self, results):
        calls, sleeps = [], []

        def attempt(number):
            calls.append(number)
            return results[number - 1]

        passed, records = registry.pull(attempt, sleep=sleeps.append)
        return passed, records, calls, sleeps

    def test_success_needs_one_attempt(self):
        passed, records, calls, sleeps = self.run_pull([(0, "done\n")])
        self.assertTrue(passed)
        self.assertEqual(calls, [1])
        self.assertEqual(sleeps, [])
        self.assertEqual(records[0]["exit_code"], 0)

    def test_transport_failures_are_retried_after_the_bounded_delays(self):
        passed, records, calls, sleeps = self.run_pull([(1, GHCR_EOF), (1, GHCR_DNS), (0, "done\n")])
        self.assertTrue(passed)
        self.assertEqual(calls, [1, 2, 3])
        self.assertEqual(sleeps, list(registry.DELAYS))
        self.assertEqual([record.get("retry_after_seconds") for record in records], [10, 30, None])
        self.assertEqual(records[0]["transport"]["phase"], "token")

    def test_attempts_are_bounded(self):
        passed, records, calls, sleeps = self.run_pull([(1, RYUK_TOKEN)] * 5)
        self.assertFalse(passed)
        self.assertEqual(calls, [1, 2, 3])
        self.assertEqual(sleeps, [10, 30])
        self.assertNotIn("retry_after_seconds", records[-1])
        self.assertEqual(records[-1]["transport"]["reason"], "tls-handshake-timeout")

    def test_refusal_is_never_retried(self):
        passed, records, calls, sleeps = self.run_pull([(1, "Error response from daemon: manifest unknown"),
                                                        (0, "done\n")])
        self.assertFalse(passed)
        self.assertEqual(calls, [1])
        self.assertEqual(sleeps, [])
        self.assertIsNone(records[0]["transport"])

    def test_refusal_after_a_transport_failure_stops(self):
        passed, records, calls, _ = self.run_pull([(1, GHCR_EOF), (1, "Error response from daemon: denied"),
                                                   (0, "done\n")])
        self.assertFalse(passed)
        self.assertEqual(calls, [1, 2])


if __name__ == "__main__":
    unittest.main()
