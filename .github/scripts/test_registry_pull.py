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

# What a role image pull printed when the registry answered 500 (issue 917).
BARE_500 = "Error response from daemon: received unexpected HTTP status: 500 Internal Server Error"
MANIFEST = "https://ghcr.io/v2/jsyoo5b/ceph-testcontainers-images/manifests/official-19.2.5-essential"
BLOB = "https://ghcr.io/v2/jsyoo5b/ceph-testcontainers-images/blobs/sha256:" + "5" * 64
TOKEN = "https://ghcr.io/token?scope=repository%3Ajsyoo5b%2Fceph-testcontainers-images%3Apull&service=ghcr.io"
# Every wording of a registry status, with {status} for the status line and
# the phase and endpoint that it names.
STATUS_FORMS = (
    # Docker's distribution client.
    ("Error response from daemon: received unexpected HTTP status: {status}", "unknown", "unknown"),
    ('Error response from daemon: Head "' + MANIFEST + '": received unexpected HTTP status: {status}',
     "manifest", MANIFEST),
    ('Error response from daemon: Get "' + BLOB + '": received unexpected HTTP status: {status}', "blob", BLOB),
    ("error pulling image configuration: download failed after attempts=6: received unexpected HTTP status: {status}",
     "unknown", "unknown"),
    ('Error response from daemon: Head "' + MANIFEST + '": Get "' + TOKEN + '": received unexpected HTTP status: '
     "{status}", "token", "https://ghcr.io/token"),
    ("Error response from daemon: Head \"" + MANIFEST + "\": token auth attempt for registry ghcr.io: " + TOKEN
     + " request failed with status: {status}", "token", "https://ghcr.io/token"),
    ("Error response from daemon: login attempt to https://ghcr.io/v2/ failed with status: {status}",
     "registry", "https://ghcr.io/v2/"),
    # containerd's resolver, fetcher and token fetcher.
    ('Error response from daemon: failed to resolve reference "ghcr.io/jsyoo5b/ceph-testcontainers-images:x": '
     "unexpected status from HEAD request to " + MANIFEST + ": {status}", "manifest", MANIFEST),
    ("failed to copy: httpReadSeeker: failed open: unexpected status from GET request to " + BLOB + ": {status}",
     "blob", BLOB),
    ("failed to copy: httpReadSeeker: failed open: unexpected status code " + BLOB + ": {status}", "blob", BLOB),
    ("Error response from daemon: failed to resolve reference: unexpected status code [manifests x]: {status}",
     "unknown", "unknown"),
    ("Error response from daemon: failed to authorize: failed to fetch anonymous token: unexpected status from GET "
     "request to " + TOKEN + ": {status}", "token", "https://ghcr.io/token"),
    ("Error response from daemon: failed to authorize: failed to fetch anonymous token: unexpected status from GET "
     "request to https://ghcr.io/token: {status}", "token", "https://ghcr.io/token"),
    ("Error response from daemon: failed to authorize: failed to fetch oauth token: unexpected status: {status}",
     "unknown", "unknown"),
)
SERVER_STATUSES = {"500 Internal Server Error": "server-error", "501 Not Implemented": "server-error",
                   "502 Bad Gateway": "server-unavailable", "503 Service Unavailable": "server-unavailable",
                   "504 Gateway Timeout": "server-unavailable", "504 Gateway Time-out": "server-unavailable",
                   "507 Insufficient Storage": "server-error", "520 ": "server-error", "500": "server-error"}
CLIENT_STATUSES = ("400 Bad Request", "401 Unauthorized", "403 Forbidden", "404 Not Found", "408 Request Timeout",
                   "429 Too Many Requests", "429", "499 ")


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

    def test_every_registry_5xx_form_is_transport(self):
        for form, phase, where in STATUS_FORMS:
            for status, reason in SERVER_STATUSES.items():
                text = form.format(status=status)
                with self.subTest(text=text):
                    failure = registry.transport_failure("Pulling from x\n" + text + "\n")
                    self.assertEqual(failure, {"reason": reason, "phase": phase, "endpoint": where})
                # The wording is matched in any case; a URL's path is not.
                for cased in (text.upper(), text.lower(), text.title()):
                    with self.subTest(text=cased):
                        failure = registry.transport_failure(cased)
                        self.assertIsNotNone(failure)
                        self.assertEqual(failure["reason"], reason)

    def test_the_recorded_bare_500_is_transport(self):
        self.assertEqual(registry.transport_failure("official-19.2.5-essential: Pulling from x\n" + BARE_500 + "\n"),
                         {"reason": "server-error", "phase": "unknown", "endpoint": "unknown"})

    def test_unparsable_5xx_body_is_transport_and_its_text_names_no_endpoint(self):
        body = ('error parsing HTTP 502 response body: invalid character \'<\' looking for beginning of value: '
                '"<html><head><title>502 Bad Gateway</title></head><a href=\\"https://status.example/help\\">x</a></html>"')
        self.assertEqual(registry.transport_failure("Error response from daemon: " + body),
                         {"reason": "server-unavailable", "phase": "unknown", "endpoint": "unknown"})
        self.assertEqual(registry.transport_failure('Error response from daemon: Get "' + BLOB + '": ' + body),
                         {"reason": "server-unavailable", "phase": "blob", "endpoint": BLOB})
        self.assertIsNone(registry.transport_failure("Error response from daemon: "
                                                     + body.replace("HTTP 502", "HTTP 404")))

    def test_status_line_in_any_other_wording_is_transport(self):
        for text, reason in (("Error response from daemon: Get \"" + MANIFEST + "\": 500 Internal Server Error",
                              "server-error"),
                             ("error: server returned 503 Service Unavailable", "server-unavailable"),
                             ("Error: 504 Gateway Time-out", "server-unavailable")):
            with self.subTest(text=text):
                self.assertEqual(registry.transport_failure(text)["reason"], reason)

    def test_every_registry_4xx_form_is_not_transport(self):
        for form, _, _ in STATUS_FORMS:
            for status in CLIENT_STATUSES:
                for text in (form.format(status=status), form.format(status=status).upper(),
                             # A transport word or a 5xx beside the 4xx does not make it retryable.
                             form.format(status=status) + ": i/o timeout",
                             form.format(status=status) + ": received unexpected HTTP status: 500 Internal Server Error"):
                    with self.subTest(text=text):
                        self.assertIsNone(registry.transport_failure(text))

    def test_5xx_in_a_refusal_line_is_not_transport(self):
        for refusal in ("manifest unknown", "denied: requested access to the resource is denied",
                        "unauthorized: authentication required", "UNAUTHORIZED",
                        "toomanyrequests: You have reached your pull rate limit",
                        "Too Many Requests", "you have reached your pull rate limit",
                        "no matching manifest for linux/amd64", "pull access Denied for x",
                        "repository x not found"):
            for form, _, _ in STATUS_FORMS:
                for text in (form.format(status="500 Internal Server Error") + ": " + refusal,
                             "Error response from daemon: " + refusal + ": "
                             + form.format(status="503 Service Unavailable")):
                    with self.subTest(text=text):
                        self.assertIsNone(registry.transport_failure(text))

    def test_digits_that_are_no_status_are_not_transport(self):
        for text in ("Error response from daemon: Get \"https://ghcr.io/v2/x/blobs/sha256:" + "500" * 21 + "5\": x",
                     "Error response from daemon: write /var/lib/docker/tmp/500: no space left on device",
                     "Error response from daemon: layer 500 of 503 failed verification",
                     "Error response from daemon: dial 10.0.0.500 Bad Gateway-like text",
                     "Error response from daemon: received unexpected HTTP status: 200 OK",
                     "Error response from daemon: received unexpected HTTP status: 302 Found",
                     "Error response from daemon: received unexpected HTTP status: 5000",
                     "Error response from daemon: received unexpected HTTP status: 600 ",
                     "Error response from daemon: unexpected status"):
            with self.subTest(text=text):
                self.assertIsNone(registry.transport_failure(text))

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

    def test_registry_5xx_forms_in_a_daemon_line(self):
        for form, phase, where in STATUS_FORMS:
            if not form.startswith("Error response from daemon:"):
                continue
            for status, reason in SERVER_STATUSES.items():
                line = "    auth_test.go:147: create container: " + form.format(status=status)
                with self.subTest(line=line):
                    self.assertEqual(registry.daemon_transport_failure(line),
                                     {"stage": "image", "reason": reason, "phase": phase, "endpoint": where})
            for status in CLIENT_STATUSES:
                line = "    auth_test.go:147: create container: " + form.format(status=status)
                with self.subTest(line=line):
                    self.assertIsNone(registry.daemon_transport_failure(line))

    def test_the_bare_500_names_its_stage(self):
        self.assertEqual(registry.daemon_transport_failure(
            "    helpers_test.go:144: create ceph network: reaper: new reaper: run container: " + BARE_500),
            {"stage": "reaper", "reason": "server-error", "phase": "unknown", "endpoint": "unknown"})
        self.assertEqual(registry.daemon_transport_failure("    x_test.go:9: create container: " + BARE_500.upper()
                                                           .replace("ERROR RESPONSE FROM DAEMON",
                                                                    "Error response from daemon"))["stage"], "image")

    def test_5xx_that_is_no_registry_answer_in_a_daemon_line_is_ignored(self):
        for line in (
                # Not the daemon's error.
                "    rgw_test.go:20: put object: received unexpected HTTP status: 500 Internal Server Error",
                "    rgw_test.go:21: Get \"http://127.0.0.1:8080/bucket\": 503 Service Unavailable",
                # The daemon's, but neither a registry request nor a registry client's wording.
                "    x_test.go:30: Error response from daemon: 500 Internal Server Error",
                "    x_test.go:31: Error response from daemon: Get \"http://127.0.0.1:2375/containers\": "
                "received unexpected HTTP status: 500 Internal Server Error",
                "    x_test.go:32: Error response from daemon: Get \"http://127.0.0.1:2375/containers\": "
                "502 Bad Gateway",
                # A refusal beside the status.
                "    x_test.go:33: Error response from daemon: pull access denied: received unexpected HTTP status: "
                "500 Internal Server Error",
                "    x_test.go:34: Error response from daemon: toomanyrequests: received unexpected HTTP status: "
                "503 Service Unavailable"):
            with self.subTest(line=line):
                self.assertIsNone(registry.daemon_transport_failure(line))

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

    def test_registry_500_is_retried(self):
        passed, records, calls, sleeps = self.run_pull([(1, "x: Pulling from y\n" + BARE_500 + "\n"),
                                                        (0, "done\n")])
        self.assertTrue(passed)
        self.assertEqual(calls, [1, 2])
        self.assertEqual(sleeps, [registry.DELAYS[0]])
        self.assertEqual(records[0]["transport"]["reason"], "server-error")

    def test_rate_limit_status_is_never_retried(self):
        for text in ("Error response from daemon: received unexpected HTTP status: 429 Too Many Requests",
                     "Error response from daemon: failed to resolve reference: unexpected status from HEAD request to "
                     + MANIFEST + ": 429 Too Many Requests"):
            with self.subTest(text=text):
                passed, records, calls, sleeps = self.run_pull([(1, text), (0, "done\n")])
                self.assertFalse(passed)
                self.assertEqual(calls, [1])
                self.assertEqual(sleeps, [])
                self.assertIsNone(records[0]["transport"])

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
