#!/usr/bin/env python3
"""Tell registry transport failures of an image pull apart and retry them.

An image pull is idempotent: it changes no Ceph state, and a repeated pull of
the same reference fetches the same manifest. A pull that fails because the
connection to the registry broke (a TLS handshake or DNS timeout, a reset, an
EOF on the token request, a 502/503/504 from a proxy) says nothing about the
image, so pull retries it a bounded number of times. A refusal from the
registry (unknown manifest, denied, unauthorized, rate limit) is never
retried, and neither is anything that this module does not recognize.

Docker reports a registry transport error of a pull as HTTP 500, and
testcontainers-go treats a 500 as permanent, so a pull inside a Go test is
not retried (issue 381). The records here name the endpoint without its query
or user information, so a token request's account or scope never reaches a
receipt.
"""

import json
import re
import time
from urllib.parse import urlsplit


# Delays before the second and the third attempt. The pull therefore makes at
# most three attempts and waits at most 40 seconds between them.
DELAYS = (10, 30)
TRANSPORT = (
    ("tls-handshake-timeout", re.compile(r"TLS handshake timeout", re.IGNORECASE)),
    ("name-resolution", re.compile(r"no such host|server misbehaving|Temporary failure in name resolution",
                                   re.IGNORECASE)),
    ("io-timeout", re.compile(r"i/o timeout", re.IGNORECASE)),
    ("connection-reset", re.compile(r"connection reset by peer", re.IGNORECASE)),
    ("connection-refused", re.compile(r"connection refused", re.IGNORECASE)),
    ("timeout", re.compile(r"Client\.Timeout exceeded|context deadline exceeded|"
                           r"request canceled while waiting for connection", re.IGNORECASE)),
    ("eof", re.compile(r"\bunexpected EOF\b|:\s*EOF\b")),
    ("server-unavailable", re.compile(r"\b50[234] (?:Bad Gateway|Service Unavailable|Gateway Time-?out)\b|"
                                      r"unexpected HTTP status: 50[234]\b", re.IGNORECASE)),
)
# A registry answer about the image or the client. Its presence makes the
# failure a refusal even when a transport word also appears.
REFUSAL = re.compile(r"manifest unknown|not found|no such image|denied|unauthorized|toomanyrequests|"
                     r"no matching manifest|invalid reference format", re.IGNORECASE)
URL = re.compile(r"https?://[^\s\"'<>]+")
LOOKUP = re.compile(r"\blookup ([A-Za-z0-9][A-Za-z0-9.-]*[A-Za-z0-9])")
DAEMON = re.compile(r"Error response from daemon:")


def endpoint(url):
    """Scheme, host and path of url: no user information, query or fragment."""
    parts = urlsplit(url.rstrip(".,;:)"))
    host = parts.hostname or ""
    if parts.port is not None:
        host += ":" + str(parts.port)
    return parts.scheme + "://" + host + parts.path


def registry_phase(path):
    if path.rstrip("/").endswith("/token"):
        return "token"
    if "/manifests/" in path:
        return "manifest"
    if "/blobs/" in path:
        return "blob"
    return "registry"


def transport_failure(text):
    """The transport failure in a pull's error text, or None.

    The last line that reports an error decides. Its reason is the first
    matching TRANSPORT entry; its endpoint is the innermost URL of that line
    (Docker nests the failed request last), or the host of a failed DNS
    lookup, whose phase is then resolve."""
    lines = [line for line in str(text).splitlines() if re.search(r"error|failed|timeout|EOF", line, re.IGNORECASE)]
    if not lines:
        return None
    line = lines[-1]
    if REFUSAL.search(line):
        return None
    reason = next((name for name, pattern in TRANSPORT if pattern.search(line)), None)
    if reason is None:
        return None
    failure = {"reason": reason}
    urls = URL.findall(line)
    lookup = LOOKUP.search(line)
    if lookup:
        failure |= {"phase": "resolve", "endpoint": lookup.group(1)}
    elif urls:
        failure["endpoint"] = endpoint(urls[-1])
        failure["phase"] = registry_phase(urlsplit(urls[-1]).path)
    else:
        failure |= {"phase": "unknown", "endpoint": "unknown"}
    return failure


def daemon_transport_failure(line):
    """A Docker daemon's registry transport error in one Go test output line,
    or None. stage is reaper when testcontainers-go was starting Ryuk and
    image for any other image the test asked for."""
    if not DAEMON.search(line):
        return None
    failure = transport_failure(line)
    if failure is None:
        return None
    if failure["phase"] == "registry" and "/v2/" not in failure["endpoint"]:
        return None
    if failure["phase"] == "unknown":
        return None
    return {"stage": "reaper" if re.search(r"\breaper\b", line) else "image"} | failure


def pull(attempt, sleep=None, delays=DELAYS):
    """Run attempt(n) -> (exit_code, output) until it succeeds, it fails with
    anything but a transport failure, or len(delays) + 1 attempts are spent.
    Returns (passed, records), one record per attempt."""
    sleep = sleep or time.sleep
    records = []
    for number in range(1, len(delays) + 2):
        started = time.monotonic()
        code, output = attempt(number)
        record = {"attempt": number, "exit_code": code, "seconds": round(time.monotonic() - started, 3)}
        records.append(record)
        if code == 0:
            return True, records
        failure = transport_failure(output)
        record["transport"] = failure
        if failure is None or number > len(delays):
            return False, records
        record["retry_after_seconds"] = delays[number - 1]
        sleep(delays[number - 1])
    return False, records


def describe(failure):
    return "registry transport %s at %s %s" % (failure["reason"], failure["phase"], failure["endpoint"])


IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}\Z")
REPO_DIGEST = re.compile(r"[^\s]+@sha256:[a-f0-9]{64}\Z")
ABSENT = re.compile(r"\bNo such (?:image|object):", re.IGNORECASE)


class PreparationError(RuntimeError):
    """An image that could not be prepared; failure names the registry
    transport failure when that was the cause."""

    def __init__(self, message, failure=None):
        super().__init__(message)
        self.failure = failure


def resolve(name, reference, platform, inspect, attempt, pulls, sleep=None):
    """Inspect reference, pull it only when Docker reports it absent, and
    return its local identity.

    inspect(reference) returns a CompletedProcess of docker image inspect;
    attempt(number) runs one docker pull and returns (exit code, output).
    The pull attempts are recorded in pulls[name]."""
    # Ordinary inspection keeps this compatible with engines predating the
    # inspect --platform API (1.49).
    result = inspect(reference)
    if result.returncode:
        # Permission/daemon/API errors are not evidence of an absent image
        # and must not cause an unrelated registry pull.
        if not ABSENT.search(result.stderr or ""):
            raise PreparationError(name + " image inspection failed; image absence was not confirmed")
        passed, attempts = pull(attempt, sleep=sleep)
        pulls[name] = attempts
        if not passed:
            last = attempts[-1]
            message = name + " image pull failed (exit " + str(last["exit_code"]) + ")"
            failure = None
            if last.get("transport"):
                # The registry was not reached; nothing about the image is
                # established.
                failure = {"image": name, "reference": reference, "attempts": len(attempts)} | last["transport"]
                message += " after " + str(len(attempts)) + " attempts: " + describe(last["transport"])
            raise PreparationError(message, failure)
        result = inspect(reference)
        if result.returncode:
            raise PreparationError(name + " image is unavailable after pull")
    try:
        inspected = json.loads(result.stdout)
    except ValueError:
        inspected = None
    if not isinstance(inspected, list) or len(inspected) != 1 or not isinstance(inspected[0], dict):
        raise PreparationError(name + " image inspection has an invalid shape")
    image = inspected[0]
    image_id = image.get("Id")
    if not isinstance(image_id, str) or IMAGE_ID.fullmatch(image_id) is None:
        raise PreparationError(name + " image has no immutable local image ID")
    if image.get("Os") != "linux" or image.get("Architecture") != platform.split("/")[1]:
        raise PreparationError(name + " image platform differs from the native Docker engine")
    repo_digests = image.get("RepoDigests")
    if repo_digests is None:
        repo_digests = []
    if (not isinstance(repo_digests, list)
            or any(not isinstance(value, str) or REPO_DIGEST.fullmatch(value) is None for value in repo_digests)):
        raise PreparationError(name + " image has invalid registry digest metadata")
    return {"reference": reference, "image_id": image_id, "repo_digests": repo_digests, "platform": platform}
