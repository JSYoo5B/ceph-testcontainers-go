#!/usr/bin/env python3
"""Tell registry transport failures of an image pull apart and retry them.

An image pull is idempotent: it changes no Ceph state, and a repeated pull of
the same reference fetches the same manifest. A pull that fails because the
connection to the registry broke (a TLS handshake or DNS timeout, a reset, an
EOF on the token request) or because the registry or a proxy in front of it
answered a manifest, blob or token request with a 5xx status says nothing
about the image, so pull retries it a bounded number of times. A refusal from
the registry (unknown manifest, denied, unauthorized, rate limit, any 4xx
status) is never retried, and neither is anything that this module does not
recognize.

Docker reports a registry transport error of a pull as HTTP 500, and
testcontainers-go treats a 500 as permanent, so a pull inside a Go test is
not retried (issue 381). The records here name the endpoint without its query
or user information, so a token request's account or scope never reaches a
receipt.
"""

from http import HTTPStatus
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
)
# The wordings with which a registry client reports the HTTP status of a
# response it did not expect; the status code is the only group that matched.
#   Docker's distribution client, for a manifest, blob or token request:
#     received unexpected HTTP status: 500 Internal Server Error
#     error parsing HTTP 502 response body: invalid character '<' ...
#     token auth attempt for registry ...: ... request failed with status: 503 Service Unavailable
#     login attempt to https://.../v2/ failed with status: 500 Internal Server Error
#   containerd's resolver, fetcher and token fetcher (the containerd image
#   store), current and earlier wordings:
#     unexpected status from HEAD request to https://.../manifests/x: 500 Internal Server Error
#     unexpected status code https://.../blobs/sha256:...: 502 Bad Gateway
#     unexpected status code [manifests x]: 504 Gateway Timeout
#     failed to fetch anonymous token: unexpected status: 503 Service Unavailable
# A proxy's status without a standard reason phrase (520) has only the code.
CLIENT_STATUS = re.compile(
    r"unexpected HTTP status: ([1-5]\d\d)\b|"
    r"error parsing HTTP ([1-5]\d\d) response body|"
    r"failed with status: ([1-5]\d\d)\b|"
    r"unexpected status(?: code)?(?: from [A-Z]+ request to \S+| \S+| \[[^\]]*\])?: ([1-5]\d\d)\b",
    re.IGNORECASE)
# A status line as net/http prints it, code and standard reason phrase, in
# any other wording. "Gateway Time-out" is how some proxies spell 504.
STATUS_LINE = re.compile(
    r"(?<![\w.:/-])([1-5]\d\d) (?:" + "|".join(sorted({re.escape(status.phrase) for status in HTTPStatus}
                                                      | {"Gateway Time-out"}, key=len, reverse=True)) + r")\b",
    re.IGNORECASE)
# The quoted body of an unparsable response follows this; it is the
# server's text, not a request that the client made.
RESPONSE_BODY = re.compile(r"error parsing HTTP \d{3} response body", re.IGNORECASE)
# A registry answer about the image or the client. Its presence makes the
# failure a refusal even when a transport word or a 5xx status also appears,
# and so does any 4xx status.
REFUSAL = re.compile(r"manifest unknown|not found|no such image|denied|unauthorized|toomanyrequests|"
                     r"too many requests|rate limit|no matching manifest|invalid reference format", re.IGNORECASE)
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


def client_statuses(line):
    """The HTTP status codes that line reports in a registry client's own
    wording (CLIENT_STATUS)."""
    return [int(next(filter(None, match.groups()))) for match in CLIENT_STATUS.finditer(line)]


def http_statuses(line):
    """Every HTTP status code that line reports, in a registry client's
    wording or as a status line."""
    return client_statuses(line) + [int(match[1]) for match in STATUS_LINE.finditer(line)]


def status_reason(statuses):
    """The transport reason of the 5xx statuses a line reports, or None.
    502, 503 and 504 are the answers of a proxy or an overloaded registry;
    any other 5xx is the registry's own failure to serve the request."""
    server = [status for status in statuses if 500 <= status <= 599]
    if not server:
        return None
    return "server-unavailable" if any(status in (502, 503, 504) for status in server) else "server-error"


def transport_failure(text):
    """The transport failure in a pull's error text, or None.

    The last line that reports an error decides. A refusal or a 4xx status in
    it is no transport failure. Its reason is the first matching TRANSPORT
    entry, or else that of a 5xx status; its endpoint is the innermost URL of
    that line (Docker nests the failed request last), or the host of a failed
    DNS lookup, whose phase is then resolve."""
    lines = [line for line in str(text).splitlines()
             if re.search(r"error|failed|timeout|EOF|unexpected", line, re.IGNORECASE)]
    if not lines:
        return None
    line = lines[-1]
    statuses = http_statuses(line)
    if REFUSAL.search(line) or any(400 <= status <= 499 for status in statuses):
        return None
    reason = next((name for name, pattern in TRANSPORT if pattern.search(line)), None) or status_reason(statuses)
    if reason is None:
        return None
    failure = {"reason": reason}
    body = RESPONSE_BODY.search(line)
    urls = [url.rstrip(".,;:)") for url in URL.findall(line[:body.start()] if body else line)]
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
    image for any other image the test asked for.

    The line must name a registry request. The exception is a 5xx status in
    a registry client's own wording: the daemon speaks as that client only to
    a registry, and Docker's client prints such a status without the request
    (received unexpected HTTP status: 500 Internal Server Error)."""
    if not DAEMON.search(line):
        return None
    failure = transport_failure(line)
    if failure is None:
        return None
    if failure["phase"] == "registry" and "/v2/" not in failure["endpoint"]:
        return None
    if failure["phase"] == "unknown" and status_reason(client_statuses(line)) is None:
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
    # The pulled reference can be a multi-platform index: its digest is in
    # repo_digests, while the local image and its recorded os/architecture
    # are the platform child that Docker selected for this engine.
    selected = {"os": image["Os"], "architecture": image["Architecture"]}
    if image.get("Variant"):
        selected["variant"] = image["Variant"]
    return {"reference": reference, "image_id": image_id, "repo_digests": repo_digests, "platform": platform,
            "selected_platform": selected}
