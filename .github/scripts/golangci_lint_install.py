#!/usr/bin/env python3
"""Install the pinned golangci-lint release from a checksum-verified archive.

The Code checks lint step and the structural review collector run one pinned
golangci-lint release. golangci-lint-action downloads it from the GitHub
release on every run and gives up after three quick attempts (issue 647: a
required job ended on HTTP 504 before the linter ran). The archive never
changes for a version, so this installer:

- uses the archive that the job restored from the Actions cache when its
  SHA-256 is the pinned one, and makes no request at all;
- otherwise downloads it, retrying only transport failures and 429/5xx
  answers with a longer backoff; any other HTTP status stops at once;
- rejects any archive whose SHA-256 is not the pinned one, cached or
  downloaded, so the cache cannot supply a different binary; a complete
  download (as long as its Content-Length) with another SHA-256 means a
  wrong pin and stops at once, a shorter or unannounced one is retried;
- extracts only the golangci-lint executable and checks that it reports the
  pinned version before it is put on PATH.

It prints `downloaded=true|false` for the cache-save step.
"""

import argparse
import hashlib
import http.client
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.request


URL = "https://github.com/golangci/golangci-lint/releases/download/v{version}/{asset}"
# Delays before the second to the fifth attempt: at most five attempts and two
# and a half minutes of waiting, against the three attempts in about 35 seconds
# that golangci-lint-action makes.
DELAYS = (10, 20, 40, 80)
TIMEOUT = 60
RETRIED_STATUS = {408, 429, 500, 502, 503, 504}


class InstallError(Exception):
    pass


def asset_name(version, platform="linux-amd64"):
    return "golangci-lint-%s-%s.tar.gz" % (version, platform)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified(path, expected):
    return path.is_file() and sha256(path) == expected


def retryable(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in RETRIED_STATUS
    return isinstance(error, (urllib.error.URLError, http.client.HTTPException, TimeoutError, ConnectionError))


def download(url, target, expected, opener=urllib.request.urlopen, sleep=time.sleep, log=print):
    """Download url to target until its SHA-256 is expected; bounded retries."""
    partial = target.with_name(target.name + ".partial")
    for attempt in range(len(DELAYS) + 1):
        if attempt:
            sleep(DELAYS[attempt - 1])
        received = 0
        try:
            with opener(url, timeout=TIMEOUT) as response, open(partial, "wb") as out:
                length = response.headers.get("Content-Length") if getattr(response, "headers", None) else None
                while chunk := response.read(1 << 20):
                    out.write(chunk)
                    received += len(chunk)
        except Exception as error:  # noqa: BLE001 - classified below
            partial.unlink(missing_ok=True)
            if not retryable(error):
                raise InstallError("download %s: %s" % (url, error)) from error
            log("attempt %d of %d: download %s: %s" % (attempt + 1, len(DELAYS) + 1, url, error))
            continue
        actual = sha256(partial)
        if actual != expected:
            partial.unlink()
            if length is not None and length.isdigit() and int(length) == received:
                # The whole announced asset arrived: the pin or the release is wrong,
                # and another attempt would fetch the same bytes.
                raise InstallError("%s has SHA-256 %s, want %s" % (url, actual, expected))
            # A truncated or altered transfer: never kept, retried as a transfer failure.
            log("attempt %d of %d: %s has SHA-256 %s, want %s" % (attempt + 1, len(DELAYS) + 1, url, actual, expected))
            continue
        partial.replace(target)
        return
    raise InstallError("download %s: no verified archive after %d attempts" % (url, len(DELAYS) + 1))


def extract(archive, version, bin_dir):
    member = "golangci-lint-%s-linux-amd64/golangci-lint" % version
    with tarfile.open(archive, "r:gz") as tar:
        try:
            info = tar.getmember(member)
        except KeyError as error:
            raise InstallError("%s has no %s" % (archive, member)) from error
        if not info.isfile():
            raise InstallError("%s in %s is not a regular file" % (member, archive))
        source = tar.extractfile(info)
        bin_dir.mkdir(parents=True, exist_ok=True)
        binary = bin_dir / "golangci-lint"
        with source, open(binary, "wb") as out:
            out.write(source.read())
    binary.chmod(0o755)
    return binary


def check_version(binary, version, run=subprocess.run):
    result = run([str(binary), "version", "--short"], capture_output=True, text=True, check=False)
    reported = result.stdout.strip()
    if result.returncode != 0 or reported.lstrip("v") != version:
        raise InstallError("%s reports version %r (exit %d), want %s" % (binary, reported, result.returncode, version))


def install(version, expected, cache_dir, bin_dir, opener=urllib.request.urlopen, sleep=time.sleep,
            run=subprocess.run, log=print):
    """Install and return whether the archive was downloaded."""
    version = version.lstrip("v")
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise InstallError("version %r is not an exact release" % version)
    if not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise InstallError("SHA-256 %r is not 64 lowercase hex digits" % expected)
    cache_dir.mkdir(parents=True, exist_ok=True)
    asset = asset_name(version)
    archive = cache_dir / asset
    downloaded = False
    if verified(archive, expected):
        log("using cached %s (SHA-256 %s)" % (asset, expected))
    else:
        if archive.exists():
            log("discarding cached %s: SHA-256 %s, want %s" % (asset, sha256(archive), expected))
            archive.unlink()
        download(URL.format(version=version, asset=asset), archive, expected, opener=opener, sleep=sleep, log=log)
        downloaded = True
    binary = extract(archive, version, bin_dir)
    check_version(binary, version, run=run)
    return downloaded


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--version", required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        downloaded = install(args.version, args.sha256, args.cache_dir, args.bin_dir)
    except InstallError as error:
        print("::error::golangci-lint install: %s" % error)
        return 1
    print("downloaded=%s" % ("true" if downloaded else "false"))
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as handle:
            handle.write("downloaded=%s\n" % ("true" if downloaded else "false"))
    path = os.environ.get("GITHUB_PATH")
    if path:
        with open(path, "a") as handle:
            handle.write("%s\n" % args.bin_dir.resolve())
    return 0


if __name__ == "__main__":
    sys.exit(main())
