"""Read-only storage discovery; uncertain layouts cannot skip reclamation."""

import os
from pathlib import Path
import re
import shlex
import json
import subprocess
import tomllib


def read_file(path):
    return Path(path).read_text(encoding="utf-8")


def storage_roots(info, run, exists=os.path.exists, read_text=read_file):
    """Return roots, verified, and reasons for a local Linux Docker engine.

    run accepts an argv list and returns stdout, raising on command failure.
    Classic overlay2 and socket-linked containerd overlayfs layouts can be
    verified. Unlinked containerd roots remain diagnostic candidates. The
    caller must separately establish that the Docker endpoint is local.
    """
    roots, reasons = [], []

    def add(path):
        if isinstance(path, str) and path.startswith("/") and exists(path):
            path = os.path.normpath(path)
            if path not in roots:
                roots.append(path)
            return True
        return False

    root = info.get("DockerRootDir")
    if not add(root):
        reasons.append("docker-root-unavailable")
    driver = info.get("Driver")
    statuses = info.get("DriverStatus") or []
    snapshotter = any("containerd" in str(row).lower() for row in statuses)
    if driver not in ("overlay2", "overlayfs") or driver == "overlay2" and snapshotter:
        reasons.append("storage-driver-not-qualified")

    # Local volume options may point at a device or remote filesystem which
    # is mounted only when a container starts; its current directory is not
    # enough to prove that future allocation's filesystem.
    try:
        names = run(["docker", "volume", "ls", "--quiet"]).splitlines()
        if names:
            volumes = json.loads(run(["docker", "volume", "inspect", "--", *names]))
            if not isinstance(volumes, list) or len(volumes) != len(names):
                raise ValueError("invalid volume inspection")
            for volume in volumes:
                if not add(volume.get("Mountpoint")):
                    reasons.append("volume-mountpoint-unavailable")
                if volume.get("Driver") != "local" or volume.get("Options"):
                    reasons.append("volume-storage-not-qualified")
    except (OSError, RuntimeError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
        reasons.append("volume-discovery-unavailable")

    if snapshotter or driver == "overlayfs":
        discovered, linked = False, []
        endpoint = (info.get("Containerd") or {}).get("Address")
        if not isinstance(endpoint, str) or not endpoint.startswith("/"):
            endpoint = None
        try:
            processes = run(["ps", "-eo", "args="])
            for line in processes.splitlines():
                args = shlex.split(line)
                if not args or os.path.basename(args[0]) != "containerd":
                    continue

                def argument(name, short=None):
                    for index, arg in enumerate(args[1:], 1):
                        if arg.startswith(name + "="):
                            return arg.split("=", 1)[1]
                        if arg in (name, short) and index + 1 < len(args):
                            return args[index + 1]
                    return None

                config = argument("--config", "-c") or "/etc/containerd/config.toml"
                try:
                    settings = tomllib.loads(read_text(config))
                except FileNotFoundError:
                    settings = {}
                candidate = argument("--root") or settings.get("root")
                address = argument("--address", "-a") or settings.get("grpc", {}).get("address")
                known = add(candidate)
                discovered |= known
                plugins = settings.get("plugins", {})
                overlay = plugins.get("io.containerd.snapshotter.v1.overlayfs", {})
                override = overlay.get("root_path")
                if override:
                    add(override)
                # Imported/proxy configuration or a snapshotter override may
                # allocate outside the established root. Do not guess.
                ordinary = not (settings.get("imports") or settings.get("proxy_plugins") or override)
                if endpoint and address == endpoint:
                    linked.append(known and ordinary)
        except (OSError, RuntimeError, ValueError, TypeError, AttributeError, subprocess.SubprocessError):
            reasons.append("containerd-process-discovery-unavailable")
        if not discovered:
            reasons.append("containerd-root-unresolved")
        # These are candidates only: the process may use another root.
        add("/var/lib/containerd")
        if isinstance(root, str) and root.startswith("/"):
            add(os.path.join(root, "containerd", "daemon"))
        if linked != [True] or driver != "overlayfs" or not snapshotter:
            reasons.append("containerd-daemon-association-unverified")

    # Include mountpoints under the data roots, notably separate volume
    # mounts. The caller deduplicates filesystems by device, not pathname.
    try:
        mounts = read_text("/proc/self/mountinfo").splitlines()
        if not mounts:
            raise ValueError("empty mount table")
        parents = list(roots)
        for line in mounts:
            fields = line.split()
            if len(fields) < 10 or "-" not in fields[6:]:
                raise ValueError("invalid mount table")
            mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
            if not mount.startswith("/"):
                raise ValueError("relative mountpoint")
            if any(mount == p or mount.startswith(p.rstrip("/") + "/") for p in parents):
                if not add(mount):
                    reasons.append("nested-mount-unavailable")
    except (OSError, ValueError, TypeError):
        reasons.append("mount-table-unavailable")

    return {"roots": roots, "verified": not reasons,
            "reasons": list(dict.fromkeys(reasons))}
