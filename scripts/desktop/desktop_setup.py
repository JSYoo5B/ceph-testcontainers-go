#!/usr/bin/env python3
"""Plan, apply, verify and undo Docker Desktop resource settings for a test workload.

The default command, plan, only reads: it resolves the engine that
Testcontainers would use, compares it with a workload profile and prints what
differs. apply writes the one thing that has a supported write route, the
[wsl2] resource keys of the Windows user's .wslconfig, and nothing else; every
other backend gets exact manual steps. Nothing here restarts Docker Desktop or
shuts WSL down. See docs/guides/desktop-setup.md.
"""

import argparse
import gzip
import json
import os
import platform as host_platform
import subprocess
import sys
import zlib

import wslconfig

SCHEMA_VERSION = 1
EXIT_OK, EXIT_UNMET, EXIT_REFUSED, EXIT_FAILED = 0, 1, 2, 3
GIB = 1 << 30
# An engine reports a little less memory than its VM is given: the kernel
# keeps some for itself. A value within this share of the request counts.
MEMORY_TOLERANCE = 0.9
KERNEL_FEATURES = {
    "dm-delay": "CONFIG_DM_DELAY",
    "dm-flakey": "CONFIG_DM_FLAKEY",
    "dm-crypt": "CONFIG_DM_CRYPT",
    "fuse": "CONFIG_FUSE_FS",
    "nbd": "CONFIG_BLK_DEV_NBD",
    "netem": "CONFIG_NET_SCH_NETEM",
}
KNOWN_CONTEXTS = ("default", "desktop-linux", "desktop-windows")
WSL_SCOPE = ("%UserProfile%\\.wslconfig is global: its [wsl2] values apply to every WSL 2 distribution of "
             "this Windows user, not only to Docker Desktop")
WSL_RESTART = ("Quit Docker Desktop, run `wsl --shutdown` (it stops every running WSL 2 distribution), "
               "start Docker Desktop again, then run this script with `verify`")
# Settings Management files of Docker Desktop. Their presence means an
# administrator may have locked settings; this script never writes them.
ADMIN_SETTINGS = {
    "darwin": "/Library/Application Support/com.docker.docker/admin-settings.json",
    "win32": "C:\\ProgramData\\DockerDesktop\\admin-settings.json",
    "linux": "/usr/share/docker-desktop/admin-settings.json",
}
BACKENDS = {
    "desktop-macos": "Docker Desktop on macOS",
    "desktop-windows-wsl2": "Docker Desktop on Windows, WSL 2 backend",
    "desktop-windows-hyperv": "Docker Desktop on Windows, Hyper-V backend",
    "desktop-windows-containers": "Docker Desktop on Windows, Windows containers",
    "desktop-linux": "Docker Desktop on Linux",
    "linux-engine": "Docker Engine on this Linux host",
    "remote": "remote daemon",
    "unrecognized": "unrecognized engine",
    "unreachable": "no engine answered",
}


class System:
    """What the script reads from its machine, replaceable in tests."""

    platform = sys.platform
    machine = host_platform.machine().lower()

    def __init__(self):
        self.environ = dict(os.environ)

    def home(self):
        return os.path.expanduser("~")

    def read_text(self, path):
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                return handle.read()
        except OSError:
            return None

    def exists(self, path):
        return os.path.exists(path)

    def run(self, args, timeout=30):
        """(exit status, stdout bytes); the status is None when nothing ran to an end."""
        try:
            result = subprocess.run(args, capture_output=True, timeout=timeout, check=False, env=self.environ)
        except (OSError, subprocess.TimeoutExpired):
            return None, b""
        return result.returncode, result.stdout


def load_profiles(path):
    with open(path, encoding="utf-8") as handle:
        profiles = json.load(handle)["profiles"]
    for name, profile in profiles.items():
        check_profile(name, profile)
    return profiles


def check_profile(name, profile):
    for field in ("cpus", "memory_gib", "swap_gib", "disk_free_gib"):
        value = profile.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
            raise ValueError("profile %s: %s must be a number of 0 or more" % (name, field))
    if profile["cpus"] < 1 or profile["cpus"] != int(profile["cpus"]) or profile["memory_gib"] <= 0:
        raise ValueError("profile %s: cpus must be a whole number of 1 or more and memory_gib positive" % name)
    unknown = sorted(set(profile.get("kernel_features", ())) - set(KERNEL_FEATURES))
    if unknown:
        raise ValueError("profile %s: unknown kernel feature %s (known: %s)"
                         % (name, ", ".join(unknown), ", ".join(sorted(KERNEL_FEATURES))))


def parse_properties(text):
    values = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if line and line[0] not in "#!" and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def select_engine(system):
    """Resolve the Docker host in the order testcontainers-go uses.

    That order starts with tc.host of ~/.testcontainers.properties, then
    DOCKER_HOST, then the current Docker context. Its later fallbacks (the
    default socket, docker.host, the rootless socket) apply only when a
    context names no host, and are reported as unknown here.
    """
    properties = parse_properties(system.read_text(os.path.join(system.home(), ".testcontainers.properties")))
    if properties.get("tc.host"):
        return {"source": "tc.host of ~/.testcontainers.properties", "endpoint": properties["tc.host"],
                "command": ["docker", "--host", properties["tc.host"]], "context": None}
    if system.environ.get("DOCKER_HOST"):
        return {"source": "DOCKER_HOST", "endpoint": system.environ["DOCKER_HOST"], "command": ["docker"],
                "context": None}
    status, output = system.run(["docker", "context", "inspect", "--format", "{{json .}}"])
    try:
        context = json.loads(output.decode("utf-8", "replace")) if status == 0 else None
        endpoint = context["Endpoints"]["docker"]["Host"]
        name = context["Name"]
    except (ValueError, KeyError, TypeError):
        return {"source": "unknown", "endpoint": None, "command": ["docker"], "context": None}
    return {"source": "Docker context", "endpoint": endpoint, "command": ["docker"],
            "context": name if name in KNOWN_CONTEXTS else "custom"}


def transport(endpoint):
    """(scheme, locality) of a Docker host. Only the scheme is ever printed."""
    if not endpoint or "://" not in endpoint:
        return "unknown", "unknown"
    scheme = endpoint.split("://", 1)[0].lower()
    if scheme in ("unix", "npipe"):
        return scheme, "local socket"
    if scheme in ("tcp", "ssh", "http", "https"):
        return scheme, "remote"
    return "other", "unknown"


def docker_json(system, command, *args):
    status, output = system.run(list(command) + list(args) + ["--format", "{{json .}}"])
    try:
        value = json.loads(output.decode("utf-8", "replace"))
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def inspect_engine(system):
    """The selection and the allowlisted facts of the selected engine."""
    selection = select_engine(system)
    scheme, locality = transport(selection["endpoint"])
    info = docker_json(system, selection["command"], "info")
    version = docker_json(system, selection["command"], "version")
    server = (version or {}).get("Server") or {}
    reachable = bool(info) and bool(info.get("ServerVersion")) and not info.get("ServerErrors")
    info = info if reachable else {}
    proc_version = system.read_text("/proc/version") if system.platform.startswith("linux") else ""
    engine = {
        "selected_by": selection["source"],
        "context": selection["context"],
        "transport": scheme,
        "locality": locality,
        "reachable": reachable,
        "operating_system": info.get("OperatingSystem"),
        "desktop_version": ((server.get("Platform") or {}).get("Name") or None),
        "server_version": info.get("ServerVersion"),
        "os_type": info.get("OSType"),
        "architecture": info.get("Architecture"),
        "kernel": info.get("KernelVersion"),
        "cpus": info.get("NCPU"),
        "memory_bytes": info.get("MemTotal"),
        "cgroup_version": info.get("CgroupVersion"),
        "storage_driver": info.get("Driver"),
    }
    client = {"os": system.platform, "architecture": system.machine,
              "inside_wsl": "microsoft" in (proc_version or "").lower()}
    engine["backend"] = classify(client, engine)
    return selection, client, engine


def classify(client, engine):
    """Name the backend from where the client runs and what the engine reports.

    Docker Desktop is recognized only on a local socket, by the operating
    system name its engine reports. A remote endpoint is never called a
    Desktop backend of this machine, whatever it reports.
    """
    if not engine["reachable"]:
        return "unreachable"
    if engine["locality"] == "remote":
        return "remote"
    if engine["locality"] != "local socket":
        return "unrecognized"
    kernel = (engine["kernel"] or "").lower()
    if engine["operating_system"] != "Docker Desktop":
        if client["os"].startswith("linux") and not client["inside_wsl"] and engine["os_type"] == "linux":
            return "linux-engine"
        return "unrecognized"
    if client["os"] == "darwin":
        return "desktop-macos"
    if client["os"] == "win32" or client["inside_wsl"]:
        if engine["os_type"] == "windows":
            return "desktop-windows-containers"
        if "microsoft" in kernel:
            return "desktop-windows-wsl2"
        if "linuxkit" in kernel and client["os"] == "win32":
            return "desktop-windows-hyperv"
        return "unrecognized"
    if client["os"].startswith("linux"):
        return "desktop-linux"
    return "unrecognized"


def wslconfig_route(system, client, engine):
    """(path, None) when this run may edit .wslconfig, else (None, reason)."""
    backend = engine["backend"]
    if backend == "unreachable":
        return None, "the engine Testcontainers would use did not answer, so the local backend cannot be established"
    if backend == "remote":
        return None, ("the selected daemon is remote: its resources belong to the machine that runs it, "
                      "and no file on this machine changes them")
    if backend != "desktop-windows-wsl2":
        return None, "%s has no supported write interface for these settings" % BACKENDS[backend]
    if client["os"] != "win32":
        return None, ("the script runs inside a WSL distribution, where the Windows user profile that holds "
                      ".wslconfig is not established; run it with Python for Windows")
    profile = system.environ.get("USERPROFILE")
    if not profile or not os.path.isabs(profile):
        return None, "USERPROFILE does not name the Windows user profile"
    return os.path.join(profile, ".wslconfig"), None


def probe_engine(system, command, image):
    """Read swap, free disk and the kernel configuration from one-shot containers.

    The image must already be on the engine (nothing is pulled) and needs cat
    and df. Each container has no network and is removed when it exits.
    """
    def run(*program):
        status, output = system.run(list(command) + ["run", "--rm", "--pull=never", "--network", "none",
                                                     "--entrypoint", program[0], image] + list(program[1:]), 60)
        return output if status == 0 else None

    result = {"image_ran": False, "swap_bytes": None, "disk_free_bytes": None, "kernel_config": None}
    meminfo = run("cat", "/proc/meminfo")
    if meminfo is None:
        return result
    result["image_ran"] = True
    for line in meminfo.decode("utf-8", "replace").splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] == "SwapTotal:" and fields[1].isdigit():
            result["swap_bytes"] = int(fields[1]) * 1024
    disk = run("df", "-Pk", "/")
    rows = (disk or b"").decode("utf-8", "replace").splitlines()
    if len(rows) >= 2 and len(rows[-1].split()) >= 4 and rows[-1].split()[3].isdigit():
        result["disk_free_bytes"] = int(rows[-1].split()[3]) * 1024
    packed = run("cat", "/proc/config.gz")
    try:
        config = gzip.decompress(packed).decode("utf-8", "replace") if packed else None
    except (OSError, EOFError, zlib.error):
        config = None
    if config is not None:
        result["kernel_config"] = dict(line.split("=", 1) for line in config.splitlines()
                                       if line.startswith("CONFIG_") and "=" in line)
    return result


def gib(value):
    return "unknown" if value is None else "%.2f GiB" % (value / GIB)


def build_report(system, mode, profile_name, profile, probe_image=None):
    """Inspect the engine and the config, and decide the outcome of every setting."""
    selection, client, engine = inspect_engine(system)
    path, reason = wslconfig_route(system, client, engine)
    document = state = None
    problem = None
    if path:
        try:
            document, state = wslconfig.inspect(path, newline="\r\n")
            for key in wslconfig.MANAGED_KEYS:
                if document is not None:
                    document.value(key)
        except wslconfig.Refusal as refusal:
            document = state = None
            path, problem, reason = None, refusal.code, ".wslconfig cannot be edited safely: %s" % refusal
    probe = probe_engine(system, selection["command"], probe_image) if probe_image and engine["reachable"] else None

    settings = []
    limits = (("cpus", "processors", profile["cpus"], engine["cpus"], 1.0, str),
              ("memory", "memory", int(profile["memory_gib"] * GIB), engine["memory_bytes"], MEMORY_TOLERANCE, gib),
              ("swap", "swap", int(profile["swap_gib"] * GIB), (probe or {}).get("swap_bytes"),
               MEMORY_TOLERANCE, gib))
    for name, key, requested, effective, tolerance, show in limits:
        if not requested:
            continue
        configured = document.value(key) if document is not None else None
        met = effective is not None and effective >= requested * tolerance
        if met:
            outcome = "applied"
        elif path and configured is not None and configured >= requested:
            outcome = "pending-restart" if effective is not None else "unverifiable"
        elif path:
            outcome = "planned"
        elif effective is None:
            outcome = "unverifiable"
        else:
            outcome = "manual"
        settings.append({"name": name, "kind": "limit", "requested": show(requested), "effective": show(effective)
                         if effective is not None else "unknown", "outcome": outcome, "config_key": key,
                         "config_value": requested, "owned": bool(state and key in state["keys"])})
    if profile["disk_free_gib"]:
        requested = int(profile["disk_free_gib"] * GIB)
        effective = (probe or {}).get("disk_free_bytes")
        outcome = "unverifiable" if effective is None else "applied" if effective >= requested else "manual"
        settings.append({"name": "disk-free", "kind": "capacity", "requested": gib(requested),
                         "effective": gib(effective), "outcome": outcome})
    for feature in profile.get("kernel_features", ()):
        config = (probe or {}).get("kernel_config")
        value = None if config is None else config.get(KERNEL_FEATURES[feature], "not set")
        outcome = {"y": "available", "not set": "unavailable"}.get(value, "unverifiable")
        settings.append({"name": "kernel:" + feature, "kind": "kernel-feature", "requested": "built in",
                         "effective": {None: "unknown", "y": "built in", "m": "module, load state not observed"}
                         .get(value, value), "outcome": outcome})

    admin = ADMIN_SETTINGS.get("linux" if system.platform.startswith("linux") else system.platform)
    report = {
        "schema_version": SCHEMA_VERSION,
        "mode": mode,
        "client": client,
        "engine": engine,
        "profile": dict(name=profile_name, **{field: profile[field] for field in
                                              ("cpus", "memory_gib", "swap_gib", "disk_free_gib")},
                        kernel_features=list(profile.get("kernel_features", ()))),
        "config": {
            "route": "wslconfig" if path else "none",
            "reason": reason,
            "problem": problem,
            "scope": WSL_SCOPE if engine["backend"] == "desktop-windows-wsl2" else None,
            "owned_keys": sorted(state["keys"]) if state else [],
        },
        "managed_settings": "present" if admin and system.exists(admin) else "not found",
        "probe": None if probe is None else {"image_ran": probe["image_ran"]},
        "settings": settings,
        "writes": [],
    }
    report["restart"] = {"pending": any(item["outcome"] == "pending-restart" for item in settings),
                         "action": WSL_RESTART if path or engine["backend"] == "desktop-windows-wsl2" else None}
    report["planned_changes"] = ["%s=%s" % (item["config_key"],
                                            wslconfig.format_value(item["config_key"], item["config_value"]))
                                 for item in settings if item["outcome"] == "planned"]
    report["manual_steps"] = manual_steps(report, probe_image)
    report["satisfied"] = all(item["outcome"] in ("applied", "available") for item in settings)
    return report, path


def manual_steps(report, probe_image):
    """Exact operator steps for everything the script will not or cannot do."""
    backend = report["engine"]["backend"]
    settings = report["settings"]
    open_limits = [item for item in settings if item["kind"] == "limit" and item["outcome"] in ("manual", "unverifiable")]
    steps = []
    if open_limits and backend in ("desktop-macos", "desktop-windows-hyperv", "desktop-linux"):
        labels = {"cpus": "CPU limit", "memory": "Memory limit", "swap": "Swap"}
        steps.append("In Docker Desktop, open Settings > Resources > Advanced and set %s. Apply the change; Docker "
                     "Desktop restarts its VM and every container in it stops."
                     % ", ".join("%s to at least %s" % (labels[item["name"]], item["requested"]) for item in open_limits))
        if report["managed_settings"] == "present":
            steps.append("An administrator settings file is present. A setting it locks cannot be changed in the "
                         "dashboard; ask the administrator instead of working around it.")
    elif open_limits and backend == "desktop-windows-wsl2":
        lines = "; ".join("%s=%s" % (item["config_key"], wslconfig.format_value(item["config_key"], item["config_value"]))
                          for item in open_limits)
        steps.append("In %%UserProfile%%\\.wslconfig, under the one [wsl2] section, set: %s. Keep every other line. "
                     "%s. %s." % (lines, WSL_SCOPE[0].upper() + WSL_SCOPE[1:], WSL_RESTART))
    elif open_limits and backend == "remote":
        steps.append("Size the machine or VM that runs the selected daemon; nothing on this machine changes it. "
                     "The engine values above still describe what the tests will get.")
    elif open_limits and backend != "unreachable":
        steps.append("Give the engine at least the requested resources with the tool that manages it. This script "
                     "does not recognize the backend and changes nothing on it.")
    if backend == "unreachable":
        steps.append("Start the engine, or point DOCKER_HOST or the Docker context at the engine the tests use, "
                     "and run the script again.")
    if any(item["name"] == "disk-free" and item["outcome"] == "manual" for item in settings):
        steps.append("Free space is below the request. Raise the disk usage limit of the Docker Desktop VM "
                     "(Settings > Resources > Advanced; on the WSL 2 backend the virtual disk grows on demand) or "
                     "remove images and volumes you no longer need yourself. This script deletes nothing.")
    if report["restart"]["pending"]:
        steps.append(WSL_RESTART + ".")
    if report["probe"] and not report["probe"]["image_ran"]:
        steps.append("The probe image did not run. It must already be on the selected engine (nothing is pulled) "
                     "and contain cat and df.")
    if any(item["outcome"] == "unverifiable" for item in settings) and report["engine"]["reachable"] and not probe_image:
        steps.append("Swap, free disk and kernel features are not in the engine metadata. Pass --probe-image with "
                     "an image already on the engine that has cat and df to read them from a one-shot container.")
    for item in settings:
        if item["kind"] == "kernel-feature" and item["outcome"] == "unavailable":
            steps.append("%s is not in the engine kernel. That is not a limit: no profile, VM size or restart adds "
                         "it. Run the scenario on a Linux engine whose kernel has it, such as a managed Linux VM "
                         "or a remote engine." % item["name"])
    return steps


def render_text(report):
    engine = report["engine"]
    profile = report["profile"]
    lines = ["Docker Desktop setup: " + report["mode"],
             "Client: %s/%s%s" % (report["client"]["os"], report["client"]["architecture"],
                                  " (inside WSL)" if report["client"]["inside_wsl"] else ""),
             "Engine selected by: %s%s; transport %s (%s)"
             % (engine["selected_by"], " (%s)" % engine["context"] if engine["context"] else "",
                engine["transport"], engine["locality"])]
    if engine["reachable"]:
        lines.append("Engine: %s, engine %s, %s/%s, kernel %s, %s CPUs, %s memory"
                     % (engine["desktop_version"] or engine["operating_system"] or "unknown", engine["server_version"],
                        engine["os_type"], engine["architecture"], engine["kernel"], engine["cpus"],
                        gib(engine["memory_bytes"])))
    lines.append("Backend: %s (%s)" % (engine["backend"], BACKENDS[engine["backend"]]))
    lines.append("Profile: %s (cpus=%s memory=%sGiB swap=%sGiB disk-free=%sGiB kernel=%s)"
                 % (profile["name"], profile["cpus"], profile["memory_gib"], profile["swap_gib"],
                    profile["disk_free_gib"], ",".join(profile["kernel_features"]) or "-"))
    config = report["config"]
    lines.append("Config writes: %s" % ("%UserProfile%\\.wslconfig [wsl2]" if config["route"] == "wslconfig"
                                        else "none (%s)" % config["reason"]))
    if config["scope"]:
        lines.append("Scope: " + config["scope"])
    if config["owned_keys"]:
        lines.append("Script-owned keys: " + ", ".join(config["owned_keys"]))
    lines.append("Managed settings file: " + report["managed_settings"])
    lines.append("Settings:")
    for item in report["settings"]:
        lines.append("  %-16s requested %-10s effective %-32s %s"
                     % (item["name"], item["requested"], item["effective"], item["outcome"]))
    if report["planned_changes"]:
        lines.append("Planned [wsl2] changes (written only by `apply --confirm-wsl-global`): "
                     + ", ".join(report["planned_changes"]))
    lines.append("Written: " + (", ".join(report["writes"]) or "nothing"))
    lines.append("Restart pending: " + ("yes" if report["restart"]["pending"] else "no"))
    if report["manual_steps"]:
        lines.append("Manual steps:")
        lines.extend("  %d. %s" % (number, step) for number, step in enumerate(report["manual_steps"], 1))
    if report.get("refusal"):
        lines.append("Refused: " + report["refusal"])
    if report.get("failure"):
        lines.append("Failed: " + report["failure"])
    return "\n".join(lines) + "\n"


def emit(report, fmt, out):
    out.write(json.dumps(report, indent=2, sort_keys=True) + "\n" if fmt == "json" else render_text(report))


def parse_arguments(argv):
    parser = argparse.ArgumentParser(
        description="Plan, apply, verify or undo Docker Desktop resource settings for a Ceph fixture workload. "
                    "Without a command it plans and writes nothing.")
    parser.add_argument("command", nargs="?", default="plan", choices=("plan", "apply", "verify", "restore"))
    parser.add_argument("--profile", default="single-cluster", help="workload profile (default: %(default)s)")
    parser.add_argument("--profile-file", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                               "profiles.json"),
                        help="JSON file of profiles (default: the profiles.json next to this script)")
    parser.add_argument("--cpus", type=int, help="override the profile's CPU count")
    parser.add_argument("--memory-gib", type=float, help="override the profile's memory")
    parser.add_argument("--swap-gib", type=float, help="override the profile's swap; 0 requests none")
    parser.add_argument("--disk-free-gib", type=float, help="override the profile's free disk; 0 requests none")
    parser.add_argument("--kernel-feature", action="append", choices=sorted(KERNEL_FEATURES),
                        help="also require this engine kernel feature (repeatable)")
    parser.add_argument("--probe-image", help="image already on the engine, with cat and df, to read swap, free "
                                              "disk and the kernel configuration from one-shot containers")
    parser.add_argument("--confirm-wsl-global", action="store_true",
                        help="apply only: confirm that .wslconfig changes every WSL 2 distribution of the user")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def resolve_profile(arguments):
    profiles = load_profiles(arguments.profile_file)
    if arguments.profile not in profiles:
        raise ValueError("unknown profile %s (known: %s)" % (arguments.profile, ", ".join(sorted(profiles))))
    profile = dict(profiles[arguments.profile])
    for field in ("cpus", "memory_gib", "swap_gib", "disk_free_gib"):
        if getattr(arguments, field) is not None:
            profile[field] = getattr(arguments, field)
    profile["kernel_features"] = sorted(set(profile.get("kernel_features", ())) | set(arguments.kernel_feature or ()))
    check_profile(arguments.profile, profile)
    return profile


def main(argv=None, system=None, out=None):
    system = system or System()
    out = out or sys.stdout
    arguments = parse_arguments(sys.argv[1:] if argv is None else argv)
    try:
        profile = resolve_profile(arguments)
    except (OSError, ValueError, KeyError) as error:
        print("desktop_setup: %s" % error, file=sys.stderr)
        return EXIT_REFUSED
    report, path = build_report(system, arguments.command, arguments.profile, profile, arguments.probe_image)
    status = EXIT_OK
    if arguments.command == "verify":
        status = EXIT_OK if report["satisfied"] else EXIT_UNMET
    elif arguments.command == "apply":
        report, status = run_apply(system, arguments, profile, report, path)
    elif arguments.command == "restore":
        report, status = run_restore(system, arguments, profile, report)
    emit(report, arguments.format, out)
    return status


def rebuild(system, arguments, profile, extra):
    report, _ = build_report(system, arguments.command, arguments.profile, profile, arguments.probe_image)
    report.update(extra)
    return report


def run_apply(system, arguments, profile, report, path):
    planned = {item["config_key"]: item["config_value"] for item in report["settings"] if item["outcome"] == "planned"}
    if not path:
        open_items = [item for item in report["settings"] if item["kind"] == "limit" and item["outcome"] != "applied"]
        if not open_items:
            return report, EXIT_OK
        report["refusal"] = "no configuration was written: " + report["config"]["reason"]
        return report, EXIT_REFUSED
    if not planned:
        return report, EXIT_OK
    if not arguments.confirm_wsl_global:
        report["refusal"] = ("nothing was written. %s. Run apply again with --confirm-wsl-global to accept that scope"
                             % (WSL_SCOPE[0].upper() + WSL_SCOPE[1:]))
        return report, EXIT_REFUSED
    try:
        written = wslconfig.apply(path, planned, newline="\r\n")
    except wslconfig.Refusal as refusal:
        return rebuild(system, arguments, profile, {"refusal": "nothing was written: %s" % refusal}), EXIT_REFUSED
    except wslconfig.PartialFailure as failure:
        return rebuild(system, arguments, profile, {"failure": str(failure)}), EXIT_FAILED
    except OSError as error:
        return rebuild(system, arguments, profile,
                       {"failure": "the config file could not be written (%s); it is unchanged"
                                   % type(error).__name__}), EXIT_FAILED
    return rebuild(system, arguments, profile, {"writes": written}), EXIT_OK


def run_restore(system, arguments, profile, report):
    user_profile = system.environ.get("USERPROFILE")
    if system.platform != "win32" or not user_profile or not os.path.isabs(user_profile):
        report["refusal"] = ("restore undoes this script's .wslconfig edit and runs only with Python for Windows, "
                             "as the user whose profile holds the file")
        return report, EXIT_REFUSED
    try:
        restored = wslconfig.restore(os.path.join(user_profile, ".wslconfig"), newline="\r\n")
    except wslconfig.Refusal as refusal:
        report["refusal"] = "nothing was restored: %s" % refusal
        return report, EXIT_REFUSED
    except wslconfig.PartialFailure as failure:
        return rebuild(system, arguments, profile, {"failure": str(failure)}), EXIT_FAILED
    except OSError as error:
        report["failure"] = "the config file could not be restored (%s)" % type(error).__name__
        return report, EXIT_FAILED
    report = rebuild(system, arguments, profile, {"writes": restored})
    report["restart"] = {"pending": True, "action": WSL_RESTART}
    report["manual_steps"] = [WSL_RESTART.replace("then run this script with `verify`",
                                                  "for the restored values to take effect") + "."]
    return report, EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
