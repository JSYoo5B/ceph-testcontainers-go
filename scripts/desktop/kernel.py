"""Inspect and prepare the engine kernel through one-shot helper containers.

The kernel that matters is the one of the VM or host the selected Docker
daemon runs on. A privileged helper container in the daemon's PID namespace
enters the namespaces of its PID 1 to read which modules exist, to load one
that exists but is not loaded, and to raise a kernel parameter. Nothing is
built, downloaded or pulled, and nothing here survives a restart of that VM.
What this module changed is recorded in a labelled Docker volume on the same
engine, so undoing it works from any client of that engine.
"""

import json
import re

# feature: (kernel build option, module name). Each is something a fixture or
# test of this repository uses from the engine kernel; docs/guides/desktop-setup.md
# names the consumer of each.
FEATURES = {
    "netem": ("CONFIG_NET_SCH_NETEM", "sch_netem"),
    "prio": ("CONFIG_NET_SCH_PRIO", "sch_prio"),
    "u32": ("CONFIG_NET_CLS_U32", "cls_u32"),
    "packet": ("CONFIG_PACKET", "af_packet"),
    "loop": ("CONFIG_BLK_DEV_LOOP", "loop"),
    "dm": ("CONFIG_BLK_DEV_DM", "dm_mod"),
    "dm-delay": ("CONFIG_DM_DELAY", "dm_delay"),
    "dm-crypt": ("CONFIG_DM_CRYPT", "dm_crypt"),
    "fuse": ("CONFIG_FUSE_FS", "fuse"),
    "nbd": ("CONFIG_BLK_DEV_NBD", "nbd"),
    "libceph": ("CONFIG_CEPH_LIB", "libceph"),
    "krbd": ("CONFIG_BLK_DEV_RBD", "rbd"),
    "cephfs-kernel": ("CONFIG_CEPH_FS", "ceph"),
}
PARAMETER_NAME = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_-]+)+$")
STATE_VOLUME = "ceph-testcontainers-desktop-setup-state"
STATE_LABEL = "org.ceph-testcontainers.desktop-setup=kernel-state"
STATE_VERSION = 1
# Backends whose kernel belongs to a VM that Docker Desktop manages on this
# machine. The kernel of a plain Linux engine is the host's own, and a remote
# engine's belongs to another machine: both are left to their operator.
PREPARABLE_BACKENDS = ("desktop-macos", "desktop-windows-wsl2", "desktop-windows-hyperv", "desktop-linux")

# Arguments are "module:<name>" and "param:<proc path>". Output is one
# tab-separated record per line.
_INSPECT = r'''
printf 'boot\t%s\n' "$(cat /proc/sys/kernel/random/boot_id)"
printf 'modules_disabled\t%s\n' "$(cat /proc/sys/kernel/modules_disabled 2>/dev/null)"
(zcat /proc/config.gz 2>/dev/null || true) | grep -E '^CONFIG_[A-Z0-9_]+=' | sed 's/^/config\t/'
cut -d' ' -f1 /proc/modules 2>/dev/null | sed 's/^/loaded\t/'
nsenter -t 1 -m -- sh -c 'cat "/lib/modules/$(uname -r)/modules.dep"' 2>/dev/null | sed 's/:.*//; s/^/file\t/'
nsenter -t 1 -m -- sh -c 'command -v modprobe' >/dev/null 2>&1 && printf 'modprobe\tyes\n'
nsenter -t 1 -m -- true 2>/dev/null && printf 'entered\tyes\n'
for item in "$@"; do
  case "$item" in
    module:*) name=${item#module:}; [ -e "/sys/module/$name/refcnt" ] && printf 'refcnt\t%s\t%s\n' "$name" "$(cat "/sys/module/$name/refcnt")";;
    param:*) path=${item#param:}; printf 'param\t%s\t%s\n' "$path" "$(nsenter -t 1 -n -i -u -- cat "/proc/sys/$path" 2>/dev/null | tr '\t\n' '  ')";;
  esac
done
'''


def module_name(name):
    return name.replace("-", "_")


def parameter_path(name):
    return name.replace(".", "/")


class Helper:
    """Runs the one-shot containers of the kernel step on the selected engine."""

    def __init__(self, system, command, image):
        self.system = system
        self.command = list(command)
        self.image = image

    def _run(self, options, program, *arguments):
        status, output = self.system.run(
            self.command + ["run", "--rm", "--pull=never", "--network", "none"] + list(options)
            + ["--entrypoint", program, self.image] + list(arguments), 60)
        return status, output.decode("utf-8", "replace")

    def _privileged(self, program, *arguments):
        return self._run(["--privileged", "--pid=host"], program, *arguments)

    def inspect(self, modules, parameters):
        """Facts of the engine kernel, or None when the helper could not enter the VM."""
        status, output = self._privileged(
            "sh", "-c", _INSPECT, "sh", *(["module:" + module_name(name) for name in modules]
                                          + ["param:" + parameter_path(name) for name in parameters]))
        facts = {"boot": None, "modules_disabled": False, "config": {}, "loaded": set(), "files": set(),
                 "modprobe": False, "entered": False, "refcnt": {}, "parameters": {}}
        for line in output.splitlines():
            fields = line.split("\t")
            if fields[0] == "boot" and len(fields) == 2:
                facts["boot"] = fields[1].strip() or None
            elif fields[0] == "modules_disabled" and len(fields) == 2:
                facts["modules_disabled"] = fields[1].strip() == "1"
            elif fields[0] == "config" and len(fields) == 2 and "=" in fields[1]:
                option, value = fields[1].split("=", 1)
                facts["config"][option] = value
            elif fields[0] == "loaded" and len(fields) == 2:
                facts["loaded"].add(module_name(fields[1].strip()))
            elif fields[0] == "file" and len(fields) == 2:
                facts["files"].add(module_name(fields[1].rsplit("/", 1)[-1].split(".ko", 1)[0]))
            elif fields[0] in ("modprobe", "entered"):
                facts[fields[0]] = True
            elif fields[0] == "refcnt" and len(fields) == 3 and fields[2].strip().isdigit():
                facts["refcnt"][fields[1]] = int(fields[2])
            elif fields[0] == "param" and len(fields) == 3:
                facts["parameters"][fields[1].replace("/", ".")] = fields[2].strip()
        if status != 0 or not facts["boot"] or not facts["entered"]:
            return None
        return facts

    def load(self, module):
        return self._privileged("nsenter", "-t", "1", "-m", "--", "modprobe", module_name(module))[0] == 0

    def unload(self, module):
        return self._privileged("nsenter", "-t", "1", "-m", "--", "modprobe", "-r", module_name(module))[0] == 0

    def set_parameter(self, name, value):
        return self._privileged("nsenter", "-t", "1", "-n", "-i", "-u", "--", "sh", "-c",
                                'printf "%s\\n" "$2" > "/proc/sys/$1"', "sh", parameter_path(name),
                                str(value))[0] == 0

    def read_state(self):
        """(state, problem): the recorded changes, or why the record cannot be used."""
        status, output = self.system.run(self.command + ["volume", "inspect", STATE_VOLUME, "--format", "{{json .}}"])
        if status != 0:
            return None, None
        try:
            labels = json.loads(output.decode("utf-8", "replace")).get("Labels") or {}
        except ValueError:
            labels = {}
        key, value = STATE_LABEL.split("=")
        if labels.get(key) != value:
            return None, "a Docker volume with the state volume's name exists and is not this tool's"
        status, output = self._run(["-v", STATE_VOLUME + ":/state:ro"], "cat", "/state/state.json")
        try:
            state = json.loads(output) if status == 0 else None
            valid = (state["version"] == STATE_VERSION and isinstance(state["boot"], str)
                     and all(isinstance(name, str) for name in state["modules"])
                     and all(PARAMETER_NAME.match(name) and isinstance(entry["previous"], int)
                             and isinstance(entry["written"], int) for name, entry in state["parameters"].items()))
        except (ValueError, KeyError, TypeError, AttributeError):
            valid = False
        if not valid:
            return None, "the state volume of this tool is unreadable"
        return state, None

    def write_state(self, state):
        status, _ = self.system.run(self.command + ["volume", "create", "--label", STATE_LABEL, STATE_VOLUME])
        if status != 0:
            return False
        return self._run(["-v", STATE_VOLUME + ":/state"], "sh", "-c", 'printf "%s" "$1" > /state/state.json', "sh",
                         json.dumps(state, sort_keys=True))[0] == 0

    def remove_state(self):
        return self.system.run(self.command + ["volume", "rm", STATE_VOLUME])[0] == 0


def parameter_value(facts, name):
    """The single whole number a parameter holds, or None."""
    text = facts["parameters"].get(name, "")
    return int(text) if text.isdigit() else None


def feature_status(feature, facts, owned):
    """(outcome, detail, persists) of one kernel feature on the inspected engine."""
    option, module = FEATURES[feature]
    built = facts["config"].get(option) if facts["config"] else None
    if built == "y":
        return "available", "built in", True
    if module in facts["loaded"]:
        if module in owned:
            return "applied", "loaded by this tool; lost when the engine VM restarts", False
        return "available", "module already loaded", None
    if module in facts["files"]:
        if facts["modules_disabled"]:
            return "unavailable", "module present, module loading disabled", None
        if not facts["modprobe"]:
            return "unavailable", "module present, no modprobe in the engine VM", None
        return "planned", "module present in the engine VM, not loaded", False
    if not facts["config"]:
        return "unverifiable", "kernel configuration not readable", None
    if built == "m":
        return "unavailable", "built as a module, file not in the engine VM", None
    return "unavailable", "not built, no module file in the engine VM", None


def parameter_status(name, requested, facts, state):
    """(outcome, detail, persists, current) of one kernel parameter."""
    current = parameter_value(facts, name)
    if current is None:
        return "unverifiable", "not readable as a single whole number", None, None
    entry = (state or {}).get("parameters", {}).get(name)
    if current >= requested:
        if entry and entry["written"] == current:
            return "applied", "set by this tool; lost when the engine VM restarts", False, current
        return "applied", "already at least the requested value", None, current
    return "planned", "below the requested value", False, current


def current_state(helper, facts):
    """(state of this boot or None, problem). A record of an earlier boot is void."""
    state, problem = helper.read_state()
    if state is not None and state["boot"] != facts["boot"]:
        return None, None
    return state, problem


def apply(helper, features, parameters):
    """Load the present, unloaded modules of features and raise parameters.

    Returns (actions, error). Each action is {item, result}; error is a
    sentence when nothing more could be done, in which case every change of
    this call has been put back where the kernel allows it.
    """
    facts = helper.inspect([FEATURES[name][1] for name in features], parameters)
    if facts is None:
        return [], "the helper image could not enter the engine VM's namespaces"
    state, problem = current_state(helper, facts)
    if problem:
        return [], problem
    state = state or {"version": STATE_VERSION, "boot": facts["boot"], "modules": [], "parameters": {}}
    for name, entry in state["parameters"].items():
        if parameter_value(facts, name) != entry["written"]:
            return [], ("kernel parameter %s changed after this tool set it; run restore-kernel first" % name)
    actions, undo = [], []
    for feature in features:
        module = FEATURES[feature][1]
        if feature_status(feature, facts, state["modules"])[0] != "planned":
            continue
        if helper.load(module):
            state["modules"].append(module)
            undo.append(("module", module))
            actions.append({"item": "kernel:" + feature, "result": "loaded"})
        else:
            actions.append({"item": "kernel:" + feature, "result": "failed"})
    for name, requested in parameters.items():
        outcome, _, _, current = parameter_status(name, requested, facts, state)
        if outcome != "planned":
            continue
        if helper.set_parameter(name, requested):
            state["parameters"].setdefault(name, {"previous": current})["written"] = requested
            undo.append(("parameter", name, current))
            actions.append({"item": "sysctl:" + name, "result": "set"})
        else:
            actions.append({"item": "sysctl:" + name, "result": "failed"})
    if undo and not helper.write_state(state):
        kept = []
        for change in undo:
            done = helper.unload(change[1]) if change[0] == "module" else helper.set_parameter(change[1], change[2])
            if not done:
                kept.append(change[1])
        return [], ("the record of the changes could not be written, so they were put back"
                    + (", except %s" % ", ".join(kept) if kept else ""))
    return actions, None


def restore(helper):
    """Undo what apply recorded on this boot of the engine VM.

    Returns (actions, error). A parameter that no longer holds the value this
    tool wrote is kept, and so is a module that is in use or that the kernel
    cannot unload; each is reported and the record is removed either way.
    """
    state, problem = helper.read_state()
    if problem:
        return [], problem
    if state is None:
        return [], "this tool has no recorded kernel change on the selected engine"
    facts = helper.inspect(state["modules"], state["parameters"])
    if facts is None:
        return [], "the helper image could not enter the engine VM's namespaces"
    actions = []
    if state["boot"] != facts["boot"]:
        actions.append({"item": "record", "result": "void: the engine VM restarted since, so the changes are gone"})
    else:
        for name, entry in sorted(state["parameters"].items()):
            if parameter_value(facts, name) != entry["written"]:
                result = "kept: changed after this tool set it"
            elif helper.set_parameter(name, entry["previous"]):
                result = "restored"
            else:
                result = "kept: writing the previous value failed"
            actions.append({"item": "sysctl:" + name, "result": result})
        for module in state["modules"]:
            if module not in facts["loaded"]:
                result = "already unloaded"
            elif module not in facts["refcnt"]:
                result = "kept: this kernel cannot unload modules; it stays until the engine VM restarts"
            elif facts["refcnt"][module] != 0:
                result = "kept: in use"
            elif helper.unload(module):
                result = "unloaded"
            else:
                result = "kept: unloading failed"
            actions.append({"item": "module:" + module, "result": result})
    if not helper.remove_state():
        return actions, "the state volume could not be removed; remove the Docker volume %s by hand" % STATE_VOLUME
    return actions, None
