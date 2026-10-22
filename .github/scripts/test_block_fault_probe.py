import errno
import importlib.util
import json
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "block_fault_probe", Path(__file__).with_name("block_fault_probe.py"))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)

Result = probe.Result
BACKING = {"dev": "259:1", "ino": 4242, "size": probe.SIZE_BYTES}
SECTORS = probe.SIZE_BYTES // probe.SECTOR


class FakeHost:
    def __init__(self):
        self.tools = {"losetup": "/usr/sbin/losetup", "dmsetup": "/usr/sbin/dmsetup"}
        self.uid = 0
        self.files = {}
        self.texts = {}
        self.dirs = set()
        self.holders = []
        self.nodes = {}
        self.now = 0.0
        self.write_cache = "write back"

    def which(self, name):
        return self.tools.get(name)

    def euid(self):
        return self.uid

    def platform(self):
        return "linux"

    def uname(self):
        return "6.8.0-test", "x86_64"

    def make_workdir(self, parent):
        path = parent + "/ctc-blockfault-test"
        self.dirs.add(path)
        return path

    def create_sparse(self, path, size):
        self.files[path] = dict(BACKING, size=size)
        return dict(self.files[path])

    def file_identity(self, path):
        return self.files.get(path)

    def block_rdev(self, path):
        return self.nodes.get(path)

    def read_text(self, path):
        return self.write_cache if path.endswith("/queue/write_cache") else None

    def write_text(self, path, text):
        assert path not in self.texts
        self.texts[path] = text

    def openers(self, major_minor):
        return list(self.holders)

    def remove_file(self, path):
        if path in self.texts:
            del self.texts[path]
        else:
            del self.files[path]

    def remove_dir(self, path):
        self.dirs.remove(path)

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeKernel:
    """A small device-mapper/loop model that answers the probe's commands."""

    def __init__(self, host):
        self.host = host
        self.targets = {"linear": "1.4.0", "striped": "1.6.0", "error": "1.6.0"}
        self.modules = {"dm_delay": "/lib/modules/dm-delay.ko", "dm_flakey": "/lib/modules/dm-flakey.ko"}
        self.loops = {}
        self.maps = {}
        self.data = probe.pattern("initial", probe.IO_LENGTH)
        self.calls = []
        # Hooks: command prefix (tuple) -> callable(argv) returning Result or None.
        self.hooks = {}
        self.reject = set()
        self.honor_delay = True
        self.error_mutates = False

    def run(self, argv, timeout):
        self.calls.append(list(argv))
        for prefix, hook in self.hooks.items():
            if tuple(argv[:len(prefix)]) == prefix:
                result = hook(argv)
                if result is not None:
                    return result
        if argv[0] == "losetup":
            return self.losetup(argv)
        if argv[0] == "dmsetup":
            return self.dmsetup(argv)
        if argv[0] == "modinfo":
            path = self.modules.get(argv[2])
            return self.ok(argv, path + "\n") if path else Result(argv, 1, "", "not found")
        if "io" in argv:
            return self.io(argv)
        raise AssertionError(argv)

    def ok(self, argv, out=""):
        return Result(argv, 0, out, "")

    def losetup(self, argv):
        if argv[1] == "--version":
            return self.ok(argv, "losetup from util-linux 2.39.3\n")
        if argv[1] == "--find":
            name = "/dev/loop%d" % len(self.loops)
            self.loops[name] = {"file": argv[3], "dev": "7:%d" % len(self.loops)}
            return self.ok(argv, name + "\n")
        if argv[1] == "--detach":
            self.loops.pop(argv[2])
            return self.ok(argv)
        if argv[1] == "--json":
            entries = []
            for name, loop in self.loops.items():
                ident = self.host.files.get(loop["file"], BACKING)
                entries.append({"name": name, "back-file": loop["file"],
                                "back-ino": ident["ino"], "back-maj:min": ident["dev"],
                                "maj:min": loop["dev"], "offset": 0, "sizelimit": 0,
                                "autoclear": False})
            return self.ok(argv, json.dumps({"loopdevices": entries}) if entries else "")
        raise AssertionError(argv)

    def table_ok(self, table):
        kinds = [line.split()[2] for line in table.splitlines()]
        kind = next((k for k in kinds if k != "linear"), "linear")
        if kind in self.reject or any(line in self.reject for line in table.splitlines()):
            return False
        if kind not in self.targets:
            module = {"delay": "dm_delay", "flakey": "dm_flakey"}.get(kind)
            if module not in self.modules:
                return False
            self.targets[kind] = "1.0.0"
        return True

    def dmsetup(self, argv):
        cmd = argv[1]
        if cmd == "version":
            return self.ok(argv, "Library version: 1.02\nDriver version:  4.48.0\n")
        if cmd == "targets":
            return self.ok(argv, "".join("%-16s v%s\n" % t for t in self.targets.items()))
        if cmd == "create":
            name, dm_uuid, table = argv[2], argv[4], argv[6]
            dev = "252:%d" % len(self.maps)
            self.maps[name] = {"uuid": dm_uuid, "dev": dev, "live": table, "inactive": None,
                               "suspended": False, "open": 0}
            self.host.nodes["/dev/mapper/" + name] = dev
            return self.ok(argv)
        if cmd == "udevcookies":
            return self.ok(argv, "Cookie       Semid      Value      Last semop time           Last change time\n")
        if cmd == "info" and argv[2] == "--columns":
            lines = ["%s|%s" % (n, m["uuid"]) for n, m in self.maps.items()]
            return self.ok(argv, "\n".join(lines) + "\n" if lines else "No devices found\n")
        name = argv[2] if cmd == "load" else argv[-1]
        mapping = self.maps.get(name)
        if mapping is None:
            return Result(argv, 1, "", "Device does not exist.\n")
        if cmd == "info":
            tables = " & ".join(t for t, v in (("LIVE", mapping["live"]),
                                               ("INACTIVE", mapping["inactive"])) if v)
            major, minor = mapping["dev"].split(":")
            text = ("Name:              %s\nState:             %s\nTables present:    %s\n"
                    "Open count:        %d\nMajor, minor:      %s, %s\nUUID: %s\n") % (
                name, "SUSPENDED" if mapping["suspended"] else "ACTIVE", tables or "None",
                mapping["open"], major, minor, mapping["uuid"])
            return self.ok(argv, text)
        if cmd == "table":
            key = "inactive" if "--inactive" in argv else "live"
            return self.ok(argv, (mapping[key] + "\n") if mapping[key] else "")
        if cmd == "status":
            return self.ok(argv, mapping["live"] + "\n")
        if cmd == "load":
            table = self.host.texts[argv[3]].strip()
            if not self.table_ok(table):
                return Result(argv, 1, "", "device-mapper: reload ioctl failed: Invalid argument\n")
            mapping["inactive"] = table
            return self.ok(argv)
        if cmd == "suspend":
            mapping["suspended"] = True
            return self.ok(argv)
        if cmd == "resume":
            if mapping["inactive"]:
                mapping["live"], mapping["inactive"] = mapping["inactive"], None
            mapping["suspended"] = False
            return self.ok(argv)
        if cmd == "remove":
            del self.maps[name]
            del self.host.nodes["/dev/mapper/" + name]
            return self.ok(argv)
        raise AssertionError(argv)

    def io(self, argv):
        opts = dict(zip(argv[4::2], argv[5::2]))
        name = opts["--device"].rsplit("/", 1)[1]
        sector = int(opts["--offset"]) // probe.SECTOR
        table = next(line.split() for line in self.maps[name]["live"].splitlines()
                     if int(line.split()[0]) <= sector < int(line.split()[0]) + int(line.split()[1]))
        op, seed = opts["--op"], opts["--seed"]
        out = {"op": op, "errno": None, "errno_name": None, "elapsed_s": 0.002}
        kind = table[2]
        failed = (kind == "error" or kind == "flakey" and (
            op == "write" and "error_writes" in table or op == "read" and "error_reads" in table))
        if kind == "delay" and self.honor_delay:
            out["elapsed_s"] = {"read": 0.301, "write": 0.602, "flush": 0.903}[op]
        if failed and op != "flush":
            out["errno"], out["errno_name"] = errno.EIO, "EIO"
        if op == "write" and (not failed or self.error_mutates):
            self.data = probe.pattern(seed, probe.IO_LENGTH)
        if op == "read" and not failed:
            out["sha256"] = probe.sha256(self.data)
        return self.ok(argv, json.dumps(out))


def make(allow=True, **kwargs):
    host = FakeHost()
    kernel = FakeKernel(host)
    for key, value in kwargs.items():
        setattr(kernel, key, value)
    p = probe.Probe(kernel, host, "/scratch", allow_target_autoload=allow,
                    script="block_fault_probe.py", python="python3")
    return p, kernel, host


def commands(kernel, *prefix):
    return [c for c in kernel.calls if tuple(c[:len(prefix)]) == prefix]


def timeout(argv):
    return Result(argv, None, "", "", timed_out=True, elapsed_s=15)


class ParserTests(unittest.TestCase):
    def test_targets(self):
        text = "flakey           v1.5.0\ndelay            v1.4.0\nerror            v1.6.0\n\n"
        self.assertEqual(probe.parse_targets(text),
                         {"flakey": "1.5.0", "delay": "1.4.0", "error": "1.6.0"})

    def test_dm_info_live_and_inactive_suspended(self):
        text = ("Name:              x\nState:             SUSPENDED\nRead Ahead:        256\n"
                "Tables present:    LIVE & INACTIVE\nOpen count:        1\n"
                "Event number:      0\nMajor, minor:      252, 3\nNumber of targets: 1\n"
                "UUID: CTC-BLOCKFAULT-1\n")
        info = probe.parse_dm_info(text)
        self.assertEqual(info["major_minor"], "252:3")
        self.assertTrue(info["suspended"] and info["live"] and info["inactive"])
        self.assertEqual(info["open_count"], 1)
        self.assertEqual(info["uuid"], "CTC-BLOCKFAULT-1")

    def test_dm_info_without_state_is_unparsed(self):
        self.assertIsNone(probe.parse_dm_info("Device does not exist.\n"))

    def test_dm_names(self):
        self.assertEqual(probe.parse_dm_names("No devices found\n"), [])
        self.assertEqual(probe.parse_dm_names("a|U1\nb|\n"),
                         [{"name": "a", "uuid": "U1"}, {"name": "b", "uuid": ""}])

    def test_losetup_types_and_deleted_suffix(self):
        text = json.dumps({"loopdevices": [
            {"name": "/dev/loop3", "back-file": "/w/backing.img (deleted)", "back-ino": "77",
             "back-maj:min": "259:1", "maj:min": "7:3", "offset": "0", "sizelimit": None}]})
        (entry,) = probe.parse_losetup(text)
        self.assertEqual(entry["back_file"], "/w/backing.img")
        self.assertEqual((entry["back_ino"], entry["back_dev"], entry["dev"]), (77, "259:1", "7:3"))
        self.assertEqual(probe.parse_losetup(""), [])

    def test_table_normalization(self):
        self.assertEqual(probe.normalize_table(" 0  8 linear 7:0 0\n"), "0 8 linear 7:0 0")

    def test_classify_delay(self):
        self.assertEqual(probe.classify_delay(0.31, 300, 0.002), "observed")
        self.assertEqual(probe.classify_delay(0.05, 300, 0.002), "not-observed")
        # A slow baseline cannot separate the delay from ordinary latency.
        self.assertEqual(probe.classify_delay(0.4, 300, 0.1), "unverified")
        self.assertEqual(probe.classify_delay(None, 300, 0.002), "unverified")


class EligibilityTests(unittest.TestCase):
    def test_missing_tool_creates_nothing(self):
        p, kernel, host = make()
        del host.tools["dmsetup"]
        report = p.execute()
        self.assertEqual(report["verdict"], "ineligible")
        self.assertIn("missing-tool:dmsetup", report["eligibility"]["reasons"])
        self.assertEqual(kernel.calls, [])
        self.assertEqual(host.files, {})

    def test_not_root(self):
        p, kernel, host = make()
        host.uid = 1000
        report = p.execute()
        self.assertEqual(report["eligibility"]["reasons"], ["not-root"])
        self.assertEqual(probe.EXIT_CODES[report["verdict"]], 0)

    def test_unregistered_target_without_autoload_is_ineligible(self):
        p, kernel, host = make(allow=False)
        report = p.execute()
        self.assertEqual(report["verdict"], "ineligible")
        self.assertIn("target-not-registered:delay", report["eligibility"]["reasons"])
        self.assertFalse(commands(kernel, "losetup", "--find"))
        self.assertNotIn("delay", kernel.targets)

    def test_absent_module_is_ineligible(self):
        p, kernel, host = make()
        del kernel.modules["dm_delay"]
        report = p.execute()
        self.assertIn("target-module-absent:delay", report["eligibility"]["reasons"])

    def test_missing_optional_target_skips_only_its_phases(self):
        p, kernel, host = make()
        del kernel.modules["dm_flakey"]
        report = p.execute()
        self.assertEqual(report["verdict"], "complete")
        effects = {ph["name"]: ph["effect"] for ph in report["phases"] if ph["name"] != "baseline"}
        self.assertEqual(effects["flakey-error-writes"], "skipped-ineligible")
        self.assertEqual(effects["delay"], "observed")
        self.assertFalse(any("flakey" in " ".join(c) for c in commands(kernel, "dmsetup", "load")))


class ProbeTests(unittest.TestCase):
    def test_complete_run(self):
        p, kernel, host = make()
        report = p.execute()
        self.assertEqual(report["verdict"], "complete", report["uncertain"] + report["failures"])
        phases = {ph["name"]: ph for ph in report["phases"]}
        self.assertEqual(phases["baseline"]["outcome"], "exact")
        for name in ("delay", "error", "flakey-error-writes", "flakey-error-reads"):
            self.assertEqual(phases[name]["effect"], "observed", name)
            self.assertEqual(phases[name]["restore"]["outcome"], "restored", name)
            self.assertEqual(phases[name]["install"]["installed_table"],
                             phases[name]["requested_fault"])
        delay = phases["delay"]["requested_fault"]
        start = probe.WINDOW_START // probe.SECTOR
        end = start + probe.WINDOW_LENGTH // probe.SECTOR
        self.assertEqual(delay.splitlines(), [
            "0 %d linear 7:0 0" % start,
            "%d %d delay 7:0 %d 300 7:0 %d 600 7:0 %d 900" % (start, end - start, start, start, start),
            "%d %d linear 7:0 %d" % (end, SECTORS - end, end)])
        self.assertEqual(host.texts, {})
        self.assertTrue(report["cleanup"]["udev_cookies"].startswith("Cookie"))
        self.assertEqual(report["cleanup"]["mapping"]["observed"], "absent")
        self.assertEqual(report["cleanup"]["loop"]["observed"], "unbound")
        self.assertEqual(report["cleanup"]["backing"], "removed")
        self.assertEqual((host.files, host.dirs, kernel.maps, kernel.loops), ({}, set(), {}, {}))
        # Non-forced, non-deferred removal.
        self.assertEqual(commands(kernel, "dmsetup", "remove"),
                         [["dmsetup", "remove", p.name]])
        self.assertNotIn("drop_writes", json.dumps(report["phases"]))

    def test_io_finishes_before_the_next_control(self):
        p, kernel, host = make()
        p.execute()
        sequence = [("io" if "io" in c else c[1]) for c in kernel.calls
                    if "io" in c or c[:2] in (["dmsetup", "load"], ["dmsetup", "suspend"],
                                              ["dmsetup", "resume"])]
        delay_load = sequence.index("load")
        # baseline: write, flush, read; delay: load, suspend, resume, three I/Os; restore.
        self.assertEqual(sequence[:delay_load], ["io", "io", "io"])
        self.assertEqual(sequence[delay_load:delay_load + 10],
                         ["load", "suspend", "resume", "io", "io", "io",
                          "load", "suspend", "resume", "io"])

    def test_suspend_timeout_retains_everything(self):
        p, kernel, host = make()

        def hook(argv):
            if "delay" in (kernel.maps[p.name]["inactive"] or ""):
                kernel.maps[p.name]["suspended"] = True
                return timeout(argv)
            return None
        kernel.hooks[("dmsetup", "suspend")] = hook
        report = p.execute()
        self.assertEqual(report["verdict"], "uncertain")
        self.assertIn("dm-suspend-timed-out:delay", report["uncertain"])
        transition = report["phases"][1]["install"]["transitions"][-1]
        self.assertTrue(transition["observed"]["info"]["suspended"])
        self.assertTrue(transition["observed"]["info"]["inactive"])
        self.assertFalse(commands(kernel, "dmsetup", "resume"))
        self.assertFalse(commands(kernel, "dmsetup", "remove"))
        self.assertFalse(commands(kernel, "losetup", "--detach"))
        self.assertEqual(report["cleanup"]["retained"], ["dm-mapping", "loop", "backing-file"])
        self.assertIn(p.backing, host.files)

    def test_successful_remove_with_mapping_still_present(self):
        p, kernel, host = make()
        kernel.hooks[("dmsetup", "remove")] = lambda argv: Result(argv, 0, "", "")
        report = p.execute()
        self.assertEqual(report["verdict"], "uncertain")
        self.assertEqual(report["cleanup"]["mapping"]["observed"], "present")
        self.assertFalse(commands(kernel, "losetup", "--detach"))
        self.assertIn(p.backing, host.files)

    def test_successful_detach_with_association_still_bound(self):
        p, kernel, host = make()
        kernel.hooks[("losetup", "--detach")] = lambda argv: Result(argv, 0, "", "")
        report = p.execute()
        self.assertEqual(report["cleanup"]["loop"]["observed"], "bound")
        self.assertEqual(report["cleanup"]["retained"], ["loop", "backing-file"])
        self.assertIn(p.backing, host.files)

    def test_lingering_opener_blocks_removal_and_is_recorded(self):
        p, kernel, host = make()
        host.holders = [{"pid": 4321, "comm": "systemd-udevd"}]
        original = kernel.dmsetup

        def dmsetup(argv):
            if argv[1] == "info" and argv[2] == p.name and p.report["cleanup"]:
                kernel.maps[p.name]["open"] = 1
            return original(argv)
        kernel.dmsetup = dmsetup
        report = p.execute()
        mapping = report["cleanup"]["mapping"]
        self.assertEqual(mapping["skipped"], "suspended-or-open")
        self.assertEqual(mapping["before"]["openers"], host.holders)
        self.assertGreaterEqual(host.now, probe.OPEN_WAIT)
        self.assertFalse(commands(kernel, "dmsetup", "remove"))
        self.assertEqual(report["verdict"], "uncertain")

    def test_opener_that_closes_within_the_wait_allows_removal(self):
        p, kernel, host = make()
        original = kernel.dmsetup
        state = {"polls": 0}

        def dmsetup(argv):
            if argv[1] == "info" and argv[2] == p.name and p.report["cleanup"]:
                state["polls"] += 1
                kernel.maps[p.name]["open"] = 1 if state["polls"] < 3 else 0
            return original(argv)
        kernel.dmsetup = dmsetup
        report = p.execute()
        self.assertEqual(report["verdict"], "complete")
        self.assertEqual(report["cleanup"]["mapping"]["observed"], "absent")

    def test_unsupported_optional_feature_is_not_a_failure(self):
        p, kernel, host = make()
        start = probe.WINDOW_START // probe.SECTOR
        kernel.reject.add("%d %d flakey 7:0 %d 0 3600 1 error_reads" % (
            start, probe.WINDOW_LENGTH // probe.SECTOR, start))
        report = p.execute()
        self.assertEqual(report["verdict"], "complete")
        phase = [ph for ph in report["phases"] if ph["name"] == "flakey-error-reads"][0]
        self.assertEqual((phase["install"]["outcome"], phase["effect"]), ("rejected", "not-installed"))

    def test_rejected_required_target_is_ineligible_after_cleanup(self):
        p, kernel, host = make()
        kernel.reject.add("delay")
        report = p.execute()
        self.assertEqual(report["verdict"], "ineligible")
        self.assertIn("table-rejected:delay", report["eligibility"]["reasons"])
        self.assertEqual((kernel.maps, kernel.loops, host.files), ({}, {}, {}))

    def test_delay_not_observed_fails(self):
        p, kernel, host = make(honor_delay=False)
        report = p.execute()
        self.assertEqual(report["verdict"], "failed")
        self.assertIn("effect-not-observed:delay", report["failures"])
        # Restoration and cleanup still run.
        self.assertEqual(report["phases"][1]["restore"]["outcome"], "restored")
        self.assertEqual(kernel.maps, {})

    def test_error_that_mutates_is_caught_by_readback(self):
        p, kernel, host = make(error_mutates=True)
        report = p.execute()
        self.assertEqual(report["verdict"], "failed")
        self.assertIn("restore-readback-differs:error", report["failures"])

    def test_foreign_identity_stops_control(self):
        p, kernel, host = make()

        def hook(argv):
            if "delay" in (kernel.maps[p.name]["inactive"] or ""):
                kernel.maps[p.name]["uuid"] = "FOREIGN"
            return None
        kernel.hooks[("dmsetup", "resume")] = hook
        report = p.execute()
        self.assertEqual(report["verdict"], "uncertain")
        self.assertIn("mapping-identity-changed:delay-resume", report["uncertain"])
        self.assertEqual(len(commands(kernel, "dmsetup", "load")), 1)
        self.assertFalse(commands(kernel, "dmsetup", "remove"))

    def test_io_timeout_leaves_mapping_in_place(self):
        p, kernel, host = make()
        original = kernel.io

        def io(argv):
            if "delay" in kernel.maps[p.name]["live"]:
                return Result(argv, None, "", "", timed_out=True, reaped=False)
            return original(argv)
        kernel.io = io
        report = p.execute()
        self.assertEqual(report["verdict"], "uncertain")
        self.assertIn("io-read-timed-out", report["uncertain"])
        # The delay table stays installed: no restore, removal or detach.
        self.assertEqual(len(commands(kernel, "dmsetup", "resume")), 1)
        self.assertFalse(commands(kernel, "dmsetup", "remove"))
        self.assertFalse(commands(kernel, "losetup", "--detach"))

    def test_create_refused_releases_loop_and_file(self):
        p, kernel, host = make()
        kernel.hooks[("dmsetup", "create")] = lambda argv: Result(argv, 1, "", "ioctl failed")
        report = p.execute()
        self.assertEqual(report["verdict"], "failed")
        self.assertEqual((kernel.loops, host.files), ({}, {}))

    def test_wrong_device_node_is_not_used(self):
        p, kernel, host = make()
        original = kernel.dmsetup

        def dmsetup(argv):
            result = original(argv)
            if argv[1] == "create":
                host.nodes["/dev/mapper/" + p.name] = "8:0"
            return result
        kernel.dmsetup = dmsetup
        report = p.execute()
        self.assertIn("dm-node-missing", report["failures"])
        self.assertFalse([c for c in kernel.calls if "io" in c])


if __name__ == "__main__":
    unittest.main()
