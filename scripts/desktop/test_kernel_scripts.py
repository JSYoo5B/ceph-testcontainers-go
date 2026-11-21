"""Execute the helper's real shell texts with substituted commands.

The scripts run in a container on the engine; here the commands they call are
replaced by small programs on PATH, so their output and above all their exit
status are those of the shipped text, not of a model of it.
"""
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import unittest

import kernel

SHELL = shutil.which("sh")
COMMANDS = {
    # The files of a synthetic engine VM. Any other path does not exist.
    "cat": r"""#!/bin/sh
case "$1" in
  /proc/sys/kernel/random/boot_id) [ -n "$NO_BOOT" ] && exit 1; echo boot-1;;
  /proc/sys/kernel/modules_disabled) echo 0;;
  /lib/modules/1.0/modules.dep) [ -n "$NO_DEP" ] && exit 1; printf '%s\n' 'kernel/drivers/block/nbd.ko.xz: kernel/x.ko' 'kernel/md/dm-delay.ko:';;
  /sys/module/dm_crypt/refcnt) echo 2;;
  /proc/sys/fs/aio-max-nr) echo 65536;;
  /proc/sys/net/ipv4/ip_local_port_range) printf '32768\t60999\n';;
  *) exit 1;;
esac
""",
    "zcat": "#!/bin/sh\n[ -n \"$NO_CONFIG\" ] && exit 1\nprintf '%s\\n' '# CONFIG_DM_DELAY is not set' CONFIG_FUSE_FS=y CONFIG_BLK_DEV_NBD=m\n",
    "cut": "#!/bin/sh\n[ -n \"$NO_MODULES\" ] && exit 1\n[ -n \"$EMPTY_MODULES\" ] && exit 0\nprintf '%s\\n' dm_crypt fakeowner\n",
    "uname": "#!/bin/sh\necho 1.0\n",
    "modprobe": "#!/bin/sh\nexit 0\n",
    # Enters nothing: runs the command after "--", or fails like a refused namespace entry.
    "nsenter": "#!/bin/sh\n[ -n \"$NO_ENTRY\" ] && exit 1\nwhile [ \"$1\" != -- ]; do shift; done\nshift\nexec \"$@\"\n",
}


@unittest.skipUnless(SHELL, "needs a POSIX sh")
class ScriptCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        for name, text in COMMANDS.items():
            (self.bin / name).write_text(text)
            (self.bin / name).chmod(stat.S_IRWXU)

    def run_script(self, script, *arguments, **environment):
        path = "%s%s%s" % (self.bin, os.pathsep, os.environ.get("PATH", os.defpath))
        return subprocess.run([SHELL, "-c", script, "sh"] + list(arguments), capture_output=True, text=True,
                              env=dict(environment, PATH=path), check=False)

    def records(self, *arguments, **environment):
        result = self.run_script(kernel.INSPECT_SCRIPT, *arguments, **environment)
        return result.returncode, [tuple(line.split("\t")) for line in result.stdout.splitlines()]


class InspectScriptTest(ScriptCase):
    def test_every_argument_order_exits_zero_with_the_same_base_records(self):
        base = None
        for arguments in ((), ("module:nbd",), ("module:dm_crypt",), ("param:fs/aio-max-nr",), ("param:no/such",),
                          ("param:fs/aio-max-nr", "module:nbd"), ("module:dm_crypt", "module:nbd"),
                          ("module:nbd", "param:no/such"), ("module:nbd", "module:dm_crypt")):
            with self.subTest(arguments=arguments):
                status, records = self.records(*arguments)
                self.assertEqual(status, 0)
                common = [record for record in records if record[0] not in ("refcnt", "param")]
                base = base or common
                self.assertEqual(common, base)
        self.assertEqual(base, [("boot", "boot-1"), ("modules_disabled", "0"), ("config", "CONFIG_FUSE_FS=y"),
                                ("config", "CONFIG_BLK_DEV_NBD=m"), ("modules", "read"), ("loaded", "dm_crypt"),
                                ("loaded", "fakeowner"), ("file", "kernel/drivers/block/nbd.ko.xz"),
                                ("file", "kernel/md/dm-delay.ko"), ("modprobe", "yes"), ("entered", "yes")])

    def test_optional_observations_are_records_or_nothing(self):
        _, records = self.records("module:nbd", "module:dm_crypt", "param:fs/aio-max-nr", "param:no/such",
                                  "param:net/ipv4/ip_local_port_range")
        self.assertEqual([record for record in records if record[0] in ("refcnt", "param")],
                         [("refcnt", "dm_crypt", "2"), ("param", "fs/aio-max-nr", "65536"),
                          ("param", "net/ipv4/ip_local_port_range", "32768 60999")])

    def test_unloaded_module_only_profile_reaches_the_helper_as_facts(self):
        # The whole path: the real script's status and output through Helper.inspect.
        test = self

        class System:
            def run(self, args, timeout=30):
                result = test.run_script(args[args.index("-c") + 1], *args[args.index("-c") + 3:])
                return result.returncode, result.stdout.encode()

        facts = kernel.Helper(System(), ["docker"], "busybox").inspect(["nbd"], {})
        self.assertIsNotNone(facts)
        self.assertEqual((facts["boot"], facts["entered"], facts["refcnt"], "nbd" in facts["files"],
                          "nbd" in facts["loaded"]), ("boot-1", True, {}, True, False))
        self.assertEqual(kernel.feature_status("nbd", facts, ())[0], "planned")

    def test_each_failed_read_only_drops_its_own_records(self):
        cases = {
            "NO_CONFIG": lambda kinds: self.assertNotIn("config", kinds),
            "NO_MODULES": lambda kinds: (self.assertNotIn("modules", kinds), self.assertNotIn("loaded", kinds)),
            "EMPTY_MODULES": lambda kinds: (self.assertIn("modules", kinds), self.assertNotIn("loaded", kinds)),
            "NO_DEP": lambda kinds: self.assertNotIn("file", kinds),
            "NO_ENTRY": lambda kinds: self.assertEqual(kinds & {"entered", "modprobe", "file", "param"}, set()),
        }
        for variable, check in cases.items():
            with self.subTest(variable=variable):
                status, records = self.records("param:fs/aio-max-nr", "module:nbd", **{variable: "1"})
                self.assertEqual(status, 0)
                kinds = {record[0] for record in records}
                self.assertIn("boot", kinds)
                check(kinds)

    def test_unreadable_boot_identity_is_an_empty_record_the_helper_rejects(self):
        status, records = self.records("module:nbd", NO_BOOT="1")
        self.assertEqual((status, records[0]), (0, ("boot", "")))


class WriteScriptTest(ScriptCase):
    def setUp(self):
        super().setUp()
        shutil.rmtree(self.bin)  # the write script runs with the real cat
        self.bin.mkdir()

    def test_written_value_that_reads_back_exits_zero(self):
        target = self.directory / "value"
        for content in ("1048576", '{"boot": "b", "modules": ["nbd"]}', "it's $HOME `x` \\n"):
            with self.subTest(content=content):
                self.assertEqual(self.run_script(kernel.WRITE_SCRIPT, str(target), content).returncode, 0)
                self.assertEqual(target.read_text(), content + "\n")

    def test_write_that_did_not_take_exits_nonzero(self):
        # /dev/null accepts the write and keeps nothing, like a kernel that ignores a value.
        missing = self.directory / "absent" / "value"
        for target in ("/dev/null", str(missing), str(self.directory)):
            with self.subTest(target=target):
                result = self.run_script(kernel.WRITE_SCRIPT, target, "1048576")
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
