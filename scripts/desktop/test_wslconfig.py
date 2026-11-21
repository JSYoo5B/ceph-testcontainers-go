"""Exercise the .wslconfig editor against files in temporary directories."""
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

import wslconfig

GB = 1 << 30
WANTED = {"memory": 8 * GB, "processors": 4}


class FailingFiles(wslconfig.Files):
    """Fail the write of every path that ends with one of the given suffixes."""

    def __init__(self, *suffixes):
        self.suffixes = suffixes
        self.writes = []

    def write_atomic(self, path, data):
        if path.endswith(self.suffixes):
            raise OSError("injected failure")
        self.writes.append(path)
        super().write_atomic(path, data)


class WslConfigCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.path = str(self.directory / ".wslconfig")

    def write(self, data):
        Path(self.path).write_bytes(data)

    def read(self):
        return Path(self.path).read_bytes()

    def names(self):
        return sorted(entry.name for entry in self.directory.iterdir())

    def state(self):
        return json.loads(Path(wslconfig.state_path(self.path)).read_text())


class SizeTest(unittest.TestCase):
    def test_sizes_parse_with_and_without_units(self):
        self.assertEqual(wslconfig.parse_size("8GB"), 8 * GB)
        self.assertEqual(wslconfig.parse_size(" 512 mb "), 512 << 20)
        self.assertEqual(wslconfig.parse_size("1073741824"), GB)
        for text in ("", "8G", "eight", "-1GB", "4GB # note", "1.5GB"):
            self.assertIsNone(wslconfig.parse_size(text), text)

    def test_sizes_format_exactly(self):
        self.assertEqual(wslconfig.format_size(8 * GB), "8GB")
        self.assertEqual(wslconfig.format_size(6 * GB + (512 << 20)), "6656MB")
        self.assertEqual(wslconfig.parse_size(wslconfig.format_size(GB + 1)), GB + 1)


class ApplyTest(WslConfigCase):
    def test_missing_file_is_created_with_only_the_wanted_keys(self):
        self.assertEqual(wslconfig.apply(self.path, WANTED, newline="\r\n"), ["memory", "processors"])
        self.assertEqual(self.read(), b"[wsl2]\r\nmemory=8GB\r\nprocessors=4\r\n")
        self.assertTrue(self.state()["created_file"])
        # Nothing existed, so there is nothing to back up.
        self.assertEqual(self.names(), [".wslconfig", ".wslconfig" + wslconfig.STATE_SUFFIX])

    def test_unrelated_content_and_key_spelling_are_preserved(self):
        original = (b"# my settings\n[wsl2]\nkernelCommandLine = vsyscall=emulate\n  Memory = 2GB\n"
                    b"swapfile=C:\\\\temp\\\\swap.vhdx\n\n[experimental]\nsparseVhd=true\n")
        self.write(original)
        wslconfig.apply(self.path, WANTED)
        self.assertEqual(self.read(), b"# my settings\n[wsl2]\nkernelCommandLine = vsyscall=emulate\n  Memory = 8GB\n"
                                      b"swapfile=C:\\\\temp\\\\swap.vhdx\nprocessors=4\n\n[experimental]\nsparseVhd=true\n")
        self.assertEqual(Path(wslconfig.backup_path(self.path)).read_bytes(), original)
        self.assertEqual(self.state()["keys"]["memory"]["previous"], "  Memory = 2GB")

    def test_section_is_appended_when_absent_and_the_last_line_is_terminated(self):
        self.write(b"[experimental]\r\nsparseVhd=true")
        wslconfig.apply(self.path, {"memory": 8 * GB})
        self.assertEqual(self.read(), b"[experimental]\r\nsparseVhd=true\r\n[wsl2]\r\nmemory=8GB\r\n")

    def test_byte_order_mark_encoding_and_line_endings_are_preserved(self):
        text = "# caf\u00e9\r\n[wsl2]\r\nmemory=2GB\r\n"
        for bom, encoding in ((b"\xef\xbb\xbf", "utf-8"), (b"\xff\xfe", "utf-16-le"), (b"\xfe\xff", "utf-16-be"),
                              (b"", "utf-8")):
            with self.subTest(encoding=encoding, bom=bom):
                for name in self.names():
                    os.remove(self.directory / name)
                self.write(bom + text.encode(encoding))
                wslconfig.apply(self.path, WANTED)
                expected = "# caf\u00e9\r\n[wsl2]\r\nmemory=8GB\r\nprocessors=4\r\n"
                self.assertEqual(self.read(), bom + expected.encode(encoding))
                wslconfig.restore(self.path)
                self.assertEqual(self.read(), bom + text.encode(encoding))

    def test_mixed_line_endings_keep_every_existing_ending(self):
        self.write(b"[wsl2]\nmemory=2GB\r\nkernelCommandLine=quiet\n")
        wslconfig.apply(self.path, WANTED)
        self.assertEqual(self.read(), b"[wsl2]\nmemory=8GB\r\nkernelCommandLine=quiet\nprocessors=4\n")

    def test_second_apply_writes_nothing(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, WANTED)
        before = {name: (self.directory / name).read_bytes() for name in self.names()}
        files = FailingFiles()
        self.assertEqual(wslconfig.apply(self.path, WANTED, files=files), [])
        self.assertEqual(files.writes, [])
        self.assertEqual({name: (self.directory / name).read_bytes() for name in self.names()}, before)

    def test_a_value_that_is_already_enough_is_not_lowered_or_owned(self):
        self.write(b"[wsl2]\nmemory=32GB\nprocessors=2\n")
        self.assertEqual(wslconfig.apply(self.path, WANTED), ["processors"])
        self.assertEqual(self.read(), b"[wsl2]\nmemory=32GB\nprocessors=4\n")
        self.assertEqual(sorted(self.state()["keys"]), ["processors"])

    def test_a_larger_request_keeps_the_first_previous_value(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, {"memory": 8 * GB})
        wslconfig.apply(self.path, {"memory": 16 * GB, "swap": 2 * GB})
        self.assertEqual(self.read(), b"[wsl2]\nmemory=16GB\nswap=2GB\n")
        self.assertEqual(self.state()["keys"]["memory"], {"previous": "memory=2GB", "written": "memory=16GB"})
        self.assertEqual(Path(wslconfig.backup_path(self.path)).read_bytes(), b"[wsl2]\nmemory=2GB\n")
        wslconfig.restore(self.path)
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")

    def test_malformed_and_ambiguous_files_are_refused_unchanged(self):
        cases = {
            "malformed": (b"[wsl2]\nthis is not a setting\n", b"memory=2GB\n[wsl2]\n", b"[wsl2\nmemory=2GB\n"),
            "ambiguous": (b"[wsl2]\nmemory=2GB\n[WSL2]\nprocessors=2\n", b"[wsl2]\nmemory=2GB\nMemory=4GB\n",
                          b"[wsl2]\nmemory=lots\n", b"[wsl2]\nmemory=4GB # note\n", b"[wsl2]\nprocessors=two\n"),
            "encoding": (b"\xff\xfe[\x00w", b"[wsl2]\nmemory=\xff\n", "[wsl2]\n".encode("utf-16-le")),
        }
        for code, contents in cases.items():
            for content in contents:
                with self.subTest(code=code, content=content):
                    self.write(content)
                    with self.assertRaises(wslconfig.Refusal) as raised:
                        wslconfig.apply(self.path, WANTED)
                    self.assertEqual(raised.exception.code, code)
                    self.assertEqual(self.read(), content)
                    self.assertEqual(self.names(), [".wslconfig"])

    def test_refusal_messages_do_not_repeat_file_content_or_paths(self):
        self.write(b"[wsl2]\nmemory=secret-value\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, WANTED)
        self.assertNotIn("secret-value", str(raised.exception))
        self.assertNotIn(str(self.directory), str(raised.exception))

    def test_a_file_that_is_not_an_editable_regular_file_is_refused(self):
        target = self.directory / "managed.conf"
        target.write_bytes(b"[wsl2]\nmemory=2GB\n")
        os.symlink(target, self.path)
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, WANTED)
        self.assertEqual(raised.exception.code, "not-writable")
        self.assertEqual(target.read_bytes(), b"[wsl2]\nmemory=2GB\n")
        os.remove(self.path)
        os.mkdir(self.path)
        with self.assertRaises(wslconfig.Refusal):
            wslconfig.apply(self.path, WANTED)
        os.rmdir(self.path)
        self.write(b"[wsl2]\nmemory=2GB\n")
        os.chmod(self.path, stat.S_IRUSR)
        if not os.access(self.path, os.W_OK):  # A privileged user can write regardless of the mode.
            with self.assertRaises(wslconfig.Refusal) as raised:
                wslconfig.apply(self.path, WANTED)
            self.assertEqual(raised.exception.code, "not-writable")
            self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")

    def test_write_replaces_atomically_and_leaves_no_temporary_file(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        os.chmod(self.path, 0o640)
        replaced = []
        real = os.replace

        def replace(source, target):
            # The whole new content is in the temporary file before the swap.
            replaced.append((os.path.dirname(source), Path(source).read_bytes(), target))
            real(source, target)

        with patch("os.replace", replace):
            wslconfig.apply(self.path, {"memory": 8 * GB})
        self.assertEqual(replaced[0], (str(self.directory), b"[wsl2]\nmemory=8GB\n", self.path))
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o640)
        self.assertEqual(self.names(), [".wslconfig", ".wslconfig" + wslconfig.BACKUP_SUFFIX,
                                        ".wslconfig" + wslconfig.STATE_SUFFIX])

    def test_failed_swap_leaves_the_config_and_no_tool_files(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        with patch("os.replace", side_effect=OSError("injected failure")):
            with self.assertRaises(OSError):
                wslconfig.apply(self.path, WANTED)
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")
        self.assertEqual(self.names(), [".wslconfig"])

    def test_failed_state_write_rolls_the_config_back(self):
        for original in (b"[wsl2]\nmemory=2GB\n", None):
            with self.subTest(original=original):
                for name in self.names():
                    os.remove(self.directory / name)
                if original is not None:
                    self.write(original)
                with self.assertRaises(wslconfig.PartialFailure) as raised:
                    wslconfig.apply(self.path, WANTED, files=FailingFiles(wslconfig.STATE_SUFFIX))
                self.assertTrue(raised.exception.rolled_back)
                self.assertEqual(self.names(), [] if original is None else [".wslconfig"])
                if original is not None:
                    self.assertEqual(self.read(), original)

    def test_failed_roll_back_is_reported_and_keeps_the_backup(self):
        self.write(b"[wsl2]\nmemory=2GB\n")

        class Files(wslconfig.Files):
            calls = 0

            def write_atomic(self, path, data):
                self.calls += 1
                if self.calls > 1:
                    raise OSError("injected failure")
                super().write_atomic(path, data)

        with self.assertRaises(wslconfig.PartialFailure) as raised:
            wslconfig.apply(self.path, WANTED, files=Files())
        self.assertFalse(raised.exception.rolled_back)
        self.assertEqual(Path(wslconfig.backup_path(self.path)).read_bytes(), b"[wsl2]\nmemory=2GB\n")
        self.assertNotIn(str(self.directory), str(raised.exception))

    def test_edit_during_the_run_is_refused(self):
        self.write(b"[wsl2]\nmemory=2GB\n")

        class Files(wslconfig.Files):
            def create_exclusive(inner, path, data):
                super().create_exclusive(path, data)
                self.write(b"[wsl2]\nmemory=2GB\nkernelCommandLine=quiet\n")

        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, WANTED, files=Files())
        self.assertEqual(raised.exception.code, "concurrent-edit")
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\nkernelCommandLine=quiet\n")
        self.assertEqual(self.names(), [".wslconfig"])

    def test_owned_key_edited_later_blocks_another_apply(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, {"memory": 8 * GB})
        self.write(b"[wsl2]\nmemory=6GB\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, {"memory": 16 * GB})
        self.assertEqual(raised.exception.code, "owned-key-changed")
        self.assertEqual(self.read(), b"[wsl2]\nmemory=6GB\n")

    def test_unreadable_state_or_orphan_backup_is_refused(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        for content in (b"not json", b"[]", b'{"version": 2, "keys": {"memory": {"previous": null, "written": "x"}}}',
                        b'{"version": 1, "keys": {"kernel": {"previous": null, "written": "x"}}}',
                        b'{"version": 1, "keys": {}}'):
            with self.subTest(content=content):
                Path(wslconfig.state_path(self.path)).write_bytes(content)
                for operation in (lambda: wslconfig.apply(self.path, WANTED), lambda: wslconfig.restore(self.path)):
                    with self.assertRaises(wslconfig.Refusal) as raised:
                        operation()
                    self.assertEqual(raised.exception.code, "state")
        os.remove(wslconfig.state_path(self.path))
        Path(wslconfig.backup_path(self.path)).write_bytes(b"older")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, WANTED)
        self.assertEqual(raised.exception.code, "state")
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")


class ListKeyTest(WslConfigCase):
    def test_list_key_is_extended_never_shortened_and_restored(self):
        wanted = {"loadkernelmodules": ["nbd", "dm_delay"]}
        for original, expected in (
                (None, b"[wsl2]\nloadKernelModules=nbd,dm_delay\n"),
                (b"[wsl2]\nmemory=2GB\n", b"[wsl2]\nmemory=2GB\nloadKernelModules=nbd,dm_delay\n"),
                (b"[wsl2]\nLoadKernelModules = tun, dm-delay\n", b"[wsl2]\nLoadKernelModules = tun,dm-delay,nbd\n"),
                (b"[wsl2]\nloadKernelModules=\n", b"[wsl2]\nloadKernelModules=nbd,dm_delay\n")):
            with self.subTest(original=original):
                for name in self.names():
                    os.remove(self.directory / name)
                if original is not None:
                    self.write(original)
                self.assertEqual(wslconfig.apply(self.path, wanted), ["loadkernelmodules"])
                self.assertEqual(self.read(), expected)
                self.assertEqual(wslconfig.apply(self.path, wanted), [])
                wslconfig.restore(self.path)
                self.assertEqual(self.names(), [] if original is None else [".wslconfig"])
                if original is not None:
                    self.assertEqual(self.read(), original)

    def test_list_that_already_has_the_modules_is_left_alone(self):
        self.write(b"[wsl2]\nloadKernelModules=dm_delay,nbd,tun\n")
        self.assertEqual(wslconfig.apply(self.path, {"loadkernelmodules": ["nbd", "dm-delay"]}), [])
        self.assertEqual(self.names(), [".wslconfig"])

    def test_unreadable_list_is_refused(self):
        self.write(b"[wsl2]\nloadKernelModules=nbd dm_delay\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.apply(self.path, {"loadkernelmodules": ["nbd"]})
        self.assertEqual(raised.exception.code, "ambiguous")


class RestoreTest(WslConfigCase):
    def test_restore_returns_every_starting_shape_to_its_bytes(self):
        shapes = (None, b"", b"[wsl2]", b"[wsl2]\n", b"[wsl2]\nmemory=2GB", b"[wsl2]\r\nprocessors=2\r\nmemory=2GB\r\n",
                  b"[experimental]\nsparseVhd=true", b"# only a comment\n", b"[wsl2]\n# sized by hand\n\n[general]\n",
                  b"[wsl2]\nswap=0\n\n\n")
        for original in shapes:
            with self.subTest(original=original):
                for name in self.names():
                    os.remove(self.directory / name)
                if original is not None:
                    self.write(original)
                wslconfig.apply(self.path, dict(WANTED, swap=2 * GB))
                self.assertEqual(wslconfig.restore(self.path), ["memory", "processors", "swap"])
                self.assertEqual(self.names(), [] if original is None else [".wslconfig"])
                if original is not None:
                    self.assertEqual(self.read(), original)

    def test_restore_keeps_later_unrelated_edits(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, WANTED)
        self.write(b"# added later\n[wsl2]\nmemory=8GB\nkernelCommandLine=quiet\nprocessors=4\n\n"
                   b"[experimental]\nsparseVhd=true\n")
        wslconfig.restore(self.path)
        self.assertEqual(self.read(), b"# added later\n[wsl2]\nmemory=2GB\nkernelCommandLine=quiet\n\n"
                                      b"[experimental]\nsparseVhd=true\n")

    def test_restore_keeps_a_created_file_and_section_that_gained_content(self):
        wslconfig.apply(self.path, WANTED)
        self.write(b"[wsl2]\nmemory=8GB\nprocessors=4\nkernelCommandLine=quiet\n")
        wslconfig.restore(self.path)
        self.assertEqual(self.read(), b"[wsl2]\nkernelCommandLine=quiet\n")

    def test_restore_refuses_to_overwrite_a_later_edit_of_an_owned_key(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, WANTED)
        for later in (b"[wsl2]\nmemory=12GB\nprocessors=4\n", b"[wsl2]\nmemory=8GB\n",
                      b"[wsl2]\nmemory = 8GB\nprocessors=4\n", b"[other]\nmemory=8GB\nprocessors=4\n"):
            with self.subTest(later=later):
                self.write(later)
                before = {name: (self.directory / name).read_bytes() for name in self.names()}
                with self.assertRaises(wslconfig.Refusal) as raised:
                    wslconfig.restore(self.path)
                self.assertEqual(raised.exception.code, "owned-key-changed")
                self.assertEqual({name: (self.directory / name).read_bytes() for name in self.names()}, before)

    def test_restore_refuses_a_deleted_or_malformed_config(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, WANTED)
        self.write(b"[wsl2]\nmemory=8GB\nprocessors=4\nbroken line\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.restore(self.path)
        self.assertEqual(raised.exception.code, "malformed")
        os.remove(self.path)
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.restore(self.path)
        self.assertEqual(raised.exception.code, "owned-key-changed")

    def test_restore_without_a_recorded_change_is_refused(self):
        self.write(b"[wsl2]\nmemory=8GB\nprocessors=4\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.restore(self.path)
        self.assertEqual(raised.exception.code, "no-state")
        self.assertEqual(self.read(), b"[wsl2]\nmemory=8GB\nprocessors=4\n")

    def test_second_restore_is_refused_after_a_failed_state_removal(self):
        self.write(b"[wsl2]\nmemory=2GB\n")
        wslconfig.apply(self.path, WANTED)

        class Files(wslconfig.Files):
            def remove(self, path):
                raise OSError("injected failure")

        with self.assertRaises(wslconfig.PartialFailure):
            wslconfig.restore(self.path, files=Files())
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")
        with self.assertRaises(wslconfig.Refusal) as raised:
            wslconfig.restore(self.path)
        self.assertEqual(raised.exception.code, "owned-key-changed")
        self.assertEqual(self.read(), b"[wsl2]\nmemory=2GB\n")


if __name__ == "__main__":
    unittest.main()
