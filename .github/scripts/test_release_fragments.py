import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest

import release_fragments

ROOT = Path(__file__).resolve().parents[2]


def fragment(change, kind="feature", body="A consumer can now do the thing.\n", **extra):
    lines = ["---", "change: %s" % change, "kind: %s" % kind]
    lines += ["%s: %s" % item for item in extra.items()]
    lines += ["---", body]
    return "\n".join(lines)


class Base(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "docs/changes").mkdir(parents=True)
        (self.root / "docs/guides").mkdir(parents=True)
        (self.root / "docs/guides/upgrading-example.md").write_text("# Upgrading\n")

    def write(self, name, text, directory="docs/changes"):
        path = self.root / directory / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def problems(self):
        return release_fragments.validate(str(self.root))

    def assertProblem(self, needle):
        found = self.problems()
        self.assertTrue(any(needle in item for item in found), "%r not in %r" % (needle, found))

    def assertValid(self):
        self.assertEqual(self.problems(), [])


class ValidTests(Base):
    def test_every_kind(self):
        for kind in ("feature", "behavior", "compatibility", "scope"):
            self.write("1-%s.md" % kind, fragment("1-%s" % kind, kind))
        self.write("2-break.md", fragment("2-break", "api-break", migration="docs/guides/upgrading-example.md#top"))
        self.write("3-none.md", fragment("3-none", "none", body="", reason="internal refactor"))
        self.assertValid()

    def test_missing_directory_is_valid(self):
        (self.root / "docs/changes").rmdir()
        self.assertValid()

    def test_empty_directory_is_valid(self):
        self.assertValid()

    def test_supersedes_an_existing_fragment(self):
        self.write("1-old.md", fragment("1-old"))
        self.write("2-new.md", fragment("2-new", supersedes="[1-old]"))
        self.assertValid()

    def test_the_repository_tree_is_valid(self):
        self.assertEqual(release_fragments.validate(str(ROOT)), [])


class RejectTests(Base):
    def test_duplicate_identifier(self):
        self.write("1-a.md", fragment("1-a"))
        self.write("1-b.md", fragment("1-a"))
        self.assertProblem("duplicate change identifier '1-a'")

    def test_filename_and_identifier_mismatch(self):
        self.write("1-a.md", fragment("1-b"))
        self.assertProblem("does not match the file name")

    def test_unknown_kind(self):
        self.write("1-a.md", fragment("1-a", "bugfix"))
        self.assertProblem("unknown kind 'bugfix'")

    def test_none_without_reason(self):
        self.write("1-a.md", fragment("1-a", "none", body=""))
        self.assertProblem("kind none requires a reason")

    def test_reason_with_another_kind(self):
        self.write("1-a.md", fragment("1-a", reason="why"))
        self.assertProblem("reason is only allowed with kind none")

    def test_api_break_without_migration(self):
        self.write("1-a.md", fragment("1-a", "api-break"))
        self.assertProblem("requires a migration link")

    def test_migration_must_resolve_under_guides(self):
        for value in ("docs/guides/missing.md", "docs/contributing/ci.md", "../x.md", "https://example.com/x",
                      "docs/guides/sub/x.md", "docs/guides/upgrading-example.md#bad anchor"):
            with self.subTest(value):
                self.write("1-a.md", fragment("1-a", "api-break", migration=value))
                self.assertTrue(self.problems(), value)

    def test_migration_symlink_is_not_a_target(self):
        (self.root / "docs/guides/link.md").symlink_to("upgrading-example.md")
        self.write("1-a.md", fragment("1-a", "api-break", migration="docs/guides/link.md"))
        self.assertProblem("is not an existing regular file")

    def test_text_cannot_open_a_changelog_heading_or_forge_a_marker(self):
        for body in ("## v9.9.9 (2026-01-01)\n", "text\n# Title\n", "<!-- change: 1-z sha256:0123456789abcdef -->\n",
                     "text <!-- x\n"):
            with self.subTest(body):
                self.write("1-a.md", fragment("1-a", body=body))
                self.assertProblem("starts with '#' or contains '<!--'")
        self.write("1-a.md", fragment("1-a", body="Use a `#` in the middle: a # b.\n    # indented code\n"))
        self.assertValid()

    def test_missing_body(self):
        self.write("1-a.md", fragment("1-a", body="\n  \n"))
        self.assertProblem("requires user-facing text")

    def test_malformed_front_matter(self):
        cases = {
            "no front matter": "text only\n",
            "not closed": "---\nchange: 1-a\nkind: feature\ntext\n",
            "empty value": "---\nchange: 1-a\nkind:\n---\ntext\n",
            "no space after colon": "---\nchange:1-a\nkind: feature\n---\ntext\n",
            "unknown key": "---\nchange: 1-a\nkind: feature\nowner: x\n---\ntext\n",
            "duplicate key": "---\nchange: 1-a\nkind: feature\nkind: scope\n---\ntext\n",
            "yaml comment line": "---\nchange: 1-a\n# comment\nkind: feature\n---\ntext\n",
            "leading blank line": "\n---\nchange: 1-a\nkind: feature\n---\ntext\n",
            "carriage return": "---\r\nchange: 1-a\r\nkind: feature\r\n---\r\ntext\r\n",
            "trailing space": "---\nchange: 1-a\nkind: feature \n---\ntext\n",
            "missing change": "---\nkind: feature\n---\ntext\n",
            "missing kind": "---\nchange: 1-a\n---\ntext\n",
            "empty file": "",
        }
        for label, text in cases.items():
            with self.subTest(label):
                self.write("1-a.md", text)
                self.assertTrue(self.problems(), label)

    def test_invalid_identifier(self):
        for value in ("Upper", "-lead", "has_underscore", "a" * 65, "sp ace"):
            with self.subTest(value):
                self.write("1-a.md", fragment(value))
                self.assertTrue(self.problems())

    def test_not_utf8(self):
        (self.root / "docs/changes/1-a.md").write_bytes(b"---\nchange: 1-a\nkind: feature\n---\n\xff\xfe\n")
        self.assertProblem("not valid UTF-8")

    def test_oversized(self):
        self.write("1-a.md", fragment("1-a", body="x" * (release_fragments.MAX_BYTES + 1)))
        self.assertProblem("larger than")

    def test_bad_file_names(self):
        for name in ("Upper.md", "1-a.MD", "1-a.markdown", "1_a.md", ".md", "-a.md", "README.md.md.md", "1 a.md"):
            with self.subTest(name):
                self.write(name, fragment("1-a"))
                self.assertTrue(self.problems(), name)
                (self.root / "docs/changes" / name).unlink()

    def test_names_with_odd_characters(self):
        for name in ("1-a.md\n", "\n1-a.md", "1-\na.md", "1-a.md ", " 1-a.md", "1-a .md", "1-a.md\t",
                     "1-a.md\r", "1-\u00e4.md", "1-a.md\u200b"):
            with self.subTest(repr(name)):
                try:
                    self.write(name, fragment("1-a"))
                except (OSError, ValueError):
                    self.skipTest("file system cannot hold this name")
                self.assertProblem("only <change-id>.md files are allowed")
                (self.root / "docs/changes" / name).unlink()

    def test_stem_requires_an_exact_name(self):
        self.assertEqual(release_fragments.fragment_stem("1-a.md"), "1-a")
        for name in ("1-a.md\n", "1-a.md ", "1-a.mdx", "1-a."):
            self.assertIsNone(release_fragments.fragment_stem(name), repr(name))

    def test_non_markdown_files(self):
        for name in ("1-a.json", "1-a.txt", "1-a", ".gitkeep", "1-a.md.orig", "1-a.go"):
            with self.subTest(name):
                self.write(name, fragment("1-a"))
                self.assertProblem("only <change-id>.md files are allowed")
                (self.root / "docs/changes" / name).unlink()

    def test_nested_paths(self):
        self.write("sub/1-a.md", fragment("1-a"))
        self.assertProblem("nested directories are not allowed")

    def test_empty_nested_directory(self):
        (self.root / "docs/changes/sub").mkdir()
        self.assertProblem("nested directories are not allowed")

    def test_symlink_fragment(self):
        self.write("target.md", fragment("target"))
        (self.root / "docs/changes/1-a.md").symlink_to("target.md")
        self.assertProblem("1-a.md: a symbolic link is not allowed")

    def test_symlink_to_a_code_file(self):
        (self.root / "code.go").write_text("package x\n")
        (self.root / "docs/changes/1-a.md").symlink_to("../../code.go")
        self.assertProblem("a symbolic link is not allowed")

    def test_dangling_symlink(self):
        (self.root / "docs/changes/1-a.md").symlink_to("missing.md")
        self.assertProblem("a symbolic link is not allowed")

    def test_symlinked_directory(self):
        (self.root / "docs/changes").rmdir()
        (self.root / "elsewhere").mkdir()
        (self.root / "elsewhere/1-a.md").write_text(fragment("1-a"))
        (self.root / "docs/changes").symlink_to("../elsewhere")
        self.assertProblem("must be a real directory")

    def test_supersedes_problems(self):
        self.write("1-a.md", fragment("1-a", supersedes="[9-missing]"))
        self.assertProblem("not a fragment in the tree")
        self.write("1-a.md", fragment("1-a", supersedes="[1-a]"))
        self.assertProblem("cannot supersede itself")
        self.write("1-a.md", fragment("1-a", supersedes="1-b"))
        self.assertProblem("must be a list")
        self.write("1-b.md", fragment("1-b"))
        self.write("1-a.md", fragment("1-a", supersedes="[1-b, 1-b]"))
        self.assertProblem("lists a change twice")

    def test_supersedes_cycle(self):
        self.write("1-a.md", fragment("1-a", supersedes="[1-b]"))
        self.write("1-b.md", fragment("1-b", supersedes="[1-a]"))
        self.assertProblem("forms a cycle")

    def test_every_problem_is_reported(self):
        self.write("1-a.md", fragment("1-b", "bugfix"))
        self.write("1-c.json", "x")
        self.assertGreaterEqual(len(self.problems()), 3)


class CommandTests(Base):
    def run_main(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = release_fragments.main([*args])
        return code, out.getvalue(), err.getvalue()

    def test_valid_exit_code(self):
        self.write("1-a.md", fragment("1-a"))
        code, out, _ = self.run_main("--root", str(self.root))
        self.assertEqual(code, 0)
        self.assertIn("1 fragment(s) valid", out)

    def test_invalid_exit_code_and_report(self):
        self.write("1-a.md", fragment("1-a", "bugfix"))
        code, _, err = self.run_main("--root", str(self.root))
        self.assertEqual(code, 1)
        self.assertIn("unknown kind", err)
        self.assertNotIn(str(self.root), err)


if __name__ == "__main__":
    unittest.main()
