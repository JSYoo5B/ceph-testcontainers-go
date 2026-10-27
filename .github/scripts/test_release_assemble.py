import contextlib
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import release_assemble
import release_fragments

ROOT = Path(__file__).resolve().parents[2]
GIT = shutil.which("git")

CHANGELOG = """# Changelog

Intro.

## Unreleased

- Legacy entry one.

### Legacy group

- Legacy entry two.

## v0.1.0 (2026-10-11)

- First release.
"""


def fragment(change, kind="feature", body="A consumer can now do the thing.\n", **extra):
    lines = ["---", "change: %s" % change, "kind: %s" % kind]
    lines += ["%s: %s" % item for item in extra.items()]
    lines += ["---", body]
    return "\n".join(lines)


def short(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


class Base(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "docs/changes").mkdir(parents=True)
        (self.root / "docs/guides").mkdir(parents=True)
        (self.root / "docs/guides/upgrading-example.md").write_text("# Upgrading\n")
        (self.root / "CHANGELOG.md").write_text(CHANGELOG)

    def write(self, name, text):
        path = self.root / "docs/changes" / name
        path.write_text(text)
        return path

    def add(self, change, kind="feature", **kwargs):
        text = fragment(change, kind, **kwargs)
        self.write(change + ".md", text)
        return text

    def changelog(self, text):
        (self.root / "CHANGELOG.md").write_text(text)

    def assemble(self, version="v0.2.0", date="2026-11-01"):
        return release_assemble.assemble(str(self.root), version, date)

    def refused(self, needle, call=None):
        with self.assertRaises(release_assemble.AssembleError) as caught:
            (call or self.assemble)()
        text = "\n".join(caught.exception.problems)
        self.assertIn(needle, text)

    def failures(self, **kwargs):
        return release_assemble.check(str(self.root), **kwargs).failures

    def assertFails(self, needle, **kwargs):
        found = self.failures(**kwargs)
        self.assertTrue(any(needle in item for item in found), "%r not in %r" % (needle, found))


class AssembleTests(Base):
    def test_ordering_is_by_kind_then_change_with_api_break_first(self):
        self.add("5-scope", "scope")
        self.add("4-compat", "compatibility")
        self.add("3-behavior", "behavior")
        self.add("2-b-feature", "feature")
        self.add("2-a-feature", "feature")
        self.add("1-break-b", "api-break", migration="docs/guides/upgrading-example.md#b")
        self.add("1-break-a", "api-break", migration="docs/guides/upgrading-example.md")
        draft = self.assemble()
        order = [line for line in draft.split("\n") if line.startswith("### ") or "<!-- change:" in line]
        self.assertEqual(order, [
            "### Legacy group",
            "### Breaking changes",
            "  <!-- change: 1-break-a sha256:%s -->" % short(fragment("1-break-a", "api-break", migration="docs/guides/upgrading-example.md")),
            "  <!-- change: 1-break-b sha256:%s -->" % short(fragment("1-break-b", "api-break", migration="docs/guides/upgrading-example.md#b")),
            "### Features",
            "  <!-- change: 2-a-feature sha256:%s -->" % short(fragment("2-a-feature")),
            "  <!-- change: 2-b-feature sha256:%s -->" % short(fragment("2-b-feature")),
            "### Behavior",
            "  <!-- change: 3-behavior sha256:%s -->" % short(fragment("3-behavior", "behavior")),
            "### Compatibility",
            "  <!-- change: 4-compat sha256:%s -->" % short(fragment("4-compat", "compatibility")),
            "### Supported scope",
            "  <!-- change: 5-scope sha256:%s -->" % short(fragment("5-scope", "scope")),
        ])

    def test_exact_output(self):
        self.add("1-a", body="First line.\n\nSecond paragraph.  \n")
        self.add("2-b", "api-break", body="Renamed X.\n", migration="docs/guides/upgrading-example.md#top")
        expected = """## v0.2.0 (2026-11-01)

- Legacy entry one.

### Legacy group

- Legacy entry two.

### Breaking changes

- Renamed X.

  Migration: [docs/guides/upgrading-example.md#top](docs/guides/upgrading-example.md#top)
  <!-- change: 2-b sha256:{b} -->

### Features

- First line.

  Second paragraph.
  <!-- change: 1-a sha256:{a} -->
""".format(a=short(fragment("1-a", body="First line.\n\nSecond paragraph.  \n")),
           b=short(fragment("2-b", "api-break", body="Renamed X.\n", migration="docs/guides/upgrading-example.md#top")))
        self.assertEqual(self.assemble(), expected)

    def test_api_break_carries_its_migration_link(self):
        self.add("1-break", "api-break", migration="docs/guides/upgrading-example.md#top")
        self.assertIn("Migration: [docs/guides/upgrading-example.md#top](docs/guides/upgrading-example.md#top)",
                      self.assemble())

    def test_deterministic_across_creation_order_and_runs(self):
        names = ["3-c", "1-a", "2-b", "4-d"]
        outputs = []
        for order in (names, list(reversed(names))):
            for name in order:
                self.add(name, "feature")
            outputs.append(self.assemble())
            outputs.append(self.assemble())
            for name in names:
                (self.root / "docs/changes" / (name + ".md")).unlink()
        self.assertEqual(len(set(outputs)), 1)

    def test_assemble_does_not_modify_the_tree(self):
        self.add("1-a")
        before = {p.name: p.read_bytes() for p in (self.root / "docs/changes").iterdir()}
        changelog = (self.root / "CHANGELOG.md").read_bytes()
        self.assemble()
        self.assertEqual(changelog, (self.root / "CHANGELOG.md").read_bytes())
        self.assertEqual(before, {p.name: p.read_bytes() for p in (self.root / "docs/changes").iterdir()})

    def test_supersession_chain_keeps_only_the_newest(self):
        self.add("1-first")
        self.add("2-second", supersedes="[1-first]")
        self.add("3-third", supersedes="[2-second]")
        draft = self.assemble()
        self.assertIn("<!-- change: 3-third ", draft)
        self.assertNotIn("<!-- change: 1-first", draft)
        self.assertNotIn("<!-- change: 2-second", draft)
        self.assertIn("<!-- omitted: 1-first sha256:", draft)
        self.assertIn("<!-- omitted: 2-second sha256:", draft)

    def test_supersession_with_two_targets(self):
        self.add("1-a")
        self.add("1-b")
        self.add("2-c", supersedes="[1-a, 1-b]")
        draft = self.assemble()
        self.assertEqual(draft.count("<!-- change:"), 1)
        self.assertEqual(draft.count("<!-- omitted:"), 2)

    def test_kind_none_is_omitted_from_text_but_recorded(self):
        self.add("1-a")
        text = fragment("2-quiet", "none", body="", reason="internal only")
        self.write("2-quiet.md", text)
        draft = self.assemble()
        self.assertNotIn("internal only", draft)
        self.assertNotIn("<!-- change: 2-quiet", draft)
        self.assertIn("<!-- omitted: 2-quiet sha256:%s -->" % short(text), draft)

    def test_legacy_block_is_placed_first_and_unmodified(self):
        self.add("1-a")
        draft = self.assemble()
        legacy = "- Legacy entry one.\n\n### Legacy group\n\n- Legacy entry two."
        self.assertIn("## v0.2.0 (2026-11-01)\n\n" + legacy + "\n\n### Features", draft)

    def test_fragments_only_when_there_is_no_legacy_text(self):
        self.changelog("# Changelog\n\n## Unreleased\n\n## v0.1.0 (2026-10-11)\n\n- First.\n")
        self.add("1-a")
        self.assertTrue(self.assemble().startswith("## v0.2.0 (2026-11-01)\n\n### Features\n"))

    def test_nothing_to_assemble(self):
        self.changelog("# Changelog\n\n## Unreleased\n\n## v0.1.0 (2026-10-11)\n\n- First.\n")
        self.add("1-quiet", "none", body="", reason="internal only")
        self.refused("nothing to assemble")

    def test_missing_unreleased_heading_and_missing_directory(self):
        self.changelog("# Changelog\n\n## v0.1.0 (2026-10-11)\n\n- First.\n")
        (self.root / "docs/changes").rmdir()
        self.refused("nothing to assemble")

    def test_duplicate_identifier_is_refused(self):
        self.write("1-a.md", fragment("1-a"))
        self.write("1-b.md", fragment("1-a"))
        self.refused("duplicate change identifier '1-a'")

    def test_invalid_fragments_are_refused(self):
        cases = {
            "malformed front matter": ("1-a.md", "no front matter\n", "first line must be"),
            "unknown kind": ("1-a.md", fragment("1-a", "bugfix"), "unknown kind"),
            "missing migration": ("1-a.md", fragment("1-a", "api-break"), "requires a migration link"),
            "unresolvable migration": ("1-a.md", fragment("1-a", "api-break", migration="docs/guides/missing.md"),
                                       "is not an existing regular file"),
            "dangling supersedes": ("1-a.md", fragment("1-a", supersedes="[9-gone]"), "not a fragment in the tree"),
            "heading in text": ("1-a.md", fragment("1-a", body="## v9.9.9 (2026-01-01)\n"), "starts with '#'"),
            "forged marker": ("1-a.md", fragment("1-a", body="<!-- change: 1-z sha256:0123456789abcdef -->\n"),
                              "contains '<!--'"),
        }
        for label, (name, text, needle) in cases.items():
            with self.subTest(label):
                self.write(name, text)
                self.refused(needle)
                (self.root / "docs/changes" / name).unlink()

    def test_bad_version_and_date(self):
        self.add("1-a")
        for version in ("0.2.0", "v0.2", "v01.2.3", "v0.2.0\n", "v0.2.0-rc1", " v0.2.0", ""):
            with self.subTest(version):
                self.refused("must look like v1.2.3", lambda v=version: self.assemble(version=v))
        for date in ("2026-13-01", "2026-02-30", "26-11-01", "2026-11-01\n", "tomorrow", "2026-1-1"):
            with self.subTest(date):
                self.refused("real YYYY-MM-DD date", lambda d=date: self.assemble(date=d))

    def test_existing_version_is_refused(self):
        self.add("1-a")
        self.refused("already has a section for v0.1.0", lambda: self.assemble(version="v0.1.0"))

    def test_legacy_block_with_a_marker_like_line_is_refused(self):
        self.changelog(CHANGELOG.replace("- Legacy entry one.", "- Legacy.\n  <!-- change: 1-a sha256:0123456789abcdef -->"))
        self.add("1-b")
        self.refused("looks like an identifier marker")

    def test_multi_paragraph_text_keeps_one_bullet(self):
        self.add("1-a", body="One.\n\n  Indented.\n\nThree.\n")
        draft = self.assemble()
        self.assertIn("- One.\n\n    Indented.\n\n  Three.\n  <!-- change: 1-a", draft)


class CheckTests(Base):
    def release(self, ids=("1-a",), version="v0.2.0", omitted=()):
        """Write a changelog whose released section records the given fragments."""
        for change in ids:
            self.add(change)
        draft = self.assemble(version=version)
        for change in ids:
            (self.root / "docs/changes" / (change + ".md")).unlink()
        text = CHANGELOG.replace("## v0.1.0", draft + "\n## v0.1.0", 1)
        self.changelog(text)
        return draft

    def test_clean_tree_passes(self):
        self.assertEqual(self.failures(), [])

    def test_released_then_deleted_fragment_passes(self):
        self.release()
        report = release_assemble.check(str(self.root))
        self.assertEqual(report.failures, [])
        self.assertEqual(sorted(report.ledger), ["1-a"])

    def test_fragment_left_in_the_tree_after_release_is_refused(self):
        self.release()
        self.add("1-a")
        self.assertFails("already assembled into v0.2.0")

    def test_fragment_edited_after_release_is_refused(self):
        self.release()
        self.add("1-a", body="A different claim.\n")
        self.assertFails("edited after release or re-used")

    def test_identifier_reuse_is_refused(self):
        self.release()
        self.add("1-a", "behavior", body="Unrelated new text.\n")
        self.assertFails("was released in v0.2.0")

    def test_reuse_of_an_omitted_identifier_is_refused(self):
        self.add("1-quiet", "none", body="", reason="internal only")
        self.add("2-a")
        draft = self.assemble()
        (self.root / "docs/changes/1-quiet.md").unlink()
        (self.root / "docs/changes/2-a.md").unlink()
        self.changelog(CHANGELOG.replace("## v0.1.0", draft + "\n## v0.1.0", 1))
        self.assertEqual(self.failures(), [])
        self.add("1-quiet", "feature")
        self.assertFails("was released in v0.2.0")

    def test_assemble_refuses_a_stale_released_fragment(self):
        self.release()
        self.add("1-a")
        self.add("2-new")
        self.refused("already assembled into v0.2.0", lambda: self.assemble(version="v0.3.0"))

    def test_identifier_recorded_twice_is_refused(self):
        draft = self.release()
        self.changelog((self.root / "CHANGELOG.md").read_text().replace(
            "## v0.1.0", draft.replace("v0.2.0 (2026-11-01)", "v0.1.5 (2026-10-20)") + "\n## v0.1.0", 1))
        self.assertFails("recorded in both")

    def test_malformed_markers_and_headings_are_refused(self):
        self.release()
        text = (self.root / "CHANGELOG.md").read_text()
        broken = {
            "short digest": text.replace("sha256:", "sha256:0").replace(" -->", " -->", 1),
            "upper-case id": text.replace("change: 1-a", "change: 1-A"),
            "text after the marker": text.replace(" -->\n", " --> and more\n", 1),
            "text before the marker": text.replace("  <!-- change", "x <!-- change", 1),
            "tab indent": text.replace("  <!-- change", "\t<!-- change"),
            "heading without date": text.replace("## v0.2.0 (2026-11-01)", "## v0.2.0"),
            "trailing space heading": text.replace("## v0.2.0 (2026-11-01)", "## v0.2.0 (2026-11-01) "),
            "second unreleased": text + "\n## Unreleased\n",
            "duplicate version": text + "\n## v0.1.0 (2026-10-11)\n",
        }
        for label, value in broken.items():
            with self.subTest(label):
                self.changelog(value)
                self.assertTrue(self.failures(), label)

    def test_fragment_names_must_match_exactly(self):
        # A name that is not exactly <id>.md never counts as the released fragment.
        self.release()
        for name in ("1-a.md\n", "1-a.md ", "1-a.mdx"):
            try:
                (self.root / "docs/changes" / name).write_text(fragment("1-a"))
            except (OSError, ValueError):
                self.skipTest("file system cannot hold this name")
            with self.subTest(repr(name)):
                found = [item for item in self.failures() if "already assembled" in item or "was released" in item]
                self.assertEqual(found, [])
            (self.root / "docs/changes" / name).unlink()

    def test_symbolic_link_is_not_read(self):
        self.release()
        outside = self.root / "outside.md"
        outside.write_text(fragment("1-a"))
        (self.root / "docs/changes/1-a.md").symlink_to(outside)
        self.assertEqual([item for item in self.failures() if "already assembled" in item], [])

    def test_no_git_is_skipped_not_passed(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = release_assemble.main(["--root", str(self.root), "check"])
        self.assertEqual(code, 0)
        self.assertIn("tag comparison: SKIPPED", output.getvalue())
        self.assertNotIn("verified", output.getvalue())

    def test_require_tags_fails_without_tags(self):
        self.assertFails("--require-tags", require_tags=True)
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            code = release_assemble.main(["--root", str(self.root), "check", "--require-tags"])
        self.assertEqual(code, 1)


@unittest.skipUnless(GIT, "git is not installed")
class GitTests(Base):
    def git(self, *args):
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                        "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false"] + list(args),
                       check=True, capture_output=True, env=env)

    def commit_all(self, message):
        self.git("add", "-A")
        self.git("commit", "-q", "-m", message)

    def setUp(self):
        super().setUp()
        self.git("init", "-q")
        self.changelog(CHANGELOG)
        self.commit_all("release 0.1.0")
        self.git("tag", "v0.1.0")

    def test_matching_tag_is_verified(self):
        report = release_assemble.check(str(self.root))
        self.assertEqual((report.failures, report.verified, report.unverified), ([], ["v0.1.0"], []))

    def test_edited_released_section_is_refused(self):
        self.changelog(CHANGELOG.replace("First release.", "First release, reworded."))
        self.assertFails("differs from tag v0.1.0")

    def test_deleted_released_entry_is_refused(self):
        self.changelog(CHANGELOG.replace("- First release.\n", ""))
        self.assertFails("differs from tag v0.1.0")

    def test_deleted_released_section_is_refused(self):
        self.changelog(CHANGELOG.split("## v0.1.0")[0])
        self.assertFails("tag v0.1.0 has no released section")

    def test_edit_to_a_new_release_section_against_a_base_commit(self):
        self.add("1-a")
        draft = self.assemble("v0.2.0")
        (self.root / "docs/changes/1-a.md").unlink()
        self.changelog(CHANGELOG.replace("## v0.1.0", draft + "\n## v0.1.0", 1))
        self.commit_all("release 0.2.0")
        self.assertEqual(self.failures(bases=["HEAD"]), [])
        # An entry later removed from the published v0.2.0 section is caught against that commit.
        self.changelog((self.root / "CHANGELOG.md").read_text().replace("- A consumer can now do the thing.", "- Gone."))
        self.assertFails("differs from base HEAD", bases=["HEAD"])
        self.assertEqual(self.failures(), [])  # without a base the new section is not yet protected

    def test_deleted_entry_marker_against_a_base_commit(self):
        self.add("1-a")
        draft = self.assemble("v0.2.0")
        (self.root / "docs/changes/1-a.md").unlink()
        text = CHANGELOG.replace("## v0.1.0", draft + "\n## v0.1.0", 1)
        self.changelog(text)
        self.commit_all("release 0.2.0")
        lines = [line for line in text.split("\n") if "<!-- change: 1-a" not in line]
        self.changelog("\n".join(lines))
        self.assertFails("differs from base HEAD", bases=["HEAD"])

    def test_unknown_or_hostile_base_is_refused(self):
        for base in ("no-such-ref", "--output=x", "", "a b", "HEAD\n"):
            with self.subTest(base):
                self.assertTrue(self.failures(bases=[base]))

    def test_tag_without_a_released_section_is_refused(self):
        self.git("tag", "v0.9.0")
        self.assertFails("tag v0.9.0 has no released section")

    def test_non_release_tags_are_ignored(self):
        self.git("tag", "v0.2.0-rc1")
        self.git("tag", "vnext")
        self.assertEqual(release_assemble.release_tags(str(self.root)), ["v0.1.0"])

    def test_assemble_refuses_while_a_tag_has_no_section(self):
        self.add("1-a")
        self.git("tag", "v0.2.0")
        self.assertFails("tag v0.2.0 has no released section")
        self.refused("tag v0.2.0 has no released section")

    def test_unverified_version_is_listed_and_require_tags_fails(self):
        draft_root = self.root
        self.add("1-a")
        draft = self.assemble("v0.2.0")
        (draft_root / "docs/changes/1-a.md").unlink()
        self.changelog(CHANGELOG.replace("## v0.1.0", draft + "\n## v0.1.0", 1))
        report = release_assemble.check(str(self.root))
        self.assertEqual((report.verified, report.unverified), (["v0.1.0"], ["v0.2.0"]))
        self.assertEqual(report.failures, [])
        self.assertFails("v0.2.0", require_tags=True)

    def test_accepted_drift_is_reported_separately(self):
        section = release_assemble.split_sections(CHANGELOG)[1]["v0.1.0"]
        digest = hashlib.sha256(section.encode()).hexdigest()
        reworded = CHANGELOG.replace("First release.", "First release, reworded.")
        reworded_section = release_assemble.split_sections(reworded)[1]["v0.1.0"]
        accepted = {"v0.1.0": hashlib.sha256(reworded_section.encode()).hexdigest()}
        self.assertEqual(release_assemble.compare_released(CHANGELOG, reworded, "tag", accepted), [])
        self.assertTrue(release_assemble.compare_released(CHANGELOG, reworded, "tag", {"v0.1.0": digest}))
        self.assertTrue(release_assemble.compare_released(CHANGELOG, reworded, "tag", {}))
        # The repository's own accepted digests only match the exact text they were taken from.
        self.assertEqual(set(release_assemble.ACCEPTED_DRIFT), {"v0.2.0", "v0.3.0"})


class RepositoryTests(unittest.TestCase):
    def test_the_repository_changelog_passes(self):
        report = release_assemble.check(str(ROOT))
        self.assertEqual(report.failures, [])

    def test_the_repository_assembles_a_draft(self):
        draft = release_assemble.assemble(str(ROOT), "v9.9.9", "2099-01-01")
        self.assertTrue(draft.startswith("## v9.9.9 (2099-01-01)\n"))
        self.assertTrue(draft.endswith("\n"))
        self.assertFalse(draft.endswith("\n\n"))

    def test_the_validator_still_accepts_the_repository(self):
        self.assertEqual(release_fragments.validate(str(ROOT)), [])


class CommandLineTests(Base):
    def run_main(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = release_assemble.main(["--root", str(self.root)] + list(args))
        return code, out.getvalue(), err.getvalue()

    def test_assemble_to_stdout_and_file_match(self):
        self.add("1-a")
        code, out, _ = self.run_main("assemble", "--version", "v0.2.0", "--date", "2026-11-01")
        self.assertEqual(code, 0)
        target = self.root / "draft.md"
        code, _, _ = self.run_main("assemble", "--version", "v0.2.0", "--date", "2026-11-01", "--output", str(target))
        self.assertEqual(code, 0)
        self.assertEqual(target.read_text(), out)

    def test_output_never_overwrites(self):
        self.add("1-a")
        target = self.root / "draft.md"
        target.write_text("keep")
        code, _, err = self.run_main("assemble", "--version", "v0.2.0", "--date", "2026-11-01", "--output", str(target))
        self.assertEqual(code, 1)
        self.assertEqual(target.read_text(), "keep")

    def test_refusal_exit_code(self):
        self.write("1-a.md", "broken\n")
        code, out, err = self.run_main("assemble", "--version", "v0.2.0", "--date", "2026-11-01")
        self.assertEqual((code, out), (1, ""))
        self.assertIn("problem(s)", err)


if __name__ == "__main__":
    unittest.main()
