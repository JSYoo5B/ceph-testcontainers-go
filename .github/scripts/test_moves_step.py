"""Run the real "Check move records against the merge base" step of code.yml.

The step must exit with the move checker's status. A GitHub runner executes a
step without an explicit shell as `bash -e {0}`, which has no pipefail, so a
`checker | tee` pipeline hides the checker's failure. The tests extract the
step's `run` text and its `shell` key from the workflow as written, run it the
way the runner does with a stand-in `go` command, and need no Docker or Go.
"""
import os
import pathlib
import re
import stat
import subprocess
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
STEP = "Check move records against the merge base"

# The step as first written: no shell key, so no pipefail.
BROKEN_TEE_STEP = {
    "shell": None,
    "run": textwrap.dedent("""\
        base=
        go run ./internal/tools/apinaming moves -base "$base" | tee "$RUNNER_TEMP/moves.out"
        if grep -q '^moves: SKIPPED' "$RUNNER_TEMP/moves.out"; then
          echo "::warning::skipped"
        fi
        """),
}


def real_step():
    lines = (ROOT / ".github/workflows/code.yml").read_text(encoding="utf-8").splitlines()
    starts = [i for i, line in enumerate(lines) if line.strip() == "- name: " + STEP]
    if len(starts) != 1:
        raise AssertionError("expected exactly one step named %r, found %d" % (STEP, len(starts)))
    indent = len(lines[starts[0]]) - len(lines[starts[0]].lstrip())
    body = []
    for line in lines[starts[0] + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        body.append(line)
    shell, run = None, []
    in_run = False
    for line in body:
        stripped = line.strip()
        if in_run:
            run.append(line)
            continue
        if stripped.startswith("shell:"):
            shell = stripped.split(":", 1)[1].strip()
        elif stripped == "run: |":
            in_run = True
    return {"shell": shell, "run": textwrap.dedent("\n".join(run)) + "\n"}


def runner_command(shell, script):
    # The documented runner commands: an unset shell on Linux is `bash -e {0}`,
    # and `shell: bash` is `bash --noprofile --norc -eo pipefail {0}`.
    if shell is None:
        return ["bash", "-e", script]
    if shell == "bash":
        return ["bash", "--noprofile", "--norc", "-eo", "pipefail", script]
    raise AssertionError("unexpected shell %r" % shell)


def run_step(step, checker_output, checker_status):
    with tempfile.TemporaryDirectory() as tmp:
        tmp = pathlib.Path(tmp)
        bin_dir = tmp / "bin"
        bin_dir.mkdir()
        fake = bin_dir / "go"
        fake.write_text("#!/bin/sh\necho \"go $*\" >> \"$RUNNER_TEMP/go.args\"\nprintf '%s\\n' \"$FAKE_OUTPUT\"\nexit \"$FAKE_STATUS\"\n")
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        work = tmp / "work"
        work.mkdir()
        env = dict(os.environ, PATH="%s:%s" % (bin_dir, os.environ["PATH"]), RUNNER_TEMP=str(tmp),
                   FAKE_OUTPUT=checker_output, FAKE_STATUS=str(checker_status),
                   GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_SYSTEM=os.devnull)
        for cmd in (["git", "init", "-q", "-b", "main"],
                    ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q",
                     "--allow-empty", "-m", "x"]):
            subprocess.run(cmd, cwd=work, env=env, check=True)
        script = tmp / "step.sh"
        script.write_text(step["run"])
        done = subprocess.run(runner_command(step["shell"], str(script)), cwd=work, env=env,
                              capture_output=True, text=True)
        args = (tmp / "go.args").read_text() if (tmp / "go.args").exists() else ""
        return done.returncode, done.stdout, args


class MovesStepTests(unittest.TestCase):
    def test_step_declares_pipefail_shell(self):
        self.assertEqual(real_step()["shell"], "bash")

    def test_failing_checker_fails_the_step(self):
        code, out, args = run_step(real_step(), "moves: FAILED", 1)
        self.assertEqual(code, 1, out)
        self.assertIn("moves: FAILED", out)
        self.assertIn("go run ./internal/tools/apinaming moves -base", args)

    def test_valid_tree_passes_the_step(self):
        code, out, _ = run_step(real_step(), "moves: OK (compared with merge base x)", 0)
        self.assertEqual(code, 0, out)
        self.assertIn("moves: OK", out)
        self.assertNotIn("::warning::", out)

    def test_skipped_comparison_warns_and_is_not_ok(self):
        code, out, _ = run_step(real_step(), "moves: SKIPPED (no base)", 0)
        self.assertEqual(code, 0, out)
        self.assertIn("::warning::", out)

    def test_status_is_kept_even_without_pipefail(self):
        # The explicit status handling must not depend on the shell option.
        step = dict(real_step(), shell=None)
        code, _, _ = run_step(step, "moves: FAILED", 1)
        self.assertEqual(code, 1)

    def test_control_the_tee_version_hides_the_failure(self):
        # The control: the step as first written exits 0 when the checker fails.
        code, out, _ = run_step(BROKEN_TEE_STEP, "moves: FAILED", 1)
        self.assertEqual(code, 0, out)
        self.assertIn("moves: FAILED", out)


if __name__ == "__main__":
    unittest.main()
