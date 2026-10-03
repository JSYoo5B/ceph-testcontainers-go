import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import build


class PackageInputTests(unittest.TestCase):
    def parse(self, arguments):
        with contextlib.redirect_stderr(io.StringIO()):
            return build.arguments(arguments)

    def test_source_modes_and_base_must_agree(self):
        for arguments in ([], ["--deb-packages", "ceph.deb"],
                          ["--source-image", "quay.io/ceph/ceph:v20.2.4", "--base-image", "ubuntu:24.04"],
                          ["--source-image", "source", "--deb-directory", "packages", "--base-image", "base"]):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit):
                self.parse(arguments)
        self.assertEqual(self.parse(["--deb-directory", "packages", "--base-image", "ubuntu:24.04"]).base_image,
                         "ubuntu:24.04")

    def test_untrusted_filename_never_becomes_staged_name(self):
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / 'ceph $(touch unexpected); "patched".deb'
            package.write_bytes(b"company artifact")
            args = self.parse(["--deb-packages", str(package), "--base-image", "ubuntu:24.04"])
            files = build.deb_inputs(args)
            self.assertEqual(files[0][1]["filename"], package.name)
            self.assertEqual(files[0][1]["staged_filename"], "0000.deb")
            self.assertEqual(files[0][1]["sha256"], hashlib.sha256(package.read_bytes()).hexdigest())

    def test_empty_directory_and_duplicate_input_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            args = self.parse(["--deb-directory", directory, "--base-image", "ubuntu:24.04"])
            with self.assertRaises(build.BuildError):
                build.deb_inputs(args)
            package = Path(directory) / "ceph.deb"
            package.write_bytes(b"package")
            args = self.parse(["--deb-packages", str(package), str(package), "--base-image", "ubuntu:24.04"])
            with self.assertRaises(build.BuildError):
                build.deb_inputs(args)

    def test_plan_archive_cannot_escape_build_context(self):
        for value in ("/groups/common.tar", "groups/../outside.tar", "manifests/common.tar", "groups/common.zip"):
            with self.subTest(value=value), self.assertRaises(build.BuildError):
                build.archive_path(value, "groups")


def common_only_plan():
    return {
        "source_image": "source@sha256:pinned", "oci_architecture": "arm64",
        "ceph_version": "ceph version 20.2.4 test",
        "ordered_groups": ["common"],
        "groups": {"common": {"archive": "groups/common.tar", "members": list(build.ROLES)}},
        "roles": {role: {"groups": ["common"], "manifest_archive": "manifests/" + role + ".tar",
                         "logical_regular_file_bytes": 1} for role in build.ROLES},
    }


class RuntimeEnvironmentTests(unittest.TestCase):
    def parse(self, values):
        with contextlib.redirect_stderr(io.StringIO()):
            return build.arguments(["--source-image", "source"] + values)

    def test_optional_repeatable_cli_preserves_values_and_empty_assignment(self):
        self.assertEqual(self.parse([]).runtime_env, {})
        args = self.parse(["--runtime-env", "TCMALLOC_STACKTRACE_METHOD=generic_fp",
                           "--runtime-env", 'LITERAL=$HOME/${UNSET:-x}/$(command); "quoted" \\path=a',
                           "--runtime-env", "EMPTY="])
        self.assertEqual(args.runtime_env, {
            "TCMALLOC_STACKTRACE_METHOD": "generic_fp",
            "LITERAL": '$HOME/${UNSET:-x}/$(command); "quoted" \\path=a', "EMPTY": ""})

    def test_invalid_names_duplicates_and_control_characters_rejected_before_build(self):
        invalid = [["NAME"], ["=empty"], ["1NAME=x"], ["A-B=x"], ["A B=x"],
                   ["A=x", "A=x"], ["A=x", "A=y"], ["A=first\nRUN command"],
                   ["A=tab\there"], ["A=carriage\rreturn"], ["A=nul\x00"],
                   ["A=del\x7f"], ["A=c1\x85"], ["A=bidi\u202e"], ["A=line\u2028"]]
        for values in invalid:
            arguments = [item for value in values for item in ("--runtime-env", value)]
            with self.subTest(values=values), self.assertRaises(SystemExit), \
                    mock.patch.object(build.subprocess, "run", side_effect=AssertionError("Docker must not start")):
                self.parse(arguments)

    def test_common_dockerfile_has_literal_escaped_env_inherited_by_all_roles(self):
        supplied = {"LITERAL": '$HOME ${UNSET:-fallback} $(touch marker) "quote" \\path', "EMPTY": ""}
        generated = build.dockerfile(common_only_plan(), supplied)
        expected = 'ENV LITERAL="\\$HOME \\${UNSET:-fallback} \\$(touch marker) \\"quote\\" \\\\path"'
        self.assertIn(expected + '\nENV EMPTY=""\n', generated)
        self.assertEqual(generated.count("ENV LITERAL="), 1)
        self.assertLess(generated.index(expected), generated.index("FROM common AS control"))
        for role in build.ROLES:
            self.assertIn("FROM common AS " + role, generated)
        self.assertEqual(build.dockerfile(common_only_plan()), build.dockerfile(common_only_plan(), {}))
        self.assertNotIn("TCMALLOC_STACKTRACE_METHOD", build.dockerfile(common_only_plan()))

    def test_inspect_requires_every_exact_value_including_empty_and_rejects_ambiguity(self):
        supplied = {"VALUE": '$literal "quote" \\path=tail', "EMPTY": ""}
        env = ["PATH=/usr/bin", "VALUE=" + supplied["VALUE"], "EMPTY="]
        self.assertEqual(build.check_runtime_env({"Config": {"Env": env}}, supplied), supplied)
        for image in [{}, {"Config": None}, {"Config": {"Env": None}},
                      {"Config": {"Env": ["VALUE=" + supplied["VALUE"]]}},
                      {"Config": {"Env": ["VALUE=expanded", "EMPTY="]}},
                      {"Config": {"Env": env + ["VALUE=" + supplied["VALUE"]]}}]:
            with self.subTest(image=image), self.assertRaises(build.BuildError):
                build.check_runtime_env(image, supplied)
        self.assertEqual(build.check_runtime_env({}, {}), {})

    def execute_mocked_build(self, output, env, mismatch_role=None):
        # Test host orchestration/report failure boundaries only; no Docker image
        # or native smoke result is claimed by these mocked commands.
        plan = common_only_plan()
        commands = []
        def run(arguments, **kwargs):
            commands.append(arguments)
            if arguments[:2] == ["docker", "create"]:
                return "owned-assembly"
            if arguments[:2] == ["docker", "inspect"]:
                return "0"
            if arguments[:2] == ["docker", "cp"]:
                if arguments[2] == "owned-assembly:/role-output/plan.json":
                    Path(arguments[3]).write_text(json.dumps(plan))
                elif arguments[2].startswith("owned-assembly:/role-output/"):
                    Path(arguments[3]).write_bytes(b"mock payload")
            return ""
        inspected = []
        def inspect(image, _platform=None):
            source = {"Id": "sha256:source", "Os": "linux", "Architecture": "arm64", "Size": 1}
            if image.startswith("source"):
                return source
            role = image.rsplit("-", 1)[1]; inspected.append(role)
            actual = dict(env)
            if role == mismatch_role:
                actual[next(iter(actual))] = "unexpected"
            return dict(source, RootFS={"Layers": ["shared-common", "manifest-" + role]},
                        Config={"Env": ["PATH=/usr/bin"] + [name + "=" + value for name, value in actual.items()]})
        cli = ["build.py", "--source-image", plan["source_image"], "--skip-pull", "--skip-smoke",
               "--output-dir", str(output)]
        for name, value in env.items():
            cli += ["--runtime-env", name + "=" + value]
        with mock.patch.object(build.sys, "argv", cli), \
                mock.patch.object(build.shutil, "which", return_value="test-docker"), \
                mock.patch.object(build, "run", side_effect=run), \
                mock.patch.object(build, "inspect_image", side_effect=inspect), \
                mock.patch.object(build.subprocess, "run", side_effect=AssertionError("No subprocess expected")), \
                contextlib.redirect_stdout(io.StringIO()):
            if mismatch_role:
                with self.assertRaisesRegex(build.BuildError, "differs from requested value"):
                    build.main()
            else:
                build.main()
        return json.loads((output / "build-report.json").read_text()), inspected, commands

    def test_build_report_binds_requested_and_inspected_values_for_all_five_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            supplied = {"TCMALLOC_STACKTRACE_METHOD": "generic_fp", "LITERAL": '$HOME "quote" \\path', "EMPTY": ""}
            report, inspected, commands = self.execute_mocked_build(Path(directory), supplied)
            self.assertEqual(inspected, list(build.ROLES))
            self.assertEqual(report["runtime_env"], supplied)
            self.assertEqual(report["checks"]["runtime_env"], "passed")
            for role in build.ROLES:
                self.assertEqual(report["images"][role]["runtime_env"], supplied)
            self.assertFalse(any("--runtime-env" in command or "--env" in command for command in commands))
            # Configuration is in generated image ENV, never host shell arguments.
            self.assertIn("generic_fp", (Path(directory) / "Dockerfile.generated").read_text())

    def test_unsupplied_configuration_keeps_generated_defaults_and_is_not_requested(self):
        with tempfile.TemporaryDirectory() as directory:
            report, inspected, _ = self.execute_mocked_build(Path(directory), {})
            self.assertEqual(inspected, list(build.ROLES))
            self.assertEqual(report["runtime_env"], {})
            self.assertEqual(report["checks"]["runtime_env"], "not_requested")
            generated = (Path(directory) / "Dockerfile.generated").read_text()
            self.assertEqual(generated, build.dockerfile(common_only_plan()))
            self.assertNotIn("TCMALLOC_STACKTRACE_METHOD", generated)

    def test_inspected_mismatch_fails_report_before_remaining_roles_or_smoke(self):
        with tempfile.TemporaryDirectory() as directory:
            report, inspected, _ = self.execute_mocked_build(Path(directory), {"VALUE": "wanted"}, "osd")
            self.assertEqual(inspected, ["control", "osd"])
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["checks"]["runtime_env"], "failed")
            self.assertEqual(report["runtime_env"], {"VALUE": "wanted"})
            self.assertNotIn("osd", report["images"])


if __name__ == "__main__":
    unittest.main()
