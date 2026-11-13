#!/usr/bin/env python3
"""Reproduction catalogues: validation, image resolution and the log check."""

import json
from pathlib import Path
import tempfile
import unittest

import reproductions

ROOT = Path(__file__).resolve().parents[2]
E1, C1, R1 = "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1", "sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1", "sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1"
E2, C2, R2 = "sha256:e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2", "sha256:c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2", "sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"


def catalog(affected="REPRODUCED"):
    return reproductions.validate({
        "schema": reproductions.SCHEMA,
        "roles": ["essential", "client", "rgw"],
        "units": {"rgw_case": {"network": "bridge"}, "rgw_case/host": {"network": "host"}},
        "combinations": [
            {"id": "affected", "platform": "linux/amd64", "images": {"essential": E1, "client": C1, "rgw": R1},
             "verdicts": {"rgw_case": affected, "rgw_case/host": "REPRODUCED"}, "evidence": "native run"},
            {"id": "fixed", "platform": "linux/amd64", "images": {"essential": E2, "client": C2, "rgw": R2},
             "verdicts": {"rgw_case": "NOT_REPRODUCED"}, "evidence": "native run"},
        ]})


IMAGES = json.loads((ROOT / "internal/integration/testdata/reproduction_images.json").read_text())
AFFECTED = {"CEPH_TEST_ESSENTIAL_IMAGE": E1, "CEPH_TEST_CLIENT_IMAGE": C1, "CEPH_TEST_RGW_IMAGE": R1}
FIXED = {"CEPH_TEST_ESSENTIAL_IMAGE": E2, "CEPH_TEST_CLIENT_IMAGE": C2, "CEPH_TEST_RGW_IMAGE": R2}


def line(unit, verdict, expected, combination="affected", network="bridge"):
    return ("    case_test.go:12: REPRODUCTION_VERDICT case=%s verdict=%s expected=%s combination=%s attempts=1 "
            "platform=linux/amd64 network=%s images=essential=e observation=trigger: x"
            % (unit, verdict, expected, combination, network))


class CatalogTests(unittest.TestCase):
    def test_invalid_catalogues_are_refused(self):
        valid = json.loads(json.dumps(catalog()))
        for name, change in (
            ("inconclusive expectation", lambda d: d["combinations"][0]["verdicts"].update(rgw_case="INCONCLUSIVE")),
            ("unknown verdict", lambda d: d["combinations"][0]["verdicts"].update(rgw_case="FIXED")),
            ("unknown unit", lambda d: d["combinations"][0]["verdicts"].update(rgw_other="REPRODUCED")),
            ("missing role image", lambda d: d["combinations"][0]["images"].pop("rgw")),
            ("unknown field", lambda d: d["combinations"][0].update(release="20.2.4")),
            ("unknown role", lambda d: d.update(roles=["essential", "client", "gateway"])),
            ("bad network", lambda d: d["units"]["rgw_case"].update(network="none")),
            ("no evidence", lambda d: d["combinations"][0].update(evidence=" ")),
            ("same images twice", lambda d: d["combinations"][1].update(images=dict(d["combinations"][0]["images"]))),
            ("duplicate id", lambda d: d["combinations"][1].update(id="affected")),
        ):
            with self.subTest(name):
                document = json.loads(json.dumps(valid))
                change(document)
                with self.assertRaises(ValueError):
                    reproductions.validate(document)

    def test_images_follow_the_shared_reference_cases(self):
        # The cases TestReproductionCatalogImagesAreImmutable checks against
        # the harness's loader.
        self.assertTrue(IMAGES["immutable"] and IMAGES["mutable"])
        for image in IMAGES["immutable"]:
            with self.subTest(image):
                document = catalog()
                document["combinations"][1]["images"]["rgw"] = image
                self.assertTrue(reproductions.immutable_image(image))
                reproductions.validate(document)
        for image in IMAGES["mutable"]:
            with self.subTest(image):
                self.assertFalse(reproductions.immutable_image(image))

    def test_a_package_lists_exactly_the_units_it_starts(self):
        with tempfile.TemporaryDirectory() as temporary:
            package = Path(temporary) / "case"
            package.mkdir()
            (package / "case_test.go").write_text('repro := harness.StartReproduction(t, "rgw_case", "trigger")\n'
                                                  'host := harness.StartReproduction(t, "rgw_case/host", "trigger")\n')
            (Path(temporary) / "other").mkdir()
            self.assertIsNone(reproductions.load("other", temporary))
            with self.assertRaises(ValueError):
                reproductions.load("case", temporary)
            (package / reproductions.CATALOG).write_text(json.dumps(catalog()))
            self.assertEqual(set(reproductions.load("case", temporary)["units"]), {"rgw_case", "rgw_case/host"})
            (package / "case_test.go").write_text('repro := harness.StartReproduction(t, "rgw_case", "trigger")\n')
            with self.assertRaises(ValueError):
                reproductions.load("case", temporary)

    def test_repository_catalogues(self):
        catalogs = reproductions.verify()
        self.assertGreaterEqual(len(catalogs), 19)
        sse = catalogs["internal/integration/experiments/rgw_empty_sse_header"]
        self.assertEqual({c["verdicts"]["rgw_empty_sse_header"] for c in sse["combinations"]}, {"REPRODUCED", "NOT_REPRODUCED"})

    def test_combinations_name_immutable_images_whatever_the_release_pins(self):
        with tempfile.TemporaryDirectory() as temporary:
            package = Path(temporary) / "internal/integration/experiments/rgw_case"
            package.mkdir(parents=True)
            (package / "case_test.go").write_text('//ci: timeout=20m image-roles=essential,client,rgw\n'
                                                  'a := harness.StartReproduction(t, "rgw_case", "trigger")\n'
                                                  'b := harness.StartReproduction(t, "rgw_case/host", "trigger")\n')
            document = catalog()
            # Role IDs no release pin names any more, one with a separately
            # built client, and digest references with and without a tag.
            document["combinations"][1]["images"]["client"] = "sha256:" + "d4" * 32
            document["combinations"].append({
                "id": "aggregate", "platform": "linux/amd64", "verdicts": {"rgw_case": "NOT_REPRODUCED"},
                "evidence": "native run", "images": {
                    "essential": "quay.io/ceph/ceph:v20.2.5@sha256:" + "4" * 64,
                    "client": "quay.io/ceph/ceph:v20.2.5@sha256:" + "4" * 64,
                    "rgw": "localhost:5000/ceph/rgw@sha256:" + "5" * 64}})
            (package / reproductions.CATALOG).write_text(json.dumps(document))
            self.assertEqual(len(reproductions.verify(temporary)), 1)
            for image in IMAGES["mutable"]:
                with self.subTest(image):
                    mutable = catalog()
                    mutable["combinations"][1]["images"] = {role: image for role in ("essential", "client", "rgw")}
                    with self.assertRaises(ValueError):
                        reproductions.validate(mutable)
                    (package / reproductions.CATALOG).write_text(json.dumps(mutable))
                    with self.assertRaises(ValueError):
                        reproductions.verify(temporary)
            (package / reproductions.CATALOG).write_text(json.dumps(catalog()))
            (package / "case_test.go").write_text('//ci: timeout=20m image-roles=essential,client\n'
                                                  'a := harness.StartReproduction(t, "rgw_case", "trigger")\n'
                                                  'b := harness.StartReproduction(t, "rgw_case/host", "trigger")\n')
            with self.assertRaises(ValueError):
                reproductions.verify(temporary)


class ResolutionTests(unittest.TestCase):
    def test_resolution_matches_the_harness(self):
        default = reproductions.default_image()
        for name, environment, want in (
            ("aggregate", {"CEPH_TEST_IMAGE": "A"}, {"essential": "A", "client": "A", "rgw": "A"}),
            ("default", {}, {"essential": default, "client": default, "rgw": default}),
            ("aggregate with a client override", {"CEPH_TEST_IMAGE": "A", "CEPH_TEST_CLIENT_IMAGE": "B"},
             {"essential": "A", "client": "B", "rgw": "A"}),
            ("roles", {"CEPH_TEST_ESSENTIAL_IMAGE": "E", "CEPH_TEST_CLIENT_IMAGE": "C", "CEPH_TEST_RGW_IMAGE": "R",
                       "CEPH_TEST_IMAGE": "A"}, {"essential": "E", "client": "C", "rgw": "R"}),
            ("roles without an rgw image", {"CEPH_TEST_ESSENTIAL_IMAGE": "E", "CEPH_TEST_CLIENT_IMAGE": "C"},
             {"essential": "E", "client": "C"}),
            ("role layout without essential", {"SCENARIO_IMAGE_LAYOUT": "roles", "CEPH_TEST_CLIENT_IMAGE": "C"}, {}),
        ):
            with self.subTest(name):
                images = reproductions.resolved_images(environment)
                self.assertEqual({role: images[role] for role in ("essential", "client", "rgw") if role in images}, want)

    def test_images_select_the_expectations(self):
        self.assertEqual(reproductions.expectations(catalog(), AFFECTED, "linux/amd64"),
                         ("affected", {"rgw_case": "REPRODUCED", "rgw_case/host": "REPRODUCED"}))
        self.assertEqual(reproductions.expectations(catalog(), FIXED, "linux/amd64"), ("fixed", {"rgw_case": "NOT_REPRODUCED"}))
        self.assertEqual(reproductions.expectations(catalog(), AFFECTED, "linux/amd64", "host"),
                         ("affected", {"rgw_case/host": "REPRODUCED"}))
        for name, environment, platform in (
            ("unknown images", {"CEPH_TEST_IMAGE": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111"}, "linux/amd64"),
            ("one role differs", {**AFFECTED, "CEPH_TEST_RGW_IMAGE": R2}, "linux/amd64"),
            ("a role unresolved", {"CEPH_TEST_ESSENTIAL_IMAGE": E1, "CEPH_TEST_CLIENT_IMAGE": C1}, "linux/amd64"),
            ("another platform", AFFECTED, "linux/arm64"),
        ):
            with self.subTest(name):
                self.assertEqual(reproductions.expectations(catalog(), environment, platform), (None, {}))
        self.assertEqual(reproductions.expectations(None, AFFECTED, "linux/amd64"), (None, {}))


class CheckTests(unittest.TestCase):
    def test_each_expected_unit_needs_its_own_matching_line(self):
        expected = {"rgw_case": "REPRODUCED"}
        for name, text, met in (
            ("matching line", line("rgw_case", "REPRODUCED", "REPRODUCED"), True),
            ("absent though expected", line("rgw_case", "NOT_REPRODUCED", "REPRODUCED"), False),
            ("inconclusive", line("rgw_case", "INCONCLUSIVE", "REPRODUCED"), False),
            ("the test did not select the expectation", line("rgw_case", "REPRODUCED", "none", "none"), False),
            ("another combination", line("rgw_case", "REPRODUCED", "REPRODUCED", "fixed"), False),
            ("missing line, e.g. a skip that never started the unit", "--- SKIP: TestCase (0.00s)", False),
            ("only another unit reported", line("rgw_other", "REPRODUCED", "REPRODUCED"), False),
            ("two lines", line("rgw_case", "REPRODUCED", "REPRODUCED") + "\n" + line("rgw_case", "REPRODUCED", "REPRODUCED"), False),
        ):
            with self.subTest(name):
                self.assertEqual(reproductions.check(text, "affected", expected)["met"], met)
        investigation = reproductions.check(line("rgw_case", "REPRODUCED", "none", "none"), None, {})
        self.assertTrue(investigation["met"])
        self.assertEqual(investigation["verdicts"][0]["verdict"], "REPRODUCED")

    def test_changing_only_the_expected_value_keeps_the_actual_verdict(self):
        text = line("rgw_case", "REPRODUCED", "REPRODUCED")
        met = reproductions.check(text, "affected", {"rgw_case": "REPRODUCED"})
        unmet = reproductions.check(text, "affected", {"rgw_case": "NOT_REPRODUCED"})
        self.assertTrue(met["met"])
        self.assertFalse(unmet["met"])
        self.assertEqual(met["verdicts"], unmet["verdicts"])


PURE_SOURCE = """package rgw_case_test

func TestParsesTheResponse(t *testing.T) {
	if parseResponse("x") == "" {
		t.Fatal("empty")
	}
}
"""
HELPER_SOURCE = """package rgw_case_test

func parseResponse(text string) string {
	return text
}

func runCase(t *testing.T) {
	repro := harness.StartReproduction(t, "rgw_case", "trigger")
	_ = parseResponse("y")
	repro.Finish()
}

func runHost(t *testing.T) {
	startHost(t)
}

func startHost(t *testing.T) {
	_ = harness.StartReproduction(t, "rgw_case/host", "trigger")
}
"""
NATIVE_SOURCE = """package rgw_case_test

func TestCase(t *testing.T) {
	runCase(t)
}

func TestHostCase(t *testing.T) {
	t.Run("host", func(t *testing.T) { runHost(t) })
}
"""


class StartedUnitTests(unittest.TestCase):
    def test_only_the_tests_that_run_start_units(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for name, text in (("pure_fixture_test.go", PURE_SOURCE), ("case_helpers_test.go", HELPER_SOURCE),
                               ("case_native_regression_test.go", NATIVE_SOURCE)):
                path = Path(directory) / name
                path.write_text(text)
                paths.append(path)
            # A pure profile shares the helper file but runs no reproducer.
            self.assertEqual(reproductions.started_units(paths[:2], ["TestParsesTheResponse"]), set())
            self.assertEqual(reproductions.started_units(paths, ["TestCase"]), {"rgw_case"})
            self.assertEqual(reproductions.started_units(paths, ["TestHostCase"]), {"rgw_case/host"})
            self.assertEqual(reproductions.started_units(paths, ["TestCase", "TestHostCase", "TestParsesTheResponse"]),
                             {"rgw_case", "rgw_case/host"})

    def test_declaration_boundaries_survive_one_line_functions_and_literals(self):
        source = """package rgw_case_test

import "testing"

func helper() {}

func TestNative(t *testing.T) {
	repro := harness.StartReproduction(t, "rgw_case", "trigger")
	repro.Finish()
}

func describe(v interface{}) string { return "}" + '{' + `}` } // }

/* func TestHidden(t *testing.T) { } */
var template = `
func TestRaw(t *testing.T) {
}
`

func TestOther(t *testing.T) {
	_ = describe(helper)
	_ = "\\"}"
}
"""
        names = [reproductions.FUNCTION_NAME.match(d)[1] for d in reproductions.top_level_declarations(source)
                 if reproductions.FUNCTION_NAME.match(d)]
        self.assertEqual(names, ["helper", "TestNative", "describe", "TestOther"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "case_native_regression_test.go"
            path.write_text(source)
            self.assertEqual(reproductions.source_units(directory), {"rgw_case"})
            self.assertEqual(reproductions.started_units([path], ["TestNative"]), {"rgw_case"})
            self.assertEqual(reproductions.started_units([path], ["TestOther"]), set())

    def test_repository_profiles_expect_verdicts_only_of_their_reproducers(self):
        import tag_scenarios
        source = tag_scenarios.load_catalog(ROOT)
        profiles = tag_scenarios.discover(source)["include"] + tag_scenarios.discover_optional(source)["include"]
        started = {}
        for profile in profiles:
            catalogue = reproductions.load(profile["package"], ROOT)
            if catalogue is None:
                continue
            units = reproductions.started_units(
                tag_scenarios.compiled_paths(source, profile["package"], profile["tags"], ROOT),
                tag_scenarios.selected_tests(source, profile["package"], profile["tags"]))
            if not profile["requires_ceph"]:
                self.assertEqual(units, set(), profile["id"])
            started.setdefault(profile["package"], set()).update(units)
        self.assertTrue(started)
        for package, units in started.items():
            self.assertEqual(units, set(reproductions.load(package, ROOT)["units"]), package)


if __name__ == "__main__":
    unittest.main()
