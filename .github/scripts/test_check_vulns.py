import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import check_vulns

FIXTURES = Path(__file__).resolve().parent / "fixtures"
FIXTURE = FIXTURES / "govulncheck-before-update.json"
PROVENANCE = FIXTURES / "govulncheck-before-update.provenance.json"

CONFIG = {"config": {"protocol_version": "v1.0.0", "scanner_name": "govulncheck", "scanner_version": "v1.9.0",
                     "db": "https://vuln.go.dev", "db_last_modified": "2026-10-08T22:31:09Z",
                     "go_version": "go1.27.2", "scan_level": "symbol", "scan_mode": "source"}}


def finding(osv, module="example.com/m", package=None, function=None, fixed="v1.2.3"):
    frame = {"module": module, "version": "v1.0.0"}
    if package:
        frame["package"] = package
    if function:
        frame["function"] = function
    return {"finding": {"osv": osv, "fixed_version": fixed, "trace": [frame]}}


def stream(*objects):
    return "".join(json.dumps(o, indent=2) + "\n" for o in objects)


class CheckVulnsTests(unittest.TestCase):
    def run_main(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scan.json"
            path.write_text(text)
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                code = check_vulns.main([str(path)])
        return code, out.getvalue(), err.getvalue()

    def test_fixture_is_the_recorded_extract(self):
        provenance = json.loads(PROVENANCE.read_text())
        self.assertEqual(hashlib.sha256(FIXTURE.read_bytes()).hexdigest(), provenance["fixture_sha256"])
        self.assertEqual((provenance["json_exit_code"], provenance["convert_exit_code"]), (0, 3))
        self.assertEqual(provenance["historical_origin"]["source_commit"], "0317a8f")
        self.assertEqual(provenance["historical_origin"]["unavailable"], ["source_commit"])
        self.assertTrue(provenance["evidence_role"].startswith("historical scanner fixture"))

    def test_the_scan_before_the_update_fails_although_govulncheck_exited_zero(self):
        code, out, err = self.run_main(FIXTURE.read_text())
        self.assertEqual(code, 1)
        self.assertIn("GO-2026-6253, GO-2026-6354, GO-2026-6355", err)
        config, results = check_vulns.evaluate(FIXTURE.read_text())
        levels = {result["osv"]: result["level"] for result in results}
        self.assertEqual(levels, {"GO-2026-6253": "symbol", "GO-2026-6354": "symbol", "GO-2026-6355": "symbol",
                                  "GO-2026-6303": "package", "GO-2026-5841": "module", "GO-2026-5932": "module"})
        self.assertIn("FAIL GO-2026-6303 (package level)", out)
        self.assertIn("info GO-2026-5932 (module level) in golang.org/x/crypto@v0.54.0, fixed in no fix", out)

    def test_the_most_precise_level_of_a_vulnerability_counts(self):
        text = stream(CONFIG, finding("GO-1"), finding("GO-1", package="example.com/m/p"),
                      finding("GO-1", package="example.com/m/p", function="F"), finding("GO-1"))
        self.assertEqual(check_vulns.evaluate(text)[1][0]["level"], "symbol")

    def test_module_only_findings_pass_and_package_findings_fail(self):
        code, out, _ = self.run_main(stream(CONFIG, finding("GO-2", fixed="")))
        self.assertEqual(code, 0)
        self.assertIn("info GO-2", out)
        self.assertEqual(self.run_main(stream(CONFIG, finding("GO-3", package="example.com/m/p")))[0], 1)

    def test_a_clean_scan_passes(self):
        code, out, _ = self.run_main(stream(CONFIG, {"progress": {"message": "Scanning..."}}))
        self.assertEqual(code, 0)
        self.assertIn("no vulnerabilities", out)

    def test_broken_streams_fail(self):
        for name, text in {
            "empty": "",
            "no config": stream({"progress": {"message": "x"}}),
            "truncated": stream(CONFIG, finding("GO-4", function="F"))[:-40],
            "not an object": stream(CONFIG) + "[1, 2]\n",
            "finding without trace": stream(CONFIG, {"finding": {"osv": "GO-5", "trace": []}}),
            "finding without id": stream(CONFIG, {"finding": {"trace": [{"module": "m"}]}}),
            "other scanner": stream({"config": dict(CONFIG["config"], scanner_name="other")}),
        }.items():
            with self.subTest(name):
                self.assertEqual(self.run_main(text)[0], 2)


if __name__ == "__main__":
    unittest.main()
