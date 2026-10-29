import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import check_image_vocabulary as guard  # noqa: E402

RETIRED = (
    "func (c *Container) " + "Control" + "Image() string",
    "options.control" + "Image",
    "The control " + "image supplies Python.",
    "needs the control" + "-image helper",
    "the `control` or `all` role",
    "images (control, OSD, MDS, RGW)",
    "official-20.2.4-" + "control",
    "official-<release>-" + "control",
)
VALID = (
    "ControlContainer runs the ceph CLI",
    "the control container and the control plane",
    "EssentialImage returns the essential image",
    "a negative control for the parser",
    "official-20.2.4-essential",
    "the controller image of another tool",
    "essential, client, cephfs, rbd-aux, rgw or all",
)


class VocabularyTest(unittest.TestCase):
    def test_every_retired_spelling_is_found(self):
        for text in RETIRED:
            with self.subTest(text=text):
                self.assertTrue(guard.scan_text("x.md", text), text)

    def test_current_and_control_plane_wording_passes(self):
        for text in VALID:
            with self.subTest(text=text):
                self.assertEqual(guard.scan_text("x.md", text), [])

    def test_historical_records_are_skipped_and_everything_else_is_scanned(self):
        with tempfile.TemporaryDirectory() as root:
            subprocess.check_call(["git", "init", "-q", root])
            for path in ("docs/design/api-naming/moves/227-image-role-overhaul.json", "docs/guide.md", "ceph/a.go"):
                os.makedirs(os.path.dirname(os.path.join(root, path)) or root, exist_ok=True)
                with open(os.path.join(root, path), "w") as handle:
                    handle.write("the control " + "image\n")
            subprocess.check_call(["git", "-C", root, "add", "-A"])
            found = sorted({p[0] for p in guard.scan(root)})
            self.assertEqual(found, ["ceph/a.go", "docs/guide.md"])

    def test_the_repository_is_clean(self):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        self.assertEqual(guard.scan(root), [])

    def test_allowed_paths_exist_or_are_optional_records(self):
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        required = [p for p in guard.ALLOWED if p != "docs/design/image-role-terminology.md"]
        for path in required:
            self.assertTrue(os.path.exists(os.path.join(root, path)), path)


if __name__ == "__main__":
    unittest.main()
