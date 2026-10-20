import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

import messenger_reference

REFERENCE = Path(__file__).resolve().parents[2] / "internal" / "integration" / "testdata" / "messenger_reference.json"

RADOS = """\
#define __CEPH_FORALL_OSD_OPS(f)					    \\
	f(READ,		__CEPH_OSD_OP(RD, DATA, 1),	"read")		    \\
	f(CMPEXT,	__CEPH_OSD_OP(RD, DATA, 32),	"cmpext")	    \\
	f(PULL,		__CEPH_OSD_OP1(SUB, 1),		"pull")		    \\
	f(CALL,		__CEPH_OSD_OP(RD, EXEC, 1),	"call")		    \\
"""
CEPH_FS = """\
#define CEPH_MSG_OSD_OP                 42
#define CEPH_MSG_CLIENT_CAPS            0x310
"""
MESSAGE = """\
#define MSG_OSD_SUBOP      76
#define MSG_MON_GET_PURGED_SNAPS 76  // shares a value
#define MSG_CRC_DATA           (1 << 0)
#define MSG_CRC_ALL            (MSG_CRC_DATA | MSG_CRC_HEADER)
"""
FEATURES = """\
#define DEFINE_CEPH_FEATURE(bit, incarnation, name)			\\
 *      DEFINE_CEPH_FEATURE_RETIRED( 2, 1, MON_METADATA, MIMIC, OCTOPUS)
DEFINE_CEPH_FEATURE_RETIRED(21, 1, MON_GV, HAMMER, JEWEL)
DEFINE_CEPH_FEATURE(21, 2, SERVER_LUMINOUS)  // 4.13
DEFINE_CEPH_FEATURE(21, 2, RADOS_BACKOFF)    // overlap
DEFINE_CEPH_FEATURE_DEPRECATED(5, 1, OLD, MIMIC)
DEFINE_CEPH_FEATURE(49, 2, SERVER_SQUID);
"""
MSGR = """\
#define DEFINE_MSGR2_FEATURE(bit, incarnation, name)               \\
DEFINE_MSGR2_FEATURE(0, 1, REVISION_1)   // msgr2.1
DEFINE_MSGR2_FEATURE(1, 1, COMPRESSION)  // on-wire compression
"""


def tree(root, **overrides):
    files = {"src/include/rados.h": RADOS, "src/include/ceph_fs.h": CEPH_FS, "src/msg/Message.h": MESSAGE,
             "src/include/ceph_features.h": FEATURES, "src/include/msgr.h": MSGR}
    files.update(overrides)
    for path, text in files.items():
        target = Path(root) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    return root


class ExtractTest(unittest.TestCase):
    def test_keeps_every_declaration_in_its_namespace(self):
        with tempfile.TemporaryDirectory() as root:
            entries, digests = messenger_reference.extract(tree(root))
        by_symbol = {e["symbol"]: e for e in entries}
        self.assertEqual(set(digests), {p for paths in messenger_reference.SOURCES.values() for p in paths})
        self.assertEqual(by_symbol["CEPH_OSD_OP_CMPEXT"], {
            "kind": "osd_op", "symbol": "CEPH_OSD_OP_CMPEXT", "source": "src/include/rados.h", "line": 3,
            "mode": "RD", "type": "DATA", "code": 32, "name": "cmpext"})
        self.assertEqual((by_symbol["CEPH_OSD_OP_PULL"]["mode"], by_symbol["CEPH_OSD_OP_PULL"]["type"]), ("SUB", ""))
        self.assertEqual(by_symbol["CEPH_MSG_CLIENT_CAPS"]["value"], 0x310)
        # Message types that share a value stay separate; flags are not types.
        self.assertEqual(by_symbol["MSG_OSD_SUBOP"]["value"], by_symbol["MSG_MON_GET_PURGED_SNAPS"]["value"])
        self.assertNotIn("MSG_CRC_DATA", by_symbol)
        # Feature aliases of one (bit, incarnation) keep their own names.
        self.assertEqual([(e["bit"], e["incarnation"]) for e in entries if e["symbol"] in
                          ("CEPH_FEATURE_SERVER_LUMINOUS", "CEPH_FEATURE_RADOS_BACKOFF")], [(21, 2), (21, 2)])
        self.assertEqual(by_symbol["CEPH_FEATURE_MON_GV"]["lifecycle"], "retired")
        self.assertEqual(by_symbol["CEPH_FEATURE_MON_GV"]["retired"], "JEWEL")
        self.assertEqual(by_symbol["CEPH_FEATURE_OLD"]["deprecated"], "MIMIC")
        self.assertIn("CEPH_FEATURE_SERVER_SQUID", by_symbol)
        # The documentation example inside a comment is not a declaration.
        self.assertNotIn("CEPH_FEATURE_MON_METADATA", by_symbol)
        self.assertEqual(by_symbol["CEPH_MSGR2_FEATURE_COMPRESSION"]["bit"], 1)

    def test_unparsed_declarations_fail(self):
        for path, text in (("src/include/rados.h", RADOS + "\tf(ODD, something_else, \"odd\")\n"),
                           ("src/include/ceph_fs.h", CEPH_FS + "#define CEPH_MSG_COMPUTED (CEPH_MSG_OSD_OP + 1)\n"),
                           ("src/include/ceph_features.h", FEATURES + "DEFINE_CEPH_FEATURE(BIT, 1, NAME)\n"),
                           ("src/include/msgr.h", MSGR + "DEFINE_MSGR2_FEATURE(2, 1)\n")):
            with self.subTest(path=path), tempfile.TemporaryDirectory() as root:
                with self.assertRaisesRegex(messenger_reference.ReferenceError, "unparsed"):
                    messenger_reference.extract(tree(root, **{path: text}))

    def test_duplicate_and_missing_declarations_fail(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(messenger_reference.ReferenceError, "is declared at"):
                messenger_reference.extract(tree(root, **{"src/msg/Message.h": MESSAGE + "#define CEPH_MSG_OSD_OP 43\n"}))
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(messenger_reference.ReferenceError, "no osd_op declarations"):
                messenger_reference.extract(tree(root, **{"src/include/rados.h": "/* empty */\n"}))

    def test_generate_then_check(self):
        with tempfile.TemporaryDirectory() as root:
            tree(root)
            output = Path(root) / "reference.json"
            commit = "7f793731f1b39eb4f465e960113d2363c311b964"
            self.assertEqual(messenger_reference.main(["generate", "--source", root, "--commit", commit,
                                                       "--release", "20.2.4", "--output", str(output)]), 0)
            self.assertEqual(json.loads(output.read_text())["counts"],
                             {"osd_op": 4, "message": 4, "feature": 5, "msgr2_feature": 2})
            self.assertEqual(messenger_reference.main(["check", "--source", root, "--reference", str(output)]), 0)
            tree(root, **{"src/include/rados.h": RADOS + '\tf(STAT,\t__CEPH_OSD_OP(RD, DATA, 2),\t"stat")\n'})
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(messenger_reference.main(["check", "--source", root, "--reference", str(output)]), 1)
                self.assertEqual(messenger_reference.main(["generate", "--source", root, "--commit", "main",
                                                           "--release", "x", "--output", str(output)]), 2)


class CommittedReferenceTest(unittest.TestCase):
    """The committed reference is the one the validator reads."""

    def test_committed_reference_is_consistent(self):
        doc = json.loads(REFERENCE.read_text())
        self.assertRegex(doc["ceph_commit"], r"^[0-9a-f]{40}$")
        counts = {}
        for entry in doc["entries"]:
            counts[entry["kind"]] = counts.get(entry["kind"], 0) + 1
        self.assertEqual(counts, doc["counts"])
        self.assertEqual(messenger_reference.render(doc), REFERENCE.read_text())
        symbols = [(e["kind"], e["symbol"]) for e in doc["entries"]]
        self.assertEqual(len(symbols), len(set(symbols)))


if __name__ == "__main__":
    unittest.main()
