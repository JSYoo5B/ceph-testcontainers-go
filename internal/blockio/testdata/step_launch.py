"""Exercise the keeper's step receipts without invoking device-mapper."""
import contextlib
import errno
import io
import json
import os
import subprocess
import sys
from unittest import TestCase, mock

script, work = sys.argv[1:]
check = TestCase()
check.assertIn('work = "/tc-blk"', script)
script = script.replace('work = "/tc-blk"', 'work = ' + repr(work))


def keeper(*args):
    output = io.StringIO()
    with mock.patch.object(sys, "argv", ["keeper", args[0], "owned", *args[1:]]):
        with contextlib.redirect_stdout(output):
            exec(script, {})
    return json.loads(output.getvalue())


with mock.patch.object(subprocess, "Popen", side_effect=OSError(errno.EAGAIN, "launch refused")):
    failed = keeper("step", "load", "0", "failed", "0 8 error")
for receipt in [failed, keeper("settled", "failed"), keeper("settled", "failed")]:
    check.assertTrue(receipt["done"])
    check.assertNotEqual(receipt["rc"], 0)
    check.assertIn("launch refused", receipt["error"])

popen = subprocess.Popen


def delayed(argv, **kwargs):
    # At this point the caller has claimed the step but no child exists.
    pending = keeper("settled", "delayed")
    check.assertTrue(pending["started"])
    check.assertFalse(pending["done"])
    # Run the real detached status writer around a harmless command.
    check.assertEqual(argv[4:], ["dmsetup", "suspend", "owned"])
    return popen(argv[:4] + ["true"], **kwargs)


with mock.patch.dict(os.environ, TC_DM_UUID=""):
    with mock.patch.object(subprocess, "Popen", side_effect=delayed) as launch:
        finished = keeper("step", "suspend", "5", "delayed")
        launch.assert_called_once()
for receipt in [finished, keeper("settled", "delayed")]:
    check.assertTrue(receipt["done"])
    check.assertEqual(receipt["rc"], 0)
