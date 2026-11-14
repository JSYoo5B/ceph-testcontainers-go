"""Verify cache publication at the tagged-runtime caller boundary."""

from pathlib import Path
import re
import unittest


WORKFLOWS = Path(__file__).resolve().parents[1] / "workflows"


class RuntimeCacheScopeTests(unittest.TestCase):
    def test_every_selected_ref_has_a_cache_write_policy(self):
        # Each source expression must be bound to the same immutable event SHA
        # that defines this run. A comment's selected PR head is restore-only.
        policies = {
            "${{ github.event_name == 'workflow_dispatch' && inputs.ref || '' }}":
                "${{ inputs.ref == '' || inputs.ref == github.sha }}",
            "${{ inputs.ref || github.sha }}": "${{ inputs.ref == '' || inputs.ref == github.sha }}",
            "${{ needs.pin.outputs.sha }}": "${{ needs.pin.outputs.sha == github.sha }}",
            "${{ needs.command.outputs.sha }}": "false",
        }
        found = set()
        for path in WORKFLOWS.glob("*.yml"):
            jobs = re.split(r"^  [\w-]+:\s*$", path.read_text().partition("\njobs:\n")[2], flags=re.M)
            for job in jobs:
                if "uses: ./.github/workflows/tagged-runtime.yml" not in job:
                    continue
                fields = dict(re.findall(r"^      (ref|save_caches): (.+)$", job, re.M))
                if "ref" in fields:
                    source = fields["ref"]
                    with self.subTest(workflow=path.name, source=source):
                        self.assertIn(source, policies)
                        self.assertEqual(fields.get("save_caches"), policies[source])
                    found.add(source)
        self.assertEqual(found, set(policies))
