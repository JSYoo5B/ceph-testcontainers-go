//go:build all || integration

package cephfs_test

import (
	"crypto/sha256"
	"encoding/hex"
)

// jeProbeScript is run as `sh -c` in an idle container of a role image. It
// only inspects: command lookup, one bounded filesystem search by name, and
// the Ceph version banner. It never starts a tool under test.
const jeProbeScript = `
echo probe-begin
echo "ceph-version $(ceph --version 2>&1 | head -n 1)"
for t in cephfs-journal-tool cephfs-table-tool cephfs-data-scan; do
  if p=$(command -v "$t" 2>/dev/null) && [ -n "$p" ]; then echo "tool $t present $p"; else echo "tool $t absent"; fi
done
if command -v find >/dev/null 2>&1; then
  hits=$(find / -xdev -type f \( -name cephfs-journal-tool -o -name cephfs-table-tool -o -name cephfs-data-scan \) -print 2>/dev/null)
  echo "find rc=$?"
  printf '%s\n' "$hits" | while IFS= read -r h; do [ -n "$h" ] && echo "hit $h"; done
else
  echo "find unavailable"
fi
echo probe-end
exit 0
`

func jeHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// jeClip bounds recorded process output.
func jeClip(s string) string {
	const limit = 8192
	if len(s) > limit {
		return s[:limit] + "...[clipped]"
	}
	return s
}
