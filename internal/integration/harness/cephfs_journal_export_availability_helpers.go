//go:build all || integration

package harness

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// These helpers classify what a read-only probe of one published role image
// reports about the CephFS offline journal tool. The probe asks the image
// itself; the image-build source rule that filters unselected executables is
// not evidence about an executed image. Nothing here runs a journal tool.

// jeTools are the offline repair executables that the pinned Tentacle 20.2.4
// source installs next to each other (src/tools/cephfs/CMakeLists.txt) and
// that its RPM packaging places in ceph-common. Only JeJournalTool decides
// the verdict; the other two are recorded for context.
const JeJournalTool = "cephfs-journal-tool"

var jeTools = []string{JeJournalTool, "cephfs-table-tool", "cephfs-data-scan"}

// JeProbeScript is run as `sh -c` in an idle container of a role image. It
// only inspects: command lookup, one bounded filesystem search by name, and
// the Ceph version banner. It never starts a tool under test.
const JeProbeScript = `
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

type jeVerdict string

const (
	JeAbsent       jeVerdict = "absent"
	JePresent      jeVerdict = "present"
	JeInconclusive jeVerdict = "inconclusive"
)

// jeProbeResult is the parsed outcome for one role image.
type jeProbeResult struct {
	Verdict     jeVerdict
	Reason      string
	Tools       map[string]string // tool -> "absent" or "present:<path>"
	Hits        []string
	FindRC      *int
	FindMissing bool
	CephVersion string
	CephCommit  string
	CephRelease string
}

var (
	jeVersionLine = regexp.MustCompile(`^ceph-version ceph version (\d+\.\d+\.\d+) \(([0-9a-f]{40})\) ([a-z]+) `)
	jeToolLine    = regexp.MustCompile(`^tool (cephfs-journal-tool|cephfs-table-tool|cephfs-data-scan) (absent|present (/\S+))$`)
	jeFindLine    = regexp.MustCompile(`^find rc=(\d{1,3})$`)
	jeHitLine     = regexp.MustCompile(`^hit (/\S+)$`)
)

// JeParseProbe decodes the probe output. exitCode and stopped come from the
// supervised run. Anything that is missing, repeated, unrecognized or out of
// order is inconclusive; it is never read as "the tool is absent".
func JeParseProbe(stdout string, exitCode int, stopped bool) jeProbeResult {
	res := jeProbeResult{Verdict: JeInconclusive, Tools: map[string]string{}}
	fail := func(format string, args ...any) jeProbeResult {
		res.Verdict, res.Reason = JeInconclusive, fmt.Sprintf(format, args...)
		return res
	}
	if stopped {
		return fail("probe stopped at its limit")
	}
	if exitCode != 0 {
		return fail("probe exit code %d", exitCode)
	}
	var lines []string
	sc := bufio.NewScanner(strings.NewReader(stdout))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return fail("probe output unreadable: %v", err)
	}
	if len(lines) < 2 || lines[0] != "probe-begin" || lines[len(lines)-1] != "probe-end" {
		return fail("probe-begin/probe-end framing absent")
	}
	versions, finds := 0, 0
	for _, line := range lines[1 : len(lines)-1] {
		switch {
		case strings.HasPrefix(line, "ceph-version "):
			versions++
			if m := jeVersionLine.FindStringSubmatch(line); m != nil {
				res.CephVersion, res.CephCommit, res.CephRelease = m[1], m[2], m[3]
			}
		case jeToolLine.MatchString(line):
			m := jeToolLine.FindStringSubmatch(line)
			if _, dup := res.Tools[m[1]]; dup {
				return fail("tool %q reported twice", m[1])
			}
			if m[3] != "" {
				res.Tools[m[1]] = "present:" + m[3]
			} else {
				res.Tools[m[1]] = "absent"
			}
		case line == "find unavailable":
			finds++
			res.FindMissing = true
		case jeFindLine.MatchString(line):
			finds++
			rc, _ := strconv.Atoi(jeFindLine.FindStringSubmatch(line)[1])
			res.FindRC = &rc
		case jeHitLine.MatchString(line):
			if res.FindRC == nil {
				return fail("hit before the find result")
			}
			res.Hits = append(res.Hits, jeHitLine.FindStringSubmatch(line)[1])
		default:
			return fail("unrecognized probe line %q", line)
		}
	}
	if versions != 1 {
		return fail("expected one ceph-version line, saw %d", versions)
	}
	if res.CephVersion == "" {
		return fail("ceph version banner unparsable")
	}
	for _, tool := range jeTools {
		if _, ok := res.Tools[tool]; !ok {
			return fail("tool %q not reported", tool)
		}
	}
	if finds != 1 {
		return fail("expected one find result, saw %d", finds)
	}
	sort.Strings(res.Hits)
	journalHit := false
	for _, h := range res.Hits {
		if strings.HasSuffix(h, "/"+JeJournalTool) {
			journalHit = true
		}
	}
	switch {
	case strings.HasPrefix(res.Tools[JeJournalTool], "present:") || journalHit:
		res.Verdict, res.Reason = JePresent, "journal tool found by command lookup or filesystem search"
	case res.FindMissing:
		return fail("command lookup alone cannot exclude an unlisted location: find is unavailable")
	case res.FindRC == nil || *res.FindRC != 0:
		return fail("filesystem search did not complete cleanly")
	default:
		res.Verdict, res.Reason = JeAbsent, "command lookup and a complete filesystem search found no journal tool"
	}
	return res
}

// JeRoleReceipt is one role's record. Output is bounded and copied.
type JeRoleReceipt struct {
	Role         string
	Configured   bool
	ImageRef     string
	ImageID      string
	Verdict      jeVerdict
	Reason       string
	Tools        map[string]string
	Hits         []string
	CephVersion  string
	CephCommit   string
	CephRelease  string
	ExitCode     int
	StdoutSHA256 string
	Stdout       string
	Stderr       string
}

// JeOverall combines the role receipts. A role that was not configured, or
// whose probe was not conclusive, keeps the whole answer inconclusive unless
// some other role already has the tool, which is a positive finding.
func JeOverall(roles []JeRoleReceipt) (jeVerdict, string) {
	absent, present, other := 0, []string{}, []string{}
	for _, r := range roles {
		switch {
		case !r.Configured:
			other = append(other, r.Role+": not configured")
		case r.Verdict == JePresent:
			present = append(present, r.Role)
		case r.Verdict == JeAbsent:
			absent++
		default:
			other = append(other, r.Role+": "+r.Reason)
		}
	}
	switch {
	case len(present) > 0:
		return JePresent, "present in roles " + strings.Join(present, ",")
	case len(other) > 0 || len(roles) == 0:
		return JeInconclusive, strings.Join(other, "; ")
	default:
		return JeAbsent, fmt.Sprintf("absent in all %d probed role images", absent)
	}
}

func JeHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// JeClip bounds recorded process output.
func JeClip(s string) string {
	const limit = 8192
	if len(s) > limit {
		return s[:limit] + "...[clipped]"
	}
	return s
}
