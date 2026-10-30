//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// Docker-free controls for the read-only journal export characterization.
// The probe outputs below are synthetic text in the protocol of
// JeProbeScript. No image was inspected, no journal tool ran and no export was
// taken here. The observation adapter feeds an export through the existing
// jbAdmit rule of the persisted-end versus payload-end admission model
// without changing it; the adapter's inputs are synthetic as well.

const jeCommit = "7f793731f1b39eb4f465e960113d2363c311b964"

func jeProbeOutput(version string, tools string, find string) string {
	return "probe-begin\n" + version + "\n" + tools + find + "probe-end\n"
}

const (
	jeVersionOK = "ceph-version ceph version 20.2.4 (" + jeCommit + ") tentacle (stable - RelWithDebInfo)"
	jeToolsNone = "tool cephfs-journal-tool absent\ntool cephfs-table-tool absent\ntool cephfs-data-scan absent\n"
	jeToolsAll  = "tool cephfs-journal-tool present /usr/bin/cephfs-journal-tool\n" +
		"tool cephfs-table-tool present /usr/bin/cephfs-table-tool\n" +
		"tool cephfs-data-scan present /usr/bin/cephfs-data-scan\n"
	jeFindClean = "find rc=0\n"
)

func TestJournalExportAvailabilityAbsentNeedsCompleteProbe(t *testing.T) {
	res := harness.JeParseProbe(jeProbeOutput(jeVersionOK, jeToolsNone, jeFindClean), 0, false)
	if res.Verdict != harness.JeAbsent || res.CephVersion != "20.2.4" || res.CephCommit != jeCommit || res.CephRelease != "tentacle" {
		t.Fatalf("complete negative probe: %+v", res)
	}
	if res.Tools["cephfs-table-tool"] != "absent" {
		t.Fatalf("context tools not recorded: %+v", res.Tools)
	}
}

func TestJournalExportAvailabilityPresentByLookupOrByFilesystemHit(t *testing.T) {
	byLookup := harness.JeParseProbe(jeProbeOutput(jeVersionOK, jeToolsAll, jeFindClean+"hit /usr/bin/cephfs-data-scan\nhit /usr/bin/cephfs-journal-tool\nhit /usr/bin/cephfs-table-tool\n"), 0, false)
	if byLookup.Verdict != harness.JePresent || byLookup.Tools[harness.JeJournalTool] != "present:/usr/bin/cephfs-journal-tool" || len(byLookup.Hits) != 3 {
		t.Fatalf("lookup: %+v", byLookup)
	}
	// Not on PATH, found only by the filesystem search.
	byHit := harness.JeParseProbe(jeProbeOutput(jeVersionOK, jeToolsNone, jeFindClean+"hit /opt/x/cephfs-journal-tool\n"), 0, false)
	if byHit.Verdict != harness.JePresent {
		t.Fatalf("hit: %+v", byHit)
	}
	// Another repair tool alone does not make the journal tool present.
	other := harness.JeParseProbe(jeProbeOutput(jeVersionOK, "tool cephfs-journal-tool absent\ntool cephfs-table-tool present /usr/bin/cephfs-table-tool\ntool cephfs-data-scan absent\n", jeFindClean+"hit /usr/bin/cephfs-table-tool\n"), 0, false)
	if other.Verdict != harness.JeAbsent {
		t.Fatalf("other tool: %+v", other)
	}
}

func TestJournalExportAvailabilityBrokenProbesStayInconclusive(t *testing.T) {
	ok := func(version, tools, find string) string { return jeProbeOutput(version, tools, find) }
	cases := map[string]struct {
		out     string
		exit    int
		stopped bool
	}{
		"empty output":         {out: ""},
		"no begin":             {out: strings.TrimPrefix(ok(jeVersionOK, jeToolsNone, jeFindClean), "probe-begin\n")},
		"truncated, no end":    {out: strings.TrimSuffix(ok(jeVersionOK, jeToolsNone, jeFindClean), "probe-end\n")},
		"nonzero exit":         {out: ok(jeVersionOK, jeToolsNone, jeFindClean), exit: 3},
		"stopped at limit":     {out: ok(jeVersionOK, jeToolsNone, jeFindClean), stopped: true},
		"journal line missing": {out: ok(jeVersionOK, "tool cephfs-table-tool absent\ntool cephfs-data-scan absent\n", jeFindClean)},
		"duplicate tool":       {out: ok(jeVersionOK, jeToolsNone+"tool cephfs-journal-tool absent\n", jeFindClean)},
		"relative tool path":   {out: ok(jeVersionOK, "tool cephfs-journal-tool present cephfs-journal-tool\ntool cephfs-table-tool absent\ntool cephfs-data-scan absent\n", jeFindClean)},
		"unknown tool word":    {out: ok(jeVersionOK, "tool cephfs-journal-tool maybe\ntool cephfs-table-tool absent\ntool cephfs-data-scan absent\n", jeFindClean)},
		"no version":           {out: ok("", jeToolsNone, jeFindClean)},
		"shell error version":  {out: ok("ceph-version sh: ceph: not found", jeToolsNone, jeFindClean)},
		"short commit":         {out: ok("ceph-version ceph version 20.2.4 (abc) tentacle (stable)", jeToolsNone, jeFindClean)},
		"two versions":         {out: ok(jeVersionOK+"\n"+jeVersionOK, jeToolsNone, jeFindClean)},
		"find missing":         {out: ok(jeVersionOK, jeToolsNone, "find unavailable\n")},
		"find incomplete rc":   {out: ok(jeVersionOK, jeToolsNone, "find rc=1\n")},
		"find rc text":         {out: ok(jeVersionOK, jeToolsNone, "find rc=null\n")},
		"find rc absent":       {out: ok(jeVersionOK, jeToolsNone, "")},
		"find twice":           {out: ok(jeVersionOK, jeToolsNone, jeFindClean+jeFindClean)},
		"hit before find":      {out: ok(jeVersionOK, jeToolsNone, "hit /usr/bin/cephfs-journal-tool\n"+jeFindClean)},
		"unrecognized line":    {out: ok(jeVersionOK, jeToolsNone, jeFindClean+"warning: something\n")},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if res := harness.JeParseProbe(c.out, c.exit, c.stopped); res.Verdict != harness.JeInconclusive || res.Reason == "" {
				t.Fatalf("expected inconclusive with a reason, got %+v", res)
			}
		})
	}
	// A positive finding survives an incomplete search: a present journal
	// tool is evidence even when find ended early.
	if res := harness.JeParseProbe(ok(jeVersionOK, jeToolsAll, "find rc=1\n"), 0, false); res.Verdict != harness.JePresent {
		t.Fatalf("present with early find end: %+v", res)
	}
}

func TestJournalExportAvailabilityOverallNeverInventsAbsence(t *testing.T) {
	absent := func(role string) harness.JeRoleReceipt {
		return harness.JeRoleReceipt{Role: role, Configured: true, Verdict: harness.JeAbsent}
	}
	all := []harness.JeRoleReceipt{absent("essential"), absent("client"), absent("cephfs"), absent("rbd-aux"), absent("rgw")}
	if v, _ := harness.JeOverall(all); v != harness.JeAbsent {
		t.Fatalf("all absent: %s", v)
	}
	unset := append([]harness.JeRoleReceipt{}, all...)
	unset[3] = harness.JeRoleReceipt{Role: "rbd-aux", Verdict: harness.JeAbsent}
	if v, _ := harness.JeOverall(unset); v != harness.JeInconclusive {
		t.Fatalf("an unconfigured role must keep the answer inconclusive: %s", v)
	}
	broken := append([]harness.JeRoleReceipt{}, all...)
	broken[1] = harness.JeRoleReceipt{Role: "client", Configured: true, Verdict: harness.JeInconclusive, Reason: "probe"}
	if v, _ := harness.JeOverall(broken); v != harness.JeInconclusive {
		t.Fatalf("an inconclusive role: %s", v)
	}
	found := append([]harness.JeRoleReceipt{}, broken...)
	found[2] = harness.JeRoleReceipt{Role: "cephfs", Configured: true, Verdict: harness.JePresent}
	if v, reason := harness.JeOverall(found); v != harness.JePresent || !strings.Contains(reason, "cephfs") {
		t.Fatalf("presence is a positive finding even with an inconclusive role: %s %s", v, reason)
	}
	if v, _ := harness.JeOverall(nil); v != harness.JeInconclusive {
		t.Fatalf("no roles: %s", v)
	}
}

// jeOutcome classifies one captured export attempt before the admission model
// sees any bytes.
type jeOutcome string

const (
	jeObsInconclusive jeOutcome = "inconclusive"
	jeObsToolFailed   jeOutcome = "tool-failed"
	jeObsAdmission    jeOutcome = "admission-recorded"
)

// jeObserve decodes one export capture and, only for a zero exit status with
// intact observation fields, runs the existing jbAdmit on the untouched
// export. Any classification of jbAdmit is a valid recorded result. Fields
// must be present with the exact JSON type; nothing is defaulted to zero.
func jeObserve(obs []byte, export []byte) (jeOutcome, string, jbRecord) {
	var raw map[string]any
	if err := json.Unmarshal(obs, &raw); err != nil || raw == nil {
		return jeObsInconclusive, "observation is not a JSON object", jbRecord{}
	}
	integer := func(name string) (uint64, bool) {
		f, ok := raw[name].(float64)
		if !ok || f < 0 || f != math.Trunc(f) || f > 1<<50 {
			return 0, false
		}
		return uint64(f), true
	}
	text := func(name string) (string, bool) { s, ok := raw[name].(string); return s, ok }
	exit, ok1 := raw["exit_code"].(float64)
	_, ok2 := text("stdout")
	_, ok3 := text("stderr")
	size, ok4 := integer("export_size")
	whole, ok5 := text("export_sha256")
	if !ok1 || exit != math.Trunc(exit) || !ok2 || !ok3 || !ok4 || !ok5 || len(whole) != 64 || export == nil {
		return jeObsInconclusive, "observation field missing, null or wrong-typed", jbRecord{}
	}
	if exit != 0 {
		return jeObsToolFailed, fmt.Sprintf("exit code %d", int(exit)), jbRecord{}
	}
	// The payload hash is derived from the header-declared start of the same
	// bytes; the whole-file hash and size are what the capture recorded.
	manifest := jbManifest{FileSize: size, WholeSHA256: whole}
	if fields, err := jbParseHeader(export[:min(len(export), jbHeaderLen)], false); err == nil && fields["start offset"] <= uint64(len(export)) {
		manifest.PayloadSHA256 = jbSum(export[fields["start offset"]:])
	}
	rec := jbAdmit(append([]byte(nil), export...), manifest, jbIntact)
	if rec.Class == jbInconclusive {
		return jeObsInconclusive, rec.Reason, rec
	}
	return jeObsAdmission, string(rec.Class), rec
}

func jeObsJSON(t *testing.T, export []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	obs := map[string]any{"exit_code": 0, "stdout": "", "stderr": "", "export_size": len(export), "export_sha256": jbSum(export)}
	if mutate != nil {
		mutate(obs)
	}
	data, err := json.Marshal(obs)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestJournalExportObservationRecordsEveryAdmissionClass(t *testing.T) {
	equal, _ := jbEqualBackup()
	divergent, _ := jbDivergentBackup()
	cases := []struct {
		name   string
		export []byte
		class  jbClass
	}{
		{"equal ends", equal, jbAdmitted},
		{"divergent ends", divergent, jbOutsideDivergentEnd},
		{"short transfer", equal[:len(equal)-1], jbShortOrCorrupt},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// The capture hash describes the bytes as exported; for the short
			// case the capture saw the full export, so the hash is that of
			// the full file and the transfer check must catch the difference.
			full := c.export
			if c.class == jbShortOrCorrupt {
				full = equal
			}
			obs := jeObsJSON(t, full, nil)
			outcome, detail, rec := jeObserve(obs, c.export)
			if outcome != jeObsAdmission || rec.Class != c.class {
				t.Fatalf("outcome %s %q class %s, want %s", outcome, detail, rec.Class, c.class)
			}
			if c.class == jbOutsideDivergentEnd && (rec.PersistedWritePos != jbPersisted || rec.PayloadEnd != jbProbed) {
				t.Fatalf("both ends must be kept: %+v", rec)
			}
		})
	}
}

func TestJournalExportObservationMissingOrWrongTypedStaysInconclusive(t *testing.T) {
	equal, _ := jbEqualBackup()
	cases := map[string]func(map[string]any){
		"exit missing":     func(o map[string]any) { delete(o, "exit_code") },
		"exit null":        func(o map[string]any) { o["exit_code"] = nil },
		"exit string":      func(o map[string]any) { o["exit_code"] = "0" },
		"exit fractional":  func(o map[string]any) { o["exit_code"] = 0.5 },
		"stdout null":      func(o map[string]any) { o["stdout"] = nil },
		"stderr number":    func(o map[string]any) { o["stderr"] = 1 },
		"size missing":     func(o map[string]any) { delete(o, "export_size") },
		"size negative":    func(o map[string]any) { o["export_size"] = -1 },
		"size string":      func(o map[string]any) { o["export_size"] = "4096" },
		"hash missing":     func(o map[string]any) { delete(o, "export_sha256") },
		"hash null":        func(o map[string]any) { o["export_sha256"] = nil },
		"hash short":       func(o map[string]any) { o["export_sha256"] = "abc" },
		"hash wrong typed": func(o map[string]any) { o["export_sha256"] = []string{"x"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if outcome, _, _ := jeObserve(jeObsJSON(t, equal, mutate), equal); outcome != jeObsInconclusive {
				t.Fatalf("outcome %s, want inconclusive", outcome)
			}
		})
	}
	t.Run("no export bytes", func(t *testing.T) {
		if outcome, _, _ := jeObserve(jeObsJSON(t, equal, nil), nil); outcome != jeObsInconclusive {
			t.Fatalf("outcome %s", outcome)
		}
	})
	t.Run("not an object", func(t *testing.T) {
		for _, in := range []string{"", "null", "[]", "3", "{"} {
			if outcome, _, _ := jeObserve([]byte(in), equal); outcome != jeObsInconclusive {
				t.Fatalf("%q: outcome %s", in, outcome)
			}
		}
	})
	t.Run("malformed header", func(t *testing.T) {
		bad := append([]byte(nil), equal...)
		copy(bad[:jbHeaderLen], []byte("Ceph mds0 journal dump\n start offset null\n\x04"))
		outcome, _, rec := jeObserve(jeObsJSON(t, bad, nil), bad)
		if outcome != jeObsInconclusive || rec.Class != jbInconclusive {
			t.Fatalf("outcome %s class %s", outcome, rec.Class)
		}
	})
}

func TestJournalExportObservationToolFailureIsNotAdmitted(t *testing.T) {
	equal, _ := jbEqualBackup()
	// A failed tool may leave a file behind; its bytes must not be admitted.
	outcome, _, rec := jeObserve(jeObsJSON(t, equal, func(o map[string]any) { o["exit_code"] = 1 }), equal)
	if outcome != jeObsToolFailed || rec.Class != "" {
		t.Fatalf("outcome %s class %q", outcome, rec.Class)
	}
}
