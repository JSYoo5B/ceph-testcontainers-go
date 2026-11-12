//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// verdictT records what a reproduction does to its test. FailNow stops the
// running function the way testing.T does, and end runs the cleanups.
type verdictT struct {
	logs, errors []string
	skipped      bool
	cleanups     []func()
}

type verdictStop struct{}

func (f *verdictT) Helper() {}
func (f *verdictT) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}
func (f *verdictT) Errorf(format string, args ...any) {
	f.errors = append(f.errors, fmt.Sprintf(format, args...))
}
func (f *verdictT) FailNow()          { panic(verdictStop{}) }
func (f *verdictT) Cleanup(fn func()) { f.cleanups = append(f.cleanups, fn) }
func (f *verdictT) Skipped() bool     { return f.skipped }

// run calls body like a test function, stopping at FailNow, then runs the
// cleanups in reverse order.
func (f *verdictT) run(body func()) {
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(verdictStop); !ok {
					panic(r)
				}
			}
		}()
		body()
	}()
	for i := len(f.cleanups) - 1; i >= 0; i-- {
		f.cleanups[i]()
	}
}

func (f *verdictT) verdictLine(t *testing.T) string {
	t.Helper()
	var lines []string
	for _, l := range f.logs {
		if strings.HasPrefix(l, harness.VerdictLinePrefix+" ") {
			lines = append(lines, l)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("want one verdict line, got %q", lines)
	}
	return lines[0]
}

func verdictEnv(expected string) func(string) string {
	return func(name string) string {
		switch name {
		case harness.ExpectedVerdictEnv:
			return expected
		case "CEPH_TEST_ESSENTIAL_IMAGE":
			return "sha256:e"
		case "CEPH_TEST_RGW_IMAGE":
			return "sha256:r"
		}
		return ""
	}
}

// outcome is what one evidence pattern leaves the unit with.
type outcome func(f *verdictT, r *harness.Reproduction)

var (
	observedSymptom outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Absent(f, "control", "accepted")
		r.Symptom(f, "trigger", "refused with 400")
		r.Conclude()
	}
	absentSymptom outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Absent(f, "control", "accepted")
		r.Absent(f, "trigger", "accepted")
		r.Conclude()
	}
	failedControl outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Inconclusive(f, "control failed: the omitted header was refused")
		r.Conclude()
	}
	stoppedEarly outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Absent(f, "control", "accepted")
		f.Errorf("start the gateway: timeout")
		f.FailNow()
	}
	incompleteObservation outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Absent(f, "control", "accepted")
		r.Conclude()
	}
)

func runVerdict(expected string, body outcome) *verdictT {
	f := &verdictT{}
	f.run(func() {
		r := harness.NewReproduction(f, verdictEnv(expected), "rgw_case", "control", "trigger")
		body(f, r)
	})
	return f
}

func TestReproductionVerdictComparison(t *testing.T) {
	for _, c := range []struct {
		name, expected string
		body           outcome
		verdict        harness.ReproductionVerdict
		failures       int
	}{
		{"expected reproduction matches", "rgw_case=REPRODUCED", observedSymptom, harness.Reproduced, 0},
		{"expected absence matches", "rgw_case=NOT_REPRODUCED", absentSymptom, harness.NotReproduced, 0},
		{"expected reproduction but absent", "rgw_case=REPRODUCED", absentSymptom, harness.NotReproduced, 1},
		{"expected absence but reproduced", "rgw_case=NOT_REPRODUCED", observedSymptom, harness.Reproduced, 1},
		{"expected reproduction but control failed", "rgw_case=REPRODUCED", failedControl, harness.Inconclusive, 1},
		{"expected absence but an observation is missing", "rgw_case=NOT_REPRODUCED", incompleteObservation, harness.Inconclusive, 1},
		{"expected reproduction but setup stopped", "rgw_case=REPRODUCED", stoppedEarly, harness.Inconclusive, 2},
		{"omitted expectation, reproduced fails as before", "", observedSymptom, harness.Reproduced, 1},
		{"omitted expectation, absent passes as before", "", absentSymptom, harness.NotReproduced, 0},
		{"omitted expectation, inconclusive fails as before", "", failedControl, harness.Inconclusive, 1},
		{"omitted expectation, setup failure stays the only failure", "", stoppedEarly, harness.Inconclusive, 1},
		{"another unit expected, a symptom here does not fail", "rgw_other=NOT_REPRODUCED", observedSymptom, harness.Reproduced, 0},
		{"another unit expected, inconclusive here still fails", "rgw_other=REPRODUCED", failedControl, harness.Inconclusive, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := runVerdict(c.expected, c.body)
			line := f.verdictLine(t)
			if !strings.Contains(line, " case=rgw_case verdict="+string(c.verdict)+" ") {
				t.Errorf("verdict line %q, want verdict %s", line, c.verdict)
			}
			if len(f.errors) != c.failures {
				t.Errorf("failures %q, want %d", f.errors, c.failures)
			}
		})
	}
}

// Identical evidence keeps its actual verdict; only the expected field and
// the comparison follow the expected value.
func TestReproductionExpectationChangesOnlyTheComparison(t *testing.T) {
	reproduced := runVerdict("rgw_case=REPRODUCED", observedSymptom)
	absent := runVerdict("rgw_case=NOT_REPRODUCED", observedSymptom)
	a, b := reproduced.verdictLine(t), absent.verdictLine(t)
	if strings.Replace(a, "expected=REPRODUCED", "expected=NOT_REPRODUCED", 1) != b {
		t.Errorf("lines differ beyond the expected field:\n%s\n%s", a, b)
	}
	if !strings.Contains(a, "verdict=REPRODUCED") || !strings.Contains(b, "verdict=REPRODUCED") {
		t.Errorf("actual verdict changed with the expectation:\n%s\n%s", a, b)
	}
	if len(reproduced.errors) != 0 || len(absent.errors) != 1 || !strings.Contains(absent.errors[0], "expected NOT_REPRODUCED of rgw_case, actual REPRODUCED") {
		t.Errorf("comparison: matched %q, mismatched %q", reproduced.errors, absent.errors)
	}
}

// A matching verdict does not hide a later cleanup failure.
func TestReproductionMatchKeepsACleanupFailure(t *testing.T) {
	f := &verdictT{}
	f.run(func() {
		r := harness.NewReproduction(f, verdictEnv("rgw_case=REPRODUCED"), "rgw_case", "control", "trigger")
		f.Cleanup(func() { f.Errorf("remove the buckets: 1 left") })
		observedSymptom(f, r)
	})
	if line := f.verdictLine(t); !strings.Contains(line, "verdict=REPRODUCED expected=REPRODUCED") {
		t.Errorf("verdict line %q", line)
	}
	if !slices.Equal(f.errors, []string{"remove the buckets: 1 left"}) {
		t.Errorf("failures %q, want only the cleanup failure", f.errors)
	}
}

// A skip cannot satisfy an expectation; without one it stays a skip.
func TestReproductionSkipIsNoVerdict(t *testing.T) {
	for expected, failures := range map[string]int{"rgw_case=NOT_REPRODUCED": 1, "": 0} {
		f := &verdictT{}
		f.run(func() {
			harness.NewReproduction(f, verdictEnv(expected), "rgw_case", "trigger")
			f.skipped = true
			f.FailNow()
		})
		if line := f.verdictLine(t); !strings.Contains(line, "verdict=INCONCLUSIVE") || !strings.Contains(line, "skipped") {
			t.Errorf("expected %q: verdict line %q", expected, line)
		}
		if len(f.errors) != failures {
			t.Errorf("expected %q: failures %q, want %d", expected, f.errors, failures)
		}
	}
}

func TestReproductionVerdictLineNamesTheRun(t *testing.T) {
	f := runVerdict("rgw_case=REPRODUCED", observedSymptom)
	line := f.verdictLine(t)
	for _, want := range []string{
		"REPRODUCTION_VERDICT case=rgw_case verdict=REPRODUCED expected=REPRODUCED attempts=1 platform=",
		" network=bridge images=essential=sha256:e,rgw=sha256:r ",
		" observation=trigger: refused with 400",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("verdict line %q lacks %q", line, want)
		}
	}
	omitted := runVerdict("", absentSymptom).verdictLine(t)
	if !strings.Contains(omitted, "expected=none") || !strings.Contains(omitted, "observation=control: accepted; trigger: accepted") {
		t.Errorf("verdict line %q", omitted)
	}
}

func TestParseReproductionExpectationRefusesInvalidRows(t *testing.T) {
	for _, value := range []string{
		"rgw_case=INCONCLUSIVE", "rgw_case=reproduced", "rgw_case", "=REPRODUCED",
		"Rgw_case=REPRODUCED", "rgw_case/a/b=REPRODUCED", "rgw_case=REPRODUCED,other=REPRODUCED",
	} {
		if _, err := harness.ParseReproductionExpectation(value); err == nil {
			t.Errorf("%q accepted", value)
		}
	}
	for value, want := range map[string]harness.ReproductionExpectation{
		"":                                      {},
		"rgw_case=REPRODUCED":                   {Unit: "rgw_case", Verdict: harness.Reproduced},
		"cephfs_case/same-named=NOT_REPRODUCED": {Unit: "cephfs_case/same-named", Verdict: harness.NotReproduced},
	} {
		if got, err := harness.ParseReproductionExpectation(value); err != nil || got != want {
			t.Errorf("%q: %+v, %v", value, got, err)
		}
	}
	f := &verdictT{}
	f.run(func() { harness.NewReproduction(f, verdictEnv("rgw_case=INCONCLUSIVE"), "rgw_case", "trigger") })
	if len(f.errors) != 1 || !strings.Contains(f.errors[0], "invalid expectation") {
		t.Errorf("invalid expectation at start: %q", f.errors)
	}
}
