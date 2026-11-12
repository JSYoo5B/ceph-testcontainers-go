//go:build all || integration

package harness

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// ReproductionVerdict is the actual or expected outcome of a known-defect
// experiment unit.
type ReproductionVerdict string

// The verdicts a reproduction reports. Only Reproduced and NotReproduced can
// be expected.
const (
	// Reproduced: the trigger and the controls held and the documented
	// symptom was observed.
	Reproduced ReproductionVerdict = "REPRODUCED"
	// NotReproduced: the trigger and the controls held, every planned
	// observation completed and the symptom was absent.
	NotReproduced ReproductionVerdict = "NOT_REPRODUCED"
	// Inconclusive: a missing prerequisite, a failed control, an
	// unestablished trigger or an incomplete observation leaves no verdict
	// on the symptom.
	Inconclusive ReproductionVerdict = "INCONCLUSIVE"
)

// ExpectedVerdictEnv names the variable that declares one expected verdict as
// <unit>=<REPRODUCED|NOT_REPRODUCED>, for example
// rgw_empty_sse_header=REPRODUCED.
const ExpectedVerdictEnv = "CEPH_TEST_EXPECTED_VERDICT"

// VerdictLinePrefix starts the one summary line each reproduction unit
// prints.
const VerdictLinePrefix = "REPRODUCTION_VERDICT"

var reproductionUnit = regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*(/[a-z0-9]+([_-][a-z0-9]+)*)?$`)

// ReproductionExpectation is a parsed ExpectedVerdictEnv value.
type ReproductionExpectation struct {
	Unit    string
	Verdict ReproductionVerdict
}

// ParseReproductionExpectation reads an ExpectedVerdictEnv value. Empty
// means no expectation. Anything else must name one unit and Reproduced or
// NotReproduced; Inconclusive can never satisfy a row, so it is refused.
func ParseReproductionExpectation(value string) (ReproductionExpectation, error) {
	if value == "" {
		return ReproductionExpectation{}, nil
	}
	unit, verdict, ok := strings.Cut(value, "=")
	if !ok || !reproductionUnit.MatchString(unit) {
		return ReproductionExpectation{}, fmt.Errorf("%s=%q: want <case>[/<subtest>]=<REPRODUCED|NOT_REPRODUCED>", ExpectedVerdictEnv, value)
	}
	switch v := ReproductionVerdict(verdict); v {
	case Reproduced, NotReproduced:
		return ReproductionExpectation{Unit: unit, Verdict: v}, nil
	default:
		return ReproductionExpectation{}, fmt.Errorf("%s=%q: expected verdict %q is not REPRODUCED or NOT_REPRODUCED", ExpectedVerdictEnv, value, verdict)
	}
}

// ReproductionT is the part of *testing.T a reproduction uses.
type ReproductionT interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	FailNow()
	Cleanup(func())
	Skipped() bool
}

// Reproduction collects the observations of one independently evaluated
// known-defect unit and reports its verdict once.
//
// Without an expectation a unit keeps the case's existing semantics: a
// symptom fails the test (Symptom reports it with Errorf on the test that
// observed it), an absent symptom passes, and an inconclusive run fails as
// before. With an expectation for this unit, Conclude compares the actual
// verdict with it and only a match passes. With an expectation for another
// unit, this unit prints its verdict and its symptom does not fail the run;
// an inconclusive result still does.
type Reproduction struct {
	t           ReproductionT
	unit        string
	expectation ReproductionExpectation
	images      string

	mu       sync.Mutex
	planned  []string
	records  map[string][]bool
	details  map[string]string
	reason   string
	reported bool
}

// StartReproduction starts the unit for a test, reading ExpectedVerdictEnv
// and the image variables from the environment. Observations names each
// planned observation; every one must be recorded for a verdict on the
// symptom.
func StartReproduction(t ReproductionT, unit string, observations ...string) *Reproduction {
	t.Helper()
	return NewReproduction(t, os.Getenv, unit, observations...)
}

// NewReproduction is StartReproduction with an explicit environment.
func NewReproduction(t ReproductionT, getenv func(string) string, unit string, observations ...string) *Reproduction {
	t.Helper()
	r := &Reproduction{t: t, unit: unit, planned: observations, records: map[string][]bool{}, details: map[string]string{}, images: verdictImages(getenv)}
	if !reproductionUnit.MatchString(unit) || len(observations) == 0 || hasDuplicate(observations) {
		t.Errorf("reproduction unit %q needs a case[/subtest] name and distinct planned observations, got %q", unit, observations)
		t.FailNow()
	}
	expectation, err := ParseReproductionExpectation(getenv(ExpectedVerdictEnv))
	if err != nil {
		t.Errorf("invalid expectation: %v", err)
		t.FailNow()
	}
	r.expectation = expectation
	t.Cleanup(r.finish)
	return r
}

func hasDuplicate(names []string) bool {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return len(slices.Compact(sorted)) != len(names)
}

// Expected reports whether the environment expects a verdict of this unit.
func (r *Reproduction) Expected() bool {
	return r.expectation.Unit == r.unit
}

// Symptom records that the planned observation saw the documented symptom.
// Without an expectation it fails t, the test that made the observation,
// with an "observed defect" message.
func (r *Reproduction) Symptom(t ReproductionT, observation, format string, args ...any) {
	t.Helper()
	detail := fmt.Sprintf(format, args...)
	r.record(observation, true, detail)
	if r.expectation.Unit == "" {
		t.Errorf("observed defect: %s: %s", observation, detail)
		return
	}
	t.Logf("observed defect: %s: %s", observation, detail)
}

// Absent records that the planned observation completed without the symptom.
func (r *Reproduction) Absent(t ReproductionT, observation, format string, args ...any) {
	t.Helper()
	detail := fmt.Sprintf(format, args...)
	r.record(observation, false, detail)
	t.Logf("symptom absent: %s: %s", observation, detail)
}

func (r *Reproduction) record(observation string, symptom bool, detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !slices.Contains(r.planned, observation) {
		r.t.Errorf("reproduction %s: observation %q is not planned (%q)", r.unit, observation, r.planned)
		return
	}
	r.records[observation] = append(r.records[observation], symptom)
	if symptom || r.details[observation] == "" {
		r.details[observation] = detail
	}
}

// Inconclusive reports the unit INCONCLUSIVE with the reason (it starts with
// the case's category, such as "control failed:"), and stops t. An
// inconclusive result never satisfies an expectation.
func (r *Reproduction) Inconclusive(t ReproductionT, format string, args ...any) {
	t.Helper()
	reason := fmt.Sprintf(format, args...)
	r.mu.Lock()
	first := !r.reported
	if first {
		r.reported = true
		r.reason = reason
	}
	r.mu.Unlock()
	if first {
		r.t.Logf("%s", r.line(Inconclusive, 0, reason))
	}
	t.Errorf("%s", reason)
	t.FailNow()
}

// Conclude derives the verdict from the recorded observations, prints the
// summary line and applies the expectation. Call it once every planned
// observation ran, on the test that started the unit.
func (r *Reproduction) Conclude() {
	r.t.Helper()
	r.mu.Lock()
	if r.reported {
		r.mu.Unlock()
		return
	}
	r.reported = true
	verdict, attempts, observation := DeriveReproduction(r.planned, r.records, r.details)
	r.mu.Unlock()
	r.t.Logf("%s", r.line(verdict, attempts, observation))
	if err := CompareReproduction(r.expectation, r.unit, verdict); err != nil {
		r.t.Errorf("%v", err)
	}
}

// finish reports a unit that ended before Conclude, for example after a
// setup failure or a skip, and refuses a missing verdict of an expected
// unit.
func (r *Reproduction) finish() {
	r.mu.Lock()
	reported := r.reported
	r.reported = true
	r.mu.Unlock()
	if reported {
		return
	}
	reason := "the test ended before the planned observations completed"
	if r.t.Skipped() {
		reason = "the test was skipped before the planned observations completed"
	}
	r.t.Logf("%s", r.line(Inconclusive, 0, reason))
	if r.Expected() {
		r.t.Errorf("expected %s of %s, but no verdict was reached: %s", r.expectation.Verdict, r.unit, reason)
	}
}

// DeriveReproduction derives a verdict from the recorded observations: an
// unrecorded planned observation is Inconclusive, any symptom is
// Reproduced, otherwise NotReproduced. Attempts is the most records of one
// observation. The observation text names the decisive observations.
func DeriveReproduction(planned []string, records map[string][]bool, details map[string]string) (ReproductionVerdict, int, string) {
	attempts := 0
	var missing, symptoms, absent []string
	for _, name := range planned {
		seen := records[name]
		attempts = max(attempts, len(seen))
		switch {
		case len(seen) == 0:
			missing = append(missing, name)
		case slices.Contains(seen, true):
			symptoms = append(symptoms, name+": "+details[name])
		default:
			absent = append(absent, name+": "+details[name])
		}
	}
	switch {
	case len(missing) > 0:
		return Inconclusive, attempts, "incomplete observation: " + strings.Join(missing, ", ") + " not recorded"
	case len(symptoms) > 0:
		return Reproduced, attempts, strings.Join(symptoms, "; ")
	default:
		return NotReproduced, attempts, strings.Join(absent, "; ")
	}
}

// CompareReproduction applies an expectation to the actual verdict of unit.
// No expectation, or one for another unit, keeps the existing semantics: only
// Inconclusive is an error, since a symptom has already failed the observing
// test or does not concern the expected unit. An expectation for unit is met
// only by the same verdict.
func CompareReproduction(expectation ReproductionExpectation, unit string, actual ReproductionVerdict) error {
	if expectation.Unit != unit {
		if actual == Inconclusive {
			return fmt.Errorf("%s of %s: no verdict on the symptom", Inconclusive, unit)
		}
		return nil
	}
	if actual != expectation.Verdict {
		return fmt.Errorf("expected %s of %s, actual %s", expectation.Verdict, unit, actual)
	}
	return nil
}

func (r *Reproduction) line(verdict ReproductionVerdict, attempts int, observation string) string {
	expected := "none"
	if r.Expected() {
		expected = string(r.expectation.Verdict)
	}
	return fmt.Sprintf("%s case=%s verdict=%s expected=%s attempts=%d platform=%s/%s %s observation=%s",
		VerdictLinePrefix, r.unit, verdict, expected, attempts, runtime.GOOS, runtime.GOARCH, r.images, oneLine(observation))
}

func oneLine(text string) string {
	return strings.Join(strings.Fields(text), " ")
}

// verdictImages names the images and the network mode the run resolved, in
// the order IntegrationImage resolves them.
func verdictImages(getenv func(string) string) string {
	network := getenv("CEPH_TEST_NETWORK")
	if network == "" {
		network = "bridge"
	}
	var images []string
	for _, role := range []string{"ESSENTIAL", "CLIENT", "CEPHFS", "RBD_AUX", "RGW"} {
		if image := getenv("CEPH_TEST_" + role + "_IMAGE"); image != "" {
			images = append(images, strings.ToLower(strings.ReplaceAll(role, "_", "-"))+"="+image)
		}
	}
	if len(images) == 0 {
		image := getenv("CEPH_TEST_IMAGE")
		if image == "" {
			image = ceph.DefaultImage
		}
		images = append(images, "all="+image)
	}
	return "network=" + network + " images=" + strings.Join(images, ",")
}
