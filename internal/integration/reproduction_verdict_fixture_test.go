//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// verdictT records what a reproduction does to its test. FailNow stops the
// running function the way testing.T does, and run then runs the cleanups.
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

// verdictCatalog is a case catalogue with an affected and a fixed role
// combination, an aggregate combination and a host-network unit. The
// expected verdict of the affected combination is a parameter, so a test
// can change only that value.
func verdictCatalog(t *testing.T, affected string) harness.ReproductionCatalog {
	t.Helper()
	catalog, err := harness.ParseReproductionCatalog([]byte(`{
  "schema": "ceph-reproductions/v1",
  "roles": ["essential", "client", "rgw"],
  "units": {"rgw_case": {"network": "bridge"}, "rgw_case/host": {"network": "host"}},
  "combinations": [
    {"id": "affected", "platform": "linux/amd64", "images": {"essential": "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1", "client": "sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1", "rgw": "sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1"},
     "verdicts": {"rgw_case": "` + affected + `", "rgw_case/host": "REPRODUCED"}, "evidence": "native run"},
    {"id": "fixed", "platform": "linux/amd64", "images": {"essential": "sha256:e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2", "client": "sha256:c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2", "rgw": "sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"},
     "verdicts": {"rgw_case": "NOT_REPRODUCED"}, "evidence": "native run"},
    {"id": "aggregate", "platform": "linux/amd64", "images": {"essential": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111", "client": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111", "rgw": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111"},
     "verdicts": {"rgw_case": "NOT_REPRODUCED"}, "evidence": "native run"}
  ]
}`))
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func roleEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

var (
	affectedRoles = roleEnv(map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1", "CEPH_TEST_CLIENT_IMAGE": "sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1", "CEPH_TEST_RGW_IMAGE": "sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1"})
	fixedRoles    = roleEnv(map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2e2", "CEPH_TEST_CLIENT_IMAGE": "sha256:c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2", "CEPH_TEST_RGW_IMAGE": "sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"})
	unknownRoles  = roleEnv(map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3e3", "CEPH_TEST_CLIENT_IMAGE": "sha256:c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3", "CEPH_TEST_RGW_IMAGE": "sha256:b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3b3"})
)

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
	symptomBeforeAMissingObservation outcome = func(f *verdictT, r *harness.Reproduction) {
		r.Symptom(f, "control", "refused with 400")
		r.Conclude()
	}
)

func runVerdict(t *testing.T, catalog harness.ReproductionCatalog, env func(string) string, body outcome) *verdictT {
	t.Helper()
	f := &verdictT{}
	f.run(func() {
		r := harness.NewReproduction(f, catalog, env, "linux/amd64", "rgw_case", "control", "trigger")
		body(f, r)
	})
	return f
}

func TestReproductionVerdictComparison(t *testing.T) {
	catalog := verdictCatalog(t, "REPRODUCED")
	for _, c := range []struct {
		name     string
		env      func(string) string
		body     outcome
		verdict  harness.ReproductionVerdict
		expected string
		failures int
	}{
		{"affected images expect a reproduction, observed", affectedRoles, observedSymptom, harness.Reproduced, "REPRODUCED", 0},
		{"fixed images expect absence, observed", fixedRoles, absentSymptom, harness.NotReproduced, "NOT_REPRODUCED", 0},
		{"affected images, but absent", affectedRoles, absentSymptom, harness.NotReproduced, "REPRODUCED", 1},
		{"fixed images, but reproduced", fixedRoles, observedSymptom, harness.Reproduced, "NOT_REPRODUCED", 1},
		{"affected images, but a control failed", affectedRoles, failedControl, harness.Inconclusive, "REPRODUCED", 1},
		{"fixed images, but an observation is missing", fixedRoles, incompleteObservation, harness.Inconclusive, "NOT_REPRODUCED", 1},
		{"affected images, a symptom before a missing observation", affectedRoles, symptomBeforeAMissingObservation, harness.Reproduced, "REPRODUCED", 0},
		{"affected images, but setup stopped", affectedRoles, stoppedEarly, harness.Inconclusive, "REPRODUCED", 2},
		{"unknown images, reproduced fails as before", unknownRoles, observedSymptom, harness.Reproduced, "none", 1},
		{"unknown images, absent passes as before", unknownRoles, absentSymptom, harness.NotReproduced, "none", 0},
		{"unknown images, inconclusive fails as before", unknownRoles, failedControl, harness.Inconclusive, "none", 1},
		{"unknown images, a setup failure stays the only failure", unknownRoles, stoppedEarly, harness.Inconclusive, "none", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := runVerdict(t, catalog, c.env, c.body)
			line := f.verdictLine(t)
			if !strings.Contains(line, " case=rgw_case verdict="+string(c.verdict)+" expected="+c.expected+" ") {
				t.Errorf("verdict line %q, want verdict %s expected %s", line, c.verdict, c.expected)
			}
			if len(f.errors) != c.failures {
				t.Errorf("failures %q, want %d", f.errors, c.failures)
			}
		})
	}
}

// Identical evidence keeps its actual verdict; only the expected field and
// the comparison follow the catalogue's expected value.
func TestReproductionExpectationChangesOnlyTheComparison(t *testing.T) {
	reproduced := runVerdict(t, verdictCatalog(t, "REPRODUCED"), affectedRoles, observedSymptom)
	absent := runVerdict(t, verdictCatalog(t, "NOT_REPRODUCED"), affectedRoles, observedSymptom)
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

// Changing only the image strings selects the other expectation.
func TestReproductionImagesSelectTheExpectation(t *testing.T) {
	catalog := verdictCatalog(t, "REPRODUCED")
	for _, c := range []struct {
		name, want string
		env        func(string) string
	}{
		{"affected roles", "expected=REPRODUCED combination=affected", affectedRoles},
		{"fixed roles", "expected=NOT_REPRODUCED combination=fixed", fixedRoles},
		{"aggregate", "expected=NOT_REPRODUCED combination=aggregate", roleEnv(map[string]string{"CEPH_TEST_IMAGE": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111"})},
		{"another aggregate", "expected=none combination=none", roleEnv(map[string]string{"CEPH_TEST_IMAGE": "quay.io/b@sha256:2222222222222222222222222222222222222222222222222222222222222222"})},
		{"one role differs", "expected=none combination=none", roleEnv(map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1", "CEPH_TEST_CLIENT_IMAGE": "sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1", "CEPH_TEST_RGW_IMAGE": "sha256:b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"})},
		{"a role unresolved", "expected=none combination=none", roleEnv(map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1", "CEPH_TEST_CLIENT_IMAGE": "sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1"})},
		// The aggregate with a client override runs the client on another
		// image, so it is not the aggregate combination.
		{"aggregate with a client override", "expected=none combination=none", roleEnv(map[string]string{"CEPH_TEST_IMAGE": "quay.io/a@sha256:1111111111111111111111111111111111111111111111111111111111111111", "CEPH_TEST_CLIENT_IMAGE": "quay.io/b@sha256:2222222222222222222222222222222222222222222222222222222222222222"})},
	} {
		line := runVerdict(t, catalog, c.env, absentSymptom).verdictLine(t)
		if !strings.Contains(line, " "+c.want+" ") {
			t.Errorf("%s: verdict line %q lacks %q", c.name, line, c.want)
		}
	}
	other := &verdictT{}
	other.run(func() {
		r := harness.NewReproduction(other, catalog, affectedRoles, "linux/arm64", "rgw_case", "trigger")
		r.Absent(other, "trigger", "accepted")
		r.Conclude()
	})
	if line := other.verdictLine(t); !strings.Contains(line, " expected=none combination=none ") {
		t.Errorf("another platform matched: %q", line)
	}
}

// The identity a reproduction records is the fixture's own resolution,
// also when only some roles override the aggregate.
func TestResolveImageSetIsTheRecordedIdentity(t *testing.T) {
	for _, c := range []struct {
		name string
		env  map[string]string
		set  ceph.ImageSet
		line string
	}{
		{"aggregate", map[string]string{"CEPH_TEST_IMAGE": "A"}, ceph.ImageSet{All: "A"}, "images=essential=A,client=A,rgw=A "},
		{"default", map[string]string{}, ceph.ImageSet{All: ceph.DefaultImage}, "images=essential=" + ceph.DefaultImage + ",client=" + ceph.DefaultImage + ",rgw=" + ceph.DefaultImage + " "},
		{"aggregate with a client override", map[string]string{"CEPH_TEST_IMAGE": "A", "CEPH_TEST_CLIENT_IMAGE": "B"},
			ceph.ImageSet{Essential: "A", Client: "B", CephFS: "A", RBDAux: "A", RGW: "A"}, "images=essential=A,client=B,rgw=A "},
		{"roles", map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "E", "CEPH_TEST_CLIENT_IMAGE": "C", "CEPH_TEST_RGW_IMAGE": "R", "CEPH_TEST_IMAGE": "A"},
			ceph.ImageSet{Essential: "E", Client: "C", RGW: "R"}, "images=essential=E,client=C,rgw=R "},
		{"roles without an rgw image", map[string]string{"CEPH_TEST_ESSENTIAL_IMAGE": "E", "CEPH_TEST_CLIENT_IMAGE": "C"},
			ceph.ImageSet{Essential: "E", Client: "C"}, "images=essential=E,client=C,rgw=unresolved "},
	} {
		set, err := harness.ResolveImageSet(roleEnv(c.env))
		if err != nil || set != c.set {
			t.Errorf("%s: ResolveImageSet %+v, %v; want %+v", c.name, set, err, c.set)
		}
		line := runVerdict(t, verdictCatalog(t, "REPRODUCED"), roleEnv(c.env), absentSymptom).verdictLine(t)
		if !strings.Contains(line, " "+c.line) {
			t.Errorf("%s: verdict line %q lacks %q", c.name, line, c.line)
		}
	}
	if _, err := harness.ResolveImageSet(roleEnv(map[string]string{"SCENARIO_IMAGE_LAYOUT": "roles", "CEPH_TEST_CLIENT_IMAGE": "C"})); err == nil {
		t.Error("the role layout without an essential image resolved")
	}
}

// A matching verdict does not hide a later cleanup failure.
func TestReproductionMatchKeepsACleanupFailure(t *testing.T) {
	f := &verdictT{}
	f.run(func() {
		r := harness.NewReproduction(f, verdictCatalog(t, "REPRODUCED"), affectedRoles, "linux/amd64", "rgw_case", "control", "trigger")
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
	for _, c := range []struct {
		env      func(string) string
		failures int
	}{{fixedRoles, 1}, {unknownRoles, 0}} {
		f := &verdictT{}
		f.run(func() {
			harness.NewReproduction(f, verdictCatalog(t, "REPRODUCED"), c.env, "linux/amd64", "rgw_case", "trigger")
			f.skipped = true
			f.FailNow()
		})
		if line := f.verdictLine(t); !strings.Contains(line, "verdict=INCONCLUSIVE") || !strings.Contains(line, "skipped") {
			t.Errorf("verdict line %q", line)
		}
		if len(f.errors) != c.failures {
			t.Errorf("failures %q, want %d", f.errors, c.failures)
		}
	}
}

func TestReproductionVerdictLineNamesTheRun(t *testing.T) {
	line := runVerdict(t, verdictCatalog(t, "REPRODUCED"), affectedRoles, observedSymptom).verdictLine(t)
	for _, want := range []string{
		"REPRODUCTION_VERDICT case=rgw_case verdict=REPRODUCED expected=REPRODUCED combination=affected attempts=1 platform=linux/amd64 network=bridge ",
		" images=essential=sha256:e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1,client=sha256:c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1,rgw=sha256:b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1 ",
		" observation=trigger: refused with 400",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("verdict line %q lacks %q", line, want)
		}
	}
	host := &verdictT{}
	host.run(func() {
		r := harness.NewReproduction(host, verdictCatalog(t, "REPRODUCED"), affectedRoles, "linux/amd64", "rgw_case/host", "trigger")
		r.Symptom(host, "trigger", "refused")
		r.Conclude()
	})
	if line := host.verdictLine(t); !strings.Contains(line, " network=host ") || !strings.Contains(line, " expected=REPRODUCED combination=affected ") {
		t.Errorf("host unit line %q", line)
	}
	omitted := runVerdict(t, verdictCatalog(t, "REPRODUCED"), unknownRoles, absentSymptom).verdictLine(t)
	if !strings.Contains(omitted, "expected=none combination=none") || !strings.Contains(omitted, "observation=control: accepted; trigger: accepted") {
		t.Errorf("verdict line %q", omitted)
	}
}

// TestReproductionCatalogImagesAreImmutable checks the images of
// testdata/reproduction_images.json, which test_reproductions.py checks
// against the runner's loader too: an image ID or a sha256 digest reference
// in the distribution reference grammar is a stored identity whether or not
// a CI release pin still names it, and a tag or anything else is refused.
func TestReproductionCatalogImagesAreImmutable(t *testing.T) {
	data, err := os.ReadFile("testdata/reproduction_images.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct{ Immutable, Mutable []string }
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Immutable) == 0 || len(fixture.Mutable) == 0 {
		t.Fatalf("fixture %+v", fixture)
	}
	catalog := func(image string) []byte {
		encoded, _ := json.Marshal(image)
		return []byte(`{"schema": "ceph-reproductions/v1", "roles": ["essential", "rgw"], "units": {"rgw_case": {"network": "bridge"}},
  "combinations": [{"id": "a", "platform": "linux/amd64", "images": {"essential": "sha256:` + strings.Repeat("e1", 32) + `", "rgw": ` +
			string(encoded) + `}, "verdicts": {"rgw_case": "REPRODUCED"}, "evidence": "run"}]}`)
	}
	for _, image := range fixture.Immutable {
		if _, err := harness.ParseReproductionCatalog(catalog(image)); err != nil {
			t.Errorf("immutable %q refused: %v", image, err)
		}
	}
	for _, image := range fixture.Mutable {
		if _, err := harness.ParseReproductionCatalog(catalog(image)); err == nil {
			t.Errorf("mutable %q accepted", image)
		}
	}
}

func TestParseReproductionCatalogRefusesUnusableData(t *testing.T) {
	combination := func(verdicts, images string) string {
		return `{"schema": "ceph-reproductions/v1", "roles": ["essential", "rgw"], "units": {"rgw_case": {"network": "bridge"}},
  "combinations": [{"id": "a", "platform": "linux/amd64", "images": ` + images + `, "verdicts": ` + verdicts + `, "evidence": "run"}]}`
	}
	id, digest := "sha256:"+strings.Repeat("e1", 32), "@sha256:"+strings.Repeat("4", 64)
	images := `{"essential": "` + id + `", "rgw": "quay.io/ceph/ceph` + digest + `"}`
	if _, err := harness.ParseReproductionCatalog([]byte(combination(`{"rgw_case": "REPRODUCED"}`, images))); err != nil {
		t.Fatalf("valid catalogue: %v", err)
	}
	for name, data := range map[string]string{
		"inconclusive expectation": combination(`{"rgw_case": "INCONCLUSIVE"}`, images),
		"unknown verdict":          combination(`{"rgw_case": "FIXED"}`, images),
		"unknown unit":             combination(`{"rgw_other": "REPRODUCED"}`, images),
		"no verdicts":              combination(`{}`, images),
		"missing role image":       combination(`{"rgw_case": "REPRODUCED"}`, `{"essential": "`+id+`"}`),
		"extra role image":         combination(`{"rgw_case": "REPRODUCED"}`, `{"essential": "`+id+`", "rgw": "`+id+`", "client": "`+id+`"}`),
		"unknown field":            strings.Replace(combination(`{"rgw_case": "REPRODUCED"}`, images), `"evidence"`, `"release": "20.2.4", "evidence"`, 1),
		"unknown role":             strings.Replace(combination(`{"rgw_case": "REPRODUCED"}`, images), `["essential", "rgw"]`, `["essential", "gateway"]`, 1),
		"bad network":              strings.Replace(combination(`{"rgw_case": "REPRODUCED"}`, images), `"bridge"`, `"none"`, 1),
		"same images twice": strings.Replace(combination(`{"rgw_case": "REPRODUCED"}`, images), `"evidence": "run"}]`,
			`"evidence": "run"}, {"id": "b", "platform": "linux/amd64", "images": `+images+`, "verdicts": {"rgw_case": "NOT_REPRODUCED"}, "evidence": "run"}]`, 1),
	} {
		if _, err := harness.ParseReproductionCatalog([]byte(data)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	f := &verdictT{}
	f.run(func() {
		harness.NewReproduction(f, verdictCatalog(t, "REPRODUCED"), affectedRoles, "linux/amd64", "rgw_unlisted", "trigger")
	})
	if len(f.errors) != 1 || !strings.Contains(f.errors[0], "needs an entry in reproductions.json") {
		t.Errorf("unlisted unit: %q", f.errors)
	}
}
