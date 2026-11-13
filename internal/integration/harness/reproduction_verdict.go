//go:build all || integration

package harness

import (
	"bytes"
	// Registers sha256 for the digest check of immutableImage.
	_ "crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/distribution/reference"
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

// ReproductionsFile is the catalogue, in an experiment case's directory, of
// its reproduction units and of the image combinations whose verdicts a
// native run verified.
const ReproductionsFile = "reproductions.json"

// ReproductionsSchema is the schema of a ReproductionsFile.
const ReproductionsSchema = "ceph-reproductions/v1"

// VerdictLinePrefix starts the one summary line each reproduction unit
// prints.
const VerdictLinePrefix = "REPRODUCTION_VERDICT"

var (
	reproductionUnit        = regexp.MustCompile(`^[a-z0-9]+(_[a-z0-9]+)*(/[a-z0-9]+([_-][a-z0-9]+)*)?$`)
	reproductionCombination = regexp.MustCompile(`^[a-z0-9]+([.-][a-z0-9]+)*$`)
	reproductionPlatform    = regexp.MustCompile(`^[a-z0-9]+/[a-z0-9]+$`)
	reproductionImageID     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// reproductionRoles are the image roles a combination can name, in the
// order the verdict line prints them.
var reproductionRoles = []string{"essential", "client", "cephfs", "rbd-aux", "rgw"}

// ReproductionCatalog is a parsed ReproductionsFile.
type ReproductionCatalog struct {
	Schema string `json:"schema"`
	// Roles are the image roles the case consumes; a combination names an
	// image for each of them.
	Roles []string `json:"roles"`
	// Units are the reproduction units the case starts, with the network
	// mode each one runs in.
	Units map[string]ReproductionUnit `json:"units"`
	// Combinations are the verified image combinations.
	Combinations []ReproductionCombination `json:"combinations"`
}

// ReproductionUnit describes one unit of a catalogue.
type ReproductionUnit struct {
	// Network is "bridge" or "host": the network mode the unit's test runs
	// its clusters in.
	Network string `json:"network"`
}

// ReproductionCombination is one verified image combination: the image of
// each role on a platform, and the verdict a native run observed for each
// listed unit, with that run as evidence.
type ReproductionCombination struct {
	ID       string                         `json:"id"`
	Platform string                         `json:"platform"`
	Images   map[string]string              `json:"images"`
	Verdicts map[string]ReproductionVerdict `json:"verdicts"`
	Evidence string                         `json:"evidence"`
}

// ParseReproductionCatalog reads a ReproductionsFile and refuses anything a
// run could not use to select one expectation: an unknown field, role or
// verdict, an INCONCLUSIVE expectation, a combination that misses a role,
// names a mutable image (a tag without a digest) or an unknown unit, or two
// combinations of the same images.
func ParseReproductionCatalog(data []byte) (ReproductionCatalog, error) {
	var c ReproductionCatalog
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return c, fmt.Errorf("%s: %w", ReproductionsFile, err)
	}
	if c.Schema != ReproductionsSchema {
		return c, fmt.Errorf("%s: schema %q, want %q", ReproductionsFile, c.Schema, ReproductionsSchema)
	}
	if len(c.Roles) == 0 || hasDuplicate(c.Roles) {
		return c, fmt.Errorf("%s: roles %q must be distinct and not empty", ReproductionsFile, c.Roles)
	}
	for _, role := range c.Roles {
		if !slices.Contains(reproductionRoles, role) {
			return c, fmt.Errorf("%s: unknown role %q, want one of %q", ReproductionsFile, role, reproductionRoles)
		}
	}
	if len(c.Units) == 0 {
		return c, fmt.Errorf("%s: no units", ReproductionsFile)
	}
	for name, unit := range c.Units {
		if !reproductionUnit.MatchString(name) {
			return c, fmt.Errorf("%s: unit %q is not <case>[/<subtest>]", ReproductionsFile, name)
		}
		if unit.Network != "bridge" && unit.Network != "host" {
			return c, fmt.Errorf("%s: unit %s network %q is not bridge or host", ReproductionsFile, name, unit.Network)
		}
	}
	seen := map[string]string{}
	for _, combination := range c.Combinations {
		if !reproductionCombination.MatchString(combination.ID) || seen[combination.ID] != "" {
			return c, fmt.Errorf("%s: combination id %q must be unique and lowercase", ReproductionsFile, combination.ID)
		}
		seen[combination.ID] = combination.ID
		if !reproductionPlatform.MatchString(combination.Platform) {
			return c, fmt.Errorf("%s: combination %s platform %q is not <os>/<arch>", ReproductionsFile, combination.ID, combination.Platform)
		}
		if len(combination.Images) != len(c.Roles) {
			return c, fmt.Errorf("%s: combination %s must name an image for exactly the roles %q", ReproductionsFile, combination.ID, c.Roles)
		}
		for _, role := range c.Roles {
			if image := combination.Images[role]; !immutableImage(image) {
				return c, fmt.Errorf("%s: combination %s role %s image %q is not an image ID or a digest reference", ReproductionsFile, combination.ID, role, image)
			}
		}
		if len(combination.Verdicts) == 0 || strings.TrimSpace(combination.Evidence) == "" {
			return c, fmt.Errorf("%s: combination %s needs verdicts and the native evidence that observed them", ReproductionsFile, combination.ID)
		}
		for unit, verdict := range combination.Verdicts {
			if _, ok := c.Units[unit]; !ok {
				return c, fmt.Errorf("%s: combination %s names unknown unit %s", ReproductionsFile, combination.ID, unit)
			}
			if verdict != Reproduced && verdict != NotReproduced {
				return c, fmt.Errorf("%s: combination %s expects %q of %s; want REPRODUCED or NOT_REPRODUCED", ReproductionsFile, combination.ID, verdict, unit)
			}
		}
		key := combination.Platform + " " + c.identity(combination.Images)
		if other := seen[key]; other != "" {
			return c, fmt.Errorf("%s: combinations %s and %s name the same images", ReproductionsFile, other, combination.ID)
		}
		seen[key] = combination.ID
	}
	return c, nil
}

// immutableImage reports whether image names bytes that cannot change: a
// local image ID (sha256:<64 hex>), or a reference with a sha256 digest in
// the distribution reference grammar, with or without a tag. A combination
// stores an expectation only for such images, so it stays valid when the CI
// release pins move and never applies to whatever a tag points at later.
func immutableImage(image string) bool {
	if reproductionImageID.MatchString(image) {
		return true
	}
	parsed, err := reference.Parse(image)
	if err != nil {
		return false
	}
	canonical, ok := parsed.(reference.Canonical)
	return ok && canonical.Digest().Algorithm() == "sha256"
}

// identity is the role=image list of the catalogue's roles, with
// "unresolved" for a role without an image.
func (c ReproductionCatalog) identity(images map[string]string) string {
	parts := make([]string, 0, len(c.Roles))
	for _, role := range reproductionRoles {
		if slices.Contains(c.Roles, role) {
			image := images[role]
			if image == "" {
				image = "unresolved"
			}
			parts = append(parts, role+"="+image)
		}
	}
	return strings.Join(parts, ",")
}

// Match returns the combination whose platform and images equal the
// resolved ones, if any. An unresolved role never matches.
func (c ReproductionCatalog) Match(platform string, images map[string]string) (ReproductionCombination, bool) {
	for _, role := range c.Roles {
		if images[role] == "" {
			return ReproductionCombination{}, false
		}
	}
	for _, combination := range c.Combinations {
		if combination.Platform == platform && c.identity(combination.Images) == c.identity(images) {
			return combination, true
		}
	}
	return ReproductionCombination{}, false
}

// ResolvedRoleImages is the image each role resolves to for the fixtures,
// by ResolveImageSet. A role that the role layout leaves unset, or every
// role when the essential image is missing, is absent from the map.
func ResolvedRoleImages(getenv func(string) string) map[string]string {
	images := map[string]string{}
	set, err := ResolveImageSet(getenv)
	if err != nil {
		return images
	}
	if set.All != "" {
		set.Essential, set.Client, set.CephFS, set.RBDAux, set.RGW = set.All, set.All, set.All, set.All, set.All
	}
	for role, image := range map[string]string{"essential": set.Essential, "client": set.Client, "cephfs": set.CephFS, "rbd-aux": set.RBDAux, "rgw": set.RGW} {
		if image != "" {
			images[role] = image
		}
	}
	return images
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
// The expectation comes from the case's ReproductionsFile: the combination
// whose platform and images equal the ones the run resolved selects the
// expected verdict of the unit, and Conclude passes only that verdict. With
// no matching combination, or one that lists no verdict for the unit, the
// run is an investigation and keeps the case's existing semantics: a symptom
// fails the test that observed it (Symptom reports it with Errorf), an
// absent symptom passes, and an inconclusive run fails.
type Reproduction struct {
	t           ReproductionT
	unit        string
	network     string
	platform    string
	images      string
	combination string
	expected    ReproductionVerdict

	mu       sync.Mutex
	planned  []string
	records  map[string][]bool
	details  map[string]string
	reported bool
}

// StartReproduction starts the unit for a test: it reads ReproductionsFile
// from the test's package directory and resolves the images from the
// environment. Observations names each planned observation; every one must
// be recorded for NOT_REPRODUCED.
func StartReproduction(t ReproductionT, unit string, observations ...string) *Reproduction {
	t.Helper()
	data, err := os.ReadFile(ReproductionsFile)
	if err != nil {
		t.Errorf("reproduction %s: %v", unit, err)
		t.FailNow()
	}
	catalog, err := ParseReproductionCatalog(data)
	if err != nil {
		t.Errorf("reproduction %s: %v", unit, err)
		t.FailNow()
	}
	return NewReproduction(t, catalog, os.Getenv, runtime.GOOS+"/"+runtime.GOARCH, unit, observations...)
}

// NewReproduction is StartReproduction with an explicit catalogue,
// environment and platform.
func NewReproduction(t ReproductionT, catalog ReproductionCatalog, getenv func(string) string, platform, unit string, observations ...string) *Reproduction {
	t.Helper()
	declared, ok := catalog.Units[unit]
	if !reproductionUnit.MatchString(unit) || !ok || len(observations) == 0 || hasDuplicate(observations) {
		t.Errorf("reproduction unit %q needs an entry in %s and distinct planned observations, got %q", unit, ReproductionsFile, observations)
		t.FailNow()
	}
	resolved := ResolvedRoleImages(getenv)
	r := &Reproduction{t: t, unit: unit, network: declared.Network, platform: platform, images: catalog.identity(resolved),
		combination: "none", planned: observations, records: map[string][]bool{}, details: map[string]string{}}
	if combination, ok := catalog.Match(platform, resolved); ok {
		r.combination = combination.ID
		r.expected = combination.Verdicts[unit]
	}
	t.Cleanup(r.finish)
	return r
}

func hasDuplicate(names []string) bool {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return len(slices.Compact(sorted)) != len(names)
}

// Expected is the verdict the matching combination expects of the unit, or
// "" for an investigation.
func (r *Reproduction) Expected() ReproductionVerdict {
	return r.expected
}

// Symptom records that the planned observation saw the documented symptom.
// Without an expected verdict it fails t, the test that made the
// observation, with an "observed defect" message.
func (r *Reproduction) Symptom(t ReproductionT, observation, format string, args ...any) {
	t.Helper()
	detail := fmt.Sprintf(format, args...)
	r.record(observation, true, detail)
	if r.expected == "" {
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
	r.reported = true
	r.mu.Unlock()
	if first {
		r.t.Logf("%s", r.line(Inconclusive, 0, reason))
	}
	t.Errorf("%s", reason)
	t.FailNow()
}

// Conclude derives the verdict from the recorded observations, prints the
// summary line and compares it with the expected verdict. Call it once every
// planned observation ran, on the test that started the unit.
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
	if err := CompareReproduction(r.expected, r.unit, verdict); err != nil {
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
	if r.expected != "" {
		r.t.Errorf("expected %s of %s, but no verdict was reached: %s", r.expected, r.unit, reason)
	}
}

// DeriveReproduction derives a verdict from the recorded observations: any
// symptom is Reproduced, since the trigger and the controls held up to it,
// even when a later planned observation was not recorded; otherwise an
// unrecorded planned observation is Inconclusive, and NotReproduced needs
// every one. Attempts is the most records of one observation. The
// observation text names the decisive observations.
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
	case len(symptoms) > 0:
		if len(missing) > 0 {
			symptoms = append(symptoms, strings.Join(missing, ", ")+" not recorded")
		}
		return Reproduced, attempts, strings.Join(symptoms, "; ")
	case len(missing) > 0:
		return Inconclusive, attempts, "incomplete observation: " + strings.Join(missing, ", ") + " not recorded"
	default:
		return NotReproduced, attempts, strings.Join(absent, "; ")
	}
}

// CompareReproduction compares the actual verdict of unit with the expected
// one. Without an expectation only Inconclusive is an error, since a symptom
// has already failed the observing test; with one only the same verdict
// passes.
func CompareReproduction(expected ReproductionVerdict, unit string, actual ReproductionVerdict) error {
	switch {
	case expected == "" && actual == Inconclusive:
		return fmt.Errorf("%s of %s: no verdict on the symptom", Inconclusive, unit)
	case expected != "" && actual != expected:
		return fmt.Errorf("expected %s of %s, actual %s", expected, unit, actual)
	}
	return nil
}

func (r *Reproduction) line(verdict ReproductionVerdict, attempts int, observation string) string {
	expected := "none"
	if r.expected != "" {
		expected = string(r.expected)
	}
	return fmt.Sprintf("%s case=%s verdict=%s expected=%s combination=%s attempts=%d platform=%s network=%s images=%s observation=%s",
		VerdictLinePrefix, r.unit, verdict, expected, r.combination, attempts, r.platform, r.network, r.images, oneLine(observation))
}

func oneLine(text string) string {
	return strings.Join(strings.Fields(text), " ")
}
