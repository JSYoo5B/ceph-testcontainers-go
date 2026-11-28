package expect

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/expectfmt"

	"github.com/google/uuid"
)

// OSDs states the OSDMap membership a test expects of selected owned
// OSDs. Select names the OSDs by ID and, when UUID is set, by native UUID:
// states from an earlier ceph.Observer.OSDStates baseline pin the exact
// registrations, so an OSD that a replacement re-registered under the same ID
// does not match. Only ID and UUID of a Select entry are read. Up and In are
// the reported OSDMap flags every selected OSD must have; they are not proof
// of process liveness or client reachability. Exact also requires the
// observation to contain no owned OSD outside the selection.
//
// Unlike ceph.Observer.WaitForClean, which compares native OSD counts from the
// cluster status, an expectation checks individual identities, so it can tell
// which OSD is missing, replaced, down or out.
type OSDs struct {
	Select []ceph.OSDState
	Up, In bool
	Exact  bool
}

// OSDMismatchReason names why one OSD does not satisfy an OSDs expectation.
type OSDMismatchReason string

// The reasons an OSDEvaluation reports.
const (
	// OSDMismatchAbsent: a selected ID is not in the observation.
	OSDMismatchAbsent OSDMismatchReason = "absent"
	// OSDMismatchReplaced: the ID now carries a different native UUID than the
	// selection pinned. Up and In of the replacement are not compared.
	OSDMismatchReplaced OSDMismatchReason = "replaced"
	// OSDMismatchUp: the reported up flag differs from the expectation.
	OSDMismatchUp OSDMismatchReason = "up"
	// OSDMismatchIn: the reported in flag differs from the expectation.
	OSDMismatchIn OSDMismatchReason = "in"
	// OSDMismatchUnselected: an Exact expectation found an owned OSD that the
	// selection does not name.
	OSDMismatchUnselected OSDMismatchReason = "unselected"
)

// OSDMismatch is one reason one OSD does not satisfy an expectation. UUID is
// the selected UUID, empty for a selection by ID alone and for
// OSDMismatchUnselected. Observed is the OSD's state in the evaluated
// observation and the zero value for OSDMismatchAbsent. For OSDMismatchUp and
// OSDMismatchIn the expected flag is the negation of the observed one.
type OSDMismatch struct {
	Reason   OSDMismatchReason
	ID       int
	UUID     string
	Observed ceph.OSDState
}

// String names the OSD, the reason and the evaluated values.
func (m OSDMismatch) String() string {
	switch m.Reason {
	case OSDMismatchAbsent:
		return fmt.Sprintf("osd.%d absent", m.ID)
	case OSDMismatchReplaced:
		return fmt.Sprintf("osd.%d replaced: want uuid %s, observed %s", m.ID, m.UUID, m.Observed.UUID)
	case OSDMismatchUp:
		return fmt.Sprintf("osd.%d up: want %t, observed %t", m.ID, !m.Observed.Up, m.Observed.Up)
	case OSDMismatchIn:
		return fmt.Sprintf("osd.%d in: want %t, observed %t", m.ID, !m.Observed.In, m.Observed.In)
	case OSDMismatchUnselected:
		return fmt.Sprintf("osd.%d unselected: up=%t in=%t", m.ID, m.Observed.Up, m.Observed.In)
	}
	return fmt.Sprintf("osd.%d %s", m.ID, m.Reason)
}

// OSDEvaluation is the result of evaluating one observation. Expectation and
// Observed are copies of what was evaluated, so the diagnostics describe that
// observation even when the caller later reuses its slices. Mismatches are
// ordered by OSD ID, then in the order of the reason constants; Matched means
// there are none.
type OSDEvaluation struct {
	Expectation OSDs
	Observed    []ceph.OSDState
	Mismatches  []OSDMismatch
	Matched     bool
}

// String summarizes the expectation and at most eight mismatches.
func (e OSDEvaluation) String() string {
	ids := make([]string, len(e.Expectation.Select))
	for i, s := range e.Expectation.Select {
		ids[i] = strconv.Itoa(s.ID)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "osds [%s] want up=%t in=%t", strings.Join(ids, " "), e.Expectation.Up, e.Expectation.In)
	if e.Expectation.Exact {
		b.WriteString(" exact")
	}
	fmt.Fprintf(&b, " in %d observed: ", len(e.Observed))
	expectfmt.WriteMismatches(&b, e.Matched, e.Mismatches)
	return b.String()
}

// Evaluate checks one successful ceph.Observer.OSDStates observation against the
// expectation. It reads only its arguments: no query, wait or retry, so a
// test composes it with its own polling or assertion helpers. A mismatch is
// a result; an error means no verdict. An empty selection, a negative or
// repeated selected ID and an invalid selected UUID are ceph.ErrInvalidRequest,
// and an observation with a negative or repeated ID or an invalid UUID is a
// malformed observation, never a mismatch. The nil UUID is invalid in both.
// The result shares no slice with its arguments.
//
//purpose:observation
func (e OSDs) Evaluate(observed []ceph.OSDState) (OSDEvaluation, error) {
	selected, err := e.validate()
	if err != nil {
		return OSDEvaluation{}, err
	}
	byID, err := osdObservation(observed)
	if err != nil {
		return OSDEvaluation{}, err
	}
	result := OSDEvaluation{
		Expectation: OSDs{Select: slices.Clone(e.Select), Up: e.Up, In: e.In, Exact: e.Exact},
		Observed:    slices.Clone(observed),
	}
	for _, s := range e.Select {
		observedOSD, present := byID[s.ID]
		state := observedOSD.state
		switch {
		case !present:
			result.Mismatches = append(result.Mismatches, OSDMismatch{Reason: OSDMismatchAbsent, ID: s.ID, UUID: s.UUID})
			continue
		case selected[s.ID] != uuid.Nil && observedOSD.uuid != selected[s.ID]:
			result.Mismatches = append(result.Mismatches, OSDMismatch{Reason: OSDMismatchReplaced, ID: s.ID, UUID: s.UUID, Observed: state})
			continue
		}
		if state.Up != e.Up {
			result.Mismatches = append(result.Mismatches, OSDMismatch{Reason: OSDMismatchUp, ID: s.ID, UUID: s.UUID, Observed: state})
		}
		if state.In != e.In {
			result.Mismatches = append(result.Mismatches, OSDMismatch{Reason: OSDMismatchIn, ID: s.ID, UUID: s.UUID, Observed: state})
		}
	}
	if e.Exact {
		for _, s := range observed {
			if _, ok := selected[s.ID]; !ok {
				result.Mismatches = append(result.Mismatches, OSDMismatch{Reason: OSDMismatchUnselected, ID: s.ID, Observed: s})
			}
		}
	}
	slices.SortStableFunc(result.Mismatches, func(a, b OSDMismatch) int { return a.ID - b.ID })
	result.Matched = len(result.Mismatches) == 0
	return result, nil
}

// validate refuses a selection that names no OSD, a negative or repeated ID or
// an invalid UUID, and returns the pinned UUID of each selected ID (uuid.Nil
// when the entry selects by ID alone).
func (e OSDs) validate() (map[int]uuid.UUID, error) {
	if len(e.Select) == 0 {
		return nil, fmt.Errorf("%w: OSD expectation selects no OSD", ceph.ErrInvalidRequest)
	}
	selected := make(map[int]uuid.UUID, len(e.Select))
	for _, s := range e.Select {
		if s.ID < 0 {
			return nil, fmt.Errorf("%w: OSD expectation selects negative osd.%d", ceph.ErrInvalidRequest, s.ID)
		}
		if _, dup := selected[s.ID]; dup {
			return nil, fmt.Errorf("%w: OSD expectation selects osd.%d twice", ceph.ErrInvalidRequest, s.ID)
		}
		var pinned uuid.UUID
		if s.UUID != "" {
			parsed, ok := nativeUUID(s.UUID)
			if !ok {
				return nil, fmt.Errorf("%w: OSD expectation pins an invalid UUID for osd.%d", ceph.ErrInvalidRequest, s.ID)
			}
			pinned = parsed
		}
		selected[s.ID] = pinned
	}
	return selected, nil
}

// observedOSD is one observed OSD with its parsed native UUID.
type observedOSD struct {
	state ceph.OSDState
	uuid  uuid.UUID
}

// osdObservation checks the observation and returns its OSDs by ID.
func osdObservation(observed []ceph.OSDState) (map[int]observedOSD, error) {
	byID := make(map[int]observedOSD, len(observed))
	for _, s := range observed {
		if s.ID < 0 {
			return nil, fmt.Errorf("malformed OSD observation: negative osd.%d", s.ID)
		}
		if _, dup := byID[s.ID]; dup {
			return nil, fmt.Errorf("malformed OSD observation: osd.%d reported twice", s.ID)
		}
		parsed, ok := nativeUUID(s.UUID)
		if !ok {
			return nil, fmt.Errorf("malformed OSD observation: osd.%d has no valid UUID", s.ID)
		}
		byID[s.ID] = observedOSD{state: s, uuid: parsed}
	}
	return byID, nil
}
