package expect

import (
	"errors"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// TestNativeUUIDVerdictsOfEveryEntryPoint pins how each expectation judges a
// spelling of the native identity it pins (expectation) or reads
// (observation): every spelling of the same UUID matches, and the nil UUID
// and a non-UUID are refused.
func TestNativeUUIDVerdictsOfEveryEntryPoint(t *testing.T) {
	const id = healthFSID
	spellings := []struct {
		name  string
		value string
		valid bool
	}{
		{"canonical", id, true},
		{"upper case", strings.ToUpper(id), true},
		{"braces", "{" + id + "}", true},
		{"urn", "urn:uuid:" + id, true},
		{"no hyphens", strings.ReplaceAll(id, "-", ""), true},
		{"nil", "00000000-0000-0000-0000-000000000000", false},
		{"not a uuid", "cluster", false},
	}
	members := []string{"a", "b", "c"}
	osd := func(u string) []ceph.OSDState { return []ceph.OSDState{{ID: 0, UUID: u, Up: true, In: true}} }
	entryPoints := []struct {
		name        string
		expectation func(string) (bool, error)
		observation func(string) (bool, error)
	}{
		{"Health",
			func(u string) (bool, error) {
				got, err := Health{FSID: u}.Evaluate(healthDown())
				return got.Matched, err
			},
			func(u string) (bool, error) {
				observed := healthDown()
				observed.FSID = u
				got, err := Health{FSID: id}.Evaluate(observed)
				return got.Matched, err
			}},
		{"Monitors",
			func(u string) (bool, error) {
				got, err := Monitors{FSID: u}.Evaluate(quorumStatus(t, id, members, members...))
				return got.Matched, err
			},
			func(u string) (bool, error) {
				got, err := Monitors{FSID: id}.Evaluate(quorumStatus(t, u, members, members...))
				return got.Matched, err
			}},
		{"OSDs",
			func(u string) (bool, error) {
				got, err := OSDs{Select: osd(u), Up: true, In: true}.Evaluate(osd(id))
				return got.Matched, err
			},
			func(u string) (bool, error) {
				got, err := OSDs{Select: osd(id), Up: true, In: true}.Evaluate(osd(u))
				return got.Matched, err
			}},
		{"PGs",
			func(u string) (bool, error) {
				got, err := PGs{FSID: u, Count: 2}.Evaluate(pgClean())
				return got.Matched, err
			},
			func(u string) (bool, error) {
				observed := pgClean()
				observed.FSID = u
				got, err := PGs{FSID: id, Count: 2}.Evaluate(observed)
				return got.Matched, err
			}},
	}
	for _, ep := range entryPoints {
		for _, s := range spellings {
			t.Run(ep.name+"/"+s.name, func(t *testing.T) {
				matched, err := ep.expectation(s.value)
				switch {
				case s.valid && (err != nil || !matched):
					t.Fatalf("expectation pinning %q: matched=%t err=%v, want a match", s.value, matched, err)
				case !s.valid && !errors.Is(err, ceph.ErrInvalidRequest):
					t.Fatalf("expectation pinning %q: err=%v, want ceph.ErrInvalidRequest", s.value, err)
				}
				matched, err = ep.observation(s.value)
				switch {
				case s.valid && (err != nil || !matched):
					t.Fatalf("observation of %q: matched=%t err=%v, want a match", s.value, matched, err)
				case !s.valid && (err == nil || errors.Is(err, ceph.ErrInvalidRequest)):
					t.Fatalf("observation of %q: err=%v, want a malformed observation", s.value, err)
				}
			})
		}
	}
}
