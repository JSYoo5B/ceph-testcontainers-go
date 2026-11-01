package cephfs

import (
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/mdsmap"
)

var (
	// acceptedMDSRowStates are the twelve names the strict decoders accept in a
	// daemon row, written out here so that the shared list cannot grow unseen.
	acceptedMDSRowStates = []string{"up:boot", "up:standby", "up:standby-replay", "up:creating", "up:starting", "up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active", "up:stopping"}
	// refusedMDSRowStates are the other names of ceph_mds_state_name in the
	// pinned Ceph source, plus the empty string and a near miss. up:oneshot-replay
	// is refused today; whether that is intended is an open owner question.
	refusedMDSRowStates = []string{"up:oneshot-replay", "down:dne", "down:stopped", "down:damaged", "null", "???", "", "up:", "up:Active", "up:active "}
	// assignedMDSRowStates are the accepted names that a foreign filesystem's
	// assigned rank row can carry in every decoder (boot, standby and
	// standby-replay are refused for an assigned row by their own rules).
	assignedMDSRowStates = slices.DeleteFunc(slices.Clone(acceptedMDSRowStates), func(s string) bool {
		return s == "up:boot" || s == "up:standby" || s == "up:standby-replay"
	})
)

func TestMDSRowStateAllowlistIsExactlyTheTwelveNames(t *testing.T) {
	for _, state := range acceptedMDSRowStates {
		if !mdsmap.StateAccepted(state) {
			t.Fatalf("accepted state %q was dropped", state)
		}
	}
	for _, state := range refusedMDSRowStates {
		if mdsmap.StateAccepted(state) {
			t.Fatalf("state %q is accepted; supporting it needs evidence and a separate decision", state)
		}
	}
}

// Each strict decoder must apply the shared list: an assigned row of a foreign
// filesystem passes for every accepted state that rank rows may carry and is
// refused, with the decoder's own classification, for every other name.
func TestMDSRowStateAllowlistReachesEveryStrictDecoder(t *testing.T) {
	foreign := func(state string) map[string]any {
		fsys := coldMDSTestMap("sibling", 0, 1)
		fsys["info"] = map[string]any{"gid_44": coldMDSTestRow("foreign-0", state, 44, 0, 8)}
		fsys["up"] = map[string]any{"mds_0": uint64(44)}
		return map[string]any{"id": 8, "mdsmap": fsys}
	}
	t.Run("stopped", func(t *testing.T) {
		const want = "stopped MDS global daemon identity is present or malformed"
		for _, state := range append(slices.Clone(assignedMDSRowStates), refusedMDSRowStates...) {
			refused := !mdsmap.StateAccepted(state)
			_, err := stoppedMDSDecode(t, func(n *stoppedMDSControl, _ map[string]any, _ *stoppedMDSDaemon) {
				n.native["filesystems"] = append(n.native["filesystems"].([]any), foreign(state))
			})
			if refused && (err == nil || err.Error() != want) || !refused && err != nil {
				t.Fatalf("stopped decoder, state %q: %v", state, err)
			}
		}
	})
	t.Run("cold", func(t *testing.T) {
		const want = "foreign MDS map is incomplete or conflicts with initial identity"
		fs, n := coldMDSTestConstruct(t, false)
		for _, state := range append(slices.Clone(assignedMDSRowStates), refusedMDSRowStates...) {
			refused := !mdsmap.StateAccepted(state)
			native := map[string]any{"standbys": []any{}, "filesystems": append(slices.Clone(n.native["filesystems"].([]any)), foreign(state))}
			err := decodeColdMDSMap([]byte(coldMDSTestJSON(native)), coldMDSMapExpectation{filesystem: fs.config.Name, reservedName: "cold-target-0", native: fs.mds.coldMDS.identity, dataPools: fs.mds.coldMDS.dataPools})
			if refused && (err == nil || err.Error() != want) || !refused && err != nil {
				t.Fatalf("cold decoder, state %q: %v", state, err)
			}
		}
	})
	t.Run("replacement", func(t *testing.T) {
		const want = "MDS replacement global daemon map is contradictory"
		for _, state := range append(slices.Clone(assignedMDSRowStates), refusedMDSRowStates...) {
			refused := !mdsmap.StateAccepted(state)
			r := newReplacementMDSFixture(t)
			r.n.native["filesystems"] = append(r.n.native["filesystems"].([]any), foreign(state))
			_, err := r.add(t.Context())
			if refused {
				if err == nil || err.Error() != want {
					t.Fatalf("replacement decoder, state %q: %v", state, err)
				}
				r.assertNoAttempt(t)
			} else if err != nil && err.Error() == want {
				t.Fatalf("replacement decoder refused accepted state %q", state)
			}
		}
	})
}
