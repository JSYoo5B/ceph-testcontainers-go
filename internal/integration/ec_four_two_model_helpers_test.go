//go:build all || integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

type ecFourTwoPlan struct {
	Original []int `json:"original"`
	Primary  int   `json:"initial_primary"`
	MinSize  int   `json:"min_size"`
}

func planECFourTwo(acting []int, primary, minSize int) (ecFourTwoPlan, error) {
	if len(acting) != 6 || minSize != 4 && minSize != 5 || !slices.Contains(acting, primary) {
		return ecFourTwoPlan{}, fmt.Errorf("4+2 needs six complete positions and min_size 4 or 5: %v/%d/%d", acting, primary, minSize)
	}
	seen := map[int]bool{}
	for _, osd := range acting {
		if osd < 0 || osd == ecFragmentNone || seen[osd] {
			return ecFourTwoPlan{}, fmt.Errorf("not six distinct eligible holders: %v", acting)
		}
		seen[osd] = true
	}
	return ecFourTwoPlan{Original: slices.Clone(acting), Primary: primary, MinSize: minSize}, nil
}

func (p ecFourTwoPlan) losses(newPrimary int) ([]int, error) {
	first := slices.Index(p.Original, p.Primary)
	if first < 0 {
		return nil, fmt.Errorf("initial primary missing")
	}
	if newPrimary == p.Primary || !slices.Contains(p.Original, newPrimary) {
		return nil, fmt.Errorf("no surviving replacement primary: %d", newPrimary)
	}
	for index, osd := range p.Original {
		if index != first && osd != newPrimary {
			return []int{first, index}, nil
		}
	}
	return nil, fmt.Errorf("no second non-primary holder")
}

func (p ecFourTwoPlan) actingWithout(indices []int) ([]int, error) {
	if len(indices) < 1 || len(indices) > 2 {
		return nil, fmt.Errorf("not one or two unavailable fragments: %v", indices)
	}
	want := slices.Clone(p.Original)
	for _, index := range indices {
		if index < 0 || index >= len(want) || want[index] == ecFragmentNone {
			return nil, fmt.Errorf("invalid/repeated fragment index: %v", indices)
		}
		want[index] = ecFragmentNone
	}
	return want, nil
}

func checkECFourTwoPlacement(p ecFourTwoPlan, baseline, observed ceph.PoolPGSnapshot, lost []int, epochFloor uint32, retainedPrimary int) error {
	want, err := p.actingWithout(lost)
	if err != nil {
		return err
	}
	if len(baseline.PGs) != 1 || len(observed.PGs) != 1 || baseline.FSID == "" || observed.FSID != baseline.FSID || observed.PoolBefore.ID != baseline.PoolAfter.ID || observed.PoolAfter.ID != baseline.PoolAfter.ID || observed.PoolBefore.Name != baseline.PoolAfter.Name || observed.PoolAfter.Name != baseline.PoolAfter.Name {
		return fmt.Errorf("pool/PG identity changed")
	}
	if observed.PoolBefore.Size != 6 || observed.PoolAfter.Size != 6 || observed.PoolBefore.MinSize != p.MinSize || observed.PoolAfter.MinSize != p.MinSize {
		return fmt.Errorf("4+2 size/min_size changed")
	}
	pg := observed.PGs[0]
	if observed.OSDMapEpochBefore != observed.OSDMapEpochAfter || pg.ReportedEpoch <= epochFloor || pg.PGID != baseline.PGs[0].PGID || !slices.Equal(pg.Acting, want) || pg.ActingPrimary < 0 || pg.ActingPrimary == ecFragmentNone || !slices.Contains(want, pg.ActingPrimary) {
		return fmt.Errorf("placement/epoch does not attest the planned loss: %+v", pg)
	}
	if len(lost) == 1 && pg.ActingPrimary == p.Primary || len(lost) == 2 && pg.ActingPrimary != retainedPrimary {
		return fmt.Errorf("unexpected primary transition to %d", pg.ActingPrimary)
	}
	states := strings.Split(pg.State, "+")
	if !slices.Contains(states, "undersized") || !slices.Contains(states, "degraded") || slices.Contains(states, "clean") {
		return fmt.Errorf("no degraded/undersized phase: %s", pg.State)
	}
	servable := 6-len(lost) >= p.MinSize
	if servable && !slices.Contains(states, "active") || !servable && (slices.Contains(states, "active") || !slices.Contains(states, "peered")) {
		return fmt.Errorf("admission state %s disagrees with %d remaining and min_size %d", pg.State, 6-len(lost), p.MinSize)
	}
	return nil
}

func ecFourTwoPayloadHash(size, value int) string {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*13 + value*41) % 251)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type ecFourTwoAttempt struct {
	Outcome string `json:"outcome"`
	Errno   int    `json:"errno"`
}

type ecFourTwoIO struct {
	Seq         int               `json:"seq"`
	Phase       string            `json:"phase"`
	PID         int               `json:"pid"`
	Writer      uint64            `json:"writer"`
	Reader      uint64            `json:"reader"`
	Value       int               `json:"value,omitempty"`
	Snap        uint64            `json:"snap,omitempty"`
	Size        int               `json:"size,omitempty"`
	HeadSHA256  string            `json:"head_sha256,omitempty"`
	HeadVersion uint64            `json:"head_version,omitempty"`
	SnapSHA256  string            `json:"snap_sha256,omitempty"`
	SnapVersion uint64            `json:"snap_version,omitempty"`
	Read        *ecFourTwoAttempt `json:"read,omitempty"`
	Write       *ecFourTwoAttempt `json:"write,omitempty"`
}

func checkECFourTwoAck(base, previous, now ecFourTwoIO, size, value, seq int) error {
	if now.Phase != "ack" || now.Seq != seq || now.Value != value || now.Size != size || now.PID != base.PID || now.Writer != base.Writer || now.Reader != base.Reader || now.Writer == 0 || now.Reader == 0 || now.Writer == now.Reader || now.Snap != base.Snap || now.HeadVersion <= previous.HeadVersion || now.HeadSHA256 != ecFourTwoPayloadHash(size, value) || now.SnapSHA256 != base.HeadSHA256 || now.SnapVersion != base.HeadVersion {
		return fmt.Errorf("acknowledgement does not match the retained actor, requested bytes or snapshot: %+v", now)
	}
	return nil
}

type ecFourTwoShutdown struct {
	PID      int    `json:"pid"`
	Writer   uint64 `json:"writer"`
	Reader   uint64 `json:"reader"`
	Shutdown bool   `json:"shutdown"`
	Commands int    `json:"commands"`
}

func checkECFourTwoShutdown(seed ecFourTwoIO, shutdown ecFourTwoShutdown) error {
	if !shutdown.Shutdown || shutdown.Commands != 2 || shutdown.PID != seed.PID || shutdown.Writer != seed.Writer || shutdown.Reader != seed.Reader {
		return fmt.Errorf("the original actor and both clients did not attest shutdown: %+v", shutdown)
	}
	return nil
}

type ecFourTwoObject struct {
	Present *bool  `json:"present"`
	Size    int    `json:"size,omitempty"`
	SHA256  string `json:"sha256,omitempty"`
	Version uint64 `json:"version,omitempty"`
}

type ecFourTwoReadback struct {
	Reader      uint64           `json:"reader"`
	Snap        uint64           `json:"snap"`
	Size        int              `json:"size"`
	HeadSHA256  string           `json:"head_sha256"`
	HeadVersion uint64           `json:"head_version"`
	SnapSHA256  string           `json:"snap_sha256"`
	SnapVersion uint64           `json:"snap_version"`
	Unknown     *ecFourTwoObject `json:"unknown"`
}

func checkECFourTwoReadback(seed, last ecFourTwoIO, read ecFourTwoReadback, size int, submittedUnknown bool) error {
	if read.Reader == 0 || read.Reader == seed.Writer || read.Reader == seed.Reader || read.Snap != seed.Snap || read.Size != size || read.HeadSHA256 != last.HeadSHA256 || read.HeadVersion != last.HeadVersion || read.SnapSHA256 != seed.HeadSHA256 || read.SnapVersion != seed.HeadVersion {
		return fmt.Errorf("fresh reader did not repeat acknowledged data/metadata: %+v", read)
	}
	if read.Unknown == nil || read.Unknown.Present == nil {
		return fmt.Errorf("uncertain object was not independently inspected")
	}
	if *read.Unknown.Present && (!submittedUnknown || read.Unknown.Size != size || read.Unknown.SHA256 != ecFourTwoPayloadHash(size, 2) || read.Unknown.Version == 0) {
		return fmt.Errorf("uncertain object is unexpected or incomplete: %+v", read.Unknown)
	}
	return nil
}
