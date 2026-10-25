//go:build all || integration

package integration_test

import (
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"slices"
	"strings"
)

type ecBelowKPlan struct {
	Original []int `json:"original"`
	Primary  int   `json:"primary"`
	Losses   []int `json:"nonprimary_indices"`
}

func planECBelowK(acting []int, primary int) (ecBelowKPlan, error) {
	if _, err := planECAdmission(acting, primary, 2, false); err != nil {
		return ecBelowKPlan{}, err
	}
	p := ecBelowKPlan{Original: slices.Clone(acting), Primary: primary}
	for i, id := range acting {
		if id != primary {
			p.Losses = append(p.Losses, i)
		}
	}
	if len(p.Losses) != 2 {
		return ecBelowKPlan{}, fmt.Errorf("need two nonprimary holders")
	}
	return p, nil
}
func checkECBelowKPlacement(p ecBelowKPlan, base, now ceph.PoolPGSnapshot, stopped []int, floor uint32) error {
	want := slices.Clone(p.Original)
	for _, i := range stopped {
		if !slices.Contains(p.Losses, i) || want[i] == ecFragmentNone {
			return fmt.Errorf("unplanned/repeated loss %v", stopped)
		}
		want[i] = ecFragmentNone
	}
	if len(stopped) > 2 || len(base.PGs) != 1 || len(now.PGs) != 1 || base.FSID == "" || now.FSID != base.FSID || now.PoolBefore.ID != base.PoolAfter.ID || now.PoolAfter.ID != base.PoolAfter.ID || now.PoolBefore.Name != base.PoolAfter.Name || now.PoolAfter.Name != base.PoolAfter.Name {
		return fmt.Errorf("pool/PG identity changed")
	}
	if now.PoolBefore.Size != 3 || now.PoolAfter.Size != 3 || now.PoolBefore.MinSize != 2 || now.PoolAfter.MinSize != 2 {
		return fmt.Errorf("2+1 min_size=2 changed")
	}
	pg := now.PGs[0]
	states := strings.Split(pg.State, "+")
	if now.OSDMapEpochBefore != now.OSDMapEpochAfter || pg.ReportedEpoch <= floor || pg.PGID != base.PGs[0].PGID || !slices.Equal(pg.Acting, want) || pg.ActingPrimary != p.Primary {
		return fmt.Errorf("placement/epoch/primary does not attest boundary: %+v", pg)
	}
	if len(stopped) == 0 {
		if !slices.Contains(states, "active") || !slices.Contains(states, "clean") {
			return fmt.Errorf("not restored clean")
		}
		return nil
	}
	if len(stopped) == 2 {
		// Below K, native peering cannot form a servable EC acting set. This
		// differs from the peered K-holder case blocked only by min_size.
		if pg.State != "down" {
			return fmt.Errorf("below-K peering is not down: %s", pg.State)
		}
		return nil
	}
	if slices.Contains(states, "clean") || !slices.Contains(states, "undersized") || !slices.Contains(states, "degraded") {
		return fmt.Errorf("not degraded/undersized")
	}
	if !slices.Contains(states, "active") {
		return fmt.Errorf("admission disagrees with K2 boundary: %s", pg.State)
	}
	return nil
}
func checkECBelowKUnknown(seed, now ecFourTwoIO) error {
	if now.Seq != 2 || now.Phase != "blocked" || now.PID != seed.PID || now.Writer != seed.Writer || now.Reader != seed.Reader || now.Read == nil || now.Write == nil || now.Read.Outcome != "unknown" || now.Write.Outcome != "unknown" || now.Read.Errno != 110 || now.Write.Errno != 110 {
		return fmt.Errorf("below-K operations not bounded on retained clients: %+v", now)
	}
	return nil
}
