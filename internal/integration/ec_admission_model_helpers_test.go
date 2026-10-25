//go:build all || integration

package integration_test

import (
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

type ecAdmissionPlan struct {
	Original    []int `json:"original"`
	Primary     int   `json:"primary"`
	Victim      int   `json:"victim"`
	VictimIndex int   `json:"victim_index"`
	PrimaryLost bool  `json:"primary_lost"`
	MinSize     int   `json:"min_size"`
	WantActing  []int `json:"want_acting"`
}

// ecAdmissionInstance is one observed Docker instance, not just an OSD ID.
// Keeping exit and OOM state separate from the generation lets the receipt
// distinguish an admitted Stop from an already exited or restarted holder.
type ecAdmissionInstance struct {
	ContainerID                       string `json:"container_id"`
	Running, Paused, Restarting, Dead bool
	OOMKilled                         bool
	Pid                               int
	StartedAt, FinishedAt             string
	ExitCode                          int
}

func checkECAdmissionHealthy(instance ecAdmissionInstance) error {
	if instance.ContainerID == "" || instance.StartedAt == "" || instance.Pid <= 0 ||
		!instance.Running || instance.Paused || instance.Restarting || instance.Dead || instance.OOMKilled {
		return fmt.Errorf("fragment holder is not a healthy running instance: %+v", instance)
	}
	return nil
}

// A fresh read immediately before Stop must match the healthy captured
// container/process generation. Docker inspection and Stop are not an atomic
// compare-and-stop; these checks do not claim to eliminate that final interval.
func checkECAdmissionStop(captured, current ecAdmissionInstance) error {
	if err := checkECAdmissionHealthy(captured); err != nil {
		return err
	}
	if err := checkECAdmissionHealthy(current); err != nil {
		return err
	}
	if current.ContainerID != captured.ContainerID || current.Pid != captured.Pid || current.StartedAt != captured.StartedAt {
		return fmt.Errorf("fragment holder generation changed: captured %+v; current %+v", captured, current)
	}
	return nil
}

func checkECAdmissionStopped(captured, stopped ecAdmissionInstance) error {
	if err := checkECAdmissionHealthy(captured); err != nil {
		return err
	}
	if stopped.ContainerID != captured.ContainerID || stopped.StartedAt != captured.StartedAt ||
		stopped.Running || stopped.Paused || stopped.Restarting || stopped.Dead || stopped.OOMKilled || stopped.Pid != 0 {
		return fmt.Errorf("Stop did not leave the captured holder stopped without OOM: %+v", stopped)
	}
	return nil
}

// This bounded plan is exactly 2+1. Shard positions are never compacted or
// inferred from OSD IDs, and the default threshold is not lowered for the fault.
func planECAdmission(acting []int, primary, minSize int, primaryLost bool) (ecAdmissionPlan, error) {
	if len(acting) != 3 || minSize != 2 && minSize != 3 || !slices.Contains(acting, primary) {
		return ecAdmissionPlan{}, fmt.Errorf("not a complete 2+1 admission plan: %v primary %d min_size %d", acting, primary, minSize)
	}
	seen := map[int]bool{}
	for _, osd := range acting {
		if osd < 0 || osd == ecFragmentNone || seen[osd] {
			return ecAdmissionPlan{}, fmt.Errorf("not three distinct fragment holders: %v", acting)
		}
		seen[osd] = true
	}
	p := ecAdmissionPlan{Original: slices.Clone(acting), Primary: primary, PrimaryLost: primaryLost, MinSize: minSize, VictimIndex: -1}
	for index, osd := range acting {
		if (osd == primary) == primaryLost {
			p.Victim, p.VictimIndex = osd, index
			break
		}
	}
	p.WantActing = slices.Clone(acting)
	p.WantActing[p.VictimIndex] = ecFragmentNone
	return p, nil
}

// MON-backed PG observations must attest the same pool and policy, a newer
// map, exact shard holes and native primary turnover. State words alone cannot
// substitute for the mapping or the explicitly configured admission boundary.
func checkECAdmissionState(p ecAdmissionPlan, before, after ceph.PoolPGSnapshot) error {
	if before.FSID == "" || after.FSID != before.FSID || before.PoolAfter.ID != after.PoolBefore.ID ||
		after.PoolBefore.ID != after.PoolAfter.ID || before.PoolAfter.Name != after.PoolAfter.Name ||
		after.PoolBefore.MinSize != p.MinSize || after.PoolAfter.MinSize != p.MinSize ||
		after.PoolBefore.Size != 3 || after.PoolAfter.Size != 3 || len(before.PGs) != 1 || len(after.PGs) != 1 {
		return fmt.Errorf("pool identity or 2+1 admission policy changed")
	}
	pg := after.PGs[0]
	if pg.PGID != before.PGs[0].PGID || pg.ReportedEpoch <= before.PGs[0].ReportedEpoch ||
		!slices.Equal(pg.Acting, p.WantActing) || pg.ActingPrimary < 0 || pg.ActingPrimary == p.Victim || !slices.Contains(p.WantActing, pg.ActingPrimary) {
		return fmt.Errorf("new PG report does not attest the planned shard loss: %+v", pg)
	}
	if p.PrimaryLost && pg.ActingPrimary == p.Primary || !p.PrimaryLost && pg.ActingPrimary != p.Primary {
		return fmt.Errorf("unexpected primary transition from osd.%d to osd.%d", p.Primary, pg.ActingPrimary)
	}
	states := strings.Split(pg.State, "+")
	if !slices.Contains(states, "undersized") || !slices.Contains(states, "degraded") || slices.Contains(states, "clean") {
		return fmt.Errorf("PG does not report the observed degradation: %s", pg.State)
	}
	if p.MinSize == 2 && !slices.Contains(states, "active") || p.MinSize == 3 && (slices.Contains(states, "active") || !slices.Contains(states, "peered")) {
		return fmt.Errorf("PG admission state %s disagrees with min_size=%d", pg.State, p.MinSize)
	}
	return nil
}
