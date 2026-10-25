//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestECAdmissionPlan(t *testing.T) {
	for _, primaryLost := range []bool{true, false} {
		p, err := planECAdmission([]int{7, 2, 9}, 2, 3, primaryLost)
		if err != nil || p.PrimaryLost != primaryLost || (p.Victim == 2) != primaryLost || p.WantActing[p.VictimIndex] != ecFragmentNone {
			t.Fatalf("plan %+v: %v", p, err)
		}
	}
	for _, v := range []struct {
		acting           []int
		primary, minSize int
	}{
		{[]int{1, 2}, 1, 2}, {[]int{1, 2, ecFragmentNone}, 1, 2}, {[]int{1, 2, 2}, 1, 2},
		{[]int{1, 2, -1}, 1, 2}, {[]int{1, 2, 3}, 7, 2}, {[]int{1, 2, 3}, 1, 1},
	} {
		if p, err := planECAdmission(v.acting, v.primary, v.minSize, true); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
}

func TestECAdmissionState(t *testing.T) {
	for _, minSize := range []int{2, 3} {
		p, _ := planECAdmission([]int{7, 2, 9}, 2, minSize, true)
		pool := ceph.PoolState{ID: 4, Name: "fixture", Size: 3, MinSize: minSize}
		before := ceph.PoolPGSnapshot{FSID: "cluster", PoolBefore: pool, PoolAfter: pool, PGs: []ceph.PGState{{PGID: "4.0", State: "active+clean", Acting: p.Original, ActingPrimary: 2, ReportedEpoch: 10}}}
		state := "active+undersized+degraded"
		if minSize == 3 {
			state = "undersized+degraded+peered"
		}
		makeAfter := func() ceph.PoolPGSnapshot {
			return ceph.PoolPGSnapshot{FSID: before.FSID, PoolBefore: pool, PoolAfter: pool, PGs: []ceph.PGState{{PGID: "4.0", State: state, Acting: slices.Clone(p.WantActing), ActingPrimary: 7, ReportedEpoch: 12}}}
		}
		if err := checkECAdmissionState(p, before, makeAfter()); err != nil {
			t.Fatal(err)
		}
		for name, alter := range map[string]func(*ceph.PoolPGSnapshot){
			"shifted fragments":   func(s *ceph.PoolPGSnapshot) { s.PGs[0].Acting = []int{ecFragmentNone, 7, 9} },
			"unchanged primary":   func(s *ceph.PoolPGSnapshot) { s.PGs[0].ActingPrimary = 2 },
			"unrelated PG":        func(s *ceph.PoolPGSnapshot) { s.PGs[0].PGID = "9.0" },
			"old epoch":           func(s *ceph.PoolPGSnapshot) { s.PGs[0].ReportedEpoch = 10 },
			"changed threshold":   func(s *ceph.PoolPGSnapshot) { s.PoolAfter.MinSize = 1 },
			"other pool":          func(s *ceph.PoolPGSnapshot) { s.PoolBefore.ID++ },
			"missing primary":     func(s *ceph.PoolPGSnapshot) { s.PGs[0].ActingPrimary = -1 },
			"false service state": func(s *ceph.PoolPGSnapshot) { s.PGs[0].State = "active+clean" },
		} {
			after := makeAfter()
			alter(&after)
			if err := checkECAdmissionState(p, before, after); err == nil {
				t.Errorf("min_size %d: accepted %s", minSize, name)
			}
		}
	}
}

func TestECAdmissionStopInstance(t *testing.T) {
	healthy := ecAdmissionInstance{ContainerID: "owned-container", Running: true, Pid: 40, StartedAt: "started-1"}
	if err := checkECAdmissionStop(healthy, healthy); err != nil {
		t.Fatal(err)
	}
	for name, alter := range map[string]func(*ecAdmissionInstance){
		"already exited":          func(s *ecAdmissionInstance) { s.Running, s.Pid = false, 0; s.FinishedAt = "ended" },
		"exited with OOM":         func(s *ecAdmissionInstance) { s.Running, s.Pid, s.OOMKilled = false, 0, true },
		"running with OOM":        func(s *ecAdmissionInstance) { s.OOMKilled = true },
		"paused":                  func(s *ecAdmissionInstance) { s.Paused = true },
		"restarting":              func(s *ecAdmissionInstance) { s.Restarting = true },
		"dead":                    func(s *ecAdmissionInstance) { s.Dead = true },
		"missing PID":             func(s *ecAdmissionInstance) { s.Pid = 0 },
		"missing start":           func(s *ecAdmissionInstance) { s.StartedAt = "" },
		"missing container":       func(s *ecAdmissionInstance) { s.ContainerID = "" },
		"another container":       func(s *ecAdmissionInstance) { s.ContainerID = "replacement-container" },
		"another process":         func(s *ecAdmissionInstance) { s.Pid++ },
		"new start with same PID": func(s *ecAdmissionInstance) { s.StartedAt = "started-2" },
	} {
		t.Run(name, func(t *testing.T) {
			current := healthy
			alter(&current)
			if err := checkECAdmissionStop(healthy, current); err == nil {
				t.Fatalf("admitted unplanned instance %+v", current)
			}
		})
	}
	for _, captured := range []ecAdmissionInstance{
		{}, {ContainerID: "owned-container", Pid: 40, StartedAt: "started-1"},
		{ContainerID: "owned-container", Running: true, Pid: 40, StartedAt: "started-1", OOMKilled: true},
	} {
		if err := checkECAdmissionStop(captured, healthy); err == nil {
			t.Fatalf("admitted unhealthy captured baseline %+v", captured)
		}
	}
	stopped := ecAdmissionInstance{ContainerID: healthy.ContainerID, StartedAt: healthy.StartedAt, FinishedAt: "ended", ExitCode: 0}
	if err := checkECAdmissionStopped(healthy, stopped); err != nil {
		t.Fatal(err)
	}
	for name, alter := range map[string]func(*ecAdmissionInstance){
		"still running":               func(s *ecAdmissionInstance) { s.Running, s.Pid = true, 40 },
		"OOM after Stop":              func(s *ecAdmissionInstance) { s.OOMKilled = true; s.ExitCode = 137 },
		"different stopped container": func(s *ecAdmissionInstance) { s.ContainerID = "replacement-container" },
		"restarted generation":        func(s *ecAdmissionInstance) { s.StartedAt = "started-2" },
		"live PID":                    func(s *ecAdmissionInstance) { s.Pid = 41 },
	} {
		t.Run(name, func(t *testing.T) {
			current := stopped
			alter(&current)
			if err := checkECAdmissionStopped(healthy, current); err == nil {
				t.Fatalf("accepted unconfirmed stop %+v", current)
			}
		})
	}
}
