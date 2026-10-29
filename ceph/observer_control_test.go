package ceph

import (
	"context"
	"errors"
	"testing"
	"time"
)

// controlSubject is one observation that selects the control container, or a
// captured control handle with the original identity, for itself.
type controlSubject struct {
	name string
	// build returns a cluster whose fake control container answers the
	// observation, and a count of the commands that reached it.
	build func() (*Container, func() int)
	query func(context.Context, *Observer) error
	// takesTopologyLock is false for QuorumStatus, which has no lock.
	takesTopologyLock bool
	// refusesClosed is false for QuorumStatus, which has no liveness check.
	refusesClosed bool
}

func controlSubjects() []controlSubject {
	return []controlSubject{
		{name: "QuorumStatus",
			build: func() (*Container, func() int) {
				c, ctr, _ := newClientMonitorFixture()
				return c, func() int { return ctr.execs }
			},
			query: func(ctx context.Context, o *Observer) error { _, err := o.QuorumStatus(ctx); return err }},
		{name: "MonitorBootstrapAddresses", takesTopologyLock: true, refusesClosed: true,
			build: func() (*Container, func() int) {
				c, ctr, _ := newClientMonitorFixture()
				return c, func() int { return ctr.execs }
			},
			query: func(ctx context.Context, o *Observer) error { _, err := o.MonitorBootstrapAddresses(ctx); return err }},
		{name: "HealthDetails", takesTopologyLock: true, refusesClosed: true,
			build: func() (*Container, func() int) {
				c, ctr := newHealthDetailsFixture()
				return c, func() int { return len(ctr.calls) }
			},
			query: func(ctx context.Context, o *Observer) error { _, err := o.HealthDetails(ctx); return err }},
		{name: "FullRatios", takesTopologyLock: true, refusesClosed: true,
			build: func() (*Container, func() int) {
				c, ctr := newFullRatiosFixture()
				return c, func() int { return len(ctr.calls) }
			},
			query: func(ctx context.Context, o *Observer) error { _, err := o.FullRatios(ctx); return err }},
	}
}

func TestObserverControlQueriesReadWithoutCaptureAtConstruction(t *testing.T) {
	for _, subject := range controlSubjects() {
		t.Run(subject.name, func(t *testing.T) {
			cluster, commands := subject.build()
			observer, err := Observe(cluster)
			if err != nil || commands() != 0 {
				t.Fatalf("construction: %v, %d commands", err, commands())
			}
			if err := subject.query(t.Context(), observer); err != nil {
				t.Fatalf("first query: %v", err)
			}
			first := commands()
			if first == 0 {
				t.Fatal("the query ran no command")
			}
			// A fresh fixture state would be needed for a second successful
			// HealthDetails; a failed or successful second query must still
			// reach the command path rather than return an earlier result.
			_ = subject.query(t.Context(), observer)
			if commands() <= first {
				t.Fatal("a retained observer answered without reading the cluster")
			}
		})
	}
}

func TestObserverFullRatiosReturnsTheCurrentThresholds(t *testing.T) {
	cluster, ctr := newFullRatiosFixture()
	observer, _ := Observe(cluster)
	want1 := ctr.ratios
	want2 := float32Ratios(FullRatios{NearFull: .8, BackfillFull: .85, Full: .9})
	first, err := observer.FullRatios(t.Context())
	if err != nil || first.Ratios != want1 {
		t.Fatalf("first = %+v, %v", first, err)
	}
	ctr.ratios = want2
	second, err := observer.FullRatios(t.Context())
	if err != nil || second.Ratios != want2 {
		t.Fatalf("a retained observer returned earlier thresholds: %+v, %v", second, err)
	}
}

func TestObserverControlQueriesKeepTheirLivenessAndLockBehavior(t *testing.T) {
	for _, subject := range controlSubjects() {
		t.Run(subject.name+"/terminated", func(t *testing.T) {
			cluster, commands := subject.build()
			observer, _ := Observe(cluster)
			cluster.mu.Lock()
			cluster.closed = true
			cluster.mu.Unlock()
			err := subject.query(t.Context(), observer)
			switch {
			case subject.refusesClosed && (err == nil || commands() != 0):
				t.Fatalf("a terminated cluster answered: %v, %d commands", err, commands())
			case !subject.refusesClosed && commands() == 0:
				t.Fatal("the lockless query no longer reaches the command path")
			}
		})
		t.Run(subject.name+"/topology lock held", func(t *testing.T) {
			cluster, commands := subject.build()
			observer, _ := Observe(cluster)
			cluster.mu.Lock()
			defer cluster.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			err := subject.query(ctx, observer)
			if subject.takesTopologyLock {
				if !errors.Is(err, context.DeadlineExceeded) || commands() != 0 {
					t.Fatalf("the query did not wait for the lock under its context: %v, %d commands", err, commands())
				}
			} else if err != nil || commands() == 0 {
				t.Fatalf("a lockless query waited for the topology lock: %v", err)
			}
		})
	}
}

// TestObserverControlQueriesRefuseAfterReleaseAndAfterTerminationSeparately
// isolates the two reasons a retained tenant-backed observer must refuse: the
// lease ended while the cluster lives, and the cluster ended while the lease is
// still held.
func TestObserverControlQueriesRefuseAfterReleaseAndAfterTerminationSeparately(t *testing.T) {
	for _, subject := range controlSubjects() {
		t.Run(subject.name+"/released", func(t *testing.T) {
			cluster, commands := subject.build()
			lease := newLeaseDouble(cluster)
			observer := lease.observer()
			if err := subject.query(t.Context(), observer); err != nil {
				t.Fatalf("live lease: %v", err)
			}
			before := commands()
			lease.release() // the cluster is not terminated
			if err := subject.query(t.Context(), observer); !errors.Is(err, ErrInvalidRequest) || commands() != before {
				t.Fatalf("after release: %v, %d new commands", err, commands()-before)
			}
		})
		t.Run(subject.name+"/terminated", func(t *testing.T) {
			cluster, commands := subject.build()
			lease := newLeaseDouble(cluster)
			observer := lease.observer()
			if err := subject.query(t.Context(), observer); err != nil {
				t.Fatalf("live lease: %v", err)
			}
			before := commands()
			lease.terminate() // the lease is still held
			if err := subject.query(t.Context(), observer); !errors.Is(err, ErrInvalidRequest) || commands() != before {
				t.Fatalf("after termination: %v, %d new commands", err, commands()-before)
			}
		})
	}
}
