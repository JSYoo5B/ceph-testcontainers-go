package ceph_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// An evaluation reads only the observation it is given, so its diagnostics
// name the field that failed in that observation.
func ExampleOSDExpectation_Evaluate() {
	baseline := []ceph.OSDState{
		{ID: 0, UUID: "11111111-1111-4111-8111-111111111111", Up: true, In: true, Weight: 1},
		{ID: 1, UUID: "22222222-2222-4222-8222-222222222222", Up: true, In: true, Weight: 1},
	}
	later := []ceph.OSDState{
		baseline[0],
		{ID: 1, UUID: "22222222-2222-4222-8222-222222222222", Up: false, In: true, Weight: 1},
	}
	result, err := ceph.OSDExpectation{Select: baseline, Up: true, In: true, Exact: true}.Evaluate(later)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// osds [0 1] want up=true in=true exact in 2 observed: osd.1 up: want true, observed false
}

// The test owns the polling: a failed query is kept apart from the last
// mismatch, so a timeout reports both instead of an empty "not ready".
// This example is compiled, not run.
func ExampleOSDExpectation_Evaluate_polling() {
	ctx := context.Background()
	cluster, err := ceph.Run(ctx, ceph.DefaultImage)
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	observer, err := ceph.Observe(cluster)
	if err != nil {
		panic(err)
	}
	baseline, err := observer.OSDStates(ctx)
	if err != nil {
		panic(err)
	}
	// ... stop and restart an OSD while the SUT runs ...
	want := ceph.OSDExpectation{Select: baseline, Up: true, In: true, Exact: true}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last ceph.OSDEvaluation
	var queryErr error
	for {
		states, err := observer.OSDStates(ctx)
		if err == nil {
			last, err = want.Evaluate(states)
		}
		if err == nil && last.Matched {
			return
		}
		if err != nil {
			queryErr = err
		}
		select {
		case <-ctx.Done():
			panic(fmt.Sprintf("OSDs not restored: last %v, last error %v", last, queryErr))
		case <-time.After(time.Second):
		}
	}
}
