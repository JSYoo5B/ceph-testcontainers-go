package ceph_test

import (
	"context"
	"fmt"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func ExampleCephFSContainer_WaitReady_select() {
	// This unavailable descriptor verifies expected API-error delivery and the
	// one-shot channel lifecycle without Docker. It is not a ready fixture or a
	// replacement for Run/StartCephFS; real callers use their owned filesystem.
	fs := &ceph.CephFSContainer{}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resultCh := make(chan error, 1)
	var results <-chan error = resultCh // One result for one consumer, not a broadcast.
	joined := make(chan struct{})
	defer func() { cancel(); <-joined }() // Register before launching the worker.
	go func() {
		defer close(joined)
		resultCh <- fs.WaitReady(waitCtx)
		close(resultCh)
	}()

	var outcomeErr error
	var open bool
	select {
	case outcomeErr, open = <-results:
	case <-waitCtx.Done():
		cancel()
		<-joined
		outcomeErr, open = <-results
	}
	cancel()
	<-joined // Join before callbacks or fixture cleanup with a fresh context.
	if !open {
		panic("wait closed without a terminal result")
	}
	if _, open := <-results; open {
		panic("wait sent more than one terminal result")
	}
	consume := func(err error) {
		fmt.Println("consumer received error:", err != nil)
	}
	consume(outcomeErr) // Caller code after join, outside the wait's fixture gates.
	fmt.Println("worker joined and channel closed")
	// Output:
	// consumer received error: true
	// worker joined and channel closed
}
