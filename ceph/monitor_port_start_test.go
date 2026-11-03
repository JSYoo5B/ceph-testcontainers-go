package ceph

import (
	"context"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// TestStartMonitorRetryingPortsRetriesOnlyALostPortRace pins the loop that
// every MON start shares: it retries a failure the attempt marks as a port
// race only when the container reports EADDRINUSE, discards the container
// before each retry, stops at hostPortAttempts, runs a bridge start once and
// returns the container that a failed discard keeps.
func TestStartMonitorRetryingPortsRetriesOnlyALostPortRace(t *testing.T) {
	conflicted := func() *hostNetworkFixtureContainer {
		return &hostNetworkFixtureContainer{state: &container.State{Status: "exited", ExitCode: 1}, logs: "bind failed: (98) Address already in use\n"}
	}
	other := &hostNetworkFixtureContainer{state: &container.State{Status: "exited", ExitCode: 1}, logs: "invalid fsid\n"}
	startErr := errors.New("synthetic start failure")
	discardErr := errors.New("synthetic discard failure")
	for name, test := range map[string]struct {
		host         bool
		retry        bool
		ctr          func() *hostNetworkFixtureContainer
		discardErr   error
		wantAttempts int
		wantKept     bool
		wantDiscard  error
	}{
		"race retried to the limit": {host: true, retry: true, ctr: conflicted, wantAttempts: hostPortAttempts, wantKept: true},
		"bridge runs once":          {host: false, retry: true, ctr: conflicted, wantAttempts: 1, wantKept: true},
		"unmarked failure":          {host: true, retry: false, ctr: conflicted, wantAttempts: 1, wantKept: true},
		"no port conflict":          {host: true, retry: true, ctr: func() *hostNetworkFixtureContainer { return other }, wantAttempts: 1, wantKept: true},
		"failed discard":            {host: true, retry: true, ctr: conflicted, discardErr: discardErr, wantAttempts: 1, wantKept: true, wantDiscard: discardErr},
	} {
		t.Run(name, func(t *testing.T) {
			c := &Container{}
			attempts, discards := 0, 0
			var last testcontainers.Container
			ctr, err := c.startMonitorRetryingPorts(t.Context(), monitorPortStart{
				host: test.host,
				attempt: func(context.Context) (testcontainers.Container, bool, error) {
					attempts++
					last = test.ctr()
					return last, test.retry, startErr
				},
				discard: func(_ context.Context, ctr testcontainers.Container) (testcontainers.Container, error) {
					discards++
					if ctr != last {
						t.Fatal("discard got another attempt's container")
					}
					if test.discardErr != nil {
						return ctr, test.discardErr
					}
					return nil, nil
				},
			})
			if attempts != test.wantAttempts || !errors.Is(err, startErr) {
				t.Fatalf("attempts = %d, err = %v; want %d attempts", attempts, err, test.wantAttempts)
			}
			if test.wantDiscard != nil && !errors.Is(err, test.wantDiscard) {
				t.Fatalf("err = %v, want the discard failure joined", err)
			}
			if want := attempts - 1; test.wantDiscard == nil && discards != want {
				t.Fatalf("discards = %d, want %d", discards, want)
			}
			if test.wantKept && ctr != last {
				t.Fatalf("returned %v, want the last attempt's container", ctr)
			}
		})
	}
	c := &Container{}
	attempts := 0
	ctr, err := c.startMonitorRetryingPorts(t.Context(), monitorPortStart{
		host: true,
		attempt: func(context.Context) (testcontainers.Container, bool, error) {
			attempts++
			if attempts < 3 {
				return conflicted(), true, startErr
			}
			return other, false, nil
		},
		discard: func(context.Context, testcontainers.Container) (testcontainers.Container, error) { return nil, nil },
	})
	if err != nil || ctr != other || attempts != 3 {
		t.Fatalf("a retried start = %v, %v after %d attempts", ctr, err, attempts)
	}
}
