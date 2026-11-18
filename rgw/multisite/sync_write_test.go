package multisite

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// syncWriterBoundary models an accepted write independently of its response.
// It changes no native resource and uses channels, not delayed live mutations.
type syncWriterBoundary struct {
	testcontainers.Container
	accepted      func()
	release, done chan struct{}
	lost          bool
	calls         atomic.Int32
}

func (c *syncWriterBoundary) Exec(ctx context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls.Add(1)
	if c.accepted != nil {
		c.accepted()
	}
	if c.lost {
		return 0, nil, errors.New("transport lost secret")
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	close(c.done)
	return 0, bytes.NewReader(nil), nil
}

func TestSyncWritesAwaitCompletionBeforeReadback(t *testing.T) {
	for _, args := range [][]string{{"sync", "group", "modify"}, {"sync", "group", "flow", "create"}, {"sync", "group", "pipe", "create"}, {"period", "update"}, {"period", "commit"}} {
		t.Run(args[len(args)-2]+"/"+args[len(args)-1], func(t *testing.T) {
			f, _, _, _ := newRGWSyncTestFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cli := &syncWriterBoundary{accepted: cancel, release: make(chan struct{}), done: make(chan struct{})}
			zone := &rgwZoneState{Zone: Zone{ID: "source-id", Zonegroup: "group"}, client: cli}
			if _, err := f.syncPolicyWrite(ctx, zone, "group", args...); !mutation.Uncertain(err) {
				t.Fatalf("accepted writer lost: %v", err)
			}
			probe, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer stop()
			if _, err := f.syncPolicyAdmin(probe, zone, "group", "period", "get"); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("readback passed live writer: %v", err)
			}
			if cli.calls.Load() != 1 {
				t.Fatal("dispatched work before previous completion")
			}
			close(cli.release)
			<-cli.done
			if err := f.settleSyncWrite(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSyncLostTransportCannotBeSettledByMatchingReadback(t *testing.T) {
	f, _, _, _ := newRGWSyncTestFixture()
	cli := &syncWriterBoundary{lost: true}
	zone := &rgwZoneState{Zone: Zone{ID: "source-id", Zonegroup: "group"}, client: cli}
	if _, err := f.syncPolicyWrite(t.Context(), zone, "group", "period", "commit"); !mutation.Uncertain(err) {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"period", "get"}, {"period", "commit"}, {"sync", "group", "modify"}} {
		var err error
		if args[1] == "get" {
			_, err = f.syncPolicyAdmin(t.Context(), zone, "group", args...)
		} else {
			_, err = f.syncPolicyWrite(t.Context(), zone, "group", args...)
		}
		if !mutation.Uncertain(err) {
			t.Fatalf("unconfirmed writer accepted later operation: %v", err)
		}
	}
	if cli.calls.Load() != 1 {
		t.Fatal("native readback or replay bypassed completion fence")
	}
}

// delayedPolicyCLI pauses one accepted assignment before applying it to the
// in-memory policy. Later commands cannot race its application in this model.
type delayedPolicyCLI struct {
	*rgwSyncCLI
	cancel        context.CancelFunc
	release, done chan struct{}
}

func (c *delayedPolicyCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if syncTestCommand(args, "sync", "group", "modify") && c.cancel != nil {
		cancel := c.cancel
		c.cancel = nil
		cancel()
		select {
		case <-c.release:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
		code, reader, err := c.rgwSyncCLI.Exec(ctx, args, opts...)
		close(c.done)
		return code, reader, err
	}
	return c.rgwSyncCLI.Exec(ctx, args, opts...)
}
func TestSyncPolicyLaterAssignmentWaitsForEarlierWriter(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{}, SyncGroupConfig{ID: "owned", Status: SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	delayed := &delayedPolicyCLI{rgwSyncCLI: cli, cancel: cancel, release: make(chan struct{}), done: make(chan struct{})}
	f.realm.sourceClient = delayed
	if err := g.SetStatus(ctx, SyncEnabled); !mutation.Uncertain(err) {
		t.Fatalf("lost admitted write: %v", err)
	}
	probe, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer stop()
	if err := g.SetStatus(probe, SyncForbidden); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("later assignment bypassed writer: %v", err)
	}
	close(delayed.release)
	<-delayed.done
	if err := g.SetStatus(t.Context(), SyncForbidden); err != nil {
		t.Fatal(err)
	}
	if g.state.group["status"] != "forbidden" || g.state.pending != nil {
		t.Fatal("settled earlier assignment replaced later policy")
	}
}
