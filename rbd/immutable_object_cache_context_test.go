package rbd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// Status, Stop and Restore wait for another call on the same lease only until
// the caller's context ends. A queued call stops no daemon and removes no
// file, keeps the lease open, leaves no waiter behind, and the same call
// succeeds with a fresh context afterwards.
func TestImmutableObjectCacheLeaseWaitsOnlyUntilTheContextEnds(t *testing.T) {
	client := newFakeCacheClient()
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"Status", func(ctx context.Context) error { _, err := lease.Status(ctx); return err }},
		{"Stop", lease.Stop},
		{"Restore", lease.Restore},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			files, running := len(client.files), client.daemons[0].running
			clustertest.AssertContextLockQueued(t, &lease.mu, operation.call)
			if len(client.files) != files || client.daemons[0].running != running || lease.complete {
				t.Fatal("a queued call stopped the daemon, removed files or ended the lease")
			}
			if err := operation.call(t.Context()); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
		})
	}
	if !lease.complete || client.daemons[0].running || len(client.files) != 1 {
		t.Fatalf("the fresh calls did not stop and restore the cache: %v", client.files)
	}
}

// The stop dispatch exposes both budgets of the actual automatic rollback.
// The fake can stall until the backend context ends, as with a held flock.
type cacheStartupProbe struct {
	*fakeCacheClient
	stall, failedStart, shellStopped bool
	inspect                          string
	deadline                         time.Time
	limit                            string
	sawStop                          bool
}

func (c *cacheStartupProbe) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[2] == immutableObjectCacheScript && args[4] == "inspect" && c.inspect != "" {
		return clustertest.Reply(args, 0, []byte(c.inspect))
	}
	if args[2] == immutableObjectCacheScript && args[4] == "start" && c.failedStart {
		return clustertest.Reply(args, 1, []byte("prelaunch command failed"))
	}
	if len(args) > 11 && args[2] == supervised.ShellScript && args[11] == "stop" {
		c.sawStop = true
		c.deadline, _ = ctx.Deadline()
		c.limit = args[5]
		if c.shellStopped {
			line := supervised.ShellDeadlineLine + "\n" + supervised.ShellEndPrefix + "143\n"
			header := binary.BigEndian.AppendUint32([]byte{2, 0, 0, 0}, uint32(len(line)))
			return 143, bytes.NewReader(append(header, line...)), nil
		}
		if c.stall {
			if c.deadline.IsZero() {
				return 0, nil, errors.New("unbounded rollback context")
			}
			<-ctx.Done()
			return 0, nil, ctx.Err()
		}
	}
	return c.fakeCacheClient.Exec(ctx, args, opts...)
}

func TestImmutableObjectCacheBoundsAutomaticRollback(t *testing.T) {
	c := &cacheStartupProbe{fakeCacheClient: newFakeCacheClient()}
	c.startConflict = true
	before := time.Now()
	lease, err := StartImmutableObjectCache(context.Background(), c, ImmutableObjectCache{})
	limit, limitErr := strconv.Atoi(c.limit)
	if lease != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !c.sawStop || limitErr != nil || limit < 1 || limit > int(immutableCacheRollbackTimeout/time.Second) ||
		c.deadline.Before(before) || c.deadline.After(time.Now().Add(immutableCacheRollbackTimeout)) {
		t.Fatalf("automatic conflict rollback: lease=%v error=%v deadline=%s shell limit=%s", lease, err, c.deadline, c.limit)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	c.stall = true
	lease, err = StartImmutableObjectCache(ctx, c, ImmutableObjectCache{})
	parent, _ := ctx.Deadline()
	if lease == nil || !errors.Is(err, ceph.ErrInvalidRequest) || !errors.Is(err, context.DeadlineExceeded) ||
		!c.deadline.Equal(parent) || c.limit != "1" {
		t.Fatalf("short rollback: lease=%v error=%v deadline=%s parent=%s shell limit=%s", lease, err, c.deadline, parent, c.limit)
	}
	c.stall = false
	if err := lease.Restore(context.Background()); err != nil || c.limit != "0" {
		t.Fatalf("explicit restore: %v, limit=%s", err, c.limit)
	}
}

func TestImmutableObjectCacheReportsShellRollbackDeadline(t *testing.T) {
	c := &cacheStartupProbe{fakeCacheClient: newFakeCacheClient(), shellStopped: true}
	c.startConflict = true
	lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{})
	if lease == nil || !errors.Is(err, ceph.ErrInvalidRequest) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shell deadline: lease=%v, error=%v", lease, err)
	}
	c.shellStopped = false
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestImmutableObjectCacheRollsBackDefinitiveScriptFailure(t *testing.T) {
	c := &cacheStartupProbe{fakeCacheClient: newFakeCacheClient(), failedStart: true}
	lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{})
	if lease != nil || !errors.Is(err, ceph.ErrNativeFailure) || !c.sawStop {
		t.Fatalf("reported start command failure: lease=%v error=%v saw stop=%t", lease, err, c.sawStop)
	}
}

func TestImmutableObjectCacheRequiresNoReplaceRename(t *testing.T) {
	c := &cacheStartupProbe{fakeCacheClient: newFakeCacheClient(), inspect: "binary=1\nplugin=1\nrename=0\nflock=1\nclaim=/tmp/tc-ioc-path-" + strings.Repeat("a", 64) + "\nsocket=0\npath=none\n"}
	lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{})
	if lease != nil || !errors.Is(err, ceph.ErrImageCapability) || !strings.Contains(err.Error(), "renameat2") || c.sawStop {
		t.Fatalf("missing rename capability: %v, %v", lease, err)
	}
}

func TestImmutableObjectCacheNamesTheInspectedRefusal(t *testing.T) {
	common := "binary=1\nplugin=1\nrename=1\nflock=1\nclaim=/tmp/tc-ioc-path-" + strings.Repeat("a", 64) + "\n"
	for _, tc := range []struct{ report, diagnostic string }{
		{"socket=0\npath=claimed\n", "path /cache already has a lease claim"},
		{"socket=invalid-parent\npath=none\n", "ancestor of the immutable object cache socket /blocked/sock"},
		{"socket=0\npath=invalid-parent\n", "ancestor of the immutable object cache path /cache"},
	} {
		c := &cacheStartupProbe{fakeCacheClient: newFakeCacheClient(), inspect: common + tc.report}
		lease, err := StartImmutableObjectCache(t.Context(), c, ImmutableObjectCache{Path: "/cache", Socket: "/blocked/sock"})
		if lease != nil || !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), tc.diagnostic) || c.sawStop {
			t.Fatalf("inspection %q: lease=%v error=%v", tc.report, lease, err)
		}
	}
}
