//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// This recorder forwards every real runtime operation unchanged. It introduces
// no failure; only the native observer's ready-marker rename causes refusal.
type partialDetachContainer struct {
	testcontainers.Container
	stops        atomic.Int32
	terminations atomic.Int32
}

func (c *partialDetachContainer) Stop(ctx context.Context, timeout *time.Duration) error {
	c.stops.Add(1)
	return c.Container.Stop(ctx, timeout)
}
func (c *partialDetachContainer) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	c.terminations.Add(1)
	return c.Container.Terminate(ctx, opts...)
}

type partialDetachMarker struct {
	Inode   uint64 `json:"inode"`
	Mode    uint32 `json:"mode"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	SHA256  string `json:"sha256"`
}

type partialDetachObserver struct {
	container testcontainers.Container
	pending   string
}

func partialDetachReady(t *testing.T, ctx context.Context, observer *partialDetachObserver, store, action string) partialDetachMarker {
	t.Helper()
	// A lost exec reply must settle through the existing supervisor before
	// another marker command, including the registered restoration cleanup.
	if observer.pending != "" {
		if err := supervised.Cancel(ctx, observer.container, "retained ready marker", observer.pending); err != nil {
			t.Fatal("earlier marker command cannot be settled", err)
		}
		observer.pending = ""
	}
	result, err := supervised.Command(ctx, observer.container, "retained ready marker", 30*time.Second, 4096, 4096, "python3", "-c", partialDetachReadyScript, store, action)
	if err != nil {
		if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
			observer.pending = marker
		}
		t.Fatal("supervised marker operation", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("marker operation exit=%d stderr=%s", result.ExitCode, result.Stderr)
	}
	data := result.Stdout
	var marker partialDetachMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		t.Fatal("read ready-marker receipt", err)
	}
	if marker.Inode == 0 || len(marker.SHA256) != 64 {
		t.Fatal("invalid ready-marker receipt")
	}
	return marker
}

const partialDetachReadyScript = `import hashlib,json,os,pathlib,stat,sys
store,action=sys.argv[1:]
ready=pathlib.Path(store)/'ready'
held=pathlib.Path(store).parent/('.retained-ready-'+pathlib.Path(store).name)
if action=='settle':
    if held.exists() or held.is_symlink():
        action='restore'
    else:
        action='inspect'
if action=='restore':
    assert not ready.exists() and not ready.is_symlink()
    assert held.is_file() and not held.is_symlink()
    os.rename(held,ready)
s=ready.lstat()
assert stat.S_ISREG(s.st_mode)
receipt=dict(inode=s.st_ino,mode=s.st_mode,size=s.st_size,mtime_ns=s.st_mtime_ns,sha256=hashlib.sha256(ready.read_bytes()).hexdigest())
if action=='hide':
    assert not held.exists() and not held.is_symlink()
    os.rename(ready,held)
else:
    assert action in ('restore','inspect')
print(json.dumps(receipt))
`

func partialDetachClock(t *testing.T, ctx context.Context, docker *mobycl.Client) string {
	t.Helper()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return info.Info.SystemTime
}
func partialDetachEvents(t *testing.T, ctx context.Context, docker *mobycl.Client, owner, since, until string) map[string]int {
	t.Helper()
	stream := docker.Events(ctx, mobycl.EventsListOptions{Since: since, Until: until, Filters: make(mobycl.Filters).Add("type", "container").Add("event", "stop", "destroy", "create").Add("label", "org.testcontainers.ceph.cluster="+owner)})
	found := map[string]int{}
	for {
		select {
		case event := <-stream.Messages:
			found[string(event.Action)+"/"+event.Actor.ID]++
		case err := <-stream.Err:
			if !errors.Is(err, io.EOF) {
				t.Fatal("read lifecycle events", err)
			}
			return found
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
