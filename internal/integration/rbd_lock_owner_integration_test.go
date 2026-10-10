//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_locks))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// rbdLockClient takes the image's exclusive lock by writing, reports how long
// that took, then waits for a go file and reports whether it can still write.
const rbdLockClient = `
import json, os, rados, rbd, sys, time
pool, name, workdir, payload = sys.argv[1:5]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect()
ioctx = cluster.open_ioctx(pool)
image = rbd.Image(ioctx, name)
start = time.monotonic()
image.write(payload.encode(), 0)
image.flush()
ready = {"id": cluster.get_instance_id(), "seconds": time.monotonic() - start}
with open(workdir + "/ready.tmp", "w") as out:
    json.dump(ready, out)
os.rename(workdir + "/ready.tmp", workdir + "/ready")
while not os.path.exists(workdir + "/go"):
    time.sleep(0.2)
result = {"errno": 0}
try:
    image.write(payload.encode(), 0)
    image.flush()
except rbd.Error as error:
    result["errno"] = getattr(error, "errno", -1)
with open(workdir + "/result.tmp", "w") as out:
    json.dump(result, out)
os.rename(workdir + "/result.tmp", workdir + "/result")
`

type rbdLockHolder struct {
	ID      uint64  `json:"id"`
	Seconds float64 `json:"seconds"`
}

// A frozen RBD client keeps the image's exclusive lock until its header watch
// expires. Another client then breaks the lock and blocklists the old owner.
// Fencing the frozen owner with a blocklist entry hands the lock over without
// waiting for the watch timeout. Either way the old owner cannot write again
// once it has seen the blocklist.
func TestRBDPausedLockOwner(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithOSDCount(2))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, first := newServiceClusterWithOptions(t, image, opts...)
			second, err := testcontainers.Run(ctx, image, cluster.WithClient(), ceph.WithIdleEntrypoint(),
				testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
			if second != nil {
				testcontainers.CleanupContainer(t, second)
			}
			if err != nil {
				t.Fatal(err)
			}
			const pool, imageName = "tc-lock", "disk"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rbd", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := rbd.InitPool(ctx, cluster, pool); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			execOutput(t, ctx, first, "rbd", "create", pool+"/"+imageName, "--size", "16M")
			clients := func() *rbd.ImageClientStatus {
				t.Helper()
				status, err := rbd.ImageClients(ctx, cluster, pool, "", imageName)
				if err != nil {
					t.Fatal(err)
				}
				return status
			}
			if status := clients(); len(status.Watchers) != 0 || status.ExclusiveOwner() != nil {
				t.Fatalf("idle image has clients: %+v", status)
			}
			start := func(ctr testcontainers.Container, workdir, payload string) {
				t.Helper()
				execOutput(t, ctx, ctr, "sh", "-c", `mkdir -p "$3" && printf '%s' "$1" > "$3/client.py" && nohup python3 "$3/client.py" "$2" disk "$3" "$4" > "$3/log" 2>&1 &`,
					"sh", rbdLockClient, pool, workdir, payload)
			}
			ready := func(ctr testcontainers.Container, workdir string) rbdLockHolder {
				t.Helper()
				var holder rbdLockHolder
				if err := json.Unmarshal([]byte(waitClientFile(t, ctx, ctr, workdir+"/ready", workdir+"/log")), &holder); err != nil {
					t.Fatal(err)
				}
				return holder
			}
			finish := func(ctr testcontainers.Container, workdir string) int {
				t.Helper()
				execOutput(t, ctx, ctr, "touch", workdir+"/go")
				var result struct {
					Errno int `json:"errno"`
				}
				if err := json.Unmarshal([]byte(waitClientFile(t, ctx, ctr, workdir+"/result", workdir+"/log")), &result); err != nil {
					t.Fatal(err)
				}
				return result.Errno
			}
			owner := func(id uint64) *rbd.ImageLock {
				t.Helper()
				status := clients()
				lock := status.ExclusiveOwner()
				if lock == nil || lock.Locker != "client."+strconv.FormatUint(id, 10) {
					t.Fatalf("exclusive owner = %+v, want client.%d; watchers %+v", lock, id, status.Watchers)
				}
				if !slices.ContainsFunc(status.Watchers, func(w rbd.ImageWatcher) bool { return w.Address == lock.Address }) {
					t.Fatalf("owner %s does not watch the image: %+v", lock.Address, status.Watchers)
				}
				return lock
			}
			pause := func(ctr testcontainers.Container) *ceph.ContainerPause {
				t.Helper()
				paused, err := cluster.PauseContainer(ctx, ctr)
				if paused != nil {
					t.Cleanup(func() {
						cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
						defer cancel()
						if err := paused.Resume(cleanupCtx); err != nil {
							t.Errorf("resume client: %v", err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
				return paused
			}
			blocklisted := func(address string) bool {
				t.Helper()
				entries, err := cluster.BlocklistEntries(ctx)
				if err != nil {
					t.Fatal(err)
				}
				return slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == address })
			}

			// The first owner freezes; the second client waits for its
			// watch to expire before it can take the lock.
			start(first, "/tmp/a", "owner-a")
			a := ready(first, "/tmp/a")
			aLock := owner(a.ID)
			pausedA := pause(first)
			start(second, "/tmp/b", "owner-b")
			b := ready(second, "/tmp/b")
			t.Logf("RBD_LOCK watch timeout handoff after %.1fs", b.Seconds)
			if b.Seconds < 10 {
				t.Fatalf("lock moved in %.1fs, before the frozen owner's watch could expire", b.Seconds)
			}
			bLock := owner(b.ID)
			if slices.ContainsFunc(clients().Watchers, func(w rbd.ImageWatcher) bool { return w.Address == aLock.Address }) {
				t.Fatal("expired owner still watches the image")
			}
			if !blocklisted(aLock.Address) {
				t.Fatalf("broken lock owner %s is not blocklisted", aLock.Address)
			}
			if err := pausedA.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			if errno := finish(first, "/tmp/a"); errno != 108 {
				t.Fatalf("resumed former owner write errno = %d, want 108", errno)
			}

			// Fencing the frozen second owner removes its watch at once.
			pausedB := pause(second)
			fence, err := cluster.TemporaryBlocklist(ctx, bLock.Address, 10*time.Minute)
			if err != nil {
				t.Fatal("fence frozen owner:", err)
			}
			start(first, "/tmp/c", "owner-c")
			c := ready(first, "/tmp/c")
			t.Logf("RBD_LOCK fenced handoff after %.1fs", c.Seconds)
			if c.Seconds >= b.Seconds {
				t.Fatalf("fenced handoff took %.1fs, no faster than the %.1fs watch timeout", c.Seconds, b.Seconds)
			}
			owner(c.ID)
			// Lifting the fence before the old owner sees it would let that
			// owner ask for the cooperative lock back and keep writing.
			if err := pausedB.Resume(ctx); err != nil {
				t.Fatal(err)
			}
			if errno := finish(second, "/tmp/b"); errno != 108 {
				t.Fatalf("resumed fenced owner write errno = %d, want 108", errno)
			}
			if err := fence.Restore(ctx); err != nil {
				t.Fatal("lift fence:", err)
			}
			if errno := finish(first, "/tmp/c"); errno != 0 {
				t.Fatalf("current owner write errno = %d", errno)
			}
		})
	}
}
