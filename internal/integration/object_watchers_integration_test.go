//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures_watchers))))

//ci: timeout=20m job-timeout=30 representative=flaky

package integration_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

// radosWatcher watches one object and answers every notify until stopped.
const radosWatcher = `
import os, rados, sys, time
pool, obj, workdir = sys.argv[1:4]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect()
ioctx = cluster.open_ioctx(pool)
ioctx.write_full(obj, b"watched")
watch = ioctx.watch(obj, lambda notify_id, notifier_id, watch_id, data: b"ack")
with open(workdir + "/id.tmp", "w") as out:
    out.write(str(cluster.get_instance_id()))
os.rename(workdir + "/id.tmp", workdir + "/id")
while True:
    time.sleep(1)
`

// radosNotifier sends one notify with a 5 second timeout and reports errno.
const radosNotifier = `
import json, rados, sys, time
pool, obj = sys.argv[1:3]
cluster = rados.Rados(conffile="/etc/ceph/ceph.conf")
cluster.connect()
ioctx = cluster.open_ioctx(pool)
start = time.monotonic()
errno = 0
try:
    ioctx.notify(obj, "ping", 5000)
except rados.Error as error:
    errno = getattr(error, "errno", -1)
seconds = time.monotonic() - start
# A librados client left to interpreter shutdown sometimes crashes there.
ioctx.close()
cluster.shutdown()
print(json.dumps({"errno": errno, "seconds": seconds}))
`

// A frozen watcher keeps its watch until osd_client_watch_timeout expires.
// Until then every notify to the object waits for it and times out; once the
// OSD drops the watch, notifies complete at once.
func TestPausedObjectWatcher(t *testing.T) {
	for _, mode := range networkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithOSDCount(2))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterWithOptions(t, image, opts...)
			const pool, object = "tc-watch", "watched"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			watchers := func() []ceph.ObjectWatcher {
				t.Helper()
				list, err := cluster.ObjectWatchers(ctx, pool, "", object)
				if err != nil {
					t.Fatal(err)
				}
				return list
			}
			notify := func() (int, float64) {
				t.Helper()
				code, out, err := sessionExec(ctx, cluster.ControlContainer(), "python3", "-c", radosNotifier, pool, object)
				var result struct {
					Errno   int     `json:"errno"`
					Seconds float64 `json:"seconds"`
				}
				if err != nil || code != 0 || json.Unmarshal([]byte(lastLine(out)), &result) != nil {
					t.Fatalf("notify failed: %d %v %s", code, err, out)
				}
				return result.Errno, result.Seconds
			}

			execOutput(t, ctx, client, "sh", "-c", `mkdir -p "$4" && printf '%s' "$1" > "$4/watcher.py" && nohup python3 "$4/watcher.py" "$2" "$3" "$4" > "$4/log" 2>&1 &`,
				"sh", radosWatcher, pool, object, "/tmp/watcher")
			id := waitClientFile(t, ctx, client, "/tmp/watcher/id", "/tmp/watcher/log")
			list := watchers()
			if len(list) != 1 || strconv.FormatUint(list[0].ClientID, 10) != id || list[0].Address == "" {
				t.Fatalf("watchers = %+v, want client.%s", list, id)
			}
			if errno, _ := notify(); errno != 0 {
				t.Fatalf("notify to a live watcher errno = %d", errno)
			}

			pause, err := fault.PauseContainer(ctx, cluster, client)
			if pause != nil {
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
					defer cancel()
					if err := pause.Resume(cleanupCtx); err != nil {
						t.Errorf("resume watcher: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			paused := time.Now()
			// ETIMEDOUT: the frozen watcher never acknowledges.
			if errno, seconds := notify(); errno != 110 || seconds < 4 {
				t.Fatalf("notify to a frozen watcher errno = %d after %.1fs, want 110 after its 5s timeout", errno, seconds)
			}
			// Each listing is bounded within the two minutes after the pause.
			listed, err := observe(ctx, 2*time.Minute-time.Since(paused), 30*time.Second, 2*time.Second,
				func(probeCtx context.Context) ([]ceph.ObjectWatcher, bool, error) {
					list, err := cluster.ObjectWatchers(probeCtx, pool, "", object)
					return list, err == nil && len(list) == 0, err
				})
			if err != nil {
				t.Fatalf("the OSD kept the frozen watcher's watch: %v; last listing %+v", err, listed.Value)
			}
			expired := time.Since(paused)
			t.Logf("OBJECT_WATCHER watch expired after %s", expired.Round(time.Second))
			if expired < 15*time.Second {
				t.Fatalf("watch dropped %s after the pause, before the 30s watch timeout", expired)
			}
			if errno, seconds := notify(); errno != 0 || seconds > 2 {
				t.Fatalf("notify after the watch expired errno = %d after %.1fs", errno, seconds)
			}
			entries, err := cluster.BlocklistEntries(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.Address == list[0].Address {
					t.Fatalf("an expired watch blocklisted its client: %+v", entry)
				}
			}
		})
	}
}
