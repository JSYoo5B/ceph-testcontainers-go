//go:build native_regression && (all || (integration && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_clone_timing)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestCephFSCloneTimingQualification compares deliberately injected clone
// windows, not the zero-delay default. The candidate must retain every pending,
// cancellation, collision, source-protection and data-integrity assertion. The
// MGR worker's own log confirms which delay it captured before restoration.
func TestCephFSCloneTimingQualification(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)),
		cephfs.WithFilesystems(cephfs.Config{Name: "clone-timing"}))
	fs := cephfs.Filesystems(cluster)[0]
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	managers, err := cluster.Managers(ctx)
	if err != nil || len(managers) != 1 {
		t.Fatalf("owned managers=%d error=%v", len(managers), err)
	}
	// Keep the same diagnostic level in both cohorts. It is needed to observe
	// capture at worker entry, rather than infer consumption from config get.
	debug, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "debug_mgr", Value: "10"})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := debug.Restore(cleanup); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	group, err := subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: "trials"})
	if err != nil {
		t.Fatal(err)
	}
	source, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "source", SizeBytes: 8 << 20, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := subvolume.Info(ctx, fs, source.Name, source.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, fs.FilesystemName, source.Path, "", "seed", info.DataPool, info.PoolNamespace, 8<<20)
	snapshot, err := source.CreateSnapshot(ctx, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, fs.FilesystemName, source.Path, snapshot.Path, "update-source", info.DataPool, info.PoolNamespace, 8<<20)
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	for trial := range 6 {
		baseline := trial%2 == 0
		cancelDelay, collisionDelay := 15, 15
		if baseline {
			cancelDelay, collisionDelay = 30, 60
		}
		if !t.Run(fmt.Sprintf("trial-%d-cancel-%d-collision-%d", trial, cancelDelay, collisionDelay), func(t *testing.T) {
			started := time.Now()
			var phases time.Duration
			defer func() {
				t.Logf("CLONE_TIMING_TRIAL ceph=%s cancel_delay=%d collision_delay=%d worker_phases=%s scenario=%s failed=%t", facts.CephVersion, cancelDelay, collisionDelay, phases, time.Since(started), t.Failed())
			}()
			for _, failure := range []bool{false, true} {
				label := "cancel"
				seconds := cancelDelay
				if failure {
					label = "collision"
					seconds = collisionDelay
				}
				name := fmt.Sprintf("%s-%d", label, trial)
				phase := time.Now()
				func() {
					var handles []*override.ConfigHandle
					defer func() {
						cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
						defer stop()
						for i := len(handles) - 1; i >= 0; i-- {
							if err := handles[i].Restore(cleanup); err != nil {
								t.Error(err)
							}
						}
					}()
					for _, setting := range []ceph.ConfigSetting{
						{Section: "mgr", Name: "mgr/volumes/snapshot_clone_no_wait", Value: "false"},
						{Section: "mgr", Name: "mgr/volumes/snapshot_clone_delay", Value: strconv.Itoa(seconds)},
					} {
						handle, err := override.SetConfig(ctx, cluster, setting)
						handles = append(handles, handle)
						if err != nil {
							t.Fatal(err)
						}
					}
					clone, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: name, GroupName: group.Name})
					if err != nil {
						t.Fatal(err)
					}
					cloneTimingWorkerLog(t, ctx, managers[0], "starting clone: ("+fs.FilesystemName+", "+group.Name+", "+name+")")
					// The worker entered its sleep no later than this observation,
					// so the time from here to the clone's end is a lower bound of
					// the sleep it actually took.
					entered := time.Now()
					state, err := clone.Status(ctx)
					if err != nil || state.State != "pending" {
						t.Fatalf("worker escaped required pending window: %+v %v", state, err)
					}
					snapshotInfo, err := subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
					if err != nil || !snapshotInfo.HasPendingClones || !slices.Contains(snapshotInfo.PendingClones, subvolume.SnapshotPendingClone{Name: name, GroupName: group.Name}) {
						t.Fatalf("missing pending source protection: %+v %v", snapshotInfo, err)
					}
					if err := snapshot.Remove(ctx); err == nil {
						t.Fatal("pending clone allowed snapshot removal")
					}
					if err := clone.RemovePartial(ctx); err == nil {
						t.Fatal("pending clone allowed partial removal")
					}
					wait, stop := context.WithTimeout(ctx, 150*time.Millisecond)
					target, waitErr := clone.Wait(wait)
					stop()
					if waitErr == nil || target != nil {
						t.Fatal("pending clone ignored caller wait deadline")
					}
					if duplicate, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: name, GroupName: group.Name}); err == nil || duplicate != nil {
						t.Fatal("pending clone accepted duplicate target")
					}
					if failure {
						cephFSSubvolumeExec(t, ctx, client, "python3", "-c", cephFSCloneTargetCollision, fs.FilesystemName, source.Path, source.Name, snapshot.Name, group.Name, name)
					} else if err := clone.Cancel(ctx); err != nil {
						t.Fatal(err)
					}
					// The worker has entered with the configured delay. Restoring the central
					// option now must not retroactively shorten its captured sleep. A
					// cancel ends the sleep, so only the collision can show it: its
					// clone cannot fail before the captured delay has elapsed.
					for i := len(handles) - 1; i >= 0; i-- {
						if err := handles[i].Restore(ctx); err != nil {
							t.Fatal(err)
						}
					}
					if target, err := clone.Wait(ctx); err == nil || target != nil {
						t.Fatal("non-complete clone was adopted")
					}
					slept := time.Since(entered)
					if failure && slept < time.Duration(seconds)*time.Second {
						t.Fatalf("collision clone ended %s after the worker entered, before its captured %ds delay", slept, seconds)
					}
					state, err = clone.Status(ctx)
					if failure {
						if err != nil || state.State != "failed" || state.FailureErrno != 21 {
							t.Fatalf("collision did not retain EISDIR: %+v %v", state, err)
						}
					} else if err != nil || state.State != "canceled" {
						t.Fatalf("cancel did not retain canceled state: %+v %v", state, err)
					}
					cloneTimingWorkerLog(t, ctx, managers[0], fmt.Sprintf("Delayed cloning (%s, %s, %s) -- by %d seconds", fs.FilesystemName, group.Name, name, seconds))
					for {
						snapshotInfo, err = subvolume.SnapshotInfo(ctx, fs, source.Name, source.GroupName, snapshot.Name)
						if err != nil {
							t.Fatal(err)
						}
						if !snapshotInfo.HasPendingClones && snapshotInfo.OrphanClonesCount == 0 {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatal("worker retained snapshot protection", ctx.Err())
						case <-time.After(500 * time.Millisecond):
						}
					}
					cephFSSnapshotIO(t, ctx, client, fs.FilesystemName, snapshot.Path, source.Path, "verify-clone", info.DataPool, info.PoolNamespace, 8<<20)
					if err := clone.RemovePartial(ctx); err != nil {
						t.Fatal(err)
					}
					names, err := subvolume.Names(ctx, fs, group.Name)
					if err != nil || slices.Contains(names, name) {
						t.Fatalf("clone target retained after cleanup: %v %v", names, err)
					}
					t.Logf("CLONE_TIMING_PHASE name=%s delay=%d elapsed=%s captured_delay_verified=%t worker_elapsed=%s", name, seconds, time.Since(phase), failure, slept)
				}()
				phases += time.Since(phase)
			}
		}) {
			t.FailNow()
		}
	}
	// The zero-delay default remains a real successful clone with independent
	// client data, rather than being treated as a missing worker or fast timeout.
	replacement, err := snapshot.Clone(ctx, subvolume.CloneConfig{Name: "default", GroupName: group.Name})
	if err != nil {
		t.Fatal(err)
	}
	complete, err := replacement.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cephFSSnapshotIO(t, ctx, client, fs.FilesystemName, complete.Path, source.Path, "verify-clone", info.DataPool, info.PoolNamespace, 8<<20)
	if err := snapshot.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := complete.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := source.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := group.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}

// cloneTimingWorkerLog reads only the owned MGR's bounded logs and reports the
// expected non-secret marker. Raw daemon logs are not emitted by this probe.
func cloneTimingWorkerLog(t *testing.T, parent context.Context, mgr *ceph.ManagerContainer, marker string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	for {
		reader, err := mgr.Logs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(io.LimitReader(reader, 16<<20))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("read owned manager logs: %v %v", readErr, closeErr)
		}
		if strings.Contains(string(data), marker) {
			t.Logf("CLONE_TIMING_WORKER %s", marker)
			return
		}
		if len(data) == 16<<20 {
			t.Fatal("manager log observation exceeded bounded buffer")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native worker marker missing: %s: %v", marker, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
