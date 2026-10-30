//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_removal_warning)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/containerd/errdefs"
)

// These are contract controls for the ordinary graceful public removal.
func TestCephFSRemovalWarningQualification(t *testing.T) {
	for _, condition := range []string{"unrelated", "target", "none"} {
		t.Run(condition, func(t *testing.T) { testCephFSRemovalWarning(t, condition) })
	}
}

func testCephFSRemovalWarning(t *testing.T, condition string) {
	const expect = "removed"
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	bConfig := cephfs.Config{Name: "warning-b"}
	if condition == "unrelated" {
		bConfig.StandbyMDS = 1
		bConfig.StandbyReplay = true
	}
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)), cephfs.WithFilesystems(cephfs.Config{Name: "warning-a"}, bConfig))
	a, b := cephFSOwnedFilesystem(t, cluster, "warning-a"), cephFSOwnedFilesystem(t, cluster, "warning-b")
	aBefore, bBefore := cephFSTopologyStatus(t, ctx, a), cephFSTopologyStatus(t, ctx, b)
	if len(aBefore.Active) != 1 || len(bBefore.Active) != 1 || aBefore.FilesystemID == bBefore.FilesystemID || a.MetadataPool == b.MetadataPool || a.DataPool == b.DataPool {
		t.Fatal("distinct single-MDS owned filesystems required")
	}
	bDaemons := b.MDSs()
	if condition == "unrelated" && (len(bDaemons) != 2 || len(bBefore.StandbyReplay) != 1) {
		t.Fatalf("target standby replay was not established: %+v", bBefore)
	}
	induced, inducedBefore, otherBefore := a, aBefore, bBefore
	if condition == "target" {
		induced, inducedBefore, otherBefore = b, bBefore, aBefore
	}
	inducedDaemon := cephFSOwnedMDS(t, induced, inducedBefore.Active[0].Name)
	inducedState, err := inducedDaemon.State(ctx)
	if err != nil || inducedState == nil || !inducedState.Running || inducedState.StartedAt == "" {
		t.Fatalf("induced daemon generation unavailable: %+v %v", inducedState, err)
	}
	const attached = "warning-b-attached"
	if condition == "unrelated" {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: attached, Application: "cephfs", PGNum: 1}); err != nil {
			t.Fatal(err)
		}
		if _, err := b.AddDataPool(ctx, attached); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	seed := []byte("independent-warning-a-bytes\x00\xff")
	seedB := []byte("independent-warning-b-bytes\x01\xfe")
	write := func(name string, data []byte) {
		harness.ExecOutput(t, ctx, client, "python3", "-c", `
import cephfs, os, sys
f=cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf");f.conf_set("client_mount_timeout","30");f.mount(filesystem_name=sys.argv[1].encode())
try:
 fd=f.open(b"/qualification",os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 data=bytes.fromhex(sys.argv[2]);assert f.write(fd,data,0)==len(data);f.fsync(fd,False);f.close(fd)
finally:
 f.unmount();f.shutdown()
`, name, hex.EncodeToString(data))
	}
	write(a.FilesystemName, seed)
	write(b.FilesystemName, seedB)
	versions, err := cluster.Ceph(ctx, "versions", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	readConfig := func(probe context.Context) (string, error) {
		out, err := cluster.Ceph(probe, "tell", "mds."+inducedBefore.Active[0].Name, "config", "get", "mds_cache_memory_limit")
		if err != nil {
			return "", err
		}
		var values map[string]string
		if err := json.Unmarshal(out, &values); err != nil {
			return "", err
		}
		value, ok := values["mds_cache_memory_limit"]
		if !ok || value == "" {
			return "", fmt.Errorf("missing native cache limit: %s", out)
		}
		return value, nil
	}
	prior, err := readConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	guardGeneration := func(probe context.Context) error {
		status, err := induced.MDSStatus(probe)
		if err != nil || status == nil || status.FilesystemID != inducedBefore.FilesystemID || !slices.ContainsFunc(status.Active, func(mds cephfs.MDSStatus) bool {
			return mds.Name == inducedBefore.Active[0].Name && mds.GID == inducedBefore.Active[0].GID && mds.Owned
		}) {
			return fmt.Errorf("native induced filesystem/MDS generation unavailable or changed: %+v err=%v", status, err)
		}
		state, err := inducedDaemon.State(probe)
		if err != nil || state == nil || !state.Running || state.StartedAt != inducedState.StartedAt {
			return fmt.Errorf("induced container process generation unavailable or changed: %+v err=%v", state, err)
		}
		return nil
	}
	var pending *ceph.NativeCommandUnstoppedError
	setConfig := func(probe context.Context, value string) error {
		if err := guardGeneration(probe); err != nil {
			return err
		}
		r, err := cluster.RunNativeCommand(probe, clusteraccess.ControlContainer(t, cluster), ceph.NativeCommand{
			Argv:    []string{"ceph", "tell", "mds." + inducedBefore.Active[0].Name, "config", "set", "mds_cache_memory_limit", value},
			Timeout: 30 * time.Second, MaxStdout: 16 << 10, MaxStderr: 16 << 10, Label: "owned MDS cache limit",
		})
		if err != nil {
			errors.As(err, &pending)
			return err
		}
		if r.ExitCode != 0 || !r.Complete() {
			return fmt.Errorf("native cache limit set exit=%d complete=%t stderr=%s", r.ExitCode, r.Complete(), r.Stderr)
		}
		return nil
	}
	restored := condition == "none"
	restore := func(probe context.Context) error {
		if restored {
			return nil
		}
		if pending != nil {
			if err := pending.Stop(probe); err != nil {
				return err
			}
			pending = nil
		}
		if err := guardGeneration(probe); err != nil {
			return fmt.Errorf("restore unconfirmed: %w", err)
		}
		current, err := readConfig(probe)
		if err != nil {
			return err
		}
		if current == prior {
			restored = true
			return nil
		}
		if current != "1" {
			return fmt.Errorf("owned cache limit changed outside fixture: %s", current)
		}
		if err := setConfig(probe, prior); err != nil {
			return err
		}
		current, err = readConfig(probe)
		if err != nil || current != prior {
			return fmt.Errorf("native cache restore unconfirmed: value=%s err=%v", current, err)
		}
		restored = true
		return nil
	}
	t.Cleanup(func() {
		if restored {
			return
		}
		fresh, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := restore(fresh); err != nil {
			t.Errorf("restore owned MDS cache configuration: %v", err)
		}
	})
	applied := ""
	if condition != "none" {
		if err := setConfig(ctx, "1"); err != nil {
			t.Fatal(err)
		}
		applied, err = readConfig(ctx)
		if err != nil || applied != "1" {
			t.Fatalf("native MDS cache limit not applied: value=%s err=%v", applied, err)
		}
	}
	var warning json.RawMessage
	_, _, err = boundedPoll(ctx, realPollClock, 90*time.Second, time.Second, func(probe context.Context) (bool, bool, error) {
		out, err := cluster.Ceph(probe, "health", "detail", "--format", "json")
		warning = slices.Clone(out)
		if err != nil {
			return false, false, err
		}
		var h struct {
			Checks map[string]struct {
				Detail []struct {
					Message string `json:"message"`
				} `json:"detail"`
			} `json:"checks"`
		}
		if err := json.Unmarshal(out, &h); err != nil {
			return false, false, err
		}
		found := false
		for _, code := range []string{"MDS_CACHE_OVERSIZED", "MDS_TRIM"} {
			check, present := h.Checks[code]
			if !present {
				continue
			}
			if condition == "none" {
				return false, false, fmt.Errorf("unexpected relevant warning in no-warning control: %s", out)
			}
			if len(check.Detail) == 0 {
				return false, false, fmt.Errorf("warning lacks daemon attribution: %s", out)
			}
			for _, detail := range check.Detail {
				if strings.Contains(detail.Message, "mds."+otherBefore.Active[0].Name+"(") || !strings.Contains(detail.Message, "mds."+inducedBefore.Active[0].Name+"(") {
					return false, false, fmt.Errorf("warning not exclusively on selected daemon: %s", detail.Message)
				}
				found = true
			}
		}
		ready := condition == "none" || found
		return ready, ready, nil
	})
	if err != nil {
		t.Fatalf("native selected warning condition not established: %v health=%s", err, warning)
	}
	// One ordinary public call must complete the graceful drain/removal contract.
	started := time.Now()
	removeErr := b.Remove(ctx)
	ended := time.Now()
	result := "removed"
	if removeErr != nil {
		result = "refused"
	}
	failure := ""
	if removeErr != nil {
		failure = removeErr.Error()
	}
	t.Logf("CEPHFS_REMOVAL_WARNING_ATTEMPT expected=%s observed=%s ordinary_error=%q warning=%s", expect, result, failure, warning)
	if result != expect {
		t.Fatalf("ordinary removal result %s, expected %s: %v", result, expect, removeErr)
	}

	afterConfig := ""
	disposition := "disposed-with-target"
	if condition != "target" {
		disposition = "restored"
		if condition == "none" {
			disposition = "unchanged"
		}
		if err := restore(ctx); err != nil {
			t.Fatal(err)
		}
		afterConfig, err = readConfig(ctx)
		if err != nil || afterConfig != prior {
			t.Fatalf("cache configuration not restored: prior=%s current=%s err=%v", prior, afterConfig, err)
		}
	}
	fsList, err := cluster.Ceph(ctx, "fs", "ls", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	pools, err := cluster.Ceph(ctx, "osd", "pool", "ls", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	if json.Unmarshal(pools, &names) != nil || !slices.Contains(names, a.MetadataPool) || !slices.Contains(names, a.DataPool) {
		t.Fatalf("survivor pools missing: %s", pools)
	}
	if result == "removed" && (slices.Contains(names, b.MetadataPool) || slices.Contains(names, b.DataPool)) {
		t.Fatalf("target pools remain: %s", pools)
	}
	var nativeFilesystems []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(fsList, &nativeFilesystems) != nil || slices.ContainsFunc(nativeFilesystems, func(item struct {
		Name string `json:"name"`
	}) bool {
		return item.Name == b.FilesystemName
	}) {
		t.Fatalf("target filesystem remains: %s", fsList)
	}
	if slices.ContainsFunc(cephfs.Filesystems(cluster), func(owned *cephfs.Filesystem) bool { return owned.FilesystemName == b.FilesystemName }) {
		t.Fatal("target still registered as owned")
	}
	for _, daemon := range bDaemons {
		if _, err := daemon.State(ctx); !errdefs.IsNotFound(err) {
			t.Fatalf("removed target mds.%s container still exists: %v", daemon.ID, err)
		}
		if _, err := cluster.Ceph(ctx, "auth", "get", "mds."+daemon.ID); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("target mds.%s auth key remains or lookup uncertain: %v", daemon.ID, err)
		}
	}
	if condition == "unrelated" && !slices.Contains(names, attached) {
		t.Fatalf("later-attached pool was removed: %s", pools)
	}
	if condition == "target" {
		restored = true
	}
	var clearHealth json.RawMessage
	_, _, err = boundedPoll(ctx, realPollClock, 90*time.Second, time.Second, func(probe context.Context) (bool, bool, error) {
		out, err := cluster.Ceph(probe, "health", "detail", "--format", "json")
		clearHealth = slices.Clone(out)
		if err != nil {
			return false, false, err
		}
		var health struct {
			Checks map[string]json.RawMessage `json:"checks"`
		}
		if err := json.Unmarshal(out, &health); err != nil {
			return false, false, err
		}
		_, cache := health.Checks["MDS_CACHE_OVERSIZED"]
		_, trim := health.Checks["MDS_TRIM"]
		return !cache && !trim, !cache && !trim, nil
	})
	if err != nil {
		t.Fatalf("native warning did not clear after restoration/disposal: %v health=%s", err, clearHealth)
	}
	readA, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", harness.CephFSIndependentReader, a.FilesystemName, "/qualification"}, Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "independent survivor reader"})
	var data struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int    `json:"size"`
		Data   string `json:"data"`
	}
	if err != nil || readA.ExitCode != 0 || !readA.Complete() || json.Unmarshal([]byte(harness.LastJSONLine(string(readA.Stdout))), &data) != nil || data.Client == 0 || data.Inode == 0 || data.Size != len(seed) || data.Data != hex.EncodeToString(seed) {
		t.Fatalf("survivor data: %+v exit=%d err=%v stderr=%s", data, readA.ExitCode, err, readA.Stderr)
	}
	aAfter := cephFSTopologyStatus(t, ctx, a)
	if aAfter.FilesystemID != aBefore.FilesystemID || len(aAfter.Active) != 1 || aAfter.Active[0].GID != aBefore.Active[0].GID || aAfter.Active[0].Name != aBefore.Active[0].Name {
		t.Fatalf("A identity changed: %+v -> %+v", aBefore, aAfter)
	}
	receipt, _ := json.Marshal(map[string]any{"condition": condition, "expected": expect, "observed": result, "selection": "ordinary/no-tag", "versions": json.RawMessage(versions), "warning": warning, "health_after_restoration": clearHealth, "b_owned_mds_count_removed": len(bDaemons), "later_attached_pool_retained": condition == "unrelated", "a_before": aBefore, "a_after": aAfter, "b_before": bBefore, "filesystem_list": json.RawMessage(fsList), "pool_list": json.RawMessage(pools), "config_prior": prior, "config_applied": applied, "config_after": afterConfig, "config_disposition": disposition, "survivor_read": data, "remove_started": started, "remove_ended": ended, "ordinary_error": failure, "role_images": map[string]string{"essential": os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), "client": os.Getenv("CEPH_TEST_CLIENT_IMAGE"), "cephfs": os.Getenv("CEPH_TEST_CEPHFS_IMAGE")}})
	t.Logf("CEPHFS_REMOVAL_WARNING_RECEIPT %s", receipt)
}
