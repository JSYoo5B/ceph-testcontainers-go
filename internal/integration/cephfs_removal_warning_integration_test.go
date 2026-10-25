//go:build (all || (integration && topology)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_cephfs_removal_warning)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// This qualification calls only the ordinary public operation. The caller
// supplies its cohort expectation rather than deriving bug state from a release.
func TestCephFSRemovalWarningQualification(t *testing.T) {
	expect := os.Getenv("CEPH_TEST_REMOVAL_WARNING_EXPECT")
	if expect != "refused" && expect != "removed" {
		t.Fatal("set CEPH_TEST_REMOVAL_WARNING_EXPECT to refused or removed for the actual build under qualification")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(1), cephfs.WithFilesystems(cephfs.Config{Name: "warning-a"}, cephfs.Config{Name: "warning-b"}))
	a, b := cephFSOwnedFilesystem(t, cluster, "warning-a"), cephFSOwnedFilesystem(t, cluster, "warning-b")
	aBefore, bBefore := cephFSTopologyStatus(t, ctx, a), cephFSTopologyStatus(t, ctx, b)
	if len(aBefore.Active) != 1 || len(bBefore.Active) != 1 || aBefore.FilesystemID == bBefore.FilesystemID || a.MetadataPool == b.MetadataPool || a.DataPool == b.DataPool {
		t.Fatal("distinct single-MDS owned filesystems required")
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	seed := []byte("independent-warning-a-bytes\x00\xff")
	seedB := []byte("independent-warning-b-bytes\x01\xfe")
	write := func(name string, data []byte) {
		execOutput(t, ctx, client, "python3", "-c", `
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
		out, err := cluster.Ceph(probe, "config", "get", "mds."+aBefore.Active[0].Name, "mds_cache_memory_limit")
		return strings.TrimSpace(string(out)), err
	}
	prior, err := readConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	change, err := cluster.OverrideConfig(ctx, ceph.ConfigSetting{Section: "mds." + aBefore.Active[0].Name, Name: "mds_cache_memory_limit", Value: "1"})
	restored := false
	if change != nil {
		t.Cleanup(func() {
			if restored {
				return
			}
			fresh, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := change.Restore(fresh); err != nil {
				t.Errorf("restore owned MDS cache configuration: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
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
			for _, detail := range h.Checks[code].Detail {
				if strings.Contains(detail.Message, "mds."+bBefore.Active[0].Name+"(") {
					return false, false, fmt.Errorf("target B has relevant warning: %s", detail.Message)
				}
				if strings.Contains(detail.Message, "mds."+aBefore.Active[0].Name+"(") {
					found = true
				}
			}
		}
		return found, found, nil
	})
	if err != nil {
		t.Fatalf("native warning on A alone not established: %v health=%s", err, warning)
	}
	// This single public call includes the unmodified preparatory fs fail.
	started := time.Now()
	removeErr := cephfs.Remove(ctx, cluster, b.FilesystemName)
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
	if removeErr != nil {
		var native *ceph.NativeFailure
		if !strings.Contains(failure, `fail cephfs "warning-b"`) || !errors.As(removeErr, &native) || native.Errno != "EPERM" {
			t.Fatalf("refusal was not an EPERM at preparatory fs fail: %v", removeErr)
		}
	}
	if err := change.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored = true
	afterConfig, err := readConfig(ctx)
	if err != nil || afterConfig != prior {
		t.Fatalf("cache configuration not restored: prior=%s current=%s err=%v", prior, afterConfig, err)
	}
	readA, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", cephFSIndependentReader, a.FilesystemName, "/qualification"}, Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "independent survivor reader"})
	var data struct {
		Client uint64 `json:"client"`
		Inode  uint64 `json:"inode"`
		Size   int    `json:"size"`
		Data   string `json:"data"`
	}
	if err != nil || readA.ExitCode != 0 || !readA.Complete() || json.Unmarshal([]byte(lastJSONLine(string(readA.Stdout))), &data) != nil || data.Client == 0 || data.Inode == 0 || data.Size != len(seed) || data.Data != hex.EncodeToString(seed) {
		t.Fatalf("survivor data: %+v exit=%d err=%v stderr=%s", data, readA.ExitCode, err, readA.Stderr)
	}
	aAfter := cephFSTopologyStatus(t, ctx, a)
	if aAfter.FilesystemID != aBefore.FilesystemID || len(aAfter.Active) != 1 || aAfter.Active[0].GID != aBefore.Active[0].GID || aAfter.Active[0].Name != aBefore.Active[0].Name {
		t.Fatalf("A identity changed: %+v -> %+v", aBefore, aAfter)
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
	if result == "refused" {
		bAfter := cephFSTopologyStatus(t, ctx, b)
		if len(bAfter.Active) != 1 || bAfter.FilesystemID != bBefore.FilesystemID || bAfter.Active[0].GID != bBefore.Active[0].GID || !slices.Contains(names, b.MetadataPool) || !slices.Contains(names, b.DataPool) {
			t.Fatalf("refused target state changed: %+v pools=%s", bAfter, pools)
		}
	}
	receipt, _ := json.Marshal(map[string]any{"expected": expect, "observed": result, "selection": "ordinary/no-tag", "versions": json.RawMessage(versions), "warning": warning, "a_before": aBefore, "a_after": aAfter, "b_before": bBefore, "filesystem_list": json.RawMessage(fsList), "pool_list": json.RawMessage(pools), "config_prior": prior, "config_restored": afterConfig, "survivor_read": data, "remove_started": started, "remove_ended": ended, "ordinary_error": failure, "role_images": map[string]string{"essential": os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), "client": os.Getenv("CEPH_TEST_CLIENT_IMAGE"), "cephfs": os.Getenv("CEPH_TEST_CEPHFS_IMAGE")}})
	t.Logf("CEPHFS_REMOVAL_WARNING_RECEIPT %s", receipt)
}
