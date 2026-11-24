package ceph

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func removalCluster(f *cephStateFake) *Container {
	return poolFixtureCluster(f, 3)
}

func TestRemovePoolDeletesFixtureResourcesAndRestoresPermission(t *testing.T) {
	for name, erasure := range map[string]bool{"replicated": false, "erasure": true} {
		t.Run(name, func(t *testing.T) {
			f := newCephStateFake()
			f.AddFixturePool("data", erasure)
			f.AddFixturePool("other", erasure)
			if err := poolHandle(t, t.Context(), removalCluster(f), "data").Remove(t.Context()); err != nil {
				t.Fatal(err)
			}
			if f.HasPool("data") || f.HasRule("tc-data-replicated") || f.HasRule("tc-data-ec") || strings.Contains(strings.Join(f.Profiles, ","), "tc-data-ec") {
				t.Fatalf("fixture resources remain: %s", f)
			}
			if !f.HasPool("other") || !f.HasPool(".mgr") || !f.HasRule("replicated_rule") {
				t.Fatalf("unrelated resources were removed: %s", f)
			}
			if f.AllowDelete != nil {
				t.Fatalf("mon_allow_pool_delete left at %q", *f.AllowDelete)
			}
		})
	}
}

func TestRemovePoolKeepsExistingPermission(t *testing.T) {
	for _, previous := range []string{"true", "false"} {
		f := newCephStateFake()
		f.AddFixturePool("data", false)
		f.AllowDelete = &previous
		if err := poolHandle(t, t.Context(), removalCluster(f), "data").Remove(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.HasPool("data") || f.AllowDelete == nil || *f.AllowDelete != previous {
			t.Fatalf("previous %q: %s", previous, f)
		}
	}
}

func TestRemovePoolRestoresPermissionWhenDeletionFails(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("data", false)
	previous := "false"
	f.AllowDelete = &previous
	f.FailNext("osd pool rm data data --yes-i-really-really-mean-it", 1)
	cluster := removalCluster(f)
	if err := poolHandle(t, t.Context(), cluster, "data").Remove(t.Context()); err == nil || !strings.Contains(err.Error(), `remove pool "data"`) {
		t.Fatalf("RemovePool error = %v", err)
	}
	if !f.HasPool("data") || !f.HasRule("tc-data-replicated") || *f.AllowDelete != "false" {
		t.Fatalf("failed removal changed state: %s", f)
	}
	if err := poolHandle(t, t.Context(), cluster, "data").Remove(t.Context()); err != nil {
		t.Fatal("retry:", err)
	}
	if f.HasPool("data") || f.HasRule("tc-data-replicated") || *f.AllowDelete != "false" {
		t.Fatalf("retry left %s", f)
	}
}

type poolPermissionRestoreFailure struct {
	*cephStateFake
	restoreCommand string
}

func (f *poolPermissionRestoreFailure) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if strings.Join(clustertest.ModuleArgs(args), " ") == f.restoreCommand {
		f.restoreCommand = ""
		return clustertest.Reply(args, 1, []byte("Error EPERM: injected configuration restore failure\n"))
	}
	return f.cephStateFake.Exec(ctx, args, opts...)
}

func TestRemovePoolRetriesPermissionRestoration(t *testing.T) {
	for _, previous := range []string{"absent", "false", "0"} {
		for _, outcome := range []string{"deleted", "still present"} {
			t.Run(previous+"/"+outcome, func(t *testing.T) {
				f := &poolPermissionRestoreFailure{cephStateFake: newCephStateFake()}
				f.AddFixturePool("data", false)
				f.restoreCommand = "config rm mon mon_allow_pool_delete"
				if previous != "absent" {
					value := previous
					f.AllowDelete = &value
					f.restoreCommand = "config set mon mon_allow_pool_delete " + value + " --force"
				}
				if outcome == "still present" {
					f.FailNext("osd pool rm data data --yes-i-really-really-mean-it", 1)
				}
				pool := poolHandle(t, t.Context(), poolFixtureCluster(f, 3), "data")
				if err := pool.Remove(t.Context()); err == nil || !strings.Contains(err.Error(), "restore mon_allow_pool_delete") {
					t.Fatalf("failed restoration = %v", err)
				}
				if f.HasPool("data") != (outcome == "still present") || f.AllowDelete == nil || *f.AllowDelete != "true" {
					t.Fatalf("did not reach the intended partial state: %s", f.Snapshot())
				}
				if err := pool.Remove(t.Context()); err != nil {
					t.Fatal("retry:", err)
				}
				restored := "absent"
				if f.AllowDelete != nil {
					restored = *f.AllowDelete
				}
				if restored != previous || f.HasPool("data") || f.HasRule("tc-data-replicated") {
					t.Fatalf("retry did not remove the fixture and restore %s: %s", previous, f.Snapshot())
				}
			})
		}
	}
}

func TestRemovePoolFinishesAfterPartialRemoval(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("wide", true)
	// A previous call deleted the pool and then failed on its rule.
	f.Pools = f.Pools[:1]
	if err := poolHandle(t, t.Context(), removalCluster(f), "wide").Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.HasRule("tc-wide-ec") || strings.Contains(strings.Join(f.Profiles, ","), "tc-wide-ec") {
		t.Fatalf("leftovers remain: %s", f)
	}
}

func TestRemovePoolRefusesUnownedOrUsedPools(t *testing.T) {
	for name, test := range map[string]struct {
		pool, want string
		prepare    func(*cephStateFake)
	}{
		"ceph pool": {pool: ".mgr", want: "not created by CreatePool"},
		"foreign rule": {pool: "fs-meta", want: "not created by CreatePool", prepare: func(f *cephStateFake) {
			f.Rules[9] = "custom"
			f.Pools = append(f.Pools, FakePool{ID: 9, Name: "fs-meta", Rule: 9})
		}},
		"missing":      {pool: "absent", want: `pool "absent": pool does not exist`},
		"invalid name": {pool: "bad name", want: "invalid native pool name"},
		"cephfs": {pool: "data", want: `used by CephFS filesystem "tc-cephfs"`, prepare: func(f *cephStateFake) {
			f.Filesystems = []FakeFilesystem{{Name: "tc-cephfs", Metadata: "meta", Data: []string{"data"}}}
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCephStateFake()
			f.AddFixturePool("data", false)
			if test.prepare != nil {
				test.prepare(f)
			}
			before := f.Snapshot()
			err := poolHandle(t, t.Context(), removalCluster(f), test.pool).Remove(t.Context())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("RemovePool error = %v, want %q", err, test.want)
			}
			if after := f.Snapshot(); after != before {
				t.Fatalf("refused removal changed state:\n%s\n->\n%s", before, after)
			}
		})
	}
}

func TestRemovePoolRefusesActivePermissionOverride(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("data", false)
	cluster := removalCluster(f)
	// ceph cannot call the override package, so the registry holds the key
	// that override.SetConfig tracks for the setting.
	var log []string
	hold(t, cluster, faultConfigOverride, configOverrideKey(allowPoolDeleteSetting), "", &log)
	before := f.Snapshot()
	if err := poolHandle(t, t.Context(), cluster, "data").Remove(t.Context()); err == nil || !strings.Contains(err.Error(), "override.SetConfig override") {
		t.Fatalf("RemovePool error = %v", err)
	}
	if after := f.Snapshot(); after != before {
		t.Fatalf("refused removal changed state:\n%s\n->\n%s", before, after)
	}
}
