package cluster

import (
	"strings"
	"testing"
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
			if err := removalCluster(f).RemovePool(t.Context(), "data"); err != nil {
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
		if err := removalCluster(f).RemovePool(t.Context(), "data"); err != nil {
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
	if err := cluster.RemovePool(t.Context(), "data"); err == nil || !strings.Contains(err.Error(), `remove pool "data"`) {
		t.Fatalf("RemovePool error = %v", err)
	}
	if !f.HasPool("data") || !f.HasRule("tc-data-replicated") || *f.AllowDelete != "false" {
		t.Fatalf("failed removal changed state: %s", f)
	}
	if err := cluster.RemovePool(t.Context(), "data"); err != nil {
		t.Fatal("retry:", err)
	}
	if f.HasPool("data") || f.HasRule("tc-data-replicated") || *f.AllowDelete != "false" {
		t.Fatalf("retry left %s", f)
	}
}

func TestRemovePoolFinishesAfterPartialRemoval(t *testing.T) {
	f := newCephStateFake()
	f.AddFixturePool("wide", true)
	// A previous call deleted the pool and then failed on its rule.
	f.Pools = f.Pools[:1]
	if err := removalCluster(f).RemovePool(t.Context(), "wide"); err != nil {
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
			err := removalCluster(f).RemovePool(t.Context(), test.pool)
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
	override, err := cluster.TemporaryConfig(t.Context(), ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"})
	if override == nil {
		t.Fatalf("TemporaryConfig did not return a tracked handle: %v", err)
	}
	before := f.Snapshot()
	if err := cluster.RemovePool(t.Context(), "data"); err == nil || !strings.Contains(err.Error(), "TemporaryConfig override") {
		t.Fatalf("RemovePool error = %v", err)
	}
	if after := f.Snapshot(); after != before {
		t.Fatalf("refused removal changed state:\n%s\n->\n%s", before, after)
	}
}
