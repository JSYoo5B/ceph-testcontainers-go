package ceph

import (
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
)

func subvolumeFixture() (*CephFSContainer, *poolFixtureContainer) {
	ctr := &poolFixtureContainer{output: map[string]string{
		"mgr module ls --format json":                                       `{"enabled_modules":["volumes"],"always_on_modules":[]}`,
		"fs volume ls --format json":                                        `[{"name":"fixture"}]`,
		"fs dump --format json":                                             `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{}}}],"standbys":[]}`,
		"osd pool ls detail --format json":                                  `[{"pool_id":1,"pool_name":"metadata","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":2,"pool_name":"data","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":3,"pool_name":"additional","type":1,"size":2,"min_size":1,"pg_num":8}]`,
		"fs subvolumegroup info fixture group --format json":                `{"bytes_quota":65536,"bytes_used":0,"data_pool":"additional","created_at":"2026-10-03 00:00:00.123456"}`,
		"fs subvolumegroup getpath fixture group":                           "/volumes/group\n",
		"fs subvolume info fixture volume --group_name group --format json": `{"path":"/volumes/group/volume/unique-id","bytes_quota":32768,"bytes_used":8192,"data_pool":"additional","pool_namespace":"fsvolumens_group_volume","created_at":"2026-10-03 00:00:01.123456","state":"complete","type":"subvolume"}`,
	}}
	cluster := poolFixtureCluster(ctr, 2)
	fs := &CephFSContainer{FilesystemName: "fixture", MetadataPool: "metadata", DataPool: "data", AdditionalDataPools: []string{"additional"}, cluster: cluster,
		config:         CephFSConfig{Name: "fixture", MetadataPool: PoolConfig{Name: "metadata"}, DataPool: PoolConfig{Name: "data"}, AdditionalDataPools: []PoolConfig{{Name: "additional"}}},
		nativeIdentity: &cephFSNativeIdentity{id: 41, metadataPool: 1, defaultPool: 2, attachments: make(map[string]*cephFSDataPoolIdentity)}}
	cluster.filesystems = map[string]*CephFSContainer{"fixture": fs}
	return fs, ctr
}

func TestCephFSSubvolumeValidationPreventsCommands(t *testing.T) {
	for _, config := range []CephFSSubvolumeConfig{
		{}, {Name: "_nogroup"}, {Name: ".."}, {Name: "--option"}, {Name: "bad/path"},
		{Name: "name", GroupName: "_nogroup"}, {Name: "name", SizeBytes: -1},
		{Name: "name", DataPool: "--option"}, {Name: strings.Repeat("a", 129)},
	} {
		fs, ctr := subvolumeFixture()
		if subvolume, err := fs.CreateSubvolume(t.Context(), config); err == nil || subvolume != nil || len(ctr.calls) != 0 {
			t.Fatalf("invalid config executed commands: config=%+v handle=%+v error=%v calls=%v", config, subvolume, err, ctr.calls)
		}
	}
	fs, ctr := subvolumeFixture()
	if _, err := fs.CreateSubvolumeGroup(t.Context(), CephFSSubvolumeGroupConfig{Name: "group", SizeBytes: -1}); err == nil || len(ctr.calls) != 0 {
		t.Fatal("negative group quota reached native CLI")
	}
	fs.cluster.closed = true
	if _, err := fs.SubvolumeGroups(t.Context()); err == nil || len(ctr.calls) != 0 {
		t.Fatal("terminated cluster accepted subvolume operation")
	}
}

func TestCephFSSubvolumeValidationSnapshotsConcurrentConfigReplacement(t *testing.T) {
	fs, ctr := subvolumeFixture()
	// The chosen pool exists in every complete configuration, alternating
	// between default and additional placement. Reading separate fields from
	// different revisions could reject it even though neither revision does.
	first := CephFSConfig{Name: "fixture", DataPool: PoolConfig{Name: "other"}, AdditionalDataPools: []PoolConfig{{Name: "selected"}}, ActiveMDS: 1}
	second := CephFSConfig{Name: "fixture", DataPool: PoolConfig{Name: "selected"}, AdditionalDataPools: []PoolConfig{{Name: "other"}}, ActiveMDS: 2}
	fs.cluster.mu.Lock()
	fs.config = first
	fs.cluster.mu.Unlock()
	var writer sync.WaitGroup
	writer.Add(1)
	start := make(chan struct{})
	go func() {
		defer writer.Done()
		<-start
		for i := range 20000 {
			fs.cluster.mu.Lock()
			if i%2 == 0 {
				fs.config = second
			} else {
				fs.config = first
			}
			fs.cluster.mu.Unlock()
		}
	}()
	close(start)
	for range 20000 {
		if err := fs.validateSubvolumeConfig("volume", "group", "selected", 1<<20); err != nil {
			writer.Wait()
			t.Fatalf("validation read an inconsistent configuration: %v", err)
		}
	}
	writer.Wait()
	if len(ctr.calls) != 0 {
		t.Fatal("validation executed native commands")
	}
}

func TestCephFSSubvolumeDuplicatePreflightPreservesResources(t *testing.T) {
	for _, group := range []bool{false, true} {
		fs, ctr := subvolumeFixture()
		if group {
			ctr.output["fs subvolumegroup ls fixture --format json"] = `[{"name":"group"}]`
			if handle, err := fs.CreateSubvolumeGroup(t.Context(), CephFSSubvolumeGroupConfig{Name: "group", SizeBytes: 1}); err == nil || handle != nil {
				t.Fatal("existing group accepted for native idempotent modification")
			}
		} else {
			ctr.output["fs subvolume ls fixture --group_name group --format json"] = `[{"name":"volume"}]`
			if handle, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 1}); err == nil || handle != nil {
				t.Fatal("existing subvolume accepted for native idempotent modification")
			}
		}
		for _, call := range ctr.calls {
			if slices.Contains(call, "create") || slices.Contains(call, "resize") || slices.Contains(call, "rm") {
				t.Fatalf("duplicate preflight mutated existing resource: %v", call)
			}
		}
	}
}

func TestCephFSSubvolumeCreationOwnershipAndPrivateIdentity(t *testing.T) {
	fs, ctr := subvolumeFixture()
	group, err := fs.CreateSubvolumeGroup(t.Context(), CephFSSubvolumeGroupConfig{Name: "group", SizeBytes: 65536, DataPool: "additional"})
	if err != nil || group.Path != "/volumes/group" || !group.identity.ready {
		t.Fatalf("group creation failed: %+v error=%v", group, err)
	}
	subvolume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768, DataPool: "additional", NamespaceIsolated: true})
	if err != nil || !subvolume.identity.ready || subvolume.Path != "/volumes/group/volume/unique-id" {
		t.Fatalf("subvolume creation failed: %+v error=%v", subvolume, err)
	}
	want := []string{"fs", "subvolume", "create", "fixture", "volume", "--group_name", "group", "--size", "32768", "--pool_layout", "additional", "--namespace-isolated"}
	if !slices.ContainsFunc(ctr.calls, func(call []string) bool { return slices.Equal(call, want) }) {
		t.Fatalf("native creation did not preserve requested options: %v", ctr.calls)
	}
	subvolume.Name, subvolume.GroupName, subvolume.Path, subvolume.FilesystemName = "foreign", "foreign", "/foreign", "foreign"
	if err := fs.ResizeSubvolume(t.Context(), subvolume, 0); err != nil {
		t.Fatal(err)
	}
	want = []string{"fs", "subvolume", "resize", "fixture", "volume", "inf", "--group_name", "group", "--no_shrink"}
	if !slices.Equal(ctr.calls[len(ctr.calls)-1], want) {
		t.Fatalf("public descriptor edit redirected mutation: %v", ctr.calls)
	}
	foreignFS, _ := subvolumeFixture()
	before := len(ctr.calls)
	if err := foreignFS.RemoveSubvolume(t.Context(), subvolume); err == nil || len(ctr.calls) != before {
		t.Fatal("foreign filesystem accepted owned handle")
	}
	if err := fs.ResizeSubvolume(t.Context(), subvolume, -1); err == nil || len(ctr.calls) != before {
		t.Fatal("negative resize reached Ceph")
	}
}

func TestCephFSSubvolumeReplacedResourcesRefuseMutation(t *testing.T) {
	for _, changed := range []string{"path", "birth-time", "filesystem-id"} {
		fs, ctr := subvolumeFixture()
		subvolume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
		if err != nil {
			t.Fatal(err)
		}
		switch changed {
		case "path":
			ctr.output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.output["fs subvolume info fixture volume --group_name group --format json"], "unique-id", "replacement-id")
		case "birth-time":
			ctr.output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.output["fs subvolume info fixture volume --group_name group --format json"], "00:00:01.123456", "00:00:02.123456")
		case "filesystem-id":
			ctr.output["fs dump --format json"] = strings.ReplaceAll(ctr.output["fs dump --format json"], `"id":41`, `"id":42`)
		}
		ctr.calls = nil
		if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil {
			t.Fatalf("changed %s accepted for removal", changed)
		}
		for _, call := range ctr.calls {
			if slices.Contains(call, "rm") {
				t.Fatal("replacement was removed")
			}
		}
	}
	fs, ctr := subvolumeFixture()
	group, err := fs.CreateSubvolumeGroup(t.Context(), CephFSSubvolumeGroupConfig{Name: "group", SizeBytes: 65536})
	if err != nil {
		t.Fatal(err)
	}
	ctr.output["fs subvolumegroup info fixture group --format json"] = strings.ReplaceAll(ctr.output["fs subvolumegroup info fixture group --format json"], "00:00:00.123456", "00:00:09.123456")
	if err := fs.ResizeSubvolumeGroup(t.Context(), group, 1); err == nil {
		t.Fatal("replacement group accepted for resize")
	}
}

func TestCephFSSubvolumePartialCreationAndSafeRetryableRemoval(t *testing.T) {
	fs, ctr := subvolumeFixture()
	ctr.fail = "fs subvolume create fixture volume --group_name group --size 32768"
	subvolume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err == nil || subvolume == nil || subvolume.identity.ready {
		t.Fatal("failed native create lost partial handle or marked it ready")
	}
	before := len(ctr.calls)
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil || len(ctr.calls) != before {
		t.Fatal("unconfirmed resource was mutated")
	}
	ctr.fail = ""
	subvolume, err = fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fail = "fs subvolume rm fixture volume --group_name group"
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil || subvolume.identity.removed {
		t.Fatal("failed native removal lost retry handle")
	}
	ctr.fail = ""
	ctr.output["fs subvolume ls fixture --group_name group --format json"] = `[{"name":"volume"}]`
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err != nil {
		t.Fatal(err)
	}
	before = len(ctr.calls)
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err != nil {
		t.Fatal(err)
	}
	for _, call := range ctr.calls[before:] {
		if slices.Contains(call, "rm") {
			t.Fatal("confirmed removal executed twice")
		}
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "--force") || slices.Contains(call, "--retain-snapshots") {
			t.Fatal("safe removal widened native semantics")
		}
	}
}

func TestCephFSSubvolumeCopiedHandlesShareRemovalAndUnknownSuccessConverges(t *testing.T) {
	fs, ctr := subvolumeFixture()
	subvolume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	copyBefore := *subvolume
	ctr.fail = "fs subvolume rm fixture volume --group_name group"
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil {
		t.Fatal("injected unknown removal result was ignored")
	}
	// Native deletion happened, but the command response was lost. Its empty
	// authoritative listing lets a copied handle reconcile without force.
	ctr.fail = ""
	ctr.output["fs subvolume ls fixture --group_name group --format json"] = `[]`
	ctr.calls = nil
	if err := fs.RemoveSubvolume(t.Context(), &copyBefore); err != nil {
		t.Fatal(err)
	}
	if !subvolume.identity.removed || !copyBefore.identity.removed {
		t.Fatal("copied descriptor does not share lifecycle")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "rm") {
			t.Fatal("unknown successful deletion was repeated")
		}
	}
	if err := fs.ResizeSubvolume(t.Context(), subvolume, 65536); err == nil {
		t.Fatal("removed original handle accepted quota mutation")
	}
	group, err := fs.CreateSubvolumeGroup(t.Context(), CephFSSubvolumeGroupConfig{Name: "group", SizeBytes: 65536})
	if err != nil {
		t.Fatal(err)
	}
	copyGroup := *group
	if err := fs.RemoveSubvolumeGroup(t.Context(), group); err != nil {
		t.Fatal(err)
	}
	if err := fs.ResizeSubvolumeGroup(t.Context(), &copyGroup, 1); err == nil {
		t.Fatal("copied removed group accepted mutation")
	}
}

func TestCephFSSubvolumeSnapshotRetainedAndReplacedFSRefuseRemoval(t *testing.T) {
	fs, ctr := subvolumeFixture()
	subvolume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.output["fs subvolume info fixture volume --group_name group --format json"] = `{"state":"snapshot-retained","type":"subvolume","features":["snapshot-retention"]}`
	ctr.calls = nil
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil {
		t.Fatal("retained snapshots accepted as active owned subvolume")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "rm") {
			t.Fatal("snapshot-retained subvolume mutated")
		}
	}
	subvolume.identity.removalAttempted = true
	ctr.output["fs dump --format json"] = strings.ReplaceAll(ctr.output["fs dump --format json"], `"id":41`, `"id":42`)
	if err := fs.RemoveSubvolume(t.Context(), subvolume); err == nil || subvolume.identity.removed {
		t.Fatal("replacement FS listing falsely confirmed old removal")
	}
}

func TestCephFSSubvolumeQuotaDecodeAndNativeNameListing(t *testing.T) {
	for _, test := range []struct {
		raw     string
		want    int64
		invalid bool
	}{
		{`"infinite"`, 0, false}, {`0`, 0, false}, {`9223372036854775807`, 9223372036854775807, false},
		{`null`, 0, true}, {`"64"`, 0, true}, {`-1`, 0, true}, {`9223372036854775808`, 0, true}, {`1.5`, 0, true},
	} {
		quota, err := decodeCephFSVolumeQuota(json.RawMessage(test.raw))
		if (err != nil) != test.invalid || (!test.invalid && quota != test.want) {
			t.Fatalf("quota=%s decoded as %d error=%v", test.raw, quota, err)
		}
	}
	names, err := decodeCephFSVolumeNames([]byte(`[{"name":"z"},{"name":"a"}]`))
	if err != nil || !slices.Equal(names, []string{"a", "z"}) {
		t.Fatalf("listing not sorted: %v error=%v", names, err)
	}
	for _, raw := range []string{`null`, `{}`, `[{}]`, `[{"name":"same"},{"name":"same"}]`} {
		if _, err := decodeCephFSVolumeNames([]byte(raw)); err == nil {
			t.Fatalf("malformed listing accepted: %s", raw)
		}
	}
	if _, err := decodeCephFSVolumeNames([]byte(`[]`)); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSSubvolumeEnablesVolumesAndRejectsUnavailableFilesystem(t *testing.T) {
	fs, ctr := subvolumeFixture()
	ctr.output["mgr module ls --format json"] = `{"enabled_modules":[],"always_on_modules":["status"]}`
	if _, err := fs.SubvolumeGroups(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(ctr.calls, func(call []string) bool { return slices.Equal(call, []string{"mgr", "module", "enable", "volumes"}) }) {
		t.Fatal("missing volumes module was never enabled")
	}
	ctr.calls = nil
	ctr.fail = "mgr module enable volumes"
	if _, err := fs.SubvolumeGroups(t.Context()); err == nil {
		t.Fatal("MGR module failure was ignored")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "subvolumegroup") {
			t.Fatal("volume operations ran without MGR module readiness")
		}
	}
	fs.cluster.closed = true
	_, err := fs.SubvolumeGroups(t.Context())
	if err == nil {
		t.Fatal("closed filesystem accepted")
	}
}
