package cluster

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

var _ testcontainers.Container = (*MDSContainer)(nil)

func TestCephFSConfigDefaultsAndIndependentPools(t *testing.T) {
	config, err := normalizeCephFSConfig(CephFSConfig{})
	if err != nil || config.Name != "tc-cephfs" || config.MetadataPool.Name != "tc-cephfs-metadata" || config.DataPool.Name != "tc-cephfs-data" || config.ActiveMDS != 1 || config.StandbyMDS != 0 || config.StandbyReplay {
		t.Fatalf("unexpected default filesystem: %+v error=%v", config, err)
	}
	if cephFSMDSID(config.Name, 0) != "a" || cephFSMDSID("tenant", 0) == "a" {
		t.Fatal("default MDS a compatibility or named filesystem identity lost")
	}
	input := CephFSConfig{Name: "tenant", ActiveMDS: 2, StandbyMDS: 1, StandbyReplay: true,
		AdditionalDataPools: []PoolConfig{{Name: "tenant-ec", ErasureCode: &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}}},
	}
	config, err = normalizeCephFSConfig(input)
	if err != nil || config.MetadataPool.Name != "tenant-metadata" || config.DataPool.Name != "tenant-data" || config.AdditionalDataPools[0].Application != "cephfs" || config.AdditionalDataPools[0].MinSize != 3 {
		t.Fatalf("unexpected named filesystem: %+v error=%v", config, err)
	}
	input.AdditionalDataPools[0].Name = "mutated"
	input.AdditionalDataPools[0].ErasureCode.K = 7
	if config.AdditionalDataPools[0].Name != "tenant-ec" || config.AdditionalDataPools[0].ErasureCode.K != 2 {
		t.Fatal("filesystem configuration aliases caller pool or EC memory")
	}
}

func TestInvalidCephFSConfigDoesNotExecuteCeph(t *testing.T) {
	for _, config := range []CephFSConfig{
		{Name: "--flag"}, {Name: "has space"}, {Name: strings.Repeat("a", 129)},
		{ActiveMDS: -1}, {StandbyMDS: -1}, {StandbyReplay: true},
		{MetadataPool: PoolConfig{ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}},
		{DataPool: PoolConfig{ErasureCode: &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}}},
		{MetadataPool: PoolConfig{Name: "shared"}, DataPool: PoolConfig{Name: "shared"}},
		{DataPool: PoolConfig{Application: "rbd"}},
		{AdditionalDataPools: []PoolConfig{{Name: "--bad"}}},
		{AdditionalDataPools: []PoolConfig{{Name: "tc-cephfs-data"}}},
		{AdditionalDataPools: []PoolConfig{{Name: "ec", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}}},
		{AdditionalDataPools: []PoolConfig{{Name: "valid"}, {Name: "bad", MinSize: 3, Replicas: 2}}},
	} {
		ctr := &poolFixtureContainer{}
		fs, err := poolFixtureCluster(ctr, 3).startCephFS(t.Context(), config)
		if err == nil || fs != nil || len(ctr.calls) != 0 {
			t.Errorf("invalid filesystem mutated Ceph: config=%+v fs=%+v error=%v calls=%v", config, fs, err, ctr.calls)
		}
	}
}

func TestCephFSValidatesAllPoolPlacementsBeforeCreation(t *testing.T) {
	ctr := &poolFixtureContainer{}
	config := CephFSConfig{AdditionalDataPools: []PoolConfig{{Name: "ec", ErasureCode: &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}}}}
	if fs, err := poolFixtureCluster(ctr, 2).startCephFS(t.Context(), config); err == nil || fs != nil || len(ctr.calls) != 0 {
		t.Fatalf("later EC placement failure created earlier replicated pools: fs=%+v error=%v calls=%v", fs, err, ctr.calls)
	}
	cluster := poolFixtureCluster(ctr, 3)
	for _, osd := range cluster.osds {
		osd.placement = OSDConfig{Host: "single-host", Rack: "single-rack", Root: "default"}
	}
	config.AdditionalDataPools[0].FailureDomain = "host"
	if fs, err := cluster.startCephFS(t.Context(), config); err == nil || fs != nil || len(ctr.calls) != 0 {
		t.Fatalf("three OSDs in one host falsely satisfied EC three-host placement: fs=%+v error=%v calls=%v", fs, err, ctr.calls)
	}
}

func TestCephFSUsesClusterPoolDefaultsAndPreservesPartialDescriptor(t *testing.T) {
	ctr := &poolFixtureContainer{output: map[string]string{"fs dump --format json": `{"standbys":[],"filesystems":[]}`}, fail: "osd pool set tc-cephfs-metadata min_size 2"}
	cluster := poolFixtureCluster(ctr, 3)
	cluster.settings.poolReplicas, cluster.settings.poolMinSize = 3, 2
	fs, err := cluster.startCephFS(t.Context(), CephFSConfig{})
	if err == nil || fs == nil || fs.config.MetadataPool.Replicas != 3 || fs.config.MetadataPool.MinSize != 2 || fs.config.DataPool.Replicas != 3 || fs.config.DataPool.MinSize != 2 {
		t.Fatalf("CephFS ignored cluster defaults or lost attempted resources: fs=%+v error=%v", fs, err)
	}
	owned := cluster.filesystemList()
	if len(owned) != 1 || owned[0] != fs || len(fs.MDSs()) != 0 {
		t.Fatal("partial pool creation lost its owned filesystem descriptor")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "new") || slices.Contains(call, "get-or-create") || slices.Contains(call, "rm") {
			t.Fatalf("partial pool failure continued or deleted resources: %v", call)
		}
	}
}

func TestExistingCephFSDoesNotCreatePoolsOrChangeFilesystem(t *testing.T) {
	ctr := &poolFixtureContainer{output: map[string]string{
		"fs dump --format json": `{"feature_flags":{"enable_multiple":false},"filesystems":[{"id":1,"mdsmap":{"fs_name":"tc-cephfs"}}]}`,
	}}
	fs, err := poolFixtureCluster(ctr, 2).startCephFS(t.Context(), CephFSConfig{})
	if err == nil || fs != nil || len(ctr.calls) != 1 || !slices.Equal(ctr.calls[0], []string{"fs", "dump", "--format", "json"}) {
		t.Fatalf("existing filesystem was mutated: fs=%+v error=%v calls=%v", fs, err, ctr.calls)
	}
}

func TestCephFSSetsReplayAndAffinityOnlyAfterFilesystemCreation(t *testing.T) {
	ctr := &poolFixtureContainer{
		output: map[string]string{"fs dump --format json": `{"standbys":[],"filesystems":[]}`},
		fail:   "fs set tenant refuse_standby_for_another_fs true",
	}
	cluster := poolFixtureCluster(ctr, 2)
	fs, err := cluster.startCephFS(t.Context(), CephFSConfig{Name: "tenant", ActiveMDS: 2, StandbyMDS: 1, StandbyReplay: true})
	if err == nil || fs == nil || len(fs.MDSs()) != 0 {
		t.Fatalf("configuration failure did not preserve filesystem before MDS startup: fs=%+v error=%v", fs, err)
	}
	var fsCommands [][]string
	for _, call := range ctr.calls {
		if call[0] == "fs" && call[1] != "dump" {
			fsCommands = append(fsCommands, call)
		}
		if slices.Contains(call, "get-or-create") {
			t.Fatalf("MDS credentials created before filesystem settings succeeded: %v", call)
		}
	}
	want := [][]string{
		{"fs", "new", "tenant", "tenant-metadata", "tenant-data", "set", "max_mds", "2", "standby_count_wanted", "1"},
		{"fs", "set", "tenant", "allow_standby_replay", "true"},
		{"fs", "set", "tenant", "refuse_standby_for_another_fs", "true"},
	}
	if !slices.EqualFunc(fsCommands, want, func(a, b []string) bool { return slices.Equal(a, b) }) {
		t.Fatalf("unsafe inline fs-new setters or incorrect ordering: got=%v want=%v", fsCommands, want)
	}
}

func TestCephFSMDSStatusSelectsOwnedStandbysAndExactRanks(t *testing.T) {
	data := []byte(`{"standbys":[
		{"name":"tenant-2","rank":-1,"gid":12,"state":"up:standby","join_fscid":7},
		{"name":"outsider","rank":-1,"gid":13,"state":"up:standby","join_fscid":7},
		{"name":"tenant-3","rank":-1,"gid":14,"state":"up:standby","join_fscid":8}
	],"filesystems":[{"id":7,"mdsmap":{"fs_name":"tenant","max_mds":2,"info":{
		"gid_11":{"name":"tenant-1","rank":1,"gid":11,"state":"up:active","join_fscid":7},
		"gid_10":{"name":"tenant-0","rank":0,"gid":10,"state":"up:active","join_fscid":7}
	}}},{"id":8,"mdsmap":{"fs_name":"other","max_mds":1,"info":{}}}]}`)
	var daemons []*MDSContainer
	for i := range 4 {
		daemons = append(daemons, &MDSContainer{ID: cephFSMDSID("tenant", i), FilesystemName: "tenant"})
	}
	status, err := parseCephFSMDSStatus(data, "tenant", daemons)
	if err != nil || status.FilesystemID != 7 || len(status.Active) != 2 || status.Active[0].Name != "tenant-0" || len(status.Standby) != 1 || status.Standby[0].Name != "tenant-2" {
		t.Fatalf("incorrect FSMap membership or rank ordering: %+v error=%v", status, err)
	}
	config := CephFSConfig{ActiveMDS: 2, StandbyMDS: 1}
	if !cephFSMDSReady(status, config) {
		t.Fatal("owned ranks plus standby did not reach readiness")
	}
	status.Active[1].Rank = 0
	if cephFSMDSReady(status, config) {
		t.Fatal("duplicate rank zero falsely satisfied two active ranks")
	}
	status.Active[1].Rank, status.Active[1].Owned = 1, false
	if cephFSMDSReady(status, config) {
		t.Fatal("external daemon falsely satisfied owned topology")
	}
	if _, err := parseCephFSMDSStatus(data, "missing", daemons); err == nil {
		t.Fatal("missing filesystem accepted")
	}
}

func TestCephFSReplayStandbyIsDistinctFromActiveRank(t *testing.T) {
	data := []byte(`{"standbys":[],"filesystems":[{"id":4,"mdsmap":{"fs_name":"hot","max_mds":1,"info":{
		"gid_20":{"name":"hot-0","rank":0,"gid":20,"state":"up:active","join_fscid":4},
		"gid_21":{"name":"hot-1","rank":0,"gid":21,"state":"up:standby-replay","join_fscid":4}
	}}}]}`)
	status, err := parseCephFSMDSStatus(data, "hot", []*MDSContainer{{ID: "hot-0", FilesystemName: "hot"}, {ID: "hot-1", FilesystemName: "hot"}})
	if err != nil || len(status.Active) != 1 || len(status.StandbyReplay) != 1 || len(status.Standby) != 0 {
		t.Fatalf("replay standby confused with active rank: %+v error=%v", status, err)
	}
	config := CephFSConfig{ActiveMDS: 1, StandbyMDS: 1, StandbyReplay: true}
	if !cephFSMDSReady(status, config) {
		t.Fatal("active plus registered replay standby did not satisfy readiness")
	}
	status.Standby, status.StandbyReplay = status.StandbyReplay, nil
	if cephFSMDSReady(status, config) {
		t.Fatal("ordinary standby falsely satisfied replay readiness")
	}
}

func TestCephFSScaleRejectsCountsBeforeNativeCommands(t *testing.T) {
	for _, counts := range [][2]int{{0, 0}, {-1, 0}, {257, 0}, {1, -1}, {1, int(^uint(0) >> 1)}} {
		control := &poolFixtureContainer{}
		fs := &CephFSContainer{cluster: poolFixtureCluster(control, 2)}
		if err := fs.ScaleMDS(t.Context(), counts[0], counts[1]); err == nil || len(control.calls) != 0 {
			t.Fatalf("invalid count executed native commands: counts=%v error=%v calls=%v", counts, err, control.calls)
		}
	}
}

func TestCephFSMDSRetirementRequiresSettledOwnedRanksAndPrefersIdle(t *testing.T) {
	active := MDSStatus{Name: "tenant-0", Rank: 0, GID: 10, Owned: true, State: "up:active"}
	replay := MDSStatus{Name: "tenant-1", Rank: 0, GID: 11, Owned: true, State: "up:standby-replay"}
	idle := MDSStatus{Name: "tenant-2", Rank: -1, GID: 12, Owned: true, State: "up:standby"}
	daemons := []*MDSContainer{{ID: active.Name}, {ID: replay.Name}, {ID: idle.Name}}
	status := &CephFSMDSStatus{MaxMDS: 1, Active: []MDSStatus{active}, StandbyReplay: []MDSStatus{replay}, Standby: []MDSStatus{idle}}
	if got := cephFSMDSRetirementCandidate(status, daemons, 1); got == nil || got.Name != idle.Name {
		t.Fatalf("idle standby was not selected before replay follower: %+v", got)
	}
	status.Standby = nil // A missing daemon may be up:stopping, not yet idle.
	if got := cephFSMDSRetirementCandidate(status, daemons, 1); got != nil {
		t.Fatalf("a retiring/unobserved owned rank allowed premature removal: %+v", got)
	}
	status.Standby = []MDSStatus{idle}
	status.Active[0].Owned = false
	if got := cephFSMDSRetirementCandidate(status, daemons, 1); got != nil {
		t.Fatalf("external active daemon allowed owned topology contraction: %+v", got)
	}
}

func TestCephFSReplayReadinessRejectsRetiredDuplicateAndMissingRanks(t *testing.T) {
	status := &CephFSMDSStatus{MaxMDS: 2,
		Active:        []MDSStatus{{Rank: 0, GID: 1, Owned: true}, {Rank: 1, GID: 2, Owned: true}},
		StandbyReplay: []MDSStatus{{Rank: 0, GID: 3, Owned: true}, {Rank: 1, GID: 4, Owned: true}},
	}
	config := CephFSConfig{ActiveMDS: 2, StandbyMDS: 2, StandbyReplay: true}
	if !cephFSMDSReady(status, config) {
		t.Fatal("valid replay followers did not satisfy readiness")
	}
	for _, invalid := range []MDSStatus{{Rank: 2, GID: 4, Owned: true}, {Rank: 0, GID: 4, Owned: true}, {Rank: 1, GID: 0, Owned: true}, {Rank: 1, GID: 4, Owned: false}} {
		status.StandbyReplay[1] = invalid
		if cephFSMDSReady(status, config) {
			t.Fatalf("invalid replay follower accepted: %+v", invalid)
		}
	}
}

type mdsScaleFixtureContainer struct {
	testcontainers.Container
	id, afterStop  string
	control        *poolFixtureContainer
	stopped        bool
	stops, removes int
	removeErr      error
}

func (ctr *mdsScaleFixtureContainer) GetContainerID() string { return ctr.id }
func (ctr *mdsScaleFixtureContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: !ctr.stopped}, nil
}
func (ctr *mdsScaleFixtureContainer) Stop(context.Context, *time.Duration) error {
	ctr.stops++
	ctr.stopped = true
	if ctr.afterStop != "" {
		ctr.control.output["fs dump --format json"] = ctr.afterStop
	}
	return nil
}
func (ctr *mdsScaleFixtureContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	ctr.removes++
	return ctr.removeErr
}

func TestCephFSScaleDownPreservesFailuresAndRemovesOnlyOwnedStandby(t *testing.T) {
	const before = `{"standbys":[{"name":"tenant-0","gid":20,"rank":-1,"state":"up:standby","join_fscid":7},{"name":"other-1","gid":31,"rank":-1,"state":"up:standby","join_fscid":9}],"filesystems":[{"id":7,"mdsmap":{"fs_name":"tenant","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{"21":{"name":"tenant-1","gid":21,"rank":0,"state":"up:active","join_fscid":7}}}},{"id":9,"mdsmap":{"fs_name":"other","max_mds":1,"info":{"30":{"name":"other-0","gid":30,"rank":0,"state":"up:active","join_fscid":9}}}}]}`
	const after = `{"standbys":[{"name":"other-1","gid":31,"rank":-1,"state":"up:standby","join_fscid":9}],"filesystems":[{"id":7,"mdsmap":{"fs_name":"tenant","max_mds":1,"metadata_pool":1,"data_pools":[2],"info":{"21":{"name":"tenant-1","gid":21,"rank":0,"state":"up:active","join_fscid":7}}}},{"id":9,"mdsmap":{"fs_name":"other","max_mds":1,"info":{"30":{"name":"other-0","gid":30,"rank":0,"state":"up:active","join_fscid":9}}}}]}`
	failure := errors.New("Docker removal uncertain")
	for _, tc := range []struct {
		name      string
		removeErr error
		authFail  bool
		retained  bool
	}{
		{"success", nil, false, false},
		{"already removed", errdefs.ErrNotFound, false, false},
		{"uncertain removal", failure, false, true},
		{"missing plus failed hook", errors.Join(errdefs.ErrNotFound, failure), false, true},
		{"credential cleanup failure", nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := &poolFixtureContainer{output: map[string]string{"fs dump --format json": before, "osd pool ls detail --format json": `[{"pool_id":1,"pool_name":"metadata","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":2,"pool_name":"data","type":1,"size":2,"min_size":1,"pg_num":8}]`}}
			if tc.authFail {
				control.fail = "auth del mds.tenant-0"
			}
			cluster := poolFixtureCluster(control, 2)
			idle := &mdsScaleFixtureContainer{id: "idle", control: control, afterStop: after, removeErr: tc.removeErr}
			active := &mdsScaleFixtureContainer{id: "active"}
			other := &mdsScaleFixtureContainer{id: "other"}
			fs := &CephFSContainer{Container: idle, cluster: cluster, FilesystemName: "tenant", config: CephFSConfig{Name: "tenant", MetadataPool: PoolConfig{Name: "metadata"}, DataPool: PoolConfig{Name: "data"}, ActiveMDS: 1, StandbyMDS: 1}, nextMDSIndex: 2, nativeIdentity: &cephFSNativeIdentity{id: 7, metadataPool: 1, defaultPool: 2},
				mdss: []*MDSContainer{{Container: idle, ID: "tenant-0", FilesystemName: "tenant"}, {Container: active, ID: "tenant-1", FilesystemName: "tenant"}},
			}
			cluster.filesystems = map[string]*CephFSContainer{"tenant": fs}
			cluster.services = map[string]testcontainers.Container{"mds.tenant-0": idle, "mds.tenant-1": active, "mds.other-0": other}
			err := fs.ScaleMDS(t.Context(), 1, 0)
			wantError := tc.retained || tc.authFail
			if (err != nil) != wantError || idle.stops != 1 || idle.removes != 1 || active.stops != 0 || other.stops != 0 || other.removes != 0 || cluster.services["mds.other-0"] != other {
				t.Fatalf("scale-down touched active/other resources or mishandled errors: error=%v idle=%+v active=%+v other=%+v", err, idle, active, other)
			}
			_, retained := cluster.services["mds.tenant-0"]
			if retained != tc.retained || len(fs.MDSs()) != 1+btoi(tc.retained) || fs.config.ActiveMDS != 1 || fs.config.StandbyMDS != 0 {
				t.Fatal("scale-down lost a failed removal or retained an absent container")
			}
			if !tc.retained && fs.Container != active {
				t.Fatal("retired first MDS left the compatibility container pointing at an absent daemon")
			}
			for _, call := range control.calls {
				if slices.Contains(call, "fail") && !slices.Equal(call, []string{"mds", "fail", "20"}) {
					t.Fatalf("retirement failed an active or reusable daemon identity: %v", call)
				}
				if slices.Contains(call, "new") || (slices.Contains(call, "pool") && !slices.Equal(call, []string{"osd", "pool", "ls", "detail", "--format", "json"})) || slices.Contains(call, "rm") {
					t.Fatalf("MDS scale-down modified filesystem storage: %v", call)
				}
			}
		})
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
