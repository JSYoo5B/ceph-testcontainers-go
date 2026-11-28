package mdsmap

import (
	"slices"
	"strings"
	"testing"
)

// stoppedTestMap is a settled `fs dump` document whose owned filesystem has
// one active member and the given data_pools list.
func stoppedTestMap(dataPools string) []byte {
	return []byte(`{"standbys":[],"filesystems":[{"id":1,"mdsmap":{"fs_name":"owned","max_mds":1,"standby_count_wanted":0,` +
		`"metadata_pool":2,"data_pools":` + dataPools + `,` +
		`"info":{"gid_10":{"name":"owned-a","gid":10,"rank":0,"state":"up:active","join_fscid":1}},` +
		`"up":{"mds_0":10},"in":[0],"failed":[],"damaged":[],"stopped":[],` +
		`"flags_state":{"joinable":true,"allow_standby_replay":false,"refuse_standby_for_another_fs":true}}}]}`)
}

// The stopped decoder refuses a repeated data pool ID even when the pool
// identity it is given repeats it too, as the replacement decoder does.
func TestDecodeStoppedRefusesADuplicateDataPoolID(t *testing.T) {
	want := func(pools []int64) StoppedExpectation {
		return StoppedExpectation{TargetName: "owned-gone", Filesystem: "owned", Native: &Native{ID: 1, MetadataPool: 2},
			Pools: &Pools{DataPools: pools, Names: map[int64]string{1: "owned-data"}}, ActiveMDS: 1}
	}
	state, err := DecodeStopped(stoppedTestMap(`[1]`), want([]int64{1}))
	if err != nil || !slices.Equal(state.Scope.PoolNames, []string{"owned-data"}) {
		t.Fatalf("baseline decode: %+v %v", state, err)
	}
	if _, err := DecodeStopped(stoppedTestMap(`[1,1]`), want([]int64{1, 1})); err == nil || !strings.Contains(err.Error(), "invalid or duplicated") {
		t.Fatalf("duplicate data pool ID: %v", err)
	}
}
