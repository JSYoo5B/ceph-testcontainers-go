package expect_test

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/expect"
)

// A takeover is rank 0 on a GID other than the killed one, and the restarted
// daemon is a standby again under a new GID. Evaluating the observation of a
// takeover that is still in progress names what is missing.
func ExampleMDSs_Evaluate() {
	old := cephfs.MDSStatus{Name: "tc-cephfs-0", Rank: 0, GID: 4100, State: "up:active", Owned: true}
	want := expect.MDSs{
		FilesystemID: 1,
		Ranks:        []expect.MDSRank{{Rank: 0, NotGID: old.GID}},
		Owned:        true,
		Exact:        true,
		Standby:      &expect.MDSStandbys{Count: 1, Daemons: []expect.MDSDaemon{{Name: old.Name, NotGID: old.GID}}},
	}
	// The standby took rank 0; the killed daemon has not registered again.
	later := cephfs.FilesystemStatus{
		FilesystemName: "tc-cephfs", FilesystemID: 1, MaxMDS: 1,
		Active: []cephfs.MDSStatus{{Name: "tc-cephfs-1", Rank: 0, GID: 4200, State: "up:active", Owned: true}},
	}
	result, err := want.Evaluate(later)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// mds want fs=1 ranks [0] owned exact standby=1 in "tc-cephfs" fs=1 max_mds=1 active=1 standby=0 standby-replay=0: standby count: want 1, observed 0; standby mds.tc-cephfs-0 absent
}

// The test owns the polling: a failed query is kept apart from the last
// mismatch, so a timeout reports both instead of an empty "not ready".
// This example is compiled, not run.
func ExampleMDSs_Evaluate_polling() {
	ctx := context.Background()
	cluster, err := cephfs.Run(ctx, ceph.DefaultImage, cephfs.WithFilesystems(cephfs.Config{StandbyMDS: 1}))
	if cluster != nil {
		defer func() { _ = cluster.Terminate(context.Background()) }()
	}
	if err != nil {
		panic(err)
	}
	fs := cephfs.Filesystems(cluster.Container)[0]
	baseline, err := fs.MDSStatus(ctx)
	if err != nil {
		panic(err)
	}
	old := baseline.Active[0]
	// ... stop the active MDS while the SUT runs ...
	want := expect.MDSs{FilesystemID: baseline.FilesystemID, Ranks: []expect.MDSRank{{Rank: 0, NotGID: old.GID}}, Owned: true, Exact: true}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last expect.MDSEvaluation
	var queryErr error
	for {
		status, err := fs.MDSStatus(ctx)
		if err == nil {
			last, err = want.Evaluate(*status)
		}
		if err == nil && last.Matched {
			return
		}
		if err != nil {
			queryErr = err
		}
		select {
		case <-ctx.Done():
			panic(fmt.Sprintf("rank 0 not taken over: last %v, last error %v", last, queryErr))
		case <-time.After(time.Second):
		}
	}
}
