//go:build all || integration

package cephfs_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/expect"
)

// evaluateMDSs evaluates one MDSStatus read against want. A failed read is
// returned as is, so it stays apart from a mismatch.
func evaluateMDSs(status *cephfs.FilesystemStatus, readErr error, want expect.MDSs) (expect.MDSEvaluation, error) {
	if readErr != nil {
		return expect.MDSEvaluation{}, readErr
	}
	return want.Evaluate(*status)
}

// sameMDSRank is the only active rank still held by the daemon instance of
// holder: a restart or a takeover gives the rank another GID.
func sameMDSRank(holder cephfs.MDSStatus) expect.MDSs {
	return expect.MDSs{Ranks: []expect.MDSRank{{Rank: holder.Rank, GID: holder.GID}}, Exact: true}
}
