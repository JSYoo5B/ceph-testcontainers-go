//go:build all || integration

package cephfs_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The actor owns this process and its otherwise unused ITIMER_REAL. The
// default SIGALRM action terminates even a blocked synchronous native call;
// a Python exception handler would only run after that call returns. Its
// enclosing RunNativeCommand must report and supervise this failure, never
// interpret it as a successful quota observation or retry a create.
const cephFSQuotaObservation = `
import signal, time
def quota_sample_before(sample, deadline, clock):
    remaining = deadline - clock()
    if remaining <= 0:
        raise TimeoutError("quota observation deadline expired before sample")
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError("quota actor real-time timer is already in use")
    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    try:
        signal.setitimer(signal.ITIMER_REAL, remaining)
        value = sample()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
    if clock() >= deadline:
        raise TimeoutError("quota sample completed after the observation deadline")
    return value
def observe_quota(sample, limit, entries=None, timeout=90, clock=time.monotonic, sleep=time.sleep):
    start = clock()
    deadline = start + timeout
    count = 0
    last = None
    while True:
        last = quota_sample_before(sample, deadline, clock)
        count += 1
        observed = clock()
        if observed >= deadline:
            raise TimeoutError("quota observation expired before readiness")
        if (last["limit"] == limit and last["files"] == 1 and last["directories"] > 0
                and last["entries"] == last["files"] + last["directories"]
                and (entries is None or last["entries"] == entries)):
            return dict(last, samples=count, elapsed_ms=int((observed-start)*1000))
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError("quota read-only gate did not observe limit=%r entries=%r: %r" % (limit, entries, last))
        sleep(min(0.2, remaining))
`

type cephFSQuotaStep struct {
	Limit        *uint64 `json:"limit"`
	Files        *uint64 `json:"files"`
	Directories  *uint64 `json:"directories"`
	Entries      *uint64 `json:"entries"`
	Samples      uint64  `json:"samples"`
	ElapsedMS    int64   `json:"elapsed_ms"`
	CreateErrno  *int    `json:"create_errno"`
	CreatedInode uint64  `json:"created_inode"`
	CreatedSize  int64   `json:"created_size"`
	CreatedData  string  `json:"created_data"`
}

// validateLiveQuotaStep binds a create result to the retained process, client,
// file and directory. Missing quota/count/errno evidence cannot satisfy a gate.
func validateLiveQuotaStep(first, current harness.CephFSActorReceipt, limit uint64, base, created []byte, wantErrno int) (cephFSQuotaStep, error) {
	if first.PID <= 0 || first.Client == 0 || first.Inode == 0 || first.DirInode == 0 ||
		current.Token != first.Token || current.PID != first.PID || current.Client != first.Client ||
		current.Inode != first.Inode || current.DirInode != first.DirInode || current.Errno != 0 {
		return cephFSQuotaStep{}, errors.New("quota operation changed client, process, fd or directory identity")
	}
	if current.Size != int64(len(base)) || current.Full != hex.EncodeToString(base) {
		return cephFSQuotaStep{}, errors.New("retained fd lost the acknowledged baseline bytes")
	}
	var step cephFSQuotaStep
	decoder := json.NewDecoder(bytes.NewReader(current.Ops))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&step); err != nil {
		return step, fmt.Errorf("quota operation receipt: %w", err)
	}
	if step.Limit == nil || *step.Limit != limit || step.Files == nil || *step.Files != 1 ||
		step.Directories == nil || *step.Directories == 0 || step.Entries == nil || *step.Entries != *step.Files+*step.Directories ||
		step.Samples == 0 || step.ElapsedMS < 0 || step.ElapsedMS >= 90_000 || step.CreateErrno == nil || *step.CreateErrno != wantErrno {
		return step, fmt.Errorf("quota gate/create result is incomplete or unexpected: %s", current.Ops)
	}
	if wantErrno != 0 {
		if *step.Entries < limit {
			return step, errors.New("denial gate has not reached its recursive entry limit")
		}
		if step.CreatedInode != 0 || step.CreatedSize != 0 || step.CreatedData != "" {
			return step, errors.New("denied create nevertheless reported a created object")
		}
	} else if *step.Entries >= limit || step.CreatedInode == 0 || step.CreatedInode == first.Inode ||
		step.CreatedSize != int64(len(created)) || step.CreatedData != hex.EncodeToString(created) {
		return step, errors.New("relaxed quota did not preserve the independently selected new bytes")
	}
	return step, nil
}
