//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

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
func validateLiveQuotaStep(first, current cephFSActorReceipt, limit uint64, base, created []byte, wantErrno int) (cephFSQuotaStep, error) {
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
		step.Samples == 0 || step.ElapsedMS < 0 || step.CreateErrno == nil || *step.CreateErrno != wantErrno {
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
