//go:build all || integration

package cephfs_test

import (
	"errors"
	"fmt"
	"io/fs"

	"github.com/containerd/errdefs"
)

func cephFSRemovedContainerEvidence(err error) error {
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("container deletion was not observed: %v", err)
	}
	return nil
}

func cephFSRemovedMDSEvidence(containerErr, authErr error) error {
	var authProblem error
	if !errors.Is(authErr, fs.ErrNotExist) {
		authProblem = fmt.Errorf("auth key deletion was not observed: %v", authErr)
	}
	return errors.Join(cephFSRemovedContainerEvidence(containerErr), authProblem)
}
