//go:build all || integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func blockIOExpectedFailure(r ceph.NativeCommandResult) bool {
	if r.TimedOut {
		return true
	}
	if !r.Complete() || r.ExitCode == 0 {
		return false
	}
	stderr := strings.ToLower(string(r.Stderr))
	return strings.Contains(stderr, "input/output error") || strings.Contains(stderr, "connection timed out") ||
		strings.Contains(stderr, "operation timed out")
}

func checkBlockIOLogWindow(logs io.Reader, prefixBytes int64, prefixHash []byte) error {
	digest := sha256.New()
	if _, err := io.CopyN(digest, logs, prefixBytes); err != nil {
		return err
	}
	if !bytes.Equal(digest.Sum(nil), prefixHash) {
		return errors.New("OSD log prefix changed before the fault window could be checked")
	}
	const limit = 4 << 20
	data, err := io.ReadAll(io.LimitReader(logs, limit+1))
	if err != nil {
		return err
	}
	if len(data) > limit {
		return fmt.Errorf("OSD fault-window log exceeds %d bytes", limit)
	}
	text := strings.ToLower(string(data))
	if !strings.Contains(text, "input/output error") && !strings.Contains(text, "i/o error") {
		return errors.New("no OSD I/O error in the EIO fault window")
	}
	return nil
}
