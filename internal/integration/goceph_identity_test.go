//go:build all || (integration && (goceph || !ci || (ci_code && (!ci_batch || ci_batch_pure))))

package integration_test

import (
	"errors"
	"fmt"
	"regexp"
)

// goCephVersionPattern matches a released go-ceph module version.
var goCephVersionPattern = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

// goCephIdentity is what a go-ceph probe receipt says about itself: the
// go-ceph version that the probe was linked with, the cluster and the phase.
type goCephIdentity struct {
	GoCeph string `json:"go_ceph"`
	CephX  bool   `json:"cephx"`
	FSID   string `json:"fsid"`
	Phase  string `json:"phase"`
}

// checkGoCephIdentity compares a receipt with the expected identity. The
// expected go-ceph version must itself be a released version: without one,
// the receipt cannot show which go-ceph performed its operations.
func checkGoCephIdentity(got, want goCephIdentity) error {
	if !goCephVersionPattern.MatchString(want.GoCeph) {
		return fmt.Errorf("no expected go-ceph version: %q", want.GoCeph)
	}
	var problems []error
	if got.GoCeph != want.GoCeph {
		problems = append(problems, fmt.Errorf("go-ceph %q, want %q", got.GoCeph, want.GoCeph))
	}
	if got.FSID != want.FSID {
		problems = append(problems, fmt.Errorf("fsid %q, want %q", got.FSID, want.FSID))
	}
	if got.Phase != want.Phase {
		problems = append(problems, fmt.Errorf("phase %q, want %q", got.Phase, want.Phase))
	}
	if got.CephX != want.CephX {
		problems = append(problems, fmt.Errorf("cephx %t, want %t", got.CephX, want.CephX))
	}
	return errors.Join(problems...)
}
