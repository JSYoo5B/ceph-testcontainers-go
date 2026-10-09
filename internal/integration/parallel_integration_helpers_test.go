//go:build all || integration

package integration_test

import (
	"os"
	"strconv"
	"sync"
	"testing"
)

var (
	parallelOnce  sync.Once
	parallelSlots chan struct{}
	parallelErr   error
)

// parallelWhenEnabled lets an independent top-level test run alongside other
// opted-in tests when CEPH_TEST_PARALLEL is a limit above one, as on CI runners
// with more memory than the local 4 GiB Docker VM. Without it, tests stay
// sequential. Call it first, before creating any cluster: the slot is held
// until the test's later cleanups, including cluster termination, have run.
// Do not opt in tests that inspect engine-wide Docker inventories or events.
func parallelWhenEnabled(t *testing.T) {
	t.Helper()
	parallelOnce.Do(func() {
		value := os.Getenv("CEPH_TEST_PARALLEL")
		if value == "" {
			return
		}
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 {
			parallelErr = strconv.ErrSyntax
			return
		}
		if limit > 1 {
			parallelSlots = make(chan struct{}, limit)
		}
	})
	if parallelErr != nil {
		t.Fatal("CEPH_TEST_PARALLEL must be a positive integer")
	}
	if parallelSlots == nil {
		return
	}
	t.Parallel()
	parallelSlots <- struct{}{}
	t.Cleanup(func() { <-parallelSlots })
}
