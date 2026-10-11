//go:build (all || integration) && goceph && (!ci || (ci_sdk && (!ci_batch || ci_batch_goceph)))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"os"
	"runtime"
	"testing"
)

// The orchestrator stays cgo-free. A separately compiled Linux go-ceph probe
// links the native libraries; its module is excluded from the library's deps.
func TestGoCephLinux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Fatal("go-ceph integration must run on Linux; use make goceph-linux")
	}
	clientImage := os.Getenv("CEPH_TEST_GOCEPH_CLIENT_IMAGE")
	if clientImage == "" {
		t.Fatal("CEPH_TEST_GOCEPH_CLIENT_IMAGE must contain the compiled Linux go-ceph probe")
	}
	for _, mode := range []string{"bridge", "host"} {
		t.Run(mode, func(t *testing.T) {
			goCephLinuxClusterPair(t, mode, clientImage)
		})
	}
}
