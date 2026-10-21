//go:build (all || integration) && goceph && (!ci || (ci_sdk && (!ci_batch || ci_batch_goceph)))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs,rbd-aux,rgw

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
	// The harness reads the version from the probe module's go.mod, the one
	// place that pins it; the probe reports the version it was linked with.
	goCeph := os.Getenv("CEPH_TEST_GOCEPH_VERSION")
	if !goCephVersionPattern.MatchString(goCeph) {
		t.Fatalf("CEPH_TEST_GOCEPH_VERSION=%q must name the go-ceph version that the probe module pins", goCeph)
	}
	for _, mode := range []string{"bridge", "host"} {
		t.Run(mode, func(t *testing.T) {
			goCephLinuxClusterPair(t, mode, clientImage, goCeph)
		})
	}
}
