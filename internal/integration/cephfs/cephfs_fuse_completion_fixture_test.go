//go:build (all || integration) && ((cephfs_fuse && !ci) || (ci_optional && (!ci_batch || ci_batch_cephfs_fuse)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"testing"
)

func TestCephFSFuseOutstandingDiagnostic(t *testing.T) {
	mount := &fuseMount{done: make(chan struct{})}
	start, finish := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(mount.done)
		close(start)
		for {
			select {
			case <-finish:
				return
			default:
				mount.result.ExitCode++
				mount.result.Stderr = []byte("still changing")
			}
		}
	}()
	<-start
	for range 1000 {
		if got := mount.exited(context.Background()); got != "ceph-fuse command is still outstanding" {
			close(finish)
			<-mount.done
			t.Fatalf("unsynchronized completion diagnostic: %s", got)
		}
	}
	close(finish)
	<-mount.done
}
