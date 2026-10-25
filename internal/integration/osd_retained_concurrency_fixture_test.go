//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
)

func TestRetainedReopenWaitObservationRequiresContention(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(map[bool]string{false: "free", true: "busy"}[busy], func(t *testing.T) {
			var mutex sync.Mutex
			if busy {
				mutex.Lock()
				defer mutex.Unlock()
			}
			base, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			queued, admitted := false, false
			ctx := &retainedReopenContext{Context: base, waiting: func() { queued = true; cancel() }, acquired: func() { admitted = true }}
			err := clusterutil.LockMutex(ctx, &mutex)
			if busy {
				if !queued || admitted || !errors.Is(err, context.Canceled) {
					t.Fatalf("busy queued=%v admitted=%v err=%v", queued, admitted, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			mutex.Unlock()
			bounded, done := context.WithTimeout(ctx, time.Minute)
			defer done()
			if queued || !admitted || bounded.Err() != nil {
				t.Fatalf("free queued=%v admitted=%v err=%v", queued, admitted, bounded.Err())
			}
		})
	}
}
