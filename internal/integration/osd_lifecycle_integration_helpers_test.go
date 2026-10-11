//go:build all || (integration && features)

package integration_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type osdRemovalContainerCalls struct {
	testcontainers.Container
	stops, terminations atomic.Int32
}

func (calls *osdRemovalContainerCalls) Stop(ctx context.Context, timeout *time.Duration) error {
	calls.stops.Add(1)
	return calls.Container.Stop(ctx, timeout)
}

func (calls *osdRemovalContainerCalls) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	calls.terminations.Add(1)
	return calls.Container.Terminate(ctx, opts...)
}

var errOSDPurgeReplyLost = errors.New("injected lost native purge reply")

type osdRemovalPurgeReplyFault struct {
	testcontainers.Container
	id        int
	lost      atomic.Bool
	purges    atomic.Int32
	mutations atomic.Int32
}

func (fault *osdRemovalPurgeReplyFault) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	command := strings.Join(args, " ")
	isPurge := strings.Contains(command, " osd purge "+strconv.Itoa(fault.id)+" ")
	if strings.Contains(command, " osd crush reweight ") || strings.Contains(command, " osd out ") || strings.Contains(command, " osd purge ") || strings.Contains(command, " osd create ") || strings.Contains(command, " osd new ") {
		fault.mutations.Add(1)
	}
	code, reader, err := fault.Container.Exec(ctx, args, opts...)
	if isPurge && err == nil && code == 0 {
		fault.purges.Add(1)
		if fault.lost.CompareAndSwap(false, true) {
			return 0, nil, errOSDPurgeReplyLost
		}
	}
	return code, reader, err
}
