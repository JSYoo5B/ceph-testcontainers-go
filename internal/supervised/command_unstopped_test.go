package supervised_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cancelFailingContainer never answers the supervisor and fails every cancel.
// It calls started when the supervisor's exec arrives.
type cancelFailingContainer struct {
	testcontainers.Container
	marker  string
	started func()
}

func (ctr *cancelFailingContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) > 3 && args[2] == supervised.CancelScript {
		ctr.marker = args[3]
		return 3, bytes.NewReader(nil), nil
	}
	if ctr.started != nil {
		ctr.started()
	}
	<-ctx.Done()
	return 0, nil, ctx.Err()
}

func TestCommandReportsAnInvocationItCouldNotStop(t *testing.T) {
	// The deadline passes once the supervisor's exec started, rather than
	// after a wall-clock interval that a slow (race-instrumented) run can
	// spend before Run even sizes the supervisor's timeout.
	ctx := clustertest.NewSignalDeadline(t.Context())
	ctr := &cancelFailingContainer{started: ctx.Expire}
	_, err := supervised.Command(ctx, ctr, "probe", time.Minute, 1024, 1024, "true")
	var notStopped *supervised.NotStoppedError
	if !errors.As(err, &notStopped) || notStopped.Marker == "" || notStopped.Marker != ctr.marker {
		t.Fatalf("a failed cancel returned %v", err)
	}
}
