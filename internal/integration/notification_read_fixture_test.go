//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// notificationReadContainer supplies a single file-read outcome to check that
// failed observations cannot establish an empty notification history.
type notificationReadContainer struct {
	testcontainers.Container
	code   int
	reader io.Reader
	err    error
}

func (c notificationReadContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if !slices.Equal(args, []string{"cat", "/tmp/tc-events.log"}) {
		return 0, nil, errors.New("notification reader must preserve the file read exit status")
	}
	return c.code, c.reader, c.err
}

func TestNotificationReadRequiresReadableRetainedLog(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   int
		output string
		err    error
		want   bool
	}{
		{"empty existing log", 0, "", nil, true},
		{"retained event", 0, "ObjectCreated:Put in/a\n", nil, true},
		{"removed log", 1, "No such file or directory", nil, false},
		{"permission", 1, "Permission denied", nil, false},
		{"I/O failure", 1, "Input/output error", nil, false},
		{"transport", 0, "", errors.New("connection failed"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctr := notificationReadContainer{code: tc.code, reader: strings.NewReader(tc.output), err: tc.err}
			events, err := harness.ReadNotificationEvents(t.Context(), ctr)
			if (err == nil) != tc.want || (!tc.want && events != nil) {
				t.Fatalf("read events=%v error=%v, want success=%t", events, err, tc.want)
			}
		})
	}
	ctr := notificationReadContainer{reader: io.MultiReader(strings.NewReader("partial"), errorEventReader{})}
	if events, err := harness.ReadNotificationEvents(t.Context(), ctr); err == nil || events != nil {
		t.Fatalf("stream failure: events=%v error=%v", events, err)
	}
}

type errorEventReader struct{}

func (errorEventReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
