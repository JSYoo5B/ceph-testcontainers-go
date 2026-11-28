package supervised

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// exitedSupervisor is an exec whose supervisor exited with code and wrote
// nothing.
type exitedSupervisor struct {
	testcontainers.Container
	code int
}

func (c exitedSupervisor) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	return c.code, bytes.NewReader(nil), nil
}

// Run and RunInputWithExecCushion classify every supervisor exit the same
// way, and the same way Unconfirmed and MissingPython read it afterwards: the
// codes around the signal boundary, the supervisor's own exits and the
// shell's exits for a missing interpreter.
func TestRunnersAgreeOnSupervisorExits(t *testing.T) {
	codes := []int{-9, -1, 1, LaunchFailedExit, CleanupFailedExit, LaunchUnavailableExit, PythonCapabilityExit, 125, 126, 127, 128, 129, 137, 255}
	for _, code := range codes {
		wantUnconfirmed := code < 0 || code > 128 || code == CleanupFailedExit
		wantMissing := code == 126 || code == 127 || code == PythonCapabilityExit
		if unconfirmedExit(code) != wantUnconfirmed {
			t.Errorf("unconfirmedExit(%d) = %t", code, !wantUnconfirmed)
		}
		runners := map[string]func() error{
			"Run": func() error {
				_, err := Run(t.Context(), exitedSupervisor{code: code}, "probe", time.Minute, 1024, 1024, "true")
				return err
			},
			"RunInputWithExecCushion": func() error {
				writes, closed, finished := make(chan InputWrite), make(chan struct{}), make(chan struct{})
				_, err := RunInputWithExecCushion(t.Context(), &finishedContainer{noReport: true, code: code, onClose: func() {}}, "probe", time.Minute, ExecCushion, 1024, 1024,
					Input{Limit: 8, Writes: writes, Closed: closed, Finished: finished}, "true")
				return err
			},
		}
		for name, runner := range runners {
			err := runner()
			var exit *ExitError
			if !errors.As(err, &exit) || exit.Code != code || exit.Marker == "" {
				t.Errorf("%s with supervisor exit %d returned %v", name, code, err)
				continue
			}
			if marker, unconfirmed := Unconfirmed(err); unconfirmed != wantUnconfirmed || unconfirmed && marker != exit.Marker {
				t.Errorf("%s with supervisor exit %d: Unconfirmed = %q, %t", name, code, marker, unconfirmed)
			}
			if MissingPython(err) != wantMissing {
				t.Errorf("%s with supervisor exit %d: MissingPython = %t", name, code, !wantMissing)
			}
		}
	}
}
