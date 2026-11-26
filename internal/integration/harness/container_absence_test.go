package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
)

// absenceFailure records an assertion through the same testing.TB entry
// point as the native callers, without failing this test.
type absenceFailure struct{ testing.TB }

func (absenceFailure) Helper()                           {}
func (absenceFailure) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func absenceRejected(err error) (message string) {
	defer func() {
		if failure := recover(); failure != nil {
			message = fmt.Sprint(failure)
		}
	}()
	RequireContainerAbsent(absenceFailure{}, "manager", "fixture-container", err)
	return ""
}

func TestContainerAbsenceRejectsPresentAndFailedInspection(t *testing.T) {
	if message := absenceRejected(nil); !strings.Contains(message, "still exists") {
		t.Fatalf("present container accepted as removed: %q", message)
	}
	for _, err := range []error{context.Canceled, errdefs.ErrPermissionDenied, errors.New("Docker unavailable")} {
		if message := absenceRejected(err); !strings.Contains(message, "removal was not confirmed") {
			t.Fatalf("failed inspection accepted as absence: %v, %q", err, message)
		}
	}
	RequireContainerAbsent(t, "manager", "fixture-container", fmt.Errorf("inspect: %w", errdefs.ErrNotFound))
}
