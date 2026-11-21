package harness

import (
	"testing"

	"github.com/containerd/errdefs"
)

// RequireContainerAbsent takes the error of a Docker inspection of id, the
// container ID recorded before a removal. Only Docker's not-found answer
// shows the removal: an inspectable container was not removed, and any other
// failure observes neither state. Inspect right after the removal returns:
// the cluster's teardown removes every container with its owner label.
func RequireContainerAbsent(t testing.TB, what, id string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Fatalf("%s container %s still exists after its removal", what, id)
	case !errdefs.IsNotFound(err):
		t.Fatalf("%s container %s removal was not confirmed: %v", what, id, err)
	}
}
