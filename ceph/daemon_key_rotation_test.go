package ceph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRotateDaemonKeyRefusesBeforeRunningCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit   func(*Container)
		daemon string
		want   string
	}{
		"MON":            {nil, "mon.a", "share the mon. key"},
		"invalid name":   {nil, "osd.01", "must be"},
		"MDS":            {nil, "mds.a", "must be"},
		"unknown OSD":    {nil, "osd.7", "not owned"},
		"purged OSD":     {nil, "osd.1", "not owned"},
		"terminated MGR": {nil, "mgr.b", "not owned"},
		"closed cluster": {func(c *Container) { c.closed = true }, "osd.0", "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			err := c.RotateDaemonKey(t.Context(), test.daemon)
			if err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, control.Calls)
			}
		})
	}
}

// rotationWorld models what a key rotation can leave behind for osd.0: the
// MONs' key and pending key, the key the running daemon authenticates with,
// the daemon's keyring files and the control container's files. Faults make
// one occurrence of an operation fail; before reports an error without effect,
// after applies the effect and then reports an error, as a lost reply does.
type rotationWorld struct {
	current, pending, live    string
	daemonFiles, controlFiles map[string]string
	generated                 int
	counts                    map[string]int
	faults                    map[string]rotationFault
	// onControlCopy runs before the control container stores a file.
	onControlCopy func()
}

type rotationFault struct {
	occurrence int
	after      bool
}

func newRotationWorld() *rotationWorld {
	w := &rotationWorld{current: "key-0", live: "key-0", counts: map[string]int{}, faults: map[string]rotationFault{},
		daemonFiles: map[string]string{}, controlFiles: map[string]string{}}
	for _, path := range []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-0/keyring"} {
		w.daemonFiles[path] = string(daemonKeyring("osd.0", "key-0"))
	}
	return w
}

// fail reports whether this occurrence of op fails, and whether its effect
// still applies.
func (w *rotationWorld) fail(op string) (failed, applied bool) {
	w.counts[op]++
	fault, ok := w.faults[op]
	if !ok || fault.occurrence != w.counts[op] {
		return false, true
	}
	return true, fault.after
}

// accepted reports whether the MONs authenticate key.
func (w *rotationWorld) accepted(key string) bool {
	return key == w.current || (w.pending != "" && key == w.pending)
}

var keyringKey = regexp.MustCompile(`key = (\S+)`)

// requireUsable checks the property a rotation must never break: the running
// daemon and a restarted daemon can both still authenticate.
func (w *rotationWorld) requireUsable(t *testing.T) {
	t.Helper()
	if !w.accepted(w.live) {
		t.Fatalf("the running daemon's key %s is no longer accepted (current %s, pending %q)", w.live, w.current, w.pending)
	}
	for path, content := range w.daemonFiles {
		match := keyringKey.FindStringSubmatch(content)
		if match == nil || !w.accepted(match[1]) {
			t.Fatalf("a restart would read %q from %s, which is not accepted (current %s, pending %q)", content, path, w.current, w.pending)
		}
	}
}

type rotationControl struct {
	testcontainers.Container
	w *rotationWorld
}

func (ctr *rotationControl) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	w := ctr.w
	words := strings.Join(args, " ")
	words = strings.TrimPrefix(words, "ceph --connect-timeout 5 ")
	reply := func(output string) (int, io.Reader, error) { return clustertest.Reply(args, 0, []byte(output)) }
	entry := func() string {
		if w.pending == "" {
			return fmt.Sprintf(`[{"entity":"osd.0","key":%q}]`, w.current)
		}
		return fmt.Sprintf(`[{"entity":"osd.0","key":%q,"pending_key":%q}]`, w.current, w.pending)
	}
	op := strings.Fields(words)
	switch {
	case words == "auth get osd.0 --format json":
		return reply(entry())
	case words == "auth get-or-create-pending osd.0 --format json":
		if w.pending == "" {
			w.generated++
			w.pending = fmt.Sprintf("key-%d", w.generated)
		}
		return reply(entry())
	case words == "auth clear-pending osd.0":
		if failed, applied := w.fail("clear"); failed {
			if applied {
				w.pending = ""
			}
			return 0, nil, errors.New("injected clear-pending failure")
		}
		w.pending = ""
		return reply("")
	case words == "auth commit-pending osd.0":
		failed, applied := w.fail("commit")
		if applied && w.pending != "" {
			w.current, w.pending = w.pending, ""
		}
		if failed {
			return 0, nil, errors.New("injected commit-pending failure")
		}
		return reply("")
	case len(op) == 5 && op[0] == "tell" && op[1] == "osd.0" && op[2] == "rotate-key" && op[3] == "-i":
		failed, applied := w.fail("tell")
		if applied {
			w.live = w.controlFiles[op[4]]
		}
		if failed {
			return 0, nil, errors.New("injected rotate-key failure")
		}
		return reply("")
	case len(op) == 3 && op[0] == "rm" && op[1] == "-f":
		delete(w.controlFiles, op[2])
		return reply("")
	}
	return 0, nil, fmt.Errorf("unexpected command %q", words)
}

func (ctr *rotationControl) CopyToContainer(ctx context.Context, content []byte, path string, _ int64) error {
	if ctr.w.onControlCopy != nil {
		ctr.w.onControlCopy()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if failed, applied := ctr.w.fail("control-copy"); failed {
		if applied {
			ctr.w.controlFiles[path] = string(content)
		}
		return errors.New("injected control copy failure")
	}
	ctr.w.controlFiles[path] = string(content)
	return nil
}

type rotationDaemon struct {
	testcontainers.Container
	w *rotationWorld
}

func (ctr *rotationDaemon) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (ctr *rotationDaemon) CopyToContainer(ctx context.Context, content []byte, path string, _ int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if failed, applied := ctr.w.fail("daemon-write"); failed {
		if applied {
			ctr.w.daemonFiles[path] = string(content)
		}
		return errors.New("injected keyring write failure")
	}
	ctr.w.daemonFiles[path] = string(content)
	return nil
}

func (ctr *rotationDaemon) CopyFileFromContainer(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader([]byte(ctr.w.daemonFiles[path]))), nil
}

func rotationCluster(w *rotationWorld) *Container {
	return &Container{Container: &rotationControl{w: w}, settings: options{startupTimeout: time.Minute},
		osds: map[int]*OSDContainer{0: {ID: 0, Container: &rotationDaemon{w: w}}}}
}

func TestRotateDaemonKeyCompletes(t *testing.T) {
	w := newRotationWorld()
	if err := rotationCluster(w).RotateDaemonKey(t.Context(), "osd.0"); err != nil {
		t.Fatal(err)
	}
	w.requireUsable(t)
	if w.current != "key-1" || w.pending != "" || w.live != "key-1" || len(w.controlFiles) != 0 {
		t.Fatalf("rotation incomplete: %+v", w)
	}
}

// When the old key cannot be written back to every keyring file, the pending
// key must stay: a file may still hold it.
func TestRotateDaemonKeyKeepsPendingKeyWhenRollbackFails(t *testing.T) {
	w := newRotationWorld()
	// Both new keyrings are written, the input copy fails, and so does the
	// first rollback write.
	w.faults["control-copy"] = rotationFault{occurrence: 1}
	w.faults["daemon-write"] = rotationFault{occurrence: 3}
	err := rotationCluster(w).RotateDaemonKey(t.Context(), "osd.0")
	if err == nil || !strings.Contains(err.Error(), "pending key stays") {
		t.Fatalf("incomplete rollback reported as %v", err)
	}
	w.requireUsable(t)
	if w.pending == "" {
		t.Fatal("the pending key was cleared although a keyring file may hold it")
	}
}

// A canceled call still rolls back with its own bounded context.
func TestRotateDaemonKeyRollsBackAfterCancellation(t *testing.T) {
	w := newRotationWorld()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w.onControlCopy = cancel
	if err := rotationCluster(w).RotateDaemonKey(ctx, "osd.0"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled rotation returned %v", err)
	}
	w.requireUsable(t)
	if w.pending != "" || w.current != "key-0" || w.live != "key-0" {
		t.Fatalf("canceled rotation was not rolled back: %+v", w)
	}
	for path, content := range w.daemonFiles {
		if content != string(daemonKeyring("osd.0", "key-0")) {
			t.Fatalf("%s holds %q after the rollback", path, content)
		}
	}
}

// A failed handoff, with or without effect, and a failed commit keep both
// keys; a second call finishes with the same pending key.
func TestRotateDaemonKeyResumesAfterUnconfirmedHandoffOrCommit(t *testing.T) {
	for name, fault := range map[string]struct {
		op    string
		after bool
	}{
		"handoff never reached the daemon": {"tell", false},
		"handoff reply lost":               {"tell", true},
		"commit failed":                    {"commit", false},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults[fault.op] = rotationFault{occurrence: 1, after: fault.after}
			c := rotationCluster(w)
			if err := c.RotateDaemonKey(t.Context(), "osd.0"); err == nil || !strings.Contains(err.Error(), "RotateDaemonKey again") {
				t.Fatalf("failed rotation returned %v", err)
			}
			w.requireUsable(t)
			if w.pending != "key-1" || w.current != "key-0" {
				t.Fatalf("an uncertain rotation changed the accepted keys: %+v", w)
			}
			if err := c.RotateDaemonKey(t.Context(), "osd.0"); err != nil {
				t.Fatal(err)
			}
			w.requireUsable(t)
			if w.current != "key-1" || w.pending != "" || w.live != "key-1" {
				t.Fatalf("the resumed rotation did not finish with the same key: %+v", w)
			}
		})
	}
}

// A resumed call never clears the pending key, which the daemon may hold.
func TestRotateDaemonKeyResumedFailureKeepsPendingKey(t *testing.T) {
	w := newRotationWorld()
	w.faults["tell"] = rotationFault{occurrence: 1, after: true}
	c := rotationCluster(w)
	_ = c.RotateDaemonKey(t.Context(), "osd.0")
	w.faults["control-copy"] = rotationFault{occurrence: 2}
	if err := c.RotateDaemonKey(t.Context(), "osd.0"); err == nil {
		t.Fatal("the failed resumed rotation succeeded")
	}
	w.requireUsable(t)
	if w.pending != "key-1" {
		t.Fatalf("a resumed rotation cleared the pending key: %+v", w)
	}
}
