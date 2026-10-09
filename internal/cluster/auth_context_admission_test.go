package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestClientOwnerAdmissionQueuedDeadline(t *testing.T) {
	for _, name := range []string{"create", "delete", "capabilities", "update caps"} {
		t.Run(name, func(t *testing.T) {
			control := &authFixtureContainer{}
			cluster := authFixtureCluster(control)
			client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
			if err != nil {
				t.Fatal(err)
			}
			config, keyring := bytes.Clone(client.config), bytes.Clone(client.keyring)
			before := len(control.calls)
			var created *ClientConfig
			operation := func(ctx context.Context) error {
				switch name {
				case "create":
					created, err = cluster.CreateClient(ctx, "writer", ClientCaps{Mon: "allow r"})
				case "delete":
					err = cluster.DeleteClient(ctx, client)
				case "capabilities":
					_, err = cluster.ClientCapabilities(ctx, client)
				case "update caps":
					err = cluster.UpdateClientCaps(ctx, client, ClientCaps{OSD: "allow r pool=tenant"})
				}
				return err
			}
			assertContextLockQueued(t, &cluster.mu, operation)
			if len(control.calls) != before || created != nil || !client.created || !client.ready || client.revoked || !bytes.Equal(config, client.config) || !bytes.Equal(keyring, client.keyring) {
				t.Fatal("queued owner admission changed native auth or the existing credentials")
			}
			if err := operation(t.Context()); err != nil {
				t.Fatalf("fresh context could not use the same auth fixture: %v", err)
			}
			if name == "create" && (created == nil || !created.created || !created.ready) || name == "delete" && !client.revoked {
				t.Fatal("fresh operation did not publish the confirmed native result")
			}
		})
	}
}

func TestClientControlAdmissionQueuedDeadline(t *testing.T) {
	for _, name := range []string{"create", "delete", "capabilities", "update caps"} {
		t.Run(name, func(t *testing.T) {
			control := &authFixtureContainer{}
			cluster := authFixtureCluster(control)
			client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
			if err != nil {
				t.Fatal(err)
			}
			before := len(control.calls)
			var created *ClientConfig
			operation := func(ctx context.Context) error {
				switch name {
				case "create":
					created, err = cluster.CreateClient(ctx, "writer", ClientCaps{Mon: "allow r"})
				case "delete":
					err = cluster.DeleteClient(ctx, client)
				case "capabilities":
					_, err = cluster.ClientCapabilities(ctx, client)
				case "update caps":
					err = cluster.UpdateClientCaps(ctx, client, ClientCaps{OSD: "allow r pool=tenant"})
				}
				return err
			}
			assertSnapshotReadLockQueued(t, &cluster.controlMu, operation)
			if len(control.calls) != before || created != nil || client.revoked || !client.created || !client.ready {
				t.Fatal("queued control selection performed native auth or lost the active identity")
			}
			if err := operation(t.Context()); err != nil {
				t.Fatalf("fresh context could not use the retained control container: %v", err)
			}
		})
	}
}

func TestCreateClientLateControlAdmissionRetainsPartialIdentity(t *testing.T) {
	control := &clientAdmissionControl{authFixtureContainer: &authFixtureContainer{}}
	cluster := authFixtureCluster(control)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	observed := make(chan struct{})
	hookFailure := make(chan error, 1)
	type result struct {
		client *ClientConfig
		err    error
	}
	done, exited := make(chan result, 1), make(chan struct{})
	var publicationMu sync.Mutex
	var publicationOnce, cleanupOnce sync.Once
	held, disabled := false, false
	cleanup := func() {
		cleanupOnce.Do(func() {
			cancel()
			publicationMu.Lock()
			disabled = true
			if held {
				cluster.controlMu.Unlock()
				held = false
			}
			publicationMu.Unlock()
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Error("late control worker did not exit after fixture release")
			}
		})
	}
	// Install cleanup before launch so a callback arriving after either watchdog
	// cannot publish a leftover gate. Every exit uses this one unlock/join path.
	t.Cleanup(cleanup)
	control.afterRead = func(args []string) {
		if slices.Equal(args, []string{"auth", "ls", "--format", "json"}) {
			publicationOnce.Do(func() {
				publicationMu.Lock()
				defer publicationMu.Unlock()
				if disabled {
					return
				}
				if !cluster.controlMu.TryLock() {
					hookFailure <- errors.New("native readback unexpectedly retained the control cache gate")
					cancel()
					return
				}
				held = true
				close(observed)
			})
		}
	}
	go func() {
		defer close(exited)
		client, err := cluster.CreateClient(ctx, "reader", ClientCaps{Mon: "allow r"})
		done <- result{client, err}
	}()
	var got result
	completed := false
	select {
	case <-observed:
	case got = <-done:
		// The boundary and result may both be pending when this reader resumes.
		select {
		case <-observed:
			completed = true
		default:
			cleanup()
			t.Fatalf("creation returned before late control admission: %v", got.err)
		}
	case err := <-hookFailure:
		cleanup()
		t.Fatal(err)
	case <-time.After(time.Second):
		cleanup()
		t.Fatal("identity listing never reached the late control admission")
	}
	cancel()
	if !completed {
		select {
		case got = <-done:
		case <-time.After(500 * time.Millisecond):
			cleanup()
			t.Fatal("late control admission did not honor cancellation")
		}
	}
	if cluster.controlMu.TryLock() {
		// Restore writer ownership before failing so cleanup still unlocks a
		// held gate if a regression incorrectly released the fixture's writer.
		t.Fatal("late control admission stole or unlocked the busy cache gate")
	}
	cleanup()
	if !errors.Is(got.err, context.Canceled) || got.client == nil || got.client.owner != cluster || got.client.name != "client.reader" || got.client.created || got.client.ready || len(got.client.config) == 0 || len(got.client.keyring) != 0 || len(control.calls) != 1 || len(control.copied) != 0 {
		t.Fatal("late control cancellation lost the partial identity or attempted credential creation")
	}
	control.afterRead = nil
	fresh, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
	if err != nil || fresh == nil || !fresh.created || !fresh.ready {
		t.Fatalf("fresh context could not create the same still-unused entity: %v", err)
	}
}

func TestClientNativeContextErrorsRemainCausalAndSecretSafe(t *testing.T) {
	for _, stage := range []string{"key generation", "keyring copy", "auth add"} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			t.Run(stage+"/"+cause.Error(), func(t *testing.T) {
				control := &clientAdmissionControl{authFixtureContainer: &authFixtureContainer{}}
				cluster := authFixtureCluster(control)
				native := errors.Join(errors.New("PRIVATE-KEY native backend failure"), cause)
				if stage == "keyring copy" {
					control.copyError = func(context.Context) error { return native }
				} else {
					control.execError = func(_ context.Context, args []string) error {
						if stage == "key generation" && args[0] == "ceph-authtool" || stage == "auth add" && len(args) >= 2 && args[0] == "auth" && args[1] == "add" {
							return native
						}
						return nil
					}
				}
				client, err := cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
				if client == nil || client.created || client.ready || !errors.Is(err, cause) || strings.Contains(err.Error(), "PRIVATE-KEY") || strings.Contains(err.Error(), "native backend") {
					t.Fatal("native failure lost its canonical context cause, partial handle or secret masking")
				}
				if stage == "key generation" && len(client.keyring) != 0 || stage != "key generation" && len(client.keyring) == 0 {
					t.Fatal("failed credential stage did not preserve exactly the generated private state")
				}
				for _, call := range control.calls {
					if len(call) >= 2 && call[0] == "auth" && (call[1] == "del" || call[1] == "caps") {
						t.Fatal("partial creation revoked or changed an identity")
					}
				}
				if stage != "key generation" && !clientAdmissionCleanupSeen(control.authFixtureContainer) {
					t.Fatal("failed native credential operation abandoned temporary keyring cleanup")
				}
				control.execError, control.copyError = nil, nil
				fresh, err := cluster.CreateClient(t.Context(), "fresh", ClientCaps{Mon: "allow r"})
				if err != nil || fresh == nil || !fresh.created || !fresh.ready {
					t.Fatalf("fresh operation could not reuse the retained fixture: %v", err)
				}
			})
		}
	}
}

func TestClientKeyOperationsPreserveActualCallerCancellation(t *testing.T) {
	for _, stage := range []string{"key generation", "keyring copy"} {
		t.Run(stage, func(t *testing.T) {
			control := &clientAdmissionControl{authFixtureContainer: &authFixtureContainer{}}
			cluster := authFixtureCluster(control)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if stage == "keyring copy" {
				control.copyError = func(context.Context) error {
					cancel()
					return errors.New("PRIVATE-KEY native failure")
				}
			} else {
				control.execError = func(_ context.Context, args []string) error {
					if args[0] == "ceph-authtool" {
						cancel()
						return errors.New("PRIVATE-KEY native failure")
					}
					return nil
				}
			}
			client, err := cluster.CreateClient(ctx, "reader", ClientCaps{Mon: "allow r"})
			if !errors.Is(err, context.Canceled) || client == nil || client.created || client.ready || strings.Contains(err.Error(), "PRIVATE-KEY") {
				t.Fatal("actual caller cancellation lost cause, attempted identity or secret masking")
			}
		})
	}
}

// A caller can become canceled while an error is being sanitized. The native
// canonical cause must survive even when the first context snapshot preceded
// that cancellation; a second context read must not discard the sole cause.
func TestClientErrorCancellationTransitionPreservesNativeCause(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &clientErrorCancellationContext{Context: parent, cancel: cancel}
	native := errors.Join(errors.New("PRIVATE-KEY native backend failure"), context.Canceled)
	err := clientOperationError(ctx, "native auth operation failed", native)
	if !errors.Is(ctx.Err(), context.Canceled) || !errors.Is(err, context.Canceled) ||
		strings.Contains(err.Error(), "PRIVATE-KEY") || strings.Contains(err.Error(), "native backend") {
		t.Fatal("cancellation during secret masking discarded the sole canonical native cause")
	}
}

type clientErrorCancellationContext struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (ctx *clientErrorCancellationContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(ctx.cancel)
	return err
}

func TestClientConfirmedNativePublicationSurvivesCancellation(t *testing.T) {
	for _, stage := range []string{"auth add", "auth get", "auth del", "auth caps"} {
		t.Run(stage, func(t *testing.T) {
			control := &clientAdmissionControl{authFixtureContainer: &authFixtureContainer{}}
			cluster := authFixtureCluster(control)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var client *ClientConfig
			var err error
			if stage == "auth del" || stage == "auth caps" {
				client, err = cluster.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"})
				if err != nil {
					t.Fatal(err)
				}
			}
			control.afterRead = func(args []string) {
				if len(args) >= 2 && strings.Join(args[:2], " ") == stage {
					cancel()
				}
			}
			switch stage {
			case "auth add", "auth get":
				client, err = cluster.CreateClient(ctx, "reader", ClientCaps{Mon: "allow r"})
			case "auth del":
				err = cluster.DeleteClient(ctx, client)
			case "auth caps":
				err = cluster.UpdateClientCaps(ctx, client, ClientCaps{OSD: "allow r pool=tenant"})
			}
			if client == nil || !client.created || ctx.Err() != context.Canceled {
				t.Fatal("native success did not retain its confirmed owned identity")
			}
			if stage == "auth add" {
				if !errors.Is(err, context.Canceled) || client.ready {
					t.Fatal("canceled keyring readback lost the confirmed creation or falsely marked credentials ready")
				}
			} else if err != nil || !client.ready {
				t.Fatalf("confirmed native completion was discarded after cancellation: %v", err)
			}
			if stage == "auth del" && !client.revoked || stage != "auth del" && client.revoked {
				t.Fatal("revocation state disagrees with confirmed native deletion")
			}
			if (stage == "auth add" || stage == "auth get") && !clientAdmissionCleanupSeen(control.authFixtureContainer) {
				t.Fatal("cancellation abandoned cleanup of the generated temporary keyring")
			}
			control.afterRead = nil
			if stage == "auth caps" {
				caps, err := cluster.ClientCapabilities(t.Context(), client)
				if err != nil || caps != (ClientCaps{OSD: "allow r pool=tenant"}) {
					t.Fatal("fresh capability readback did not preserve the confirmed replacement")
				}
			} else {
				before := len(control.calls)
				if err := cluster.DeleteClient(t.Context(), client); err != nil || !client.revoked {
					t.Fatalf("fresh context could not revoke the retained confirmed identity: %v", err)
				}
				if stage == "auth del" && len(control.calls) != before {
					t.Fatal("already confirmed deletion was not a fresh-context no-op")
				}
			}
		})
	}
}

func clientAdmissionCleanupSeen(control *authFixtureContainer) bool {
	for _, call := range control.calls {
		if len(call) == 3 && call[0] == "rm" && call[1] == "-f" && call[2] == control.copyPath {
			return true
		}
	}
	return false
}

type clientAdmissionControl struct {
	*authFixtureContainer
	execError func(context.Context, []string) error
	copyError func(context.Context) error
	afterRead func([]string)
}

func (control *clientAdmissionControl) CopyToContainer(ctx context.Context, content []byte, path string, mode int64) error {
	if err := control.authFixtureContainer.CopyToContainer(ctx, content, path, mode); err != nil {
		return err
	}
	if control.copyError != nil {
		return control.copyError(ctx)
	}
	return nil
}

func (control *clientAdmissionControl) Exec(ctx context.Context, args []string, options ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := control.authFixtureContainer.Exec(ctx, args, options...)
	if args[0] == "ceph" {
		args = args[3:]
	}
	args = slices.Clone(args)
	if control.execError != nil {
		if err := control.execError(ctx, args); err != nil {
			return code, reader, err
		}
	}
	if control.afterRead == nil || err != nil {
		return code, reader, err
	}
	return code, &clientAdmissionReader{Reader: reader, after: func() { control.afterRead(args) }}, err
}

type clientAdmissionReader struct {
	io.Reader
	after func()
	once  sync.Once
}

func (reader *clientAdmissionReader) Read(target []byte) (int, error) {
	n, err := reader.Reader.Read(target)
	if err == io.EOF {
		reader.once.Do(reader.after)
	}
	return n, err
}
