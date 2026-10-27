package ceph

import (
	"context"
	"sync"

	"github.com/testcontainers/testcontainers-go"
)

// featureClientState is the cache of the lazily started feature client of one
// cluster, the container that ClientContainer returns when the client image
// differs from the essential image. It is the third component of the internal
// state split described in docs/design/container-decomposition.md.
//
// mu and publishMu are the former Container.clientMu and
// Container.clientPublishMu, moved with the fields they always guarded, so the
// split adds no lock.
//
//   - mu serializes the startup and the termination of the client. It is held
//     across the whole startup, which starts a container, and it is the only
//     lock that guards attempted and ready. A caller that holds mu may also
//     read client; a writer holds mu and publishMu.
//   - publishMu guards the publication of the client pointer for readers that
//     must not wait for a startup: snapshot (topology and native-command
//     admission) and the diagnostics read. It is a leaf lock, held only around
//     a pointer read or write.
//
// Order, unchanged by the move: Container.mu, then mu (ClientContainer and
// terminate take them in that order), then publishMu and nativeState.mu, which
// are leaves. Nothing that holds mu takes Container.mu; service callers that
// reach clientContainerContext may already hold Container.mu.
//
// The methods marked "mu held" require the caller to hold mu from a successful
// lock until unlock. The guards in feature_client_state_test.go enforce that,
// that no field is touched outside this type, that publishMu stays a leaf, and
// that the split adds no mutex.
type featureClientState struct {
	mu        sync.Mutex
	publishMu sync.RWMutex
	// client is the started container. A partial start stays published and
	// owned, and is never retried.
	client testcontainers.Container
	// attempted records that a startup began; ready that it succeeded.
	attempted, ready bool
}

// lock waits for mu until ctx ends.
func (f *featureClientState) lock(ctx context.Context) error {
	return lockTopologyMutex(ctx, &f.mu)
}

// unlock releases mu.
func (f *featureClientState) unlock() { f.mu.Unlock() }

// startupAttempted reports whether a startup began. mu held.
func (f *featureClientState) startupAttempted() bool { return f.attempted }

// startupReady reports whether the startup succeeded. mu held.
func (f *featureClientState) startupReady() bool { return f.ready }

// current returns the published client. mu held: every writer holds mu too.
func (f *featureClientState) current() testcontainers.Container { return f.client }

// markAttempted records that a startup began. mu held.
func (f *featureClientState) markAttempted() { f.attempted = true }

// publish makes ctr, also a partial start, the client. mu held.
func (f *featureClientState) publish(ctr testcontainers.Container) {
	f.publishMu.Lock()
	f.client = ctr
	f.publishMu.Unlock()
}

// markReady records the outcome of the startup. mu held.
func (f *featureClientState) markReady(ready bool) { f.ready = ready }

// clear forgets the client after Terminate removed it. attempted stays set, so
// a terminated cluster never starts another one. mu held.
func (f *featureClientState) clear() {
	f.publish(nil)
	f.ready = false
}

// snapshot returns the published client without waiting for a startup; the wait
// for the publication ends with ctx.
func (f *featureClientState) snapshot(ctx context.Context) (testcontainers.Container, error) {
	if err := lockTopologyReadMutex(ctx, &f.publishMu); err != nil {
		return nil, err
	}
	defer f.publishMu.RUnlock()
	return f.client, nil
}

// diagnosticSnapshot is snapshot for diagnostics, which poll instead of
// blocking.
func (f *featureClientState) diagnosticSnapshot(ctx context.Context) (testcontainers.Container, error) {
	if err := diagnosticLock(ctx, f.publishMu.TryRLock); err != nil {
		return nil, err
	}
	defer f.publishMu.RUnlock()
	return f.client, nil
}
