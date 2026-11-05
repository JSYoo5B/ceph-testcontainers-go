package mirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// cephFSDaemonRoster is the fixture's daemon membership and the settings
// every new daemon is launched with: the image, the source principal and its
// keyring, the constructor customizers and the optional process observer
// factory. The first daemon it ever adds is the initial one, which backs
// Mirror's embedded container; a later daemon never takes that slot. It is
// guarded by Mirror.mu.
type cephFSDaemonRoster struct {
	image, sourceID      string
	keyring              []byte
	opts                 []testcontainers.ContainerCustomizer
	processClientFactory func(context.Context) (*mobycl.Client, error)
	daemons              []*Daemon
	initial              *Daemon
}

// configured reports whether Run recorded the launch settings.
func (r *cephFSDaemonRoster) configured() bool {
	return r.image != "" && len(r.keyring) != 0
}

// admit refuses an invalid name and a name a member already uses.
func (r *cephFSDaemonRoster) admit(name string) error {
	if err := validateCephFSMirrorDaemonName(name); err != nil {
		return err
	}
	if slices.ContainsFunc(r.daemons, func(d *Daemon) bool { return d.DaemonName == name }) {
		return servicehook.Invalid(fmt.Errorf("CephFS mirror daemon %q already exists", name))
	}
	return nil
}

// customizers returns the customizers of a new daemon: the source client
// configuration, its keyring, the cephfs-mirror process and its admin
// socket, the peer network when the clusters use bridge mode, then the
// constructor customizers and this daemon's own opts.
func (r *cephFSDaemonRoster) customizers(source *ceph.Container, peerNetwork string, opts []testcontainers.ContainerCustomizer) []testcontainers.ContainerCustomizer {
	const socket = "/var/run/ceph/cephfs-mirror.asok"
	all := []testcontainers.ContainerCustomizer{
		source.WithClient(),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(r.keyring), ContainerFilePath: "/etc/ceph/ceph.client." + r.sourceID + ".keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("cephfs-mirror"),
		testcontainers.WithCmd("--id", r.sourceID, "-f", "--admin-socket", socket, "--cephfs-mirror-directory-scan-interval", "1"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", socket}).WithStartupTimeout(2 * time.Minute)),
	}
	if peerNetwork != "" {
		all = append(all, network.WithNetworkName(nil, peerNetwork))
	}
	all = append(all, r.opts...)
	return append(all, opts...)
}

// add records d as a member and reports whether it is the initial daemon.
func (r *cephFSDaemonRoster) add(d *Daemon) bool {
	r.daemons = append(r.daemons, d)
	if r.initial != nil {
		return false
	}
	r.initial = d
	return true
}

// has reports whether d is a member, by identity.
func (r *cephFSDaemonRoster) has(d *Daemon) bool { return slices.Contains(r.daemons, d) }

// remove drops member d and reports whether it was the initial daemon.
func (r *cephFSDaemonRoster) remove(d *Daemon) bool {
	r.daemons = slices.DeleteFunc(r.daemons, func(member *Daemon) bool { return member == d })
	return d == r.initial
}

// clear drops every member once fixture cleanup has removed them.
func (r *cephFSDaemonRoster) clear() { r.daemons = nil }

// sorted returns a copy of the membership sorted by DaemonName.
func (r *cephFSDaemonRoster) sorted() []*Daemon {
	daemons := slices.Clone(r.daemons)
	slices.SortFunc(daemons, func(a, b *Daemon) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return daemons
}

// requireRunning refuses an empty membership and a stopped member.
func (r *cephFSDaemonRoster) requireRunning(operation string) error {
	if len(r.daemons) == 0 {
		return fmt.Errorf("%s requires a running mirror daemon", operation)
	}
	for _, daemon := range r.daemons {
		if !daemon.IsRunning() {
			return fmt.Errorf("%s requires daemon %q to be running; remove stopped members first", operation, daemon.DaemonName)
		}
	}
	return nil
}

// observesProcesses reports whether daemons get a process observer, which a
// removal's process quiescence proof needs.
func (r *cephFSDaemonRoster) observesProcesses() bool { return r.processClientFactory != nil }

// bindProcessObserver binds a process observer to the new daemon when a
// factory is configured. startErr is the error of the daemon's own start: a
// partially started daemon is not bound. A binding failure does not fail the
// start; the daemon's ProcessObserverBindingStatus reports it. It returns the
// cleanup that closes a bound observer, or nil.
func (r *cephFSDaemonRoster) bindProcessObserver(ctx context.Context, daemon *Daemon, container testcontainers.Container, startErr error) func(context.Context) error {
	if r.processClientFactory == nil {
		return nil
	}
	if startErr != nil {
		daemon.processBindingState = "partial-startup"
		return nil
	}
	// Ordinary Run's result survives a binding failure, which is finite
	// observable capability state. The configured factory returns a raw
	// *moby.Client, so TC cached Info cannot be selected as the production
	// identity implementation.
	daemon.processBindingState = "positive-binding-failed"
	bindCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if bindCtx.Err() != nil {
		daemon.processBindingState = "context-unavailable"
		return nil
	}
	raw, err := r.processClientFactory(bindCtx)
	var candidate cephFSOriginalProcessDocker
	if raw != nil {
		candidate = raw
	}
	// Daemon.Remove/direct daemon Terminate deliberately do not close this client.
	if b := bindOriginalProcessCandidate(bindCtx, daemon, container, candidate, err); b != nil {
		return b.close
	}
	return nil
}

func validateCephFSMirrorDaemonName(name string) error {
	if name == "" || len(name) > 63 {
		return servicehook.Invalid(errors.New("CephFS mirror daemon name must contain 1 to 63 letters, digits, dots, underscores or hyphens"))
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-') {
			return servicehook.Invalid(errors.New("CephFS mirror daemon name must contain 1 to 63 letters, digits, dots, underscores or hyphens"))
		}
	}
	return nil
}
