package multicluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/testcontainers/testcontainers-go"
)

// RefreshMonitorConfig refreshes the local bootstrap monitor addresses of this
// link's source/destination setup clients and every owned destination-side
// daemon, including stopped daemons. Only the global mon_host entry changes.
// It preserves their private configuration, credentials, identities and process
// state. It never starts or restarts them.
// Original pool and default/selected namespace policies must still match.
//
// This changes local files only. After refreshing the files, call Rebootstrap
// explicitly to refresh the destination peer's remote source mon_host/key
// attributes. Verify replay readiness and application data separately.
// A partial copy failure retains successful updates; retry with a fresh context.
// Complete cluster topology changes before refreshing; topology mutations and
// external policy/config writers must not race the batch. The operation takes
// at most two minutes, bounded by the caller context, including lock waits.
func (m *RBDMirror) RefreshMonitorConfig(ctx context.Context) error {
	return m.refreshRBDMirrorMonitorConfig(ctx, func(ctx context.Context, cluster *ceph.Container, client testcontainers.Container) error {
		return cluster.RefreshClientMonitorConfig(ctx, client)
	})
}

type rbdMonitorConfigTarget struct {
	name    string
	cluster *ceph.Container
	client  testcontainers.Container
}

// The callback is private so unit tests can verify link ownership/orchestration
// without constructing a Ceph cluster. Production always uses the cluster's
// strict original/native/target FSID and stopped-container archive checks.
func (m *RBDMirror) refreshRBDMirrorMonitorConfig(ctx context.Context, refresh func(context.Context, *ceph.Container, testcontainers.Container) error) error {
	if m == nil {
		return errors.New("RBD mirror fixture is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := lockRGWSyncObservation(ctx, &m.mu); err != nil {
		return err
	}
	defer m.mu.Unlock()
	if m.closed || m.config.Source == nil || m.config.Destination == nil || m.sourceClient == nil || m.destinationClient == nil || m.poolIdentities == nil || m.policyIdentities == nil {
		return errors.New("RBD mirror bootstrap pool and policy identities are not confirmed")
	}
	targets := []rbdMonitorConfigTarget{
		{"source setup client", m.config.Source, m.sourceClient},
		{"destination setup client", m.config.Destination, m.destinationClient},
	}
	for _, daemon := range m.daemons {
		if daemon == nil || daemon.Container == nil {
			return errors.New("owned RBD mirror daemon container is unavailable")
		}
		targets = append(targets, rbdMonitorConfigTarget{"daemon " + daemon.DaemonName, m.config.Destination, daemon.Container})
	}
	// A fresh CLI process in an old setup client may no longer find any monitor
	// after full rolling replacement. Verify policy through the cluster's current
	// control instead of requiring the file being repaired to work beforehand.
	if err := m.checkRBDMonitorRefreshIdentities(ctx); err != nil {
		return err
	}
	var problems []error
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			problems = append(problems, err)
			break
		}
		if err := refresh(ctx, target.cluster, target.client); err != nil {
			problems = append(problems, fmt.Errorf("refresh RBD mirror %s monitor configuration: %w", target.name, err))
		}
	}
	if err := m.checkRBDMonitorRefreshIdentities(ctx); err != nil {
		problems = append(problems, err)
	}
	if err := ctx.Err(); err != nil {
		problems = append(problems, err)
	}
	return errors.Join(problems...)
}

// Called under m.mu. Native reads take neither a cluster topology mutex nor a
// setup-client dependency. Keep the original pool, base and selected namespace
// identity guard even when only local bootstrap files are being refreshed.
func (m *RBDMirror) checkRBDMonitorRefreshIdentities(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, site := range []struct {
		cluster   *ceph.Container
		poolID    int64
		namespace string
		identity  rbdMirrorSitePolicyIdentity
	}{
		{m.config.Source, m.poolIdentities.source, m.config.SourceNamespace, m.policyIdentities.source},
		{m.config.Destination, m.poolIdentities.destination, m.config.DestinationNamespace, m.policyIdentities.destination},
	} {
		control, err := site.cluster.ControlContainerContext(ctx)
		if err != nil {
			return fmt.Errorf("inspect RBD mirror cluster control handle: %w", err)
		}
		if control == nil {
			return errors.New("RBD mirror cluster control container is unavailable")
		}
		if err := readRBDMirrorObservedPool(ctx, control, m.config.Pool, site.poolID); err != nil {
			return err
		}
		base, err := readRBDMirrorObservedPolicy(ctx, control, m.config.Pool, "", cephBefore(site.cluster, 20))
		if err != nil {
			return err
		}
		selected := base
		if site.namespace != "" {
			selected, err = readRBDMirrorObservedPolicy(ctx, control, m.config.Pool, site.namespace, cephBefore(site.cluster, 20))
			if err != nil {
				return err
			}
		}
		if !sameRBDMirrorPolicy(base, site.identity.base) || !sameRBDMirrorPolicy(selected, site.identity.selected) {
			return errors.New("RBD mirror UUID, scope, remote mapping or site identity changed; refusing monitor refresh")
		}
		if err := readRBDMirrorObservedPool(ctx, control, m.config.Pool, site.poolID); err != nil {
			return err
		}
	}
	return ctx.Err()
}
