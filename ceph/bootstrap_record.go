package ceph

import (
	"bytes"
	"context"
	"sync"
)

// bootstrapRecord is the bootstrap record of one cluster: the first MON's
// ceph.conf and admin keyring as read after startup, the Ceph version that
// MON reported, and the read-write lock that guards the configuration and the
// keyring. It is a component of the internal state split described in
// docs/design/container-decomposition.md, the bootstrap-record lock domain of
// the membership and control group.
//
// The lock is the former Container.configMu, moved with the data it guards; no
// mutex was added and no lock order changed. mu is a leaf below Container.mu:
// newOSDLaunchRecipe, currentMonitorBootstrapAddresses, refreshMonitorConfigAfterRemoval
// and the snapshot readers take it while the topology lock is held, and
// diagnosticsSnapshot, the health and pool readers and ClusterIdentity take it
// without it. No method of the type takes another lock or calls into Container,
// so a holder of mu never waits for Container.mu or any other lock.
//
// Each reader and writer keeps the guard it always had. The locked methods
// take mu and return clones, so a caller never aliases the stored bytes. The
// raw methods (rawConfig, rawKeyring, rawVersion, hasConfig, isComplete) and
// the record methods take no lock: their callers hold Container.mu or run
// during Run, before the cluster value is shared, exactly as the former field
// reads did. cephVersion has no lock of its own; it is written once during Run
// and only read afterwards. The guards in bootstrap_record_test.go enforce that
// the fields and the lock are used only by this type and the published lock
// identity, that the fields have the audited production writers, that the
// methods call only their pinned collaborators, and that the split adds no
// mutex.
type bootstrapRecord struct {
	mu          sync.RWMutex
	config      []byte
	keyring     []byte
	cephVersion string
}

// recordConfig stores the configuration read from the first MON. Run calls it
// before the cluster value is shared; it takes no lock.
func (r *bootstrapRecord) recordConfig(config []byte) { r.config = config }

// recordKeyring stores the admin keyring read from the first MON. Run calls it
// before the cluster value is shared; it takes no lock.
func (r *bootstrapRecord) recordKeyring(keyring []byte) { r.keyring = keyring }

// recordVersion stores the Ceph version of the first MON. Run calls it before
// the cluster value is shared; it takes no lock.
func (r *bootstrapRecord) recordVersion(version string) { r.cephVersion = version }

// rawConfig returns the stored configuration without a lock and without a
// copy. The caller holds Container.mu or runs before the cluster is shared.
func (r *bootstrapRecord) rawConfig() []byte { return r.config }

// rawKeyring returns the stored keyring under the same contract as rawConfig.
func (r *bootstrapRecord) rawKeyring() []byte { return r.keyring }

// rawVersion returns the recorded Ceph version without a lock; it is written
// only during Run.
func (r *bootstrapRecord) rawVersion() string { return r.cephVersion }

// hasConfig reports whether a configuration was recorded, without a lock.
func (r *bootstrapRecord) hasConfig() bool { return len(r.config) != 0 }

// isComplete reports whether both the configuration and the keyring were
// recorded, without a lock.
func (r *bootstrapRecord) isComplete() bool { return len(r.config) != 0 && len(r.keyring) != 0 }

// clones returns copies of the configuration and the keyring under the read
// lock.
func (r *bootstrapRecord) clones() (config, keyring []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return bytes.Clone(r.config), bytes.Clone(r.keyring)
}

// templateAndConfirmed returns a copy of the configuration and whether a
// keyring was recorded, under the read lock.
func (r *bootstrapRecord) templateAndConfirmed() (config []byte, confirmed bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return bytes.Clone(r.config), len(r.keyring) != 0
}

// template returns a copy of the configuration under the read lock.
func (r *bootstrapRecord) template() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return bytes.Clone(r.config)
}

// launchStrings returns the configuration and the admin keyring as strings
// under the read lock.
func (r *bootstrapRecord) launchStrings() (config, admin string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return string(r.config), string(r.keyring)
}

// bootstrapIdentity is the cluster identity read from the recorded
// configuration: its FSID, the error of reading it, and whether a keyring was
// recorded. A caller that needs a confirmed identity requires err == nil and
// confirmed.
type bootstrapIdentity struct {
	fsid      string
	err       error
	confirmed bool
}

// identity reads the identity of the recorded configuration, waiting for the
// read lock no longer than ctx allows. The returned error is only ctx's; a
// configuration that cannot be read is in the identity.
func (r *bootstrapRecord) identity(ctx context.Context) (bootstrapIdentity, error) {
	if err := lockTopologyReadMutex(ctx, &r.mu); err != nil {
		return bootstrapIdentity{}, err
	}
	defer r.mu.RUnlock()
	fsid, err := monitorConfigClusterIdentity(r.config)
	return bootstrapIdentity{fsid: fsid, err: err, confirmed: len(r.keyring) != 0}, nil
}

// connection returns copies of the configuration and the keyring, and whether
// both were recorded, waiting for the read lock no longer than ctx allows.
func (r *bootstrapRecord) connection(ctx context.Context) (config, keyring []byte, complete bool, err error) {
	if err := lockTopologyReadMutex(ctx, &r.mu); err != nil {
		return nil, nil, false, err
	}
	defer r.mu.RUnlock()
	return bytes.Clone(r.config), bytes.Clone(r.keyring), len(r.config) != 0 && len(r.keyring) != 0, nil
}

// diagnosticKeyring returns a copy of the keyring without ever blocking past
// ctx, polling the read lock the way the diagnostics snapshot does.
func (r *bootstrapRecord) diagnosticKeyring(ctx context.Context) ([]byte, error) {
	if err := diagnosticLock(ctx, r.mu.TryRLock); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return bytes.Clone(r.keyring), nil
}

// applyDefaultCRUSHRule rewrites the recorded configuration with the default
// CRUSH rule under the write lock and returns the new bytes. A rewrite error
// leaves the record unchanged.
func (r *bootstrapRecord) applyDefaultCRUSHRule(value string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	updated, err := defaultCRUSHRuleConfig(r.config, value)
	if err != nil {
		return nil, err
	}
	r.config = updated
	return updated, nil
}

// publishConfig replaces the recorded configuration under the write lock.
func (r *bootstrapRecord) publishConfig(config []byte) {
	r.mu.Lock()
	r.config = config
	r.mu.Unlock()
}

// mutex is the lock identity that TestState publishes for the lock queueing
// tests of the service packages. Nothing else calls it.
func (r *bootstrapRecord) mutex() *sync.RWMutex { return &r.mu }
