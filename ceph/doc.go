// Package ceph provisions disposable Ceph clusters using testcontainers-go.
// It controls Ceph through container-local CLIs without linking host librados.
//
// Run starts the MON, MGR and OSD topology with cluster-wide options: daemon
// counts, OSD storage and placement, networks, pools and Messenger mode. Pool
// policies, Cephx identities, configuration overrides, fault injection and
// health observations operate on the returned Container.
//
// Client services live in their own packages, which add their options to the
// same Run and declare their own service types:
//
//   - github.com/JSYoo5B/ceph-testcontainers-go/rbd: RBD pools, namespaces,
//     mirroring and backup.
//   - github.com/JSYoo5B/ceph-testcontainers-go/cephfs: filesystems, MDS
//     topology, subvolumes and mirroring.
//   - github.com/JSYoo5B/ceph-testcontainers-go/rgw: gateways, users,
//     placement and storage classes.
//   - github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite: RGW multisite
//     realms, zones and sync policies across clusters.
//
// Each service package has its own Run that starts a cluster with that
// service's defaults. Options from several service packages can be passed to
// one ceph.Run to start a cluster that serves them together.
//
// These rules hold across the methods of a Container and its handles:
//
//   - Methods are bounded by the caller's context and most also by
//     WithStartupTimeout. Topology changes are serialized, and waiting for a
//     concurrent one honours the context. A call canceled while waiting returns
//     without native changes; one canceled after it sent a native command can
//     leave that command applied.
//   - An operation on one daemon, pool or Cephx identity is a method of its
//     handle (OSDContainer, DetachedOSD, MonitorContainer, ManagerContainer,
//     Pool, ClientConfig) and acts on the instance the handle names. The
//     handle of a removed, detached, replaced or deleted resource, or of a
//     terminated cluster, returns an error. Functions that create handles and
//     cluster-wide operations stay on Container.
//   - A method that creates something can return a non-nil result with an
//     error. The result identifies what was attempted and stays owned by the
//     cluster; inspect it, retry, or terminate the cluster. No method deletes
//     data to roll back a partial failure.
//   - Read methods return the native state at the moment of the call; several
//     reads are not one atomic snapshot, and health, PG statistics and usage
//     are reported asynchronously, so poll with a deadline. Several
//     observations verify the cluster's original FSID and pool IDs.
//   - Ceph has no compare-and-swap for configuration, flags, CRUSH or auth, so
//     do not change the same state with raw commands while a helper applies or
//     restores it.
//   - Temporary changes (package override, package fault and
//     ClientConfig.Revoke) return restoration handles; keep a non-nil handle
//     even with an error. Their shared contract is in
//     docs/guides/contexts-and-cleanup.md.
//
// This is an experimental module for application integration tests.
package ceph
