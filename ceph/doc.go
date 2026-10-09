// Package ceph provisions disposable Ceph clusters using testcontainers-go.
// It controls Ceph through container-local CLIs without linking host librados.
//
// Run starts the MON, MGR and OSD topology with cluster-wide options: daemon
// counts, OSD storage and placement, networks, pools and Messenger mode. Pool
// policies, Cephx identities, configuration overrides, fault injection and
// health observations operate on the returned Container.
//
// Client services live in their own packages, which add their options to the
// same Run and re-export their service types:
//
//   - github.com/jsyoo5b/ceph-testcontainers-go/rbd: RBD pools, namespaces,
//     mirroring and backup.
//   - github.com/jsyoo5b/ceph-testcontainers-go/cephfs: filesystems, MDS
//     topology, subvolumes and mirroring.
//   - github.com/jsyoo5b/ceph-testcontainers-go/rgw: gateways, users,
//     placement and multisite replication.
//
// Each service package has its own Run that starts a cluster with that
// service's defaults. Options from several service packages can be passed to
// one ceph.Run to start a cluster that serves them together.
//
// This is an experimental module for application integration tests.
package ceph
