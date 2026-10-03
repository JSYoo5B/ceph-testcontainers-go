// Package ceph provisions disposable Ceph clusters using testcontainers-go.
// It controls Ceph through container-local CLIs without linking host librados.
// Pool policies, Cephx identities, RBD namespaces, CephFS subvolumes and RGW
// users prepare server-side fixtures for consumer native and HTTP clients.
// This is an experimental module for application integration tests.
package ceph
