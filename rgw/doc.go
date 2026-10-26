// Package rgw prepares RADOS Gateway fixtures in disposable Ceph clusters:
// gateways, S3 users, tenants and accounts, quotas, placement targets and
// storage classes, and native TLS. Multisite orchestration across clusters is
// in package rgw/multisite.
//
// Run starts a cluster with at least one gateway. Start adds a gateway to a
// cluster created by ceph.Run or another service package's Run, and
// WithGateways selects gateways for any of those Run functions.
package rgw
