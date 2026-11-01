// Package rgw prepares RADOS Gateway fixtures in disposable Ceph clusters:
// gateways, S3 users with tenants, Admin Ops capabilities, quotas and storage
// counters, and native TLS. Accounts are in package rgw/account, placement
// targets and storage classes in package rgw/placement, and multisite
// orchestration across clusters in package rgw/multisite; this package imports
// none of them.
//
// Run starts a cluster with at least one gateway. Start adds a gateway to a
// cluster created by ceph.Run or another service package's Run, and
// WithGateways selects gateways for any of those Run functions.
package rgw
