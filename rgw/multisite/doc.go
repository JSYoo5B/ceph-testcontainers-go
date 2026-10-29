// Package multisite orchestrates RGW multisite fixtures across independent
// Ceph clusters: one realm, zones and zonegroups, period transfer, sync status
// and sync policies.
//
// Run configures a two-zone realm on two existing clusters (WithConfig) or a
// larger topology (WithTopology). It returns a Fixture that owns the gateways
// (started with rgw.Start and described by rgw.Gateway), the per-zone setup
// clients and, in bridge mode, a private HTTP network. The package imports rgw
// for single-gateway use; rgw does not import this package.
package multisite
