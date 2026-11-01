// Package diagnostics collects read-only evidence from a Ceph fixture for a
// test to save or report: a diagnostics report of the containers, networks,
// log tails and fixed Ceph queries of a cluster ([Collect]), the Ceph crash
// metadata in its daemon containers ([CollectCrashMetadata]), and the
// redaction of the secrets that the cluster knows ([Redact]).
//
// Collection never starts, stops, reconfigures or adopts a container. It
// works on a partially started or terminated cluster and on stopped or
// removed containers, and a failed observation stays in the returned report
// next to the aggregate error. A cluster needs nothing from this package to
// run; package finalize writes these reports into its failure archive.
//
// The error categories are those of package ceph: invalid limits are refused
// with an error that matches ceph.ErrInvalidRequest.
package diagnostics
