// Package diagnostics collects read-only evidence from a Ceph fixture for a
// test to save or report: a diagnostics report of the containers, networks,
// log tails and fixed Ceph queries of a cluster ([Collect]), the Ceph crash
// metadata in its daemon containers ([CollectCrashMetadata]), the log of a
// daemon's latest process ([CollectProcessLog]), the refused authentications
// of a MGR and what the MONs logged about them ([CollectManagerRefusals]),
// and the redaction of the secrets that the cluster knows ([Redact]).
//
// Collection never starts, stops, reconfigures or adopts a container. It
// works on a partially started or terminated cluster and on stopped or
// removed containers. The two reports keep what they could not observe:
// Collect returns the partial report next to the aggregate error, and
// CollectCrashMetadata returns its partial report with a transport, archive
// or cancellation error. The two log reads do not: CollectProcessLog and
// CollectManagerRefusals return their error and no result, so that no log is
// reported as read when it was not; what a read that succeeds covers is its
// LogStatus. A cluster needs nothing from this package to run; package
// finalize writes the reports into its failure archive.
//
// The error categories are those of package ceph: invalid limits are refused
// with an error that matches ceph.ErrInvalidRequest.
package diagnostics
