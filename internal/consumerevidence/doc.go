// Package consumerevidence is an external-consumer example of three evidence
// recipes for Messenger client tests, written against the public API of this
// module only: the authentication first-failure bundle, the MGR transition
// receipt and the native MGR keyring gate of a short-ticket profile. It is
// copied, not imported: its schemas and judgments belong to the consumer, and
// the module exports no recipe. TestImportsOnlyPublicPackages keeps it free of
// internal packages, internal/integration/harness included.
//
// Every observation is a native one that the public API already reaches:
// admin socket commands through MonitorContainer.AdminSocket, CLI reads
// through Container.Ceph and Observer, container instances through the
// daemon handles, MON log lines through their container logs, behavioral
// controls through Container.RunNativeCommand and redaction through
// diagnostics.Redact. Each read has its own bound, records its status, and is
// never retried; an incomplete read is never an observed absence.
//
// Command ipcfixture shows the recipes in a fixture process that a test
// drives over standard input and output, as a consumer whose fixture has no
// testing.TB does. Finish ends such a fixture: one bounded diagnostics.Collect
// before an explicit Terminate, keeping the external failure signal.
package consumerevidence
