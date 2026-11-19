//go:build all || integration

package integration_test

import "github.com/JSYoo5B/ceph-testcontainers-go/ceph/finalize"

func restartNoDownWarning() finalize.LogExpectation {
	// Docker can deliver the MON's setup warning after activation and can
	// capture it twice. Permit only this message, in either Ceph log envelope.
	const envelope = `^(?:(?:cluster )?\S+ mon\.a \(mon\.\d+\) \d+ : cluster \[WRN\] |\S+ +[0-9a-f]+ +-?\d+ +log_channel\(cluster\) log \[WRN\] : )`
	return finalize.LogExpectation{
		Name:    "restart-nodown-setup",
		Daemon:  "mon.a",
		Event:   finalize.LogClusterWarning,
		Pattern: envelope + `Health check failed: nodown flag\(s\) set \(OSDMAP_FLAGS\)$`,
	}
}
