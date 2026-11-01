package gwbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// AdminCommand runs radosgw-admin in the gateway container, in the gateway's
// configured scope, with --format json. It is called with the owning
// cluster's mutex held. Native output can include secret keys even in errors,
// so it never wraps the original error.
func (c *Core) AdminCommand(ctx context.Context, args ...string) ([]byte, error) {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json"}
	for _, scope := range []struct{ flag, value string }{
		{"--rgw-realm", c.scope.Realm}, {"--rgw-zonegroup", c.scope.Zonegroup}, {"--rgw-zone", c.scope.Zone},
	} {
		if scope.value != "" {
			argv = append(argv, scope.flag, scope.value)
		}
	}
	if c.scope.Realm != "" {
		argv = append(argv, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	limit := adminLimit(ctx)
	report, err := servicehook.RunOwnedShell(ctx, c.cluster, c.Container(), "radosgw-admin", limit, append(argv, args...)...)
	var unstopped *ceph.NativeCommandUnstoppedError
	switch {
	case errors.As(err, &unstopped):
		// Its Stop, or Terminate, confirms the stop later.
		return nil, err
	case err != nil && ctx.Err() != nil:
		return nil, fmt.Errorf("radosgw-admin: the call ended before the command, which was stopped: %w", ctx.Err())
	case err != nil:
		// The command may have run: its outcome is unknown.
		return nil, servicehook.ClassifyExec(ctx, err)
	case report.Stopped:
		return nil, &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "radosgw-admin",
			Err: fmt.Errorf("radosgw-admin still ran after %ds and was stopped", limit)}
	case report.ExitCode == 127:
		// The gateway's sh found no radosgw-admin, which never exits 127 itself.
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: "radosgw-admin", Capability: "radosgw-admin",
			Err: errors.New("the gateway image has no radosgw-admin")}
	case report.ExitCode != 0:
		failure := servicehook.NativeFailure("radosgw-admin", report.ExitCode, report.Stdout, report.Stderr)
		return nil, clusterutil.Quiet(errors.New("radosgw-admin command failed; native output is redacted"), failure)
	}
	return report.Stdout, nil
}

// adminLimit is how long radosgw-admin may run before the gateway's
// supervisor stops it on its own: the call's bound less a margin for stopping
// it and returning, so that a command that runs too long is reported as such
// within the bound. Without a bound, it is 0. A call whose context ends first
// stops the command itself.
func adminLimit(ctx context.Context) int {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	limit := remaining - 15*time.Second
	if limit < remaining/2 {
		limit = remaining / 2
	}
	return max(1, int(limit/time.Second))
}
