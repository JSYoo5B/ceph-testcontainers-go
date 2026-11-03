package gwbase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// AdminArgv returns the radosgw-admin command line that addresses scope with
// the cluster configuration and admin keyring of a gateway or CLI client
// container, followed by args. Empty scope fields select the native defaults.
func AdminArgv(scope Scope, args ...string) []string {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring"}
	for _, field := range []struct{ flag, value string }{
		{"--rgw-realm", scope.Realm}, {"--rgw-zonegroup", scope.Zonegroup}, {"--rgw-zone", scope.Zone},
	} {
		if field.value != "" {
			argv = append(argv, field.flag, field.value)
		}
	}
	if scope.Realm != "" {
		argv = append(argv, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	return append(argv, args...)
}

// ValidateAdminArgs refuses caller-supplied radosgw-admin arguments of a
// native escape hatch that are empty, contain control characters, or would
// change the scope or Ceph connection that AdminArgv sets.
func ValidateAdminArgs(args []string) error {
	if len(args) == 0 {
		return servicehook.Invalid(errors.New("radosgw-admin operation is required"))
	}
	for _, arg := range args {
		if strings.IndexFunc(arg, unicode.IsControl) != -1 {
			return servicehook.Invalid(errors.New("radosgw-admin arguments must not contain control characters"))
		}
		flag, _, _ := strings.Cut(strings.ReplaceAll(arg, "_", "-"), "=")
		switch flag {
		case "--rgw-realm", "--rgw-realm-id", "--realm-id", "--realm-name",
			"--rgw-zonegroup", "--rgw-zonegroup-id", "--zonegroup-id", "--zonegroup-name",
			"--rgw-zone", "--rgw-zone-id", "--zone-id", "--zone-name",
			"--conf", "--cluster", "--keyring", "--keyfile", "--key-file", "--key", "--id", "--name", "--client-id", "--mon-host", "--monmap", "--no-config-file":
			return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway scope or Ceph connection"))
		}
		for _, short := range []string{"-c", "-m", "-n", "-k"} {
			if strings.HasPrefix(arg, short) && !strings.HasPrefix(arg, "--") {
				return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway Ceph connection"))
			}
		}
	}
	return nil
}

// RedactAdminFailure hides the native output of a failed radosgw-admin
// command, which can include secret keys, and keeps the classification of err,
// including the *ceph.NativeFailure exit code and errno. Other errors are
// returned unchanged.
func RedactAdminFailure(err error) error {
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) {
		return err
	}
	return clusterutil.Quiet(errors.New("radosgw-admin command failed; native output is redacted"), err)
}

// AdminCommand runs radosgw-admin in the gateway container, in the gateway's
// configured scope, with --format json. It is called with the owning
// cluster's mutex held. Native output can include secret keys even in errors,
// so it never wraps the original error.
func (c *Core) AdminCommand(ctx context.Context, args ...string) ([]byte, error) {
	return c.AdminCommandIn(ctx, c.Container(), args...)
}

// AdmitAdmin validates c as ValidateAdmin does and returns the gateway
// container that a later AdminCommandIn runs in. The caller holds the cluster
// mutex; AdminCommandIn does not need it.
func (c *Core) AdmitAdmin() (testcontainers.Container, error) {
	if err := c.ValidateAdmin(); err != nil {
		return nil, err
	}
	return c.Container(), nil
}

// AdminCommandIn runs radosgw-admin as AdminCommand does, in target, a
// container that AdmitAdmin returned. It does not need the cluster mutex: the
// cluster's native command tracking refuses a terminated cluster and stops the
// command when Terminate starts, and a target removed meanwhile fails the
// command with a classified error.
func (c *Core) AdminCommandIn(ctx context.Context, target testcontainers.Container, args ...string) ([]byte, error) {
	if servicehook.DiagnosticHandle(target) == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is no longer owned by this cluster"))
	}
	limit := adminLimit(ctx)
	argv := AdminArgv(c.scope, append([]string{"--format", "json"}, args...)...)
	report, err := servicehook.RunOwnedShell(ctx, c.cluster, target, "radosgw-admin", limit, argv...)
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
		return nil, RedactAdminFailure(servicehook.NativeFailure("radosgw-admin", report.ExitCode, report.Stdout, report.Stderr))
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
