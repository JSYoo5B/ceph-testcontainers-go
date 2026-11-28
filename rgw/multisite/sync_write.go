package multisite

import (
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// settleSyncWrite runs before policy readback: observing the desired value does
// not prove that a dispatched writer has finished. topologyMu guards the fence.
func (f *Fixture) settleSyncWrite(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return f.syncWrites.Wait(ctx)
}

// syncPolicyWrite runs a radosgw-admin write in zone's CLI client in
// zonegroup group, as gwbase.RunAdmin does, under its own bound after the
// caller cancels: only a reply of the supervisor establishes completion.
func (f *Fixture) syncPolicyWrite(ctx context.Context, zone *rgwZoneState, group string, args ...string) ([]byte, error) {
	if err := f.settleSyncWrite(ctx); err != nil {
		return nil, err
	}
	argv := gwbase.CommandArgs(f.zoneScope(zone, group), args...)
	target := zone.client
	var output []byte
	err := mutation.Run(ctx, 2*time.Minute, func(runCtx context.Context) (bool, error) {
		report, limit, err := gwbase.RunAdmin(runCtx, nil, target, 0, argv)
		if err != nil {
			// The runner's errors carry no native output.
			return false, err
		}
		if report.TimedOut {
			return false, gwbase.AdminReportFailure(report, limit)
		}
		if report.ExitCode != 0 {
			return true, clusterutil.Quiet(errors.New("RGW sync policy command failed; output is redacted"), servicehook.NativeFailure(argv[0], report.ExitCode, nil, report.Stderr))
		}
		output = report.Stdout
		return true, nil
	})
	if err = f.syncWrites.Record(err); err != nil {
		return nil, err
	}
	return output, nil
}
