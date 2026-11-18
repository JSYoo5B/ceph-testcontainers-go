package multisite

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/mutation"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
)

// settleSyncWrite runs before policy readback: observing the desired value does
// not prove that a dispatched writer has finished. topologyMu guards the fence.
func (f *Fixture) settleSyncWrite(ctx context.Context) error { return f.syncWrites.Wait(ctx) }

func (f *Fixture) syncPolicyWrite(ctx context.Context, zone *rgwZoneState, group string, args ...string) ([]byte, error) {
	if err := f.settleSyncWrite(ctx); err != nil {
		return nil, err
	}
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--realm-id", f.RealmID, "--zonegroup-id", f.topology.groupIDs[group], "--zone-id", zone.ID, "--format", "json"}
	var output []byte
	err := mutation.Run(ctx, 2*time.Minute, func(runCtx context.Context) (bool, error) {
		code, reader, err := zone.client.Exec(runCtx, append(argv, args...))
		if err != nil {
			return false, errors.New("RGW sync policy transport response was lost; output is redacted")
		}
		var stdout, stderr bytes.Buffer
		if reader == nil {
			return false, errors.New("RGW sync policy response stream is missing")
		}
		if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
			return false, errors.New("RGW sync policy stream response was lost; output is redacted")
		}
		if code != 0 {
			return true, clusterutil.Quiet(errors.New("RGW sync policy command failed; output is redacted"), servicehook.NativeFailure("radosgw-admin", code, nil, stderr.Bytes()))
		}
		output = stdout.Bytes()
		return true, nil
	})
	if err = f.syncWrites.Record(err); err != nil {
		return nil, err
	}
	return output, nil
}
