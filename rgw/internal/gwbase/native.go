package gwbase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// NativeScope is the exact native scope a running gateway registered in the
// service map: the realm, zonegroup and zone IDs and names. Commands in a
// NativeScope select it by ID, so later changes of the native defaults cannot
// redirect them.
type NativeScope struct {
	RealmID, GroupID, ZoneID, Realm, Group, Zone string
}

// errScopePending reports a running gateway that has not registered its native
// scope in the service map yet.
var errScopePending = errors.New("running RGW gateway has not registered its native scope")

// RuntimeScope reads the native scope that this gateway's daemon registered
// in the service map, waiting while it is not registered yet. It is called
// with the owning cluster's mutex held.
func (c *Core) RuntimeScope(ctx context.Context) (NativeScope, error) {
	data, err := servicehook.Command(ctx, c.Container(), "hostname")
	if err != nil {
		if ctx.Err() != nil {
			return NativeScope{}, fmt.Errorf("inspect RGW container hostname: %w", ctx.Err())
		}
		return NativeScope{}, errors.New("inspect RGW container hostname")
	}
	hostname := strings.TrimSpace(string(data))
	if hostname == "" {
		return NativeScope{}, errors.New("RGW container hostname is empty")
	}
	frontend := "beast port=" + strconv.Itoa(c.port)
	facts, factsErr := ceph.FactsOf(c.cluster)
	if factsErr != nil {
		return NativeScope{}, factsErr
	}
	if facts.UsesHostNetwork {
		frontend = "beast endpoint=" + net.JoinHostPort(c.publicAddress, strconv.Itoa(c.port))
	}
	// Registering to the service map follows starting the HTTP frontend.
	// A successful listener probe can therefore precede the first MON report.
	for {
		data, err = c.cluster.Ceph(ctx, "service", "dump", "--format", "json")
		if err != nil {
			return NativeScope{}, err
		}
		scope, err := decodeRuntimeScope(data, hostname, frontend)
		if !errors.Is(err, errScopePending) {
			return scope, err
		}
		select {
		case <-ctx.Done():
			return NativeScope{}, fmt.Errorf("wait for native RGW service scope: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func decodeRuntimeScope(data []byte, hostname, frontend string) (NativeScope, error) {
	var native struct {
		Services map[string]struct {
			Daemons map[string]json.RawMessage `json:"daemons"`
		} `json:"services"`
	}
	if err := json.Unmarshal(data, &native); err != nil {
		return NativeScope{}, errors.New("decode RGW native service map")
	}
	var found *NativeScope
	for name, raw := range native.Services["rgw"].Daemons {
		// ServiceMap::dump includes this summary string alongside daemon objects.
		if name == "summary" {
			var summary string
			if err := json.Unmarshal(raw, &summary); err != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return NativeScope{}, errors.New("decode RGW native service summary")
			}
			continue
		}
		var daemon struct {
			Metadata map[string]string `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &daemon); err != nil || daemon.Metadata == nil {
			return NativeScope{}, errors.New("decode RGW native service daemon")
		}
		meta := daemon.Metadata
		// Host-network containers share hostname and PID 1. Their owned listener
		// distinguishes the gateway even when several realms use one Docker host.
		if meta["hostname"] != hostname || meta["pid"] != "1" || meta["frontend_config#0"] != frontend {
			continue
		}
		scope := NativeScope{RealmID: meta["realm_id"], GroupID: meta["zonegroup_id"], ZoneID: meta["zone_id"], Realm: meta["realm_name"], Group: meta["zonegroup_name"], Zone: meta["zone_name"]}
		if scope.GroupID == "" || scope.ZoneID == "" || scope.Group == "" || scope.Zone == "" {
			return NativeScope{}, errors.New("incomplete RGW native service scope")
		}
		if found != nil && *found != scope {
			return NativeScope{}, errors.New("ambiguous RGW native service scope for container")
		}
		found = &scope
	}
	if found == nil {
		return NativeScope{}, errScopePending
	}
	return *found, nil
}

// CommandArgs is the radosgw-admin command line of args in scope, selected by
// native IDs.
func CommandArgs(scope NativeScope, args ...string) []string {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json", "--zonegroup-id", scope.GroupID, "--zone-id", scope.ZoneID}
	if scope.RealmID != "" {
		argv = append(argv, "--realm-id", scope.RealmID, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	return append(argv, args...)
}

// Command runs radosgw-admin in the gateway container in the exact native
// scope. A failure is redacted. It is called with the owning cluster's mutex
// held.
func (c *Core) Command(ctx context.Context, scope NativeScope, args ...string) ([]byte, error) {
	data, err := servicehook.Command(ctx, c.Container(), CommandArgs(scope, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("RGW placement command: %w", ctx.Err())
		}
		return nil, clusterutil.Quiet(errors.New("RGW placement command failed; native output is redacted"), err)
	}
	return data, nil
}

// DecodeObject decodes one native JSON object with numbers preserved and
// refuses trailing output.
func DecodeObject(data []byte) (map[string]any, error) {
	var native map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&native); err != nil || native == nil {
		return nil, errors.New("decode RGW native placement configuration")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("RGW native placement configuration contains trailing output")
	}
	return native, nil
}
