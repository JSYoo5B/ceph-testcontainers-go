package override

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

var messengerSecurePolicy = []string{
	"ms cluster mode", "ms service mode", "ms client mode",
	"ms mon cluster mode", "ms mon service mode", "ms mon client mode",
	"ms bind msgr1", "ms bind msgr2",
}

func TestMessengerSecureBootstrapRejectsCentralShadowInEveryScope(t *testing.T) {
	for _, key := range messengerSecurePolicy {
		canonical := strings.ReplaceAll(key, " ", "_")
		for _, name := range []string{canonical, strings.ReplaceAll(canonical, "_", "-")} {
			for _, scope := range []ceph.ConfigSetting{
				{Section: "global"}, {Section: "mon"}, {Section: "mon.b"},
				{Section: "mgr"}, {Section: "mgr.a"}, {Section: "osd"},
				{Section: "osd.0", Mask: "host:node-a/class:ssd"},
				{Section: "mds"}, {Section: "mds.a"}, {Section: "client"},
				{Section: "client.test", Mask: "host:node-a"},
			} {
				control := &configFixture{}
				cluster := configCluster(control, ceph.MessengerV2Secure)
				scope.Name, scope.Value = name, "PRIVATE-SETTING-VALUE"
				change, err := SetConfig(t.Context(), cluster, scope)
				if err == nil || change == nil || change.state.refused == nil || len(control.calls) != 0 || len(heldConfig(cluster)) != 0 {
					t.Fatalf("secure bootstrap %s/%s/%s acquired an override or ran native commands", scope.Section, scope.Mask, scope.Name)
				}
				if !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), canonical) || !strings.Contains(err.Error(), "NetworkConfig.WithMessengerMode") || strings.Contains(err.Error(), scope.Value) {
					t.Fatal("bootstrap refusal omitted its precise reason or exposed the requested value")
				}
			}
		}
	}
	// Reserve the exact fixed keys only. Debug knobs, unrelated bind settings
	// and module-scoped names remain normal central database operations.
	for _, name := range []string{"debug_ms", "ms_bind_ipv6", "ms_cluster_mode_extra", "mgr/example/ms_client_mode"} {
		control := &configFixture{}
		cluster := configCluster(control, ceph.MessengerV2Secure)
		change, err := SetConfig(t.Context(), cluster, ceph.ConfigSetting{Section: "global", Name: name, Value: "1"})
		if err != nil || change.state.refused != nil {
			t.Fatalf("unrelated setting %s was treated as secure bootstrap policy: %v", name, err)
		}
		if err := change.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	control := &configFixture{}
	cluster := configCluster(control, ceph.MessengerDefault)
	change, err := SetConfig(t.Context(), cluster, ceph.ConfigSetting{Section: "global", Name: "ms_client_mode", Value: "secure"})
	if err != nil || change.state.refused != nil {
		t.Fatal("default bootstrap lost its existing central-configuration behavior", err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// MessengerV2Only fixes only the protocol options: the connection modes stay
// central configuration, and other bind options stay free.
func TestMessengerV2OnlyReservesOnlyProtocolOptions(t *testing.T) {
	for _, name := range []string{"ms_bind_msgr1", "ms_bind_msgr2", "ms-bind-msgr1"} {
		for _, scope := range []ceph.ConfigSetting{{Section: "global"}, {Section: "mon.b"}, {Section: "osd.0", Mask: "host:node-a"}, {Section: "client"}} {
			control := &configFixture{}
			cluster := configCluster(control, ceph.MessengerV2Only)
			scope.Name, scope.Value = name, "true"
			change, err := SetConfig(t.Context(), cluster, scope)
			if !errors.Is(err, ceph.ErrInvalidRequest) || change.state.refused == nil || len(control.calls) != 0 || !strings.Contains(err.Error(), "MessengerV2Only") {
				t.Fatalf("v2-only bootstrap allowed %s/%s/%s or ran native commands: %v", scope.Section, scope.Mask, scope.Name, err)
			}
		}
	}
	for _, name := range []string{"ms_mon_service_mode", "ms_service_mode", "ms_cluster_mode", "ms_mon_client_mode", "ms_bind_ipv6"} {
		control := &configFixture{}
		cluster := configCluster(control, ceph.MessengerV2Only)
		change, err := SetConfig(t.Context(), cluster, ceph.ConfigSetting{Section: "mon", Name: name, Value: "crc"})
		if err != nil || change.state.refused != nil {
			t.Fatalf("v2-only bootstrap refused %s: %v", name, err)
		}
		if err := change.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMessengerBootstrapGuardPreservesCallerContext(t *testing.T) {
	control := &configFixture{}
	cluster := configCluster(control, ceph.MessengerV2Secure)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, setting := range []ceph.ConfigSetting{
		{Section: "global", Name: "ms_client_mode", Value: "crc"},
		{Section: "invalid", Name: "ms_client_mode", Value: "crc"},
		{Section: "global", Name: "debug_ms", Value: "1"},
	} {
		change, err := SetConfig(ctx, cluster, setting)
		if !errors.Is(err, context.Canceled) || change.state.refused == nil || len(control.calls) != 0 {
			t.Fatal("canceled caller was replaced by validation/policy errors or ran native commands", err)
		}
	}
	// Unrelated options still wait for serialized fixture work only until the
	// caller deadline. Refused bootstrap keys need no topology admission.
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	started := time.Now()
	change, err := SetConfig(short, cluster, ceph.ConfigSetting{Section: "global", Name: "debug_ms", Value: "1"})
	if !errors.Is(err, context.DeadlineExceeded) || change.state.refused == nil || len(control.calls) != 0 || time.Since(started) > time.Second {
		t.Fatal("configuration admission ignored the caller deadline", err)
	}
	if change, err := SetConfig(t.Context(), cluster, ceph.ConfigSetting{Section: "global", Name: "ms_bind_msgr1", Value: "true"}); err == nil || change.state.refused == nil || len(control.calls) != 0 {
		t.Fatal("secure bootstrap refusal waited for topology or ran a native command")
	}
}
