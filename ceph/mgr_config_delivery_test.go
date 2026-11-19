package ceph

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMGRConfigDeliveryRequiresMONLayer(t *testing.T) {
	const key = "mon_lease_ack_timeout_factor"
	for _, tc := range []struct {
		data string
		want bool
	}{
		{`{"mon_lease_ack_timeout_factor":{"mon":17.25,"final":9.75}}`, true},
		{`{"diff":{"diff":{"mon_lease_ack_timeout_factor":{"mon":17.25}}}}`, true},
		{`{"mon_lease_ack_timeout_factor":{"mon":9.75,"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"mon":null,"final":17.25}}`, false},
		{`{"mon_lease_ack_timeout_factor":{"mon":"17.25"}}`, false},
		{`{"diff":null}`, false},
	} {
		if got := mgrConfigMONValue([]byte(tc.data), key, 17.25); got != tc.want {
			t.Errorf("MON marker from %s = %t, want %t", tc.data, got, tc.want)
		}
	}
}

func TestMGRConfigDeliverySettlesProbeBeforeRetry(t *testing.T) {
	for _, killed := range []bool{true, false} {
		for _, refused := range []bool{true, false} {
			t.Run(fmt.Sprintf("killed=%t/refused=%t", killed, refused), func(t *testing.T) {
				c, _ := adminSocketCluster()
				c.settings.startupTimeout = 5 * time.Second
				const key = "mon_lease_ack_timeout_factor"
				control := &adminLifetimeModel{
					id: "admin-control", refuseCancel: refused,
					stdout: `{"diff":{"mon_lease_ack_timeout_factor":{"mon":17.25}}}`,
				}
				c.Container = control
				manager := c.members.roster.manager("a")
				manager.owner, manager.Container = c, &adminLifetimeModel{id: "manager"}
				probes := 0
				control.during = func() {
					probes++
					control.killed = probes == 1 && killed
					control.fail = probes == 1 && !killed
					if c.native.calls.registered() != 1 {
						t.Error("probe dispatched without an exclusive registered invocation")
					}
					if probes > 1 && control.cancels == 0 {
						t.Error("second probe executed before cancellation")
					}
				}
				wait := func() error {
					c.mu.Lock()
					defer c.mu.Unlock()
					return (overrideHooks{}).WaitMGRConfigDelivery(t.Context(), c, "a", key, "17.25")
				}
				err := wait()
				if !refused {
					if err != nil || probes != 2 || control.cancels != 1 || c.native.calls.registered() != 0 {
						t.Fatalf("expected settled retry: err=%v probes=%d cancels=%d", err, probes, control.cancels)
					}
					return
				}
				var unstopped *NativeCommandUnstoppedError
				if !errors.As(err, &unstopped) || probes != 1 || control.cancels != 1 || c.native.calls.registered() != 1 {
					t.Fatalf("expected retained cleanup: err=%v probes=%d cancels=%d", err, probes, control.cancels)
				}
				if unstopped.invocation.targetID != manager.GetContainerID() {
					t.Fatal("retained probe lost its target identity")
				}
				if err := unstopped.Stop(t.Context()); err == nil || c.native.calls.registered() != 1 {
					t.Fatal("failed settlement dropped the invocation")
				}
				control.refuseCancel = false
				if err := unstopped.Stop(t.Context()); err != nil || c.native.calls.registered() != 0 {
					t.Fatalf("settlement did not release the retained invocation: %v", err)
				}
				if err := wait(); err != nil || probes != 2 {
					t.Fatalf("delivery did not recover after settlement: err=%v probes=%d", err, probes)
				}
			})
		}
	}
}
