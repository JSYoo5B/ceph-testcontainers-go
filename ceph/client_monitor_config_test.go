package ceph

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const clientMonitorFSID = "11111111-2222-3333-4444-555555555555"
const clientMonitorForeignFSID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

func newClientMonitorFixture() (*Container, *monitorConfigArchive, *monitorConfigArchive) {
	cluster, control := newMonitorConfigFixture()
	cluster.config = bytes.ReplaceAll(cluster.config, []byte("retained-fsid"), []byte(clientMonitorFSID))
	control.config = bytes.ReplaceAll(control.config, []byte("retained-fsid"), []byte(clientMonitorFSID))
	control.native = []byte(strings.Replace(monitorConfigQuorum, `"monmap":{`, `"monmap":{"fsid":"`+clientMonitorFSID+`",`, 1))
	client := newMonitorConfigArchive("external-client", "[client.test]\nkeyring = /private/client.keyring\nmon_host = private-override\n")
	client.config = bytes.ReplaceAll(client.config, []byte("retained-fsid"), []byte(clientMonitorFSID))
	return cluster, control, client
}

func TestRefreshClientMonitorConfigPreservesExplicitStoppedClient(t *testing.T) {
	cluster, control, client := newClientMonitorFixture()
	template := bytes.Clone(cluster.config)
	before := bytes.Clone(client.config)
	if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	want := bytes.Replace(before, []byte("mon host = old"), []byte("mon host = "+monitorConfigAddresses), 1)
	if !bytes.Equal(client.config, want) || !bytes.Equal(cluster.config, template) || control.reads != 0 || control.writes != 0 || client.execs != 0 || client.starts != 0 || client.stops != 0 || client.terminations != 0 {
		t.Fatal("refresh changed unrelated template, private settings, keyring path or process/lifetime")
	}
	if client.reads != 1 || client.writes != 1 || control.execs != 2 {
		t.Fatal("refresh did not use archive and before/after current native identity")
	}
	if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil || client.writes != 1 || client.reads != 2 {
		t.Fatalf("idempotent refresh rewrote a current file: %v", err)
	}
}

func TestMonitorBootstrapAddressesRejectsUnconfirmedOrForeignNativeCluster(t *testing.T) {
	for _, mutation := range []struct {
		name  string
		apply func(*Container, *monitorConfigArchive)
	}{
		{"missing template", func(c *Container, _ *monitorConfigArchive) { c.config = nil }},
		{"missing keyring", func(c *Container, _ *monitorConfigArchive) { c.keyring = nil }},
		{"invalid original fsid", func(c *Container, _ *monitorConfigArchive) { c.config = []byte(monitorConfigOld) }},
		{"missing native fsid", func(_ *Container, a *monitorConfigArchive) { a.native = []byte(monitorConfigQuorum) }},
		{"foreign native fsid", func(_ *Container, a *monitorConfigArchive) {
			a.native = bytes.ReplaceAll(a.native, []byte(clientMonitorFSID), []byte(clientMonitorForeignFSID))
		}},
		{"no majority", func(_ *Container, a *monitorConfigArchive) {
			a.native = bytes.Replace(a.native, []byte(`"quorum_names":["a","b","c"]`), []byte(`"quorum_names":["a"]`), 1)
		}},
		{"malformed map", func(_ *Container, a *monitorConfigArchive) { a.native = []byte(`{"monmap":null}`) }},
		{"closed", func(c *Container, _ *monitorConfigArchive) { c.closed = true }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			cluster, control, client := newClientMonitorFixture()
			mutation.apply(cluster, control)
			if addresses, err := cluster.MonitorBootstrapAddresses(t.Context()); err == nil || addresses != "" {
				t.Fatalf("adopted unconfirmed/foreign native identity: addresses=%q error=%v", addresses, err)
			}
			if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err == nil || client.reads != 0 || client.writes != 0 {
				t.Fatal("failed identity preflight accessed the explicit client")
			}
		})
	}
	cluster, _, _ := newClientMonitorFixture()
	if addresses, err := cluster.MonitorBootstrapAddresses(t.Context()); err != nil || addresses != monitorConfigAddresses {
		t.Fatalf("current original quorum was not returned: %q %v", addresses, err)
	}
}

func TestRefreshClientMonitorConfigRejectsForeignOrAmbiguousTarget(t *testing.T) {
	for _, raw := range []string{
		strings.Replace(monitorConfigOld, "retained-fsid", clientMonitorForeignFSID, 1),
		"[global]\nmon_host=old\n",
		"[global]\nmon_host=old\nfsid=invalid\n",
		"[global]\nmon_host=old\nfsid=" + clientMonitorFSID + "\nfsid=" + clientMonitorFSID + "\n",
		"[global]\nmon_host=old\n[client]\nfsid=" + clientMonitorFSID + "\n",
		"[global]\nmon_host=old\nfsid=" + clientMonitorFSID + "\n[client]\nfsid=" + clientMonitorFSID + "\n",
		"[global]\nmon_host=old\nfsid=00000000-0000-0000-0000-000000000000\n",
		"[global]\nmon_host=old\nfsid=" + clientMonitorFSID + "\n!include /private.cfg\n",
	} {
		cluster, control, client := newClientMonitorFixture()
		client.config = []byte(raw)
		if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err == nil || client.writes != 0 || control.execs != 1 || string(client.config) != raw {
			t.Fatalf("foreign/ambiguous target was copied or changed: %v", err)
		}
	}
	for _, quoted := range []string{"'" + clientMonitorFSID + "'", `"` + clientMonitorFSID + `"`} {
		cluster, _, client := newClientMonitorFixture()
		client.config = bytes.Replace(client.config, []byte(clientMonitorFSID), []byte(quoted), 1)
		if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil {
			t.Fatalf("valid quoted original UUID was rejected: %v", err)
		}
	}
}

func TestRefreshClientMonitorConfigRetainsUncertainCopyAndRetries(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-applied", true: "applied"}[applied], func(t *testing.T) {
			cluster, _, client := newClientMonitorFixture()
			lost := errors.New("copy reply lost")
			client.writeErr, client.applyOnError = lost, applied
			if err := cluster.RefreshClientMonitorConfig(t.Context(), client); !errors.Is(err, lost) {
				t.Fatalf("copy uncertainty lost error: %v", err)
			}
			client.writeErr = nil
			if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil {
				t.Fatal(err)
			}
			writes := 2
			if applied {
				writes = 1
			}
			if client.writes != writes || client.starts != 0 || client.stops != 0 || client.terminations != 0 {
				t.Fatal("retry failed to reconcile uncertain copy or changed lifetime")
			}
		})
	}
}

func TestRefreshClientMonitorConfigPostcopyNativeDriftRetainsCopy(t *testing.T) {
	cluster, control, client := newClientMonitorFixture()
	client.afterWrite = func() {
		control.native = bytes.ReplaceAll(control.native, []byte(clientMonitorFSID), []byte(clientMonitorForeignFSID))
	}
	if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err == nil || !bytes.Contains(client.config, []byte(monitorConfigAddresses)) || client.writes != 1 {
		t.Fatal("postcopy native replacement was adopted or successful copy lost")
	}
	client.afterWrite = nil
	control.native = bytes.ReplaceAll(control.native, []byte(clientMonitorForeignFSID), []byte(clientMonitorFSID))
	if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil || client.writes != 1 {
		t.Fatalf("fresh context retry repeated successful write: %v", err)
	}
}

func TestRefreshClientMonitorConfigDeadlineBeforeWorkAndDuringQueue(t *testing.T) {
	cluster, control, client := newClientMonitorFixture()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := cluster.RefreshClientMonitorConfig(ctx, client); !errors.Is(err, context.Canceled) || control.execs != 0 || client.reads != 0 {
		t.Fatal("already canceled refresh touched native or archive")
	}
	cluster.mu.Lock()
	queuedCtx, queuedCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- cluster.RefreshClientMonitorConfig(queuedCtx, client) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("queued refresh lost deadline: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("queued refresh waited for mutex release after deadline")
	}
	cluster.mu.Unlock()
	queuedCancel()
	if control.execs != 0 || client.reads != 0 || client.writes != 0 {
		t.Fatal("queued expired refresh mutated native or archive")
	}
	if err := cluster.RefreshClientMonitorConfig(t.Context(), client); err != nil {
		t.Fatalf("fresh-context refresh remained blocked: %v", err)
	}
}

func TestRefreshClientMonitorConfigArchiveFailureAndCancellation(t *testing.T) {
	for _, phase := range []string{"read", "close", "cancel-after-read"} {
		cluster, _, client := newClientMonitorFixture()
		ctx, cancel := context.WithCancel(t.Context())
		failure := errors.New("archive failure")
		switch phase {
		case "read":
			client.readErr = failure
		case "close":
			client.closeErr = failure
		case "cancel-after-read":
			client.afterRead = cancel
			failure = context.Canceled
		}
		if err := cluster.RefreshClientMonitorConfig(ctx, client); !errors.Is(err, failure) || client.writes != 0 {
			t.Errorf("%s did not preserve failure without mutation: %v", phase, err)
		}
		cancel()
	}
	var cluster *Container
	if _, err := cluster.MonitorBootstrapAddresses(t.Context()); err == nil {
		t.Fatal("nil cluster addresses accepted")
	}
	if err := cluster.RefreshClientMonitorConfig(t.Context(), nil); err == nil {
		t.Fatal("nil refresh accepted")
	}
}
