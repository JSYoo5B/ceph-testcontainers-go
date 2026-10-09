package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Every wrapper must preserve the complete testcontainers.Container interface;
// service-specific endpoint/name helpers must not shadow its methods.
var (
	_ testcontainers.Container = (*Container)(nil)
	_ testcontainers.Container = (*OSDContainer)(nil)
	_ testcontainers.Container = (*MonitorContainer)(nil)
	_ testcontainers.Container = (*ManagerContainer)(nil)
	_ testcontainers.Container = (*RGWContainer)(nil)
	_ testcontainers.Container = (*CephFSContainer)(nil)
)

func TestInvalidSettingsDoNotCreateResources(t *testing.T) {
	for _, test := range []struct {
		name string
		opt  Option
	}{
		{"OSD count", WithOSDCount(0)},
		{"MON count", WithMonitorCount(0)},
		{"MGR count", WithManagerCount(-1)},
		{"empty OSD layout", WithInitialOSDs()},
		{"invalid OSD layout", WithInitialOSDs(OSDConfig{Host: "bad host"})},
		{"conflicting OSD host", WithInitialOSDs(OSDConfig{Host: "host1", Rack: "rack1"}, OSDConfig{Host: "host1", Rack: "rack2"})},
		{"invalid pool defaults", WithPoolDefaults(2, 3)},
		{"OSD block size", WithOSDBlockSize(0)},
		{"negative OSD block size", WithOSDBlockSize(-1)},
		{"small OSD block size", WithOSDBlockSize((64 << 20) - 1)},
		{"startup timeout", WithStartupTimeout(0)},
		{"OSD image", WithOSDImage("")},
		{"RGW image", WithRGWImage(" \t")},
		{"MDS image", WithMDSImage("\n")},
		{"host address requires host mode", WithHostAddress("127.0.0.1")},
		{"host address empty", WithHostAddress("")},
		{"host address unspecified", WithHostAddress("0.0.0.0")},
		{"host address broadcast", WithHostAddress("255.255.255.255")},
		{"host address multicast", WithHostAddress("224.0.0.1")},
		{"host address IPv6", WithHostAddress("::1")},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster, err := Run(context.Background(), DefaultImage, test.opt)
			if err == nil || cluster != nil {
				t.Fatalf("invalid setting allocated resources: cluster=%v error=%v", cluster, err)
			}
		})
	}
}

func TestConnectionConfigCopiesCredentials(t *testing.T) {
	cluster := &Container{config: []byte("config"), keyring: []byte("keyring")}
	config, keyring, err := cluster.ConnectionConfig()
	if err != nil {
		t.Fatal(err)
	}
	config[0], keyring[0] = 'x', 'x'
	if string(cluster.config) != "config" || string(cluster.keyring) != "keyring" {
		t.Fatal("exported credentials alias the cluster bootstrap data")
	}
	cluster.closed = true
	if _, _, err := cluster.ConnectionConfig(); err == nil {
		t.Fatal("terminated cluster exported credentials")
	}
	if _, _, err := (&Container{}).ConnectionConfig(); err == nil {
		t.Fatal("incomplete bootstrap exported credentials")
	}
}

func TestCleanupRetainsFailuresAndAcceptsMissingDaemons(t *testing.T) {
	missing := &hostNetworkFixtureContainer{terminationErr: errdefs.ErrNotFound}
	manager := &hostNetworkFixtureContainer{failTerminationOnce: true}
	cluster := &Container{
		Container: missing, manager: manager,
		osds:     map[int]*OSDContainer{0: {Container: missing}},
		services: map[string]testcontainers.Container{"rgw": missing},
	}
	if err := cluster.Terminate(t.Context()); err == nil || cluster.manager != manager {
		t.Fatal("failed manager cleanup lost ownership")
	}
	if !cluster.monitorTerminated || len(cluster.osds) != 0 || len(cluster.services) != 0 {
		t.Fatal("missing daemons remained owned after cleanup")
	}
	if err := cluster.Terminate(t.Context()); err != nil || cluster.manager != nil {
		t.Fatalf("failed manager cleanup did not recover: %v", err)
	}
	if missing.terminations != 3 || manager.terminations != 2 {
		t.Fatal("cleanup repeated completed removals or skipped a failed removal")
	}
}

func TestCLIJSONSurvivesStderrWarnings(t *testing.T) {
	output, err := command(context.Background(), &commandContainer{}, "ceph", "status", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(output) {
		t.Fatalf("stderr contaminated CLI JSON: %q", output)
	}
	_, err = command(context.Background(), &commandContainer{exitCode: 1}, "ceph", "status")
	if err == nil || !strings.Contains(err.Error(), "diagnostic on stderr") {
		t.Fatalf("missing stderr on failed command: %v", err)
	}
}

type commandContainer struct {
	testcontainers.Container
	exitCode int
}

func (c *commandContainer) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	var framed bytes.Buffer
	for _, stream := range []struct {
		kind stdcopy.StdType
		text string
	}{
		{stdcopy.Stdout, `{"fsid":"test"}`}, {stdcopy.Stderr, "diagnostic on stderr"},
	} {
		var header [8]byte
		header[0] = byte(stream.kind)
		binary.BigEndian.PutUint32(header[4:], uint32(len(stream.text)))
		framed.Write(header[:])
		framed.WriteString(stream.text)
	}
	return c.exitCode, &framed, nil
}
