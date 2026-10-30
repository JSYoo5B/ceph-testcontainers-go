package rgw

import (
	"context"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
)

// rgwTLSInspectTarget answers Inspect with the given container details, so
// the bridge address lookup can be exercised without Docker.
type rgwTLSInspectTarget struct {
	rgwReadinessTarget
	info *container.InspectResponse
}

func (target *rgwTLSInspectTarget) Inspect(context.Context) (*container.InspectResponse, error) {
	return target.info, nil
}

func TestRGWTLSProbeAddressFollowsTheClusterNetwork(t *testing.T) {
	host := rgwTLSProbe{facts: ceph.ClusterFacts{UsesHostNetwork: true, PublicAddress: "10.0.0.5"}}
	if address, err := host.address(t.Context(), nil); err != nil || address != "10.0.0.5" {
		t.Fatalf("host network address: %q, %v", address, err)
	}
	bridge := rgwTLSProbe{facts: ceph.ClusterFacts{NetworkName: "ceph-net", PublicAddress: "10.0.0.5"}}
	for name, info := range map[string]*container.InspectResponse{
		"no settings":   {},
		"other network": {NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{"other": {IPAddress: netip.MustParseAddr("172.18.0.9")}}}},
		"no address":    {NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{"ceph-net": {}}}},
	} {
		if _, err := bridge.address(t.Context(), &rgwTLSInspectTarget{info: info}); err == nil {
			t.Errorf("%s: a missing bridge address was accepted", name)
		}
	}
	info := &container.InspectResponse{NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{"ceph-net": {IPAddress: netip.MustParseAddr("172.18.0.9")}}}}
	if address, err := bridge.address(t.Context(), &rgwTLSInspectTarget{info: info}); err != nil || address != "172.18.0.9" {
		t.Fatalf("bridge address: %q, %v", address, err)
	}
}

func TestRGWTLSProbeAttemptSeparatesFatalAndRetry(t *testing.T) {
	controller := &rgwTestContainer{}
	probe := rgwTLSProbe{cluster: testCluster(controller, testhelper.TestConfig{StartupTimeout: time.Second}, nil), port: "8443", fingerprint: "pin"}
	stopped := &rgwReadinessTarget{state: container.State{Status: "exited"}}
	if retry, err := probe.attempt(t.Context(), stopped, "10.0.0.5"); err == nil || retry != nil || controller.execCalls != 0 {
		t.Fatalf("stopped gateway: retry %v, err %v, probes %d", retry, err, controller.execCalls)
	}
	unowned := &rgwReadinessTarget{state: container.State{Running: true}, probeCode: 1}
	if retry, err := probe.attempt(t.Context(), unowned, "10.0.0.5"); err != nil || retry == nil || !strings.Contains(retry.Error(), "does not yet own") || controller.execCalls != 0 {
		t.Fatalf("unowned listener: retry %v, err %v, probes %d", retry, err, controller.execCalls)
	}
	owned := &rgwReadinessTarget{state: container.State{Running: true}}
	if retry, err := probe.attempt(t.Context(), owned, "10.0.0.5"); err != nil || retry != nil || controller.execCalls != 1 {
		t.Fatalf("owned listener: retry %v, err %v, probes %d", retry, err, controller.execCalls)
	}
}
