package multicluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/internal/cluster"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const receiverSourceFSID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const receiverDestinationFSID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const receiverSourceUUID = "11111111-1111-4111-8111-111111111111"
const receiverDestinationUUID = "22222222-2222-4222-8222-222222222222"
const receiverPeerUUID = "33333333-3333-4333-8333-333333333333"
const receiverReplacementUUID = "44444444-4444-4444-8444-444444444444"
const receiverPeerClient = "client.rbd-mirror-peer"

type receiverTestClient struct {
	testcontainers.Container
	output     func([]string) (string, bool)
	fsid, site string
	poolID     int64
	policies   map[string]nativeRBDMirrorPolicy
	peers      []rbdMirrorPeer
	calls      [][]string
	hook       func(context.Context, []string)
	failure    func([]string) error
	cleanupErr error
	importErr  error
	imageCalls int
}

func (c *receiverTestClient) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls = append(c.calls, slices.Clone(args))
	if c.hook != nil {
		c.hook(ctx, args)
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if c.failure != nil {
		if err := c.failure(args); err != nil {
			return 0, nil, err
		}
	}
	if c.output != nil {
		if data, ok := c.output(args); ok {
			return 0, rbdImageStatusStream(data), nil
		}
	}
	var data []byte
	switch {
	case slices.Equal(args, []string{"ceph", "fsid"}) || slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}):
		data = []byte(c.fsid + "\n")
	case slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"}):
		data = fmt.Appendf(nil, `[{"pool_id":%d,"pool_name":"images","type":1,"size":2,"min_size":1,"pg_num":8}]`, c.poolID)
	case len(args) > 5 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}):
		policy := c.policies[args[4]]
		fields := map[string]any{"mode": policy.Mode, "mirror_uuid": policy.MirrorUUID, "remote_namespace": policy.RemoteNamespace}
		if policy.SiteName != "" {
			fields["site_name"] = policy.SiteName
		}
		if !strings.Contains(args[4], "/") {
			fields["peers"] = c.peers
		}
		data, _ = json.Marshal(fields)
	case len(args) > 5 && slices.Equal(args[:5], []string{"rbd", "mirror", "pool", "peer", "bootstrap"}) && args[5] == "create":
		token, _ := json.Marshal(map[string]string{"fsid": receiverSourceFSID, "client_id": "rbd-mirror-peer", "key": "private-bootstrap-secret"})
		data = []byte(base64.StdEncoding.EncodeToString(token))
	case len(args) > 5 && slices.Equal(args[:5], []string{"rbd", "mirror", "pool", "peer", "bootstrap"}) && args[5] == "import":
		c.peers = []rbdMirrorPeer{{UUID: receiverPeerUUID, SiteName: "source", ClientName: receiverPeerClient, Direction: "rx-only"}}
		if c.importErr != nil {
			return 0, nil, c.importErr
		}
	case len(args) > 5 && slices.Equal(args[:5], []string{"rbd", "mirror", "pool", "peer", "set"}):
		for i := range c.peers {
			if args[6] == c.peers[i].UUID {
				if args[7] == "client" {
					c.peers[i].ClientName = args[8]
				} else {
					c.peers[i].Direction = args[8]
				}
			}
		}
	case len(args) == 3 && args[0] == "rm" && args[1] == "-f":
		if c.cleanupErr != nil {
			return 0, nil, c.cleanupErr
		}
		data = []byte("")
	case len(args) > 1 && args[0] == "rbd" && args[1] == "info":
		c.imageCalls++
		return 0, nil, errors.New("unexpected image query")
	default:
		return 0, nil, fmt.Errorf("unexpected native command %v", args)
	}
	return 0, rbdImageStatusStream(string(data)), nil
}

func (c *receiverTestClient) CopyToContainer(ctx context.Context, _ []byte, _ string, _ int64) error {
	return ctx.Err()
}

type receiverTestDaemon struct {
	testcontainers.Container
	cid, status             string
	state                   container.State
	statusCalls, stateCalls int
	statusErr, stateErr     error
	hook                    func(context.Context, int)
}

func (d *receiverTestDaemon) GetContainerID() string { return d.cid }
func (d *receiverTestDaemon) State(ctx context.Context) (*container.State, error) {
	d.stateCalls++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.stateErr != nil {
		return nil, d.stateErr
	}
	state := d.state
	return &state, nil
}
func (d *receiverTestDaemon) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	d.statusCalls++
	if d.hook != nil {
		d.hook(ctx, d.statusCalls)
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if !slices.Equal(args, []string{"ceph", "--admin-daemon", "/tmp/rbd-mirror.asok", "rbd", "mirror", "status"}) {
		return 0, nil, errors.New("unexpected daemon command")
	}
	return 0, rbdImageStatusStream(d.status), d.statusErr
}

type receiverHarness struct {
	m                   *RBDMirror
	source, destination *receiverTestClient
	daemons             []*receiverTestDaemon
}

func newReceiverHarness(t *testing.T, sourceNS, destinationNS string, count int) *receiverHarness {
	t.Helper()
	h := &receiverHarness{}
	h.source = &receiverTestClient{fsid: receiverSourceFSID, site: "source", poolID: 42, policies: make(map[string]nativeRBDMirrorPolicy), peers: []rbdMirrorPeer{}}
	h.destination = &receiverTestClient{fsid: receiverDestinationFSID, site: "destination", poolID: 84, policies: make(map[string]nativeRBDMirrorPolicy), peers: []rbdMirrorPeer{{UUID: receiverPeerUUID, Direction: "rx-only", SiteName: "source", ClientName: receiverPeerClient}}}
	for _, site := range []struct {
		client           *receiverTestClient
		ns, remote, uuid string
	}{{h.source, sourceNS, destinationNS, receiverSourceUUID}, {h.destination, destinationNS, sourceNS, receiverDestinationUUID}} {
		mode := "image"
		remote := site.remote
		if site.ns != "" {
			mode, remote = "init-only", ""
		}
		site.client.policies["images"] = nativeRBDMirrorPolicy{Mode: mode, MirrorUUID: site.uuid, RemoteNamespace: stringPointer(remote), SiteName: site.client.site}
		if site.ns != "" {
			site.client.policies["images/"+site.ns] = nativeRBDMirrorPolicy{Mode: "image", MirrorUUID: receiverReplacementUUID, RemoteNamespace: stringPointer(site.remote)}
		}
	}
	m := &RBDMirror{sourceClient: h.source, destinationClient: h.destination, config: RBDMirrorConfig{Pool: "images", SourceSite: "source", DestinationSite: "destination", SourceNamespace: sourceNS, DestinationNamespace: destinationNS, Scope: RBDMirrorScopeImage, Mode: RBDMirrorModeSnapshot, Source: &ceph.Container{Container: h.source}, Destination: &ceph.Container{Container: h.destination}}, poolIdentities: &rbdMirrorPoolIdentities{source: 42, destination: 84}, receiverSetupConfirmed: true, receiverPeerGeneration: 1}
	m.policyIdentities = &rbdMirrorPolicyIdentities{source: rbdMirrorSitePolicyIdentity{base: h.source.policies["images"], selected: h.source.policies[rbdMirrorNamespaceSpec("images", sourceNS)]}, destination: rbdMirrorSitePolicyIdentity{base: h.destination.policies["images"], selected: h.destination.policies[rbdMirrorNamespaceSpec("images", destinationNS)]}}
	m.receiverClusters = &rbdReceiverClusterIdentities{source: receiverSourceFSID, destination: receiverDestinationFSID, sourceCluster: m.config.Source, destinationCluster: m.config.Destination}
	m.receiverPeer = &rbdReceiverPeerIdentity{generation: 1, uuid: receiverPeerUUID, site: "source", client: receiverPeerClient, sourceMirrorUUID: receiverSourceUUID}
	h.m = m
	var members []string
	for i := range count {
		members = append(members, fmt.Sprint(100+i))
	}
	for i := range count {
		name := string(rune('a' + i))
		d := &receiverTestDaemon{cid: strings.Repeat(name, 64), state: container.State{Running: true, Status: container.StateRunning, Pid: 40 + i, StartedAt: "2026-10-07T09:00:00Z"}}
		d.status = receiverTestStatus(destinationNS, sourceNS, members[i], "100", i == 0, members)
		client := "client.rbd-mirror.tc-" + name
		m.daemons = append(m.daemons, &RBDMirrorDaemon{Container: d, DaemonName: name, ClientName: client, receiverHandle: d, receiverContainerID: d.cid, receiverClientName: client, receiverDaemonName: name, receiverStartupConfirmed: true})
		h.daemons = append(h.daemons, d)
	}
	return h
}

func receiverTestStatus(local, remote, instance, leaderID string, leader bool, members []string) string {
	row := map[string]any{"pool": "images", "peer": "uuid: " + receiverPeerUUID + " cluster: source client: " + receiverPeerClient, "state": "running", "instance_id": instance, "leader_instance_id": leaderID, "leader": leader, "namespace_replayers": []any{map[string]any{"local_namespace": local, "remote_namespace": remote, "image_replayers": []any{}}}}
	if leader {
		row["instances"] = members
	}
	data, _ := json.Marshal(map[string]any{"pool_replayers": []any{row}})
	return string(data)
}

func receiverPatchStatus(t *testing.T, data string, patch func(map[string]any)) string {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		t.Fatal(err)
	}
	patch(fields["pool_replayers"].([]any)[0].(map[string]any))
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestRBDReceiverEmptyScopeAndExactCohort(t *testing.T) {
	for _, mapping := range [][2]string{{"", ""}, {"ns-a", "ns-b"}, {"ns-a", ""}, {"", "ns-b"}} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprint(mapping, count), func(t *testing.T) {
				h := newReceiverHarness(t, mapping[0], mapping[1], count)
				status, err := h.m.ReceiverStatus(t.Context())
				if err != nil || !status.Ready || status.PeerID != receiverPeerUUID || status.SourcePoolID != 42 || status.DestinationPoolID != 84 || len(status.Daemons) != count || status.LeaderDaemonName != "a" || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] {
					t.Fatalf("empty scope readiness: %+v %v", status, err)
				}
				if h.source.imageCalls != 0 || h.destination.imageCalls != 0 {
					t.Fatal("readiness required an image")
				}
				status.ExpectedDaemons[0] = "outside"
				status.Instances[0] = "outside"
				status.DaemonProblems["a"] = "outside"
				again, err := h.m.ReceiverStatus(t.Context())
				if err != nil || !again.Ready || again.ExpectedDaemons[0] != "a" || again.Instances[0] != "100" {
					t.Fatal("report shared mutable state")
				}
				for _, client := range []*receiverTestClient{h.source, h.destination} {
					for _, args := range client.calls {
						if len(args) > 4 && args[0] == "rbd" && args[3] == "peer" {
							t.Fatal("observer mutated peer")
						}
					}
				}
			})
		}
	}
}

func TestRBDReceiverDiscoverySelectsExactScope(t *testing.T) {
	h := newReceiverHarness(t, "ns-a", "ns-b", 1)
	base := h.daemons[0].status
	h.daemons[0].status = receiverPatchStatus(t, base, func(row map[string]any) {
		row["namespace_replayers"] = append(row["namespace_replayers"].([]any), map[string]any{"local_namespace": "unrelated", "remote_namespace": "elsewhere", "image_replayers": []any{}})
	})
	var fields map[string]any
	_ = json.Unmarshal([]byte(h.daemons[0].status), &fields)
	var unrelated map[string]any
	_ = json.Unmarshal([]byte(base), &unrelated)
	row := unrelated["pool_replayers"].([]any)[0].(map[string]any)
	row["pool"] = "other"
	row["peer"] = "other-peer"
	fields["pool_replayers"] = append(fields["pool_replayers"].([]any), row)
	data, _ := json.Marshal(fields)
	h.daemons[0].status = string(data)
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatalf("unrelated valid discovery blocked selected scope: %+v %v", status, err)
	}
	h.daemons[0].status = receiverPatchStatus(t, base, func(row map[string]any) {
		row["namespace_replayers"].([]any)[0].(map[string]any)["remote_namespace"] = "elsewhere"
	})
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || status.Ready || status.Daemons["a"].NamespaceDiscovered {
		t.Fatalf("wrong mapping substituted for scope: %+v %v", status, err)
	}
}

func TestRBDReceiverDiscoverySchemaRejectsAmbiguity(t *testing.T) {
	for _, fault := range []string{"missing-pools", "null-pools", "wrong-pools", "missing-namespace", "null-namespace", "namespace-local-null", "namespace-remote-null", "missing-images", "null-images", "duplicate-namespace", "duplicate-pool", "duplicate-json", "instance-null", "instance-number", "instance-alias", "leader-null", "members-null", "member-duplicate", "member-alias", "wrong-state", "trailing"} {
		t.Run(fault, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			base := h.daemons[0].status
			var fields map[string]any
			_ = json.Unmarshal([]byte(base), &fields)
			row := fields["pool_replayers"].([]any)[0].(map[string]any)
			ns := row["namespace_replayers"].([]any)[0].(map[string]any)
			switch fault {
			case "missing-pools":
				delete(fields, "pool_replayers")
			case "null-pools":
				fields["pool_replayers"] = nil
			case "wrong-pools":
				fields["pool_replayers"] = map[string]any{}
			case "missing-namespace":
				delete(row, "namespace_replayers")
			case "null-namespace":
				row["namespace_replayers"] = nil
			case "namespace-local-null":
				ns["local_namespace"] = nil
			case "namespace-remote-null":
				ns["remote_namespace"] = nil
			case "missing-images":
				delete(ns, "image_replayers")
			case "null-images":
				ns["image_replayers"] = nil
			case "duplicate-namespace":
				row["namespace_replayers"] = []any{ns, ns}
			case "duplicate-pool":
				fields["pool_replayers"] = []any{row, row}
			case "instance-null":
				row["instance_id"] = nil
			case "instance-number":
				row["instance_id"] = 100
			case "instance-alias":
				row["instance_id"] = "0100"
			case "leader-null":
				row["leader"] = nil
			case "members-null":
				row["instances"] = nil
			case "member-duplicate":
				row["instances"] = []string{"100", "100"}
			case "member-alias":
				row["instances"] = []string{"0100"}
			case "wrong-state":
				row["state"] = "almost-running"
			}
			encoded, _ := json.Marshal(fields)
			h.daemons[0].status = string(encoded)
			if fault == "duplicate-json" {
				h.daemons[0].status = strings.Replace(base, `"leader":true`, `"leader":false,"leader":true`, 1)
			}
			if fault == "trailing" {
				h.daemons[0].status = base + `{}`
			}
			status, err := h.m.ReceiverStatus(t.Context())
			if err == nil || status.Ready || !rbdReceiverPermanent(err) {
				t.Fatalf("ambiguous schema accepted: %+v %v", status, err)
			}
		})
	}
}

func TestRBDReceiverElectionFalsePositives(t *testing.T) {
	for _, fault := range []string{"foreign", "missing-member", "dual-leader", "same-instance", "leader-disagrees", "no-leader", "changing", "empty-discovery", "manual-stop", "error-discovery", "owned-leader-undiscovered", "owned-member-undiscovered"} {
		t.Run(fault, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 2)
			switch fault {
			case "foreign":
				h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["instances"] = []string{"100", "101", "999"} })
			case "missing-member":
				h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["instances"] = []string{"100"} })
			case "dual-leader":
				h.daemons[1].status = receiverTestStatus("", "", "101", "101", true, []string{"100", "101"})
			case "same-instance":
				h.daemons[1].status = receiverTestStatus("", "", "100", "100", false, nil)
			case "leader-disagrees":
				h.daemons[1].status = receiverTestStatus("", "", "101", "999", false, nil)
			case "no-leader":
				h.daemons[0].status = receiverTestStatus("", "", "100", "100", false, nil)
			case "owned-leader-undiscovered":
				h.daemons[0].status = receiverTestStatus("other", "", "100", "100", true, []string{"100", "101"})
			case "owned-member-undiscovered":
				h.daemons[1].status = receiverTestStatus("other", "", "101", "100", false, nil)
			case "changing":
				h.daemons[0].hook = func(_ context.Context, n int) {
					if n == 2 {
						h.daemons[0].status = receiverTestStatus("", "", "102", "102", true, []string{"102", "101"})
					}
				}
			case "empty-discovery":
				h.daemons[0].status = `{"pool_replayers":[]}`
			case "manual-stop":
				h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["state"] = "stopped (manual)" })
			case "error-discovery":
				h.daemons[0].status = `{"pool_replayers":[{"peer":"uuid: ` + receiverPeerUUID + ` cluster: source client: ` + receiverPeerClient + `","state":"error","namespace_replayers":[]}]}`
			}
			status, err := h.m.ReceiverStatus(t.Context())
			if err != nil || status.Ready || len(status.DaemonProblems) == 0 {
				t.Fatalf("election transition blessed: %+v %v", status, err)
			}
			if fault == "foreign" && !slices.Equal(status.ForeignInstances, []string{"999"}) {
				t.Fatal("foreign member diagnostic missing")
			}
			if strings.HasPrefix(fault, "owned-") && len(status.ForeignInstances) != 0 {
				t.Fatal("non-ready owned instance mislabeled as foreign")
			}
		})
	}
}

func TestRBDReceiverOriginalScopeDrift(t *testing.T) {
	for _, fault := range []string{"source-fsid", "destination-fsid", "pool", "base-uuid", "selected-uuid", "mapping", "site", "peer-uuid", "peer-client", "peer-direction", "peer-mirror", "after-status", "handle", "cid", "client"} {
		t.Run(fault, func(t *testing.T) {
			h := newReceiverHarness(t, "ns-a", "ns-b", 1)
			switch fault {
			case "source-fsid":
				h.source.fsid = receiverDestinationFSID
			case "destination-fsid":
				h.destination.fsid = receiverSourceFSID
			case "pool":
				h.destination.poolID++
			case "base-uuid":
				p := h.source.policies["images"]
				p.MirrorUUID = receiverDestinationUUID
				h.source.policies["images"] = p
			case "selected-uuid":
				p := h.source.policies["images/ns-a"]
				p.MirrorUUID = receiverDestinationUUID
				h.source.policies["images/ns-a"] = p
			case "mapping":
				p := h.destination.policies["images/ns-b"]
				p.RemoteNamespace = stringPointer("outside")
				h.destination.policies["images/ns-b"] = p
			case "site":
				p := h.source.policies["images"]
				p.SiteName = "outside"
				h.source.policies["images"] = p
			case "peer-uuid":
				h.destination.peers[0].UUID = receiverReplacementUUID
			case "peer-client":
				h.destination.peers[0].ClientName = "client.outside"
			case "peer-direction":
				h.destination.peers[0].Direction = "tx-only"
			case "peer-mirror":
				h.destination.peers[0].MirrorUUID = receiverDestinationUUID
			case "after-status":
				h.daemons[0].hook = func(_ context.Context, n int) {
					if n == 2 {
						h.destination.peers[0].UUID = receiverReplacementUUID
					}
				}
			case "handle":
				sub := *h.daemons[0]
				h.m.daemons[0].Container = &sub
			case "cid":
				h.daemons[0].cid = strings.Repeat("f", 64)
			case "client":
				h.m.daemons[0].ClientName = "client.outside"
			}
			status, err := h.m.ReceiverStatus(t.Context())
			if err == nil || status.Ready || !rbdReceiverPermanent(err) {
				t.Fatalf("original scope drift adopted: %+v %v", status, err)
			}
		})
	}
	h := newReceiverHarness(t, "", "", 1)
	h.destination.peers[0].Direction = "rx-tx"
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatalf("legitimate reverse receiving upgrade rejected: %+v %v", status, err)
	}
	h.destination.peers[0].MirrorUUID = receiverSourceUUID
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatalf("asynchronous matching remote UUID rejected: %+v %v", status, err)
	}
	t.Run("source-tx-only-empty-client", func(t *testing.T) {
		h := newReceiverHarness(t, "", "", 1)
		h.source.peers = []rbdMirrorPeer{{UUID: receiverReplacementUUID, Direction: "tx-only", SiteName: "destination", MirrorUUID: receiverDestinationUUID, ClientName: ""}}
		if status, err := h.m.ReceiverStatus(t.Context()); err != nil || !status.Ready {
			t.Fatalf("native learned tx-only peer's explicit empty client rejected: %+v %v", status, err)
		}
	})
}

func TestRBDReceiverZeroPartialAndHASubset(t *testing.T) {
	h := newReceiverHarness(t, "", "", 0)
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || status.Ready || len(status.ExpectedDaemons) != 0 {
		t.Fatalf("zero cohort blessed: %+v %v", status, err)
	}
	h = newReceiverHarness(t, "", "", 2)
	h.daemons[0].state.Running = false
	h.daemons[0].state.Status = container.StateExited
	h.daemons[1].status = receiverTestStatus("", "", "101", "101", true, []string{"101"})
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || status.Ready {
		t.Fatal("default silently excluded stopped owned daemon")
	}
	if status, err := h.m.ReceiverStatus(t.Context(), "b"); err != nil || !status.Ready || status.LeaderDaemonName != "b" {
		t.Fatalf("explicit HA survivor could not converge: %+v %v", status, err)
	}
	h.m.daemons[0].receiverStartupConfirmed = false
	if status, err := h.m.ReceiverStatus(t.Context()); err != nil || status.Ready || status.DaemonProblems["a"] == "" {
		t.Fatal("partial startup blessed")
	}
	h.m.receiverSetupConfirmed = false
	before := len(h.source.calls) + len(h.destination.calls)
	if status, err := h.m.ReceiverStatus(t.Context(), "b"); err == nil || status.Ready || len(h.source.calls)+len(h.destination.calls) != before {
		t.Fatal("partial Run reached native readiness")
	}
	for _, names := range [][]string{{"outside"}, {"b", "b"}} {
		h.m.receiverSetupConfirmed = true
		if _, err := h.m.ReceiverStatus(t.Context(), names...); err == nil || len(h.source.calls)+len(h.destination.calls) != before {
			t.Fatal("invalid cohort reached native reads")
		}
	}
}

func TestRBDReceiverWaitPinsScopeAndAllowsSameContainerRestart(t *testing.T) {
	for _, change := range []string{"replacement", "peer-generation", "inventory-add", "scope-policy-pointer", "same-cid-restart"} {
		t.Run(change, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			h.daemons[0].status = receiverTestStatus("", "", "100", "", false, nil)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			attempts := 0
			status, err := waitRBDReceiverReady(ctx, time.Millisecond, func(ctx context.Context, w *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
				attempts++
				report, captured, err := h.m.receiverStatus(ctx, nil, w)
				if attempts == 1 {
					if !h.m.mu.TryLock() {
						t.Fatal("poll retained fixture lock")
					}
					h.m.mu.Unlock()
					switch change {
					case "replacement":
						original := h.m.daemons[0]
						h.m.daemons[0] = &RBDMirrorDaemon{Container: original.Container, DaemonName: original.DaemonName, ClientName: original.ClientName, receiverHandle: original.receiverHandle, receiverContainerID: original.receiverContainerID, receiverClientName: original.receiverClientName, receiverDaemonName: original.receiverDaemonName, receiverStartupConfirmed: original.receiverStartupConfirmed}
					case "peer-generation":
						sub := *h.m.receiverPeer
						sub.generation++
						h.m.receiverPeerGeneration++
						h.m.receiverPeer = &sub
					case "inventory-add":
						additional := newReceiverHarness(t, "", "", 2).m.daemons[1]
						h.m.daemons = append(h.m.daemons, additional)
					case "scope-policy-pointer":
						sub := *h.m.policyIdentities
						h.m.policyIdentities = &sub
					case "same-cid-restart":
						h.daemons[0].state.StartedAt = "2026-10-07T09:01:00Z"
						h.daemons[0].state.Pid++
						h.daemons[0].status = receiverTestStatus("", "", "200", "200", true, []string{"200"})
					}
				}
				return report, captured, err
			})
			if change == "same-cid-restart" {
				if err != nil || !status.Ready || status.Daemons["a"].InstanceID != "200" || attempts != 2 {
					t.Fatalf("valid current restart refused: %+v %v attempts=%d", status, err, attempts)
				}
			} else if err == nil || status.Ready || !rbdReceiverPermanent(err) || attempts != 2 {
				t.Fatalf("wait adopted changed original cohort: %+v %v attempts=%d", status, err, attempts)
			}
		})
	}
}

// Register cancellation/release/join before launching a potentially blocked
// observation. A gate-admission regression must fail within the watchdog rather
// than prevent t.Cleanup from ever releasing the actually held native owner gate.
func receiverTestBoundedCall[T any](t *testing.T, cancel, unlock func(), call func() T) T {
	t.Helper()
	result := make(chan T, 1)
	done := make(chan struct{})
	var release, join sync.Once
	t.Cleanup(func() {
		cancel()
		if unlock != nil {
			release.Do(unlock)
		}
		join.Do(func() {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("blocked receiver worker did not join after cancellation/release")
			}
		})
	})
	go func() { result <- call(); close(done) }()
	select {
	case got := <-result:
		return got
	case <-time.After(800 * time.Millisecond):
		t.Fatal("receiver admission watchdog expired before worker returned")
		var zero T
		return zero
	}
}

func TestRBDReceiverActualOwnerAndMemberDeadline(t *testing.T) {
	for _, gate := range []string{"owner", "member"} {
		t.Run(gate, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			var unlock func()
			if gate == "owner" {
				h.m.mu.Lock()
				unlock = h.m.mu.Unlock
			} else {
				h.m.daemons[0].mu.Lock()
				unlock = h.m.daemons[0].mu.Unlock
			}
			ctx, rawCancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			var cancellation sync.Once
			cancel := func() { cancellation.Do(rawCancel) }
			start := time.Now()
			got := receiverTestBoundedCall(t, cancel, unlock, func() struct {
				status RBDMirrorReceiverStatus
				err    error
			} {
				status, err := h.m.ReceiverStatus(ctx)
				return struct {
					status RBDMirrorReceiverStatus
					err    error
				}{status, err}
			})
			if !errors.Is(got.err, context.DeadlineExceeded) || got.status.Ready || time.Since(start) > time.Second {
				t.Fatalf("held real %s gate ignored deadline: %+v %v", gate, got.status, got.err)
			}
			if h.daemons[0].statusCalls != 0 {
				t.Fatal("native process query crossed held gate")
			}
		})
	}
}

func TestRBDReceiverFinalNativeReadBlocksQueuedMutationOnlyUntilDeadline(t *testing.T) {
	h := newReceiverHarness(t, "", "", 1)
	entered := make(chan struct{})
	finished := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("observer watchdog did not join")
		}
	})
	var blocked atomic.Bool
	h.destination.hook = func(ctx context.Context, args []string) {
		if h.daemons[0].statusCalls == 2 && slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}) && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-ctx.Done()
		}
	}
	go func() { _, err := h.m.ReceiverStatus(ctx); finished <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("final native read did not block")
	}
	queued, rawQueuedCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	var queuedCancellation sync.Once
	qcancel := func() { queuedCancellation.Do(rawQueuedCancel) }
	err := receiverTestBoundedCall(t, qcancel, nil, func() error {
		_, err := h.m.ReceiverStatus(queued)
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("queued owner admission was not bounded")
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked native cancellation lost: %v", err)
		}
		finished <- err
	case <-time.After(time.Second):
		t.Fatal("native read outlived deadline")
	}
}

func TestRBDReceiverSecretSafeCanonicalCausesAndLateCancellation(t *testing.T) {
	h := newReceiverHarness(t, "", "", 1)
	h.daemons[0].statusErr = errors.Join(errors.New("private-key transport dump"), context.Canceled, context.DeadlineExceeded)
	status, err := h.m.ReceiverStatus(t.Context())
	if status.Ready || !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(fmt.Sprintf("%+v", err), "private-key") {
		t.Fatalf("unsafe or lost query cause: %+v %v", status, err)
	}
	h = newReceiverHarness(t, "", "", 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.destination.hook = func(_ context.Context, args []string) {
		if h.daemons[0].statusCalls == 2 && slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}) {
			cancel()
		}
	}
	status, err = h.m.ReceiverStatus(ctx)
	if status.Ready || !errors.Is(err, context.Canceled) {
		t.Fatal("Ready returned after final cancellation")
	}
	before := RBDMirrorReceiverStatus{PeerID: receiverPeerUUID, Ready: false, Pool: "images"}
	ctx, cancel = context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	attempt := 0
	status, err = waitRBDReceiverReady(ctx, time.Millisecond, func(context.Context, *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
		attempt++
		if attempt == 1 {
			return before, nil, nil
		}
		return RBDMirrorReceiverStatus{}, nil, rbdReceiverQuery(ctx, "query", errors.New("private"))
	})
	if !errors.Is(err, context.DeadlineExceeded) || status.PeerID != receiverPeerUUID || status.Ready {
		t.Fatal("wait erased last useful observation")
	}
}

func TestRBDReceiverBootstrapParserAndFSIDValidation(t *testing.T) {
	for _, value := range []string{receiverSourceFSID, "'" + receiverSourceFSID + "'", `"` + receiverSourceFSID + `"`} {
		if actual, err := rbdReceiverBootstrapFSID([]byte("[global]\nfsid = " + value + "\n")); err != nil || actual != receiverSourceFSID {
			t.Fatalf("valid original FSID rejected: %q %v", actual, err)
		}
	}
	for _, config := range []string{"", "[global]\nmon_host=old\n", "[client]\nfsid=" + receiverSourceFSID, "[global]\nfsid=" + receiverSourceFSID + "\nfsid=" + receiverSourceFSID, "[global]\nfsid=" + receiverSourceFSID + "\n[client]\nfsid=" + receiverSourceFSID, "!include /other\n[global]\nfsid=" + receiverSourceFSID, "[global]\nfsid=" + strings.ToUpper(receiverSourceFSID), "[global]\nfsid=00000000-0000-0000-0000-000000000000"} {
		if _, err := rbdReceiverBootstrapFSID([]byte(config)); err == nil {
			t.Fatal("ambiguous original FSID accepted")
		}
	}
	h := newReceiverHarness(t, "", "", 1)
	h.source.fsid = receiverDestinationFSID
	if err := h.m.checkRBDReceiverClusterFSIDs(t.Context()); err == nil {
		t.Fatal("fresh cluster replacement adopted")
	}
}

func TestRBDReceiverRebootstrapCapabilityCommitAndInvalidation(t *testing.T) {
	for _, phase := range []string{"success", "create-error", "create-lost-reply", "create-cancel", "cleanup-error", "import-lost-reply", "native-identity-drift", "final-cancel"} {
		t.Run(phase, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			old, gen := h.m.receiverPeer, h.m.receiverPeerGeneration
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			createSideEffects := 0
			isCreate := func(args []string) bool {
				return len(args) > 5 && slices.Equal(args[:6], []string{"rbd", "mirror", "pool", "peer", "bootstrap", "create"})
			}
			switch phase {
			case "create-error", "create-lost-reply":
				h.source.failure = func(args []string) error {
					if isCreate(args) {
						return errors.New("private bootstrap create failure or reply loss")
					}
					return nil
				}
				if phase == "create-lost-reply" {
					h.source.hook = func(_ context.Context, args []string) {
						if isCreate(args) {
							createSideEffects++ // A simulated source site/auth change precedes a lost reply.
						}
					}
				}
			case "create-cancel":
				h.source.hook = func(_ context.Context, args []string) {
					if isCreate(args) {
						cancel()
					}
				}

			case "cleanup-error":
				h.destination.cleanupErr = errors.New("private cleanup output")
			case "import-lost-reply":
				h.destination.importErr = errors.New("lost native import reply")
			case "native-identity-drift":
				h.destination.hook = func(_ context.Context, args []string) {
					if len(args) > 5 && args[0] == "rbd" && args[5] == "import" {
						h.destination.poolID++
					}
				}
			case "final-cancel":
				h.destination.hook = func(_ context.Context, args []string) {
					if len(args) == 3 && args[0] == "rm" {
						cancel()
					}
				}
			}
			err := h.m.Rebootstrap(ctx)
			if strings.HasPrefix(phase, "create-") {
				createCalls := 0
				for _, args := range h.source.calls {
					if isCreate(args) {
						createCalls++
					}
				}
				if createCalls != 1 || phase == "create-lost-reply" && createSideEffects != 1 || phase == "create-cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("create attempt control missing: calls=%d side-effects=%d err=%v", createCalls, createSideEffects, err)
				}
				for _, args := range h.destination.calls {
					if len(args) > 5 && args[0] == "rbd" && args[5] == "import" || len(args) == 3 && args[0] == "rm" {
						t.Fatal("create fault reached destination mutation or token cleanup")
					}
				}
			}
			if h.m.receiverPeerGeneration != gen+1 {
				t.Fatalf("actual bootstrap attempt did not advance observer generation: err=%v source=%v destination=%v", err, h.source.calls, h.destination.calls)
			}
			if phase == "success" {
				if err != nil || h.m.receiverPeer == nil || h.m.receiverPeer == old || h.m.receiverPeer.generation != gen+1 {
					t.Fatalf("confirmed cleanup/readback not committed: %+v %v", h.m.receiverPeer, err)
				}
				if status, err := h.m.ReceiverStatus(t.Context()); err != nil || !status.Ready {
					t.Fatal("fresh successful bootstrap capability unavailable")
				}
			} else {
				if h.m.receiverPeer != nil {
					t.Fatal("uncertain bootstrap kept readiness capability")
				}
				if phase != "final-cancel" && err == nil {
					t.Fatal("bootstrap failure hidden")
				}
				before := len(h.source.calls) + len(h.destination.calls)
				if status, err := h.m.ReceiverStatus(t.Context()); err == nil || status.Ready || len(h.source.calls)+len(h.destination.calls) != before {
					t.Fatal("uncertain bootstrap observer adopted previous capability")
				}
			}
		})
	}
}

func TestRBDReceiverRejectedRebootstrapPreservesCapability(t *testing.T) {
	for _, rejection := range []string{"canceled", "busy", "preflight", "native-zero-cancel"} {
		t.Run(rejection, func(t *testing.T) {
			h := newReceiverHarness(t, "", "", 1)
			old, gen := h.m.receiverPeer, h.m.receiverPeerGeneration
			ctx, rawCancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			var cancellation sync.Once
			cancel := func() { cancellation.Do(rawCancel) }
			var unlock func()
			switch rejection {
			case "canceled":
				cancel()
			case "busy":
				h.m.mu.Lock()
				unlock = h.m.mu.Unlock
			case "preflight":
				h.destination.poolID++
			case "native-zero-cancel":
				h.destination.hook = func(_ context.Context, args []string) {
					if slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}) {
						cancel()
					}
				}
			}
			if err := receiverTestBoundedCall(t, cancel, unlock, func() error { return h.m.Rebootstrap(ctx) }); err == nil {
				t.Fatal("rejected bootstrap unexpectedly succeeded")
			}
			if h.m.receiverPeer != old || h.m.receiverPeerGeneration != gen {
				t.Fatal("admission/preflight rejection invalidated confirmed capability")
			}
			for _, c := range []*receiverTestClient{h.source, h.destination} {
				for _, args := range c.calls {
					if len(args) > 5 && args[0] == "rbd" && args[3] == "peer" && args[5] == "create" {
						t.Fatal("rejection reached native bootstrap mutation")
					}
				}
			}
		})
	}
}

func TestRBDReceiverCapabilityDoesNotChangeExistingImageContract(t *testing.T) {
	link, _, _, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, "", "")
	if link.receiverClusters != nil || link.receiverPeer != nil || link.receiverSetupConfirmed {
		t.Fatal("old fixture unexpectedly has new capability")
	}
	if status, err := link.ImageStatus(t.Context(), "volume"); err != nil || !status.ReplayReady {
		t.Fatalf("existing image observation contract expanded: %+v %v", status, err)
	}
	if status, err := link.ReceiverStatus(t.Context()); err == nil || status.Ready {
		t.Fatal("unconfirmed hand-built receiver capability adopted")
	}
}

func TestRBDReceiverPeerPolicySchemaPreservesOnlyExplicitPendingUUID(t *testing.T) {
	for _, field := range []string{"uuid", "direction", "site_name", "mirror_uuid", "client_name"} {
		for _, null := range []bool{false, true} {
			t.Run(fmt.Sprint(field, null), func(t *testing.T) {
				h := newReceiverHarness(t, "", "", 1)
				h.destination.output = func(args []string) (string, bool) {
					if len(args) < 5 || !slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) {
						return "", false
					}
					peer := map[string]any{"uuid": receiverPeerUUID, "direction": "rx-only", "site_name": "source", "mirror_uuid": "", "client_name": receiverPeerClient}
					if null {
						peer[field] = nil
					} else {
						delete(peer, field)
					}
					data, _ := json.Marshal(map[string]any{"mode": "image", "site_name": "destination", "mirror_uuid": receiverDestinationUUID, "remote_namespace": "", "peers": []any{peer}})
					return string(data), true
				}
				status, err := h.m.ReceiverStatus(t.Context())
				if err == nil || status.Ready || !rbdReceiverPermanent(err) {
					t.Fatalf("missing/null peer identity became readiness: %+v %v", status, err)
				}
			})
		}
	}
}

func TestRBDReceiverMixedTransientAndPermanentErrorStopsWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	calls := 0
	status, err := waitRBDReceiverReady(ctx, time.Millisecond, func(context.Context, *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
		calls++
		return RBDMirrorReceiverStatus{PeerID: receiverPeerUUID}, nil, errors.Join(rbdReceiverQuery(ctx, "transport", errors.New("private")), rbdReceiverGuard("original receiver identity changed"))
	})
	if calls != 1 || err == nil || status.Ready || !rbdReceiverPermanent(err) {
		t.Fatal("earlier transient error hid permanent guard")
	}
}

type receiverSingleSnapshotContext struct {
	context.Context
	calls int
}

func (c *receiverSingleSnapshotContext) Err() error { c.calls++; return c.Context.Err() }

type receiverCancellationInCause struct{ cancel context.CancelFunc }

func (c receiverCancellationInCause) Error() string { return "private cancellation body" }
func (c receiverCancellationInCause) Is(target error) bool {
	if target == context.Canceled {
		c.cancel()
		return true
	}
	return false
}
func TestRBDReceiverSanitizerSingleContextSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	counted := &receiverSingleSnapshotContext{Context: ctx}
	err := rbdReceiverQuery(counted, "query", receiverCancellationInCause{cancel})
	if counted.calls != 1 || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "private") {
		t.Fatal("sanitizer repeated context snapshot, lost cancellation or leaked body")
	}
	if !rbdReceiverPermanent(errors.Join(rbdReceiverQuery(ctx, "query", errors.New("private")), rbdReceiverGuard("scope differs"))) {
		t.Fatal("joined guard hidden")
	}
}

func TestRBDReceiverPoolScopeEnrollmentNeedsNoImage(t *testing.T) {
	for _, mapping := range [][2]string{{"", ""}, {"ns-a", "ns-b"}, {"ns-a", ""}, {"", "ns-b"}} {
		h := newReceiverHarness(t, mapping[0], mapping[1], 2)
		h.m.config.Scope, h.m.config.Mode = RBDMirrorScopePool, RBDMirrorModeJournal
		for _, site := range []struct {
			client   *receiverTestClient
			ns       string
			identity *rbdMirrorSitePolicyIdentity
		}{{h.source, mapping[0], &h.m.policyIdentities.source}, {h.destination, mapping[1], &h.m.policyIdentities.destination}} {
			spec := rbdMirrorNamespaceSpec("images", site.ns)
			policy := site.client.policies[spec]
			policy.Mode = "pool"
			site.client.policies[spec] = policy
			site.identity.base = site.client.policies["images"]
			site.identity.selected = policy
		}
		status, err := h.m.ReceiverStatus(t.Context())
		if err != nil || !status.Ready || h.source.imageCalls != 0 || h.destination.imageCalls != 0 {
			t.Fatalf("pool scope empty readiness: %+v %v", status, err)
		}
	}
}

// Shape captured from a Ceph 19.2.5 rbd-mirror admin socket with ns-a mirrored
// to the same-named namespace.
const rbdCeph19ReceiverStatus = `{
    "pool_replayers": [
        {
            "peer": "uuid: 57df75d3-a2d5-4320-9122-3f8598fd02b3 cluster: source client: client.rbd-mirror-peer",
            "pool": "tc-rbd-receiver-0",
            "instance_id": "4331",
            "state": "running",
            "leader_instance_id": "4331",
            "leader": true,
            "instances": [
                "4331"
            ],
            "local_cluster_admin_socket": "/var/run/ceph/client.rbd-mirror.tc-3b2f178a-5c90-4ea8-a776-f9a0c6fbd4cd.1.ceph.187650791168400.asok",
            "remote_cluster_admin_socket": "/var/run/ceph/client.rbd-mirror-peer.1.source.187650791471504.asok",
            "sync_throttler": {"max_parallel_requests": 5, "running_requests": 0, "waiting_requests": 0},
            "deletion_throttler": {"max_parallel_requests": 1, "running_requests": 0, "waiting_requests": 0},
            "image_replayers": [],
            "image_deleter": {"image_deleter_status": {"delete_images_queue": [], "failed_deletes_queue": []}},
            "namespaces": [
                {
                    "name": "ns-a",
                    "image_replayers": [],
                    "image_deleter": {"image_deleter_status": {"delete_images_queue": [], "failed_deletes_queue": []}}
                }
            ]
        }
    ]
}`

func TestRBDReceiverDiscoveryReadsCeph19Namespaces(t *testing.T) {
	peer := &rbdReceiverPeerIdentity{uuid: "57df75d3-a2d5-4320-9122-3f8598fd02b3", site: "source", client: "client.rbd-mirror-peer"}
	selected, found, err := decodeRBDReceiverDiscovery([]byte(rbdCeph19ReceiverStatus), "tc-rbd-receiver-0", peer, true)
	if err != nil || !found || selected.state != "running" || selected.instance != "4331" || !selected.leader || !slices.Equal(selected.members, []string{"4331"}) {
		t.Fatalf("Ceph 19 discovery rejected: %+v %v %v", selected, found, err)
	}
	if want := []rbdReceiverNamespace{{"", ""}, {"ns-a", "ns-a"}}; !slices.Equal(selected.namespaces, want) {
		t.Fatalf("Ceph 19 namespaces = %v, want %v", selected.namespaces, want)
	}
	// The later-release decoder must not accept the Ceph 19 shape.
	if _, _, err := decodeRBDReceiverDiscovery([]byte(rbdCeph19ReceiverStatus), "tc-rbd-receiver-0", peer, false); err == nil {
		t.Fatal("Ceph 19 status accepted without namespace_replayers")
	}
	for _, change := range []struct{ before, after string }{
		{`"name": "ns-a",`, ``},
		{`"name": "ns-a"`, `"name": ""`},
		{`"image_replayers": [],
            "image_deleter"`, `"image_deleter"`},
		{`"namespaces": [`, `"other": [`},
	} {
		if _, _, err := decodeRBDReceiverDiscovery([]byte(strings.Replace(rbdCeph19ReceiverStatus, change.before, change.after, 1)), "tc-rbd-receiver-0", peer, true); err == nil {
			t.Fatalf("malformed Ceph 19 discovery accepted after replacing %q", change.before)
		}
	}
}
