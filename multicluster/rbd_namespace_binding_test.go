package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

const bindingSourceUUID = "55555555-5555-4555-8555-555555555555"
const bindingDestinationUUID = "66666666-6666-4666-8666-666666666666"

func newNamespaceBindingHarness(t *testing.T, count int) *receiverHarness {
	t.Helper()
	h := newReceiverHarness(t, "ns-a", "ns-b", count)
	for _, site := range []struct {
		client                 *receiverTestClient
		ns, remote, mode, uuid string
	}{
		{h.source, "ns-c", "ns-d", "image", bindingSourceUUID},
		{h.destination, "ns-d", "ns-c", "image", bindingDestinationUUID},
		{h.source, "ns-e", "ns-f", "pool", bindingDestinationUUID},
		{h.destination, "ns-f", "ns-e", "pool", bindingSourceUUID},
	} {
		site.client.policies["images/"+site.ns] = nativeRBDMirrorPolicy{Mode: site.mode, MirrorUUID: site.uuid, RemoteNamespace: stringPointer(site.remote)}
	}
	for _, client := range []*receiverTestClient{h.source, h.destination} {
		client.output = func(args []string) (string, bool) {
			if !slices.Equal(args, []string{"rbd", "namespace", "list", "--pool", "images", "--format", "json"}) {
				return "", false
			}
			var entries []map[string]string
			for spec := range client.policies {
				if ns, ok := strings.CutPrefix(spec, "images/"); ok {
					entries = append(entries, map[string]string{"name": ns})
				}
			}
			data, _ := json.Marshal(entries)
			return string(data), true
		}
	}
	for _, daemon := range h.daemons {
		daemon.status = namespaceBindingSocket(t, daemon.status, [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}, {"ns-f", "ns-e"}})
	}
	return h
}

func namespaceBindingSocket(t *testing.T, data string, mappings [][2]string) string {
	t.Helper()
	return receiverPatchStatus(t, data, func(row map[string]any) {
		var entries []any
		for _, mapping := range mappings {
			entries = append(entries, map[string]any{"local_namespace": mapping[0], "remote_namespace": mapping[1], "image_replayers": []any{}})
		}
		row["namespace_replayers"] = entries
	})
}

func namespaceBindingMustBind(t *testing.T, h *receiverHarness, sourceNS, destinationNS string) *RBDMirrorNamespace {
	t.Helper()
	view, err := h.m.BindNamespace(t.Context(), sourceNS, destinationNS)
	if err != nil || view == nil {
		t.Fatalf("bind %q/%q: %v", sourceNS, destinationNS, err)
	}
	return view
}

func namespaceBindingAssertReadOnly(t *testing.T, h *receiverHarness) {
	t.Helper()
	for _, client := range []*receiverTestClient{h.source, h.destination} {
		if client.imageCalls != 0 {
			t.Fatal("binding construction/readiness required an image")
		}
		for _, args := range client.calls {
			if slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}) || slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"}) || slices.Equal(args, []string{"rbd", "namespace", "list", "--pool", "images", "--format", "json"}) || len(args) == 7 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) {
				continue
			}
			t.Fatalf("read-only binding dispatched unexpected command: %v", args)
		}
	}
}

func TestRBDNamespaceBindingReadOnlySharedCohort(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h := newNamespaceBindingHarness(t, count)
			original := h.m.policyIdentities
			for _, mapping := range [][2]string{{"ns-a", "ns-b"}, {"ns-c", "ns-d"}, {"ns-e", "ns-f"}} {
				view := namespaceBindingMustBind(t, h, mapping[0], mapping[1])
				again := namespaceBindingMustBind(t, h, mapping[0], mapping[1])
				if view == again || view.owner != h.m || view.scope.binding.peer != h.m.receiverPeer {
					t.Fatal("binding reused receipt or created another owner")
				}
				status, err := view.ReceiverStatus(t.Context())
				if err != nil || status.Ready != (count > 0) || status.Pool != "images" || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] || status.PeerID != receiverPeerUUID || status.SourceFSID != receiverSourceFSID || status.DestinationFSID != receiverDestinationFSID || status.SourcePoolID != 42 || status.DestinationPoolID != 84 || len(status.Daemons) != count {
					t.Fatalf("shared scope report: %+v %v", status, err)
				}
				for name, report := range status.Daemons {
					if report.ContainerID != h.m.daemons[int(name[0]-'a')].GetContainerID() || report.ClientName != h.m.daemons[int(name[0]-'a')].ClientName {
						t.Fatal("view borrowed/substituted an owned process")
					}
				}
			}
			if h.m.config.SourceNamespace != "ns-a" || h.m.config.DestinationNamespace != "ns-b" || h.m.policyIdentities != original || len(h.m.daemons) != count {
				t.Fatal("scope observation changed original owner config/capture/cohort")
			}
			owner, err := h.m.ReceiverStatus(t.Context())
			if err != nil || owner.Ready != (count > 0) || owner.SourceNamespace != "ns-a" || owner.DestinationNamespace != "ns-b" {
				t.Fatal("original owner semantics changed", err)
			}
			namespaceBindingAssertReadOnly(t, h)
		})
	}
}

func TestRBDNamespaceBindingDefaultPairsAndPoolScope(t *testing.T) {
	for _, mapping := range [][2]string{{"", ""}, {"ns-a", ""}, {"", "ns-b"}} {
		t.Run(fmt.Sprint(mapping), func(t *testing.T) {
			h := newReceiverHarness(t, mapping[0], mapping[1], 1)
			for _, client := range []*receiverTestClient{h.source, h.destination} {
				client.output = func(args []string) (string, bool) {
					if len(args) > 2 && slices.Equal(args[:3], []string{"rbd", "namespace", "list"}) {
						data, _ := json.Marshal([]map[string]string{{"name": "ns-a"}, {"name": "ns-b"}})
						return string(data), true
					}
					return "", false
				}
			}
			view := namespaceBindingMustBind(t, h, mapping[0], mapping[1])
			status, err := view.WaitReceiverReady(t.Context())
			if err != nil || !status.Ready || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] {
				t.Fatalf("actual default mapping lost: %+v %v", status, err)
			}
		})
	}
	h := newNamespaceBindingHarness(t, 1)
	view := namespaceBindingMustBind(t, h, "ns-e", "ns-f")
	if view.scope.policies.source.selected.Mode != "pool" {
		t.Fatal("binding inferred per-image mode instead of native scope")
	}
	if status, err := view.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatal("pool sibling scope could not share image-owner cohort", err)
	}
	t.Run("unrelated-native-catalog-name", func(t *testing.T) {
		h := newNamespaceBindingHarness(t, 1)
		original := h.source.output
		h.source.output = func(args []string) (string, bool) {
			if len(args) > 2 && slices.Equal(args[:3], []string{"rbd", "namespace", "list"}) {
				return `[{"name":"ns-a"},{"name":"ns-c"},{"name":"ns-e"},{"name":"external namespace"}]`, true
			}
			return original(args)
		}
		view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
		if status, err := view.ReceiverStatus(t.Context()); err != nil || !status.Ready {
			t.Fatal("unrelated native catalog naming became global view health", err)
		}
	})
}

func TestRBDNamespaceBindingAdmissionAndNativeSchema(t *testing.T) {
	for _, fault := range []string{"invalid-name", "partial-run", "closed", "missing-catalog", "null-catalog", "duplicate-name", "missing-name", "null-name", "empty-name", "duplicate-json", "trailing-json", "namespace-absent", "selected-disabled", "selected-init-only", "scope-mismatch", "remote-mismatch", "uuid-invalid", "uuid-null", "remote-null", "site-conflict", "base-changed", "peer-conflict"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 0)
			sourceNS := "ns-c"
			switch fault {
			case "invalid-name":
				sourceNS = "../ns-c"
			case "partial-run":
				h.m.receiverSetupConfirmed = false
			case "closed":
				h.m.closed = true
			case "namespace-absent":
				delete(h.source.policies, "images/ns-c")
			case "selected-disabled", "selected-init-only", "scope-mismatch", "remote-mismatch", "uuid-invalid", "site-conflict":
				p := h.source.policies["images/ns-c"]
				switch fault {
				case "selected-disabled":
					p.Mode = "disabled"
				case "selected-init-only":
					p.Mode = "init-only"
				case "scope-mismatch":
					p.Mode = "pool"
				case "remote-mismatch":
					p.RemoteNamespace = stringPointer("wrong")
				case "uuid-invalid":
					p.MirrorUUID = "0"
				case "site-conflict":
					p.SiteName = "outside"
				}
				h.source.policies["images/ns-c"] = p
			case "base-changed":
				p := h.source.policies["images"]
				p.Mode = "image"
				h.source.policies["images"] = p
			case "peer-conflict":
				h.destination.peers[0].MirrorUUID = receiverReplacementUUID
			default:
				original := h.source.output
				h.source.output = func(args []string) (string, bool) {
					if len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) && args[4] == "images/ns-c" && (fault == "uuid-null" || fault == "remote-null") {
						p := h.source.policies["images/ns-c"]
						fields := map[string]any{"mode": p.Mode, "mirror_uuid": p.MirrorUUID, "remote_namespace": p.RemoteNamespace}
						if fault == "uuid-null" {
							fields["mirror_uuid"] = nil
						} else {
							fields["remote_namespace"] = nil
						}
						b, _ := json.Marshal(fields)
						return string(b), true
					}
					if len(args) > 2 && slices.Equal(args[:3], []string{"rbd", "namespace", "list"}) {
						values := map[string]string{"missing-catalog": "{}", "null-catalog": "null", "duplicate-name": `[{"name":"ns-c"},{"name":"ns-c"}]`, "missing-name": "[{}]", "null-name": `[{"name":null}]`, "empty-name": `[{"name":""}]`, "duplicate-json": `[{"name":"ns-c","name":"ns-c"}]`, "trailing-json": `[{"name":"ns-c"}][]`}
						if value, ok := values[fault]; ok {
							return value, true
						}
					}
					return original(args)
				}
			}
			view, err := h.m.BindNamespace(t.Context(), sourceNS, "ns-d")
			if view != nil || err == nil || !rbdReceiverPermanent(err) {
				t.Fatalf("invalid binding accepted: %v %v", view != nil, err)
			}
			if fault == "invalid-name" || fault == "partial-run" || fault == "closed" {
				if len(h.source.calls)+len(h.destination.calls) != 0 {
					t.Fatal("rejected admission reached native policy")
				}
			}
			if h.m.receiverPeer == nil || len(h.m.daemons) != 0 {
				t.Fatal("read-only rejection mutated bootstrap/process authority")
			}
		})
	}
}

func TestRBDNamespaceBindingOriginalScopeDriftAndSiblingIsolation(t *testing.T) {
	for _, fault := range []string{"selected-uuid", "selected-remote", "selected-mode", "selected-site", "catalog-deleted", "pool-id", "fsid", "base", "owner-selected", "peer", "source-handle", "destination-handle"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 1)
			view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			switch fault {
			case "selected-uuid", "selected-remote", "selected-mode", "selected-site":
				p := h.source.policies["images/ns-c"]
				switch fault {
				case "selected-uuid":
					p.MirrorUUID = receiverReplacementUUID
				case "selected-remote":
					p.RemoteNamespace = stringPointer("wrong")
				case "selected-mode":
					p.Mode = "pool"
				case "selected-site":
					p.SiteName = "source"
				}
				h.source.policies["images/ns-c"] = p
			case "catalog-deleted":
				h.source.output = func(args []string) (string, bool) {
					if len(args) > 2 && slices.Equal(args[:3], []string{"rbd", "namespace", "list"}) {
						return `[{"name":"ns-a"},{"name":"ns-e"}]`, true
					}
					return "", false
				}
			case "pool-id":
				h.source.poolID++
			case "fsid":
				h.source.fsid = receiverDestinationFSID
			case "base":
				p := h.source.policies["images"]
				p.MirrorUUID = receiverReplacementUUID
				h.source.policies["images"] = p
			case "owner-selected":
				p := h.source.policies["images/ns-a"]
				p.MirrorUUID = bindingSourceUUID
				h.source.policies["images/ns-a"] = p
			case "peer":
				h.destination.peers[0].ClientName = "client.outside"
			case "source-handle":
				sub := *h.source
				h.m.sourceClient = &sub
			case "destination-handle":
				sub := *h.destination
				h.m.destinationClient = &sub
			}
			status, err := view.ReceiverStatus(t.Context())
			if err == nil || status.Ready || !rbdReceiverPermanent(err) {
				t.Fatalf("original scoped authority drift adopted: %+v %v", status, err)
			}
		})
	}
	h := newNamespaceBindingHarness(t, 1)
	a := namespaceBindingMustBind(t, h, "ns-a", "ns-b")
	c := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "ns-a"}, {"ns-f", "ns-e"}})
	if status, err := a.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatal("missing sibling discovery broke original scope", err)
	}
	if status, err := c.ReceiverStatus(t.Context()); err != nil || status.Ready || status.Daemons["a"].NamespaceDiscovered {
		t.Fatal("sibling view used another namespace's discovery", err)
	}
	delete(h.source.policies, "images/ns-c")
	if status, err := a.ReceiverStatus(t.Context()); err != nil || !status.Ready {
		t.Fatal("foreign sibling selected policy removal invalidated unrelated binding", err)
	}
}

func TestRBDNamespaceBindingLateSelectedPolicyDrift(t *testing.T) {
	for _, operation := range []string{"bind-second-read", "status-process-read"} {
		t.Run(operation, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 1)
			originalOwnerPolicies, originalPeer := h.m.policyIdentities, h.m.receiverPeer
			originalBase, originalOwnerSelected := h.source.policies["images"], h.source.policies["images/ns-a"]
			var view *RBDMirrorNamespace
			if operation == "status-process-read" {
				view = namespaceBindingMustBind(t, h, "ns-c", "ns-d")
				h.daemons[0].hook = func(_ context.Context, number int) {
					if number == 1 {
						policy := h.destination.policies["images/ns-d"]
						policy.MirrorUUID = receiverReplacementUUID
						h.destination.policies["images/ns-d"] = policy
					}
				}
			} else {
				reads := 0
				h.source.hook = func(_ context.Context, args []string) {
					if len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) && args[4] == "images/ns-c" {
						reads++
						if reads == 2 {
							policy := h.source.policies["images/ns-c"]
							policy.MirrorUUID = receiverReplacementUUID
							h.source.policies["images/ns-c"] = policy
						}
					}
				}
			}
			if operation == "bind-second-read" {
				bound, err := h.m.BindNamespace(t.Context(), "ns-c", "ns-d")
				if bound != nil || err == nil || !rbdReceiverPermanent(err) || h.daemons[0].statusCalls != 0 {
					t.Fatal("Bind adopted selected replacement on final readback", err)
				}
			} else {
				status, err := view.ReceiverStatus(t.Context())
				if err == nil || status.Ready || !rbdReceiverPermanent(err) || h.daemons[0].statusCalls != 2 || status.PeerID != receiverPeerUUID {
					t.Fatal("Status blessed selected replacement during actual process reads", err)
				}
			}
			if h.m.policyIdentities != originalOwnerPolicies || h.m.receiverPeer != originalPeer || !sameRBDMirrorPolicy(h.source.policies["images"], originalBase) || !sameRBDMirrorPolicy(h.source.policies["images/ns-a"], originalOwnerSelected) || len(h.m.daemons) != 1 {
				t.Fatal("late read rejection changed original owner authority or inventory")
			}
			namespaceBindingAssertReadOnly(t, h)
		})
	}
}

func TestRBDNamespaceBindingSharedCohortTopologyAndWaitWitness(t *testing.T) {
	for _, fault := range []string{"replace", "add", "rebootstrap", "new-binding"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 1)
			view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			first, w, err := h.m.receiverStatusForScope(t.Context(), &view.scope, nil, nil)
			if err != nil || !first.Ready || w == nil {
				t.Fatal(err)
			}
			switch fault {
			case "replace":
				old := h.m.daemons[0]
				d := *h.daemons[0]
				d.cid = strings.Repeat("f", 64)
				h.m.daemons[0] = &RBDMirrorDaemon{Container: &d, DaemonName: old.DaemonName, ClientName: old.ClientName, receiverHandle: &d, receiverContainerID: d.cid, receiverClientName: old.receiverClientName, receiverDaemonName: old.receiverDaemonName, receiverStartupConfirmed: true}
			case "add":
				other := newNamespaceBindingHarness(t, 2)
				h.m.daemons = append(h.m.daemons, other.m.daemons[1])
			case "rebootstrap":
				if err := h.m.Rebootstrap(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "new-binding":
				view = namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			}
			status, _, err := h.m.receiverStatusForScope(t.Context(), &view.scope, nil, w)
			if err == nil || status.Ready || !rbdReceiverPermanent(err) {
				t.Fatal("old scoped wait adopted new cohort/peer", err)
			}
		})
	}
	h := newNamespaceBindingHarness(t, 2)
	view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	h.daemons[0].state.Running = false
	h.daemons[0].state.Status = container.StateExited
	h.daemons[1].status = namespaceBindingSocket(t, receiverTestStatus("ns-d", "ns-c", "101", "101", true, []string{"101"}), [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}, {"ns-f", "ns-e"}})
	if status, err := view.ReceiverStatus(t.Context()); err != nil || status.Ready {
		t.Fatal("default scope view silently omitted stopped owner", err)
	}
	if status, err := view.ReceiverStatus(t.Context(), "b"); err != nil || !status.Ready || status.LeaderDaemonName != "b" {
		t.Fatal("explicit shared survivor rejected", err)
	}
	h.m.daemons = nil
	if status, err := view.ReceiverStatus(t.Context()); err != nil || status.Ready || len(status.Daemons) != 0 {
		t.Fatal("zero owner scope view adopted survivor", err)
	}
}

func TestRBDNamespaceBindingSameCIDRestartAndFalseElection(t *testing.T) {
	h := newNamespaceBindingHarness(t, 1)
	view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	_, w, err := h.m.receiverStatusForScope(t.Context(), &view.scope, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.daemons[0].state.StartedAt = "2026-10-07T09:10:00Z"
	h.daemons[0].state.Pid = 80
	h.daemons[0].status = namespaceBindingSocket(t, receiverTestStatus("ns-d", "ns-c", "900", "900", true, []string{"900"}), [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}, {"ns-f", "ns-e"}})
	if status, _, err := h.m.receiverStatusForScope(t.Context(), &view.scope, nil, w); err != nil || !status.Ready || status.LeaderInstanceID != "900" {
		t.Fatal("same actual CID restart rejected", err)
	}
	for _, fault := range []string{"foreign", "partial", "duplicate-namespace", "wrong-remote", "dual-leader"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 2)
			view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			switch fault {
			case "foreign":
				h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["instances"] = []string{"100", "101", "999"} })
			case "partial":
				h.m.daemons[0].receiverStartupConfirmed = false
			case "duplicate-namespace":
				h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}, {"ns-d", "ns-c"}})
			case "wrong-remote":
				h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "ns-a"}, {"ns-d", "wrong"}})
			case "dual-leader":
				h.daemons[1].status = namespaceBindingSocket(t, receiverTestStatus("ns-d", "ns-c", "101", "101", true, []string{"100", "101"}), [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}})
			}
			status, _ := view.ReceiverStatus(t.Context())
			if status.Ready {
				t.Fatal("scoped binding weakened exact cohort election")
			}
		})
	}
}

func TestRBDNamespaceBindingOwnerAndMemberGateDeadline(t *testing.T) {
	for _, operation := range []string{"bind", "status", "wait"} {
		for _, gate := range []string{"owner", "member"} {
			if operation == "bind" && gate == "member" {
				continue
			}
			t.Run(operation+"/"+gate, func(t *testing.T) {
				h := newNamespaceBindingHarness(t, 1)
				view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
				var unlock func()
				if gate == "owner" {
					h.m.mu.Lock()
					unlock = h.m.mu.Unlock
				} else {
					h.m.daemons[0].mu.Lock()
					unlock = h.m.daemons[0].mu.Unlock
				}
				ctx, rawCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
				var once sync.Once
				cancel := func() { once.Do(rawCancel) }
				got := receiverTestBoundedCall(t, cancel, unlock, func() struct {
					err   error
					ready bool
					view  *RBDMirrorNamespace
				} {
					if operation == "bind" {
						bound, err := h.m.BindNamespace(ctx, "ns-c", "ns-d")
						return struct {
							err   error
							ready bool
							view  *RBDMirrorNamespace
						}{err, false, bound}
					}
					var status RBDMirrorReceiverStatus
					var err error
					if operation == "status" {
						status, err = view.ReceiverStatus(ctx)
					} else {
						status, err = view.WaitReceiverReady(ctx)
					}
					return struct {
						err   error
						ready bool
						view  *RBDMirrorNamespace
					}{err, status.Ready, nil}
				})
				if !errors.Is(got.err, context.DeadlineExceeded) || got.ready || got.view != nil || h.daemons[0].statusCalls != 0 {
					t.Fatal("scoped admission crossed held gate or lost deadline", got.err)
				}
			})
		}
	}
}

func TestRBDNamespaceBindingFinalContextAndSecretSafeErrors(t *testing.T) {
	for _, operation := range []string{"bind", "status"} {
		t.Run(operation, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 1)
			var view *RBDMirrorNamespace
			if operation == "status" {
				view = namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			h.destination.hook = func(_ context.Context, args []string) {
				if len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) && args[4] == "images/ns-d" {
					reads++
					if reads == 2 {
						cancel()
					}
				}
			}
			if operation == "bind" {
				bound, err := h.m.BindNamespace(ctx, "ns-c", "ns-d")
				if bound != nil || !errors.Is(err, context.Canceled) {
					t.Fatal("late canceled Bind published view", err)
				}
			} else {
				status, err := view.ReceiverStatus(ctx)
				if status.Ready || !errors.Is(err, context.Canceled) {
					t.Fatal("late canceled scope reported Ready", err)
				}
			}
		})
	}
	h := newNamespaceBindingHarness(t, 1)
	h.source.failure = func(args []string) error {
		if len(args) > 2 && slices.Equal(args[:3], []string{"rbd", "namespace", "list"}) {
			return errors.Join(errors.New("private-key material"), context.Canceled, context.DeadlineExceeded)
		}
		return nil
	}
	view, err := h.m.BindNamespace(t.Context(), "ns-c", "ns-d")
	if view != nil || !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(fmt.Sprintf("%+v", err), "private-key") {
		t.Fatal("scoped binding leaked native error or lost canonical causes", err)
	}
}

func TestRBDNamespaceBindingWaitPollReleasesOriginalOwner(t *testing.T) {
	h := newNamespaceBindingHarness(t, 1)
	view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "ns-a"}, {"ns-f", "ns-e"}})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	attempts := 0
	status, err := waitRBDReceiverReady(ctx, time.Millisecond, func(ctx context.Context, previous *rbdReceiverWitness) (RBDMirrorReceiverStatus, *rbdReceiverWitness, error) {
		attempts++
		status, witness, err := h.m.receiverStatusForScope(ctx, &view.scope, nil, previous)
		if attempts == 1 {
			if status.Ready || err != nil || !h.m.mu.TryLock() {
				t.Fatal("non-ready scoped observation retained original owner gate", err)
			}
			h.m.mu.Unlock()
			if sibling, err := h.m.BindNamespace(ctx, "ns-e", "ns-f"); err != nil || sibling == nil {
				t.Fatal("between-poll sibling read could not use original owner gate", err)
			}
			h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "ns-a"}, {"ns-d", "ns-c"}, {"ns-f", "ns-e"}})
		}
		return status, witness, err
	})
	if err != nil || !status.Ready || attempts != 2 {
		t.Fatal("scope poll failed original witness convergence", err)
	}
	namespaceBindingAssertReadOnly(t, h)
}

func TestRBDNamespaceBindingRebootstrapAttemptInvalidatesAndFreshBind(t *testing.T) {
	for _, phase := range []string{"busy", "cancel", "preflight", "create-error", "create-lost-reply", "import-loss", "success"} {
		t.Run(phase, func(t *testing.T) {
			h := newNamespaceBindingHarness(t, 1)
			view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
			old, generation := h.m.receiverPeer, h.m.receiverPeerGeneration
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var err error
			createSideEffects := 0
			switch phase {
			case "busy":
				h.m.mu.Lock()
				busy, rawCancel := context.WithTimeout(ctx, 25*time.Millisecond)
				var once, release sync.Once
				done := func() { once.Do(rawCancel) }
				unlock := func() { release.Do(h.m.mu.Unlock) }
				err = receiverTestBoundedCall(t, done, unlock, func() error { return h.m.Rebootstrap(busy) })
				unlock()
			case "cancel":
				cancel()
				err = h.m.Rebootstrap(ctx)
			case "preflight":
				original := h.source.policies["images"]
				p := original
				p.Mode = "image"
				h.source.policies["images"] = p
				err = h.m.Rebootstrap(ctx)
				h.source.policies["images"] = original
			default:
				if strings.HasPrefix(phase, "create-") {
					h.source.failure = func(args []string) error {
						if len(args) > 5 && slices.Equal(args[:6], []string{"rbd", "mirror", "pool", "peer", "bootstrap", "create"}) {
							return errors.New("private create reply loss")
						}
						return nil
					}
					if phase == "create-lost-reply" {
						h.source.hook = func(_ context.Context, args []string) {
							if len(args) > 5 && slices.Equal(args[:6], []string{"rbd", "mirror", "pool", "peer", "bootstrap", "create"}) {
								createSideEffects++
							}
						}
					}
				}
				if phase == "import-loss" {
					h.destination.importErr = errors.New("private applied import reply loss")
				}
				err = h.m.Rebootstrap(ctx)
			}
			if phase == "busy" || phase == "cancel" || phase == "preflight" {
				if err == nil || h.m.receiverPeer != old || h.m.receiverPeerGeneration != generation {
					t.Fatal("native-zero rejection invalidated bound capability", err)
				}
				if status, err := view.ReceiverStatus(t.Context()); err != nil || !status.Ready {
					t.Fatal("preserved view stopped observing original scope", err)
				}
				return
			}
			if phase == "success" && err != nil || phase != "success" && err == nil || h.m.receiverPeer == old || h.m.receiverPeerGeneration == generation {
				t.Fatal("actual bootstrap attempt preserved old authority", err)
			}
			if phase == "create-lost-reply" && createSideEffects != 1 {
				t.Fatal("lost create response did not follow actual simulated source mutation")
			}
			if status, err := view.ReceiverStatus(t.Context()); err == nil || status.Ready {
				t.Fatal("old bound view reseated bootstrap authority", err)
			}
			if phase == "success" {
				fresh := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
				if fresh.scope.binding.peer == old {
					t.Fatal("fresh explicit Bind used old generation")
				}
				if status, err := fresh.WaitReceiverReady(t.Context()); err != nil || !status.Ready {
					t.Fatal("fresh explicit Bind cannot observe successful new owner capability", err)
				}
			}
		})
	}
}

func TestRBDNamespaceBindingUnavailableAndClosedOwner(t *testing.T) {
	for _, view := range []*RBDMirrorNamespace{nil, {}, {owner: &RBDMirror{}}} {
		if status, err := view.ReceiverStatus(t.Context()); err == nil || status.Ready {
			t.Fatal("zero view provided readiness")
		}
		if status, err := view.WaitReceiverReady(t.Context()); err == nil || status.Ready {
			t.Fatal("zero view wait provided readiness")
		}
	}
	h := newNamespaceBindingHarness(t, 0)
	view := namespaceBindingMustBind(t, h, "ns-c", "ns-d")
	if err := h.m.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := len(h.source.calls) + len(h.destination.calls)
	if status, err := view.ReceiverStatus(t.Context()); err == nil || status.Ready || before != len(h.source.calls)+len(h.destination.calls) {
		t.Fatal("closed owner view reached native or adopted resource")
	}
}
