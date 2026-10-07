package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
)

type namespaceImageHarness struct {
	*receiverHarness
	sourceInfo, destinationInfo, statuses map[string]string
	imageCalls                            [][]string
}

func newNamespaceImageHarness(t *testing.T, count int) *namespaceImageHarness {
	t.Helper()
	h := namespaceImageHarnessForReceiver(t, newNamespaceBindingHarness(t, count))
	for i, mapping := range [][2]string{{"ns-a", "ns-b"}, {"ns-c", "ns-d"}, {"ns-e", "ns-f"}} {
		mode := RBDMirrorModeSnapshot
		if i == 2 {
			mode = RBDMirrorModeJournal
		}
		h.setImages(mapping, mode, fmt.Sprint(i))
	}
	return h
}

func namespaceImageHarnessForReceiver(t *testing.T, base *receiverHarness) *namespaceImageHarness {
	t.Helper()
	h := &namespaceImageHarness{receiverHarness: base, sourceInfo: make(map[string]string), destinationInfo: make(map[string]string), statuses: make(map[string]string)}
	for _, client := range []*receiverTestClient{h.source, h.destination} {
		old := client.output
		client.output = func(args []string) (string, bool) {
			if len(args) == 5 && slices.Equal(args[:2], []string{"rbd", "info"}) {
				h.imageCalls = append(h.imageCalls, slices.Clone(args))
				if client == h.source {
					return h.sourceInfo[args[2]], true
				}
				return h.destinationInfo[args[2]], true
			}
			if len(args) == 7 && slices.Equal(args[:4], []string{"rbd", "mirror", "image", "status"}) {
				h.imageCalls = append(h.imageCalls, slices.Clone(args))
				return h.statuses[args[4]], true
			}
			if old != nil {
				if data, ok := old(args); ok {
					return data, ok
				}
			}
			if slices.Equal(args, []string{"rbd", "namespace", "list", "--pool", "images", "--format", "json"}) {
				var entries []map[string]string
				for spec := range client.policies {
					if ns, ok := strings.CutPrefix(spec, "images/"); ok {
						entries = append(entries, map[string]string{"name": ns})
					}
				}
				data, _ := json.Marshal(entries)
				return string(data), true
			}
			return "", false
		}
	}
	return h
}

func (h *namespaceImageHarness) setImages(mapping [2]string, mode RBDMirrorMode, suffix string) {
	source := rbdMirrorImageSpec("images", mapping[0], "volume")
	destination := rbdMirrorImageSpec("images", mapping[1], "volume")
	global := "global-" + suffix
	h.sourceInfo[source] = rbdImageStatusInfo("source-"+suffix, string(mode), global, true)
	h.destinationInfo[destination] = rbdImageStatusInfo("destination-"+suffix, string(mode), global, false)
	h.statuses[destination] = fmt.Sprintf(`{"name":"volume","global_id":%q,"state":"up+replaying","description":"replaying","last_update":"2026-10-07 00:00:00","daemon_service":{"service_id":"22","instance_id":"100","daemon_id":"tc-a"}}`, global)
}

func namespaceImagePatch(t *testing.T, input string, patch func(map[string]any)) string {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal([]byte(input), &value); err != nil {
		t.Fatal(err)
	}
	patch(value)
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func namespaceImageReadOnly(t *testing.T, h *namespaceImageHarness) {
	t.Helper()
	for _, client := range []*receiverTestClient{h.source, h.destination} {
		for _, args := range client.calls {
			if slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "fsid"}) || slices.Equal(args, []string{"ceph", "--connect-timeout", "5", "osd", "pool", "ls", "detail", "--format", "json"}) || slices.Equal(args, []string{"rbd", "namespace", "list", "--pool", "images", "--format", "json"}) || len(args) == 7 && (slices.Equal(args[:4], []string{"rbd", "mirror", "pool", "info"}) || slices.Equal(args[:4], []string{"rbd", "mirror", "image", "status"})) || len(args) == 5 && slices.Equal(args[:2], []string{"rbd", "info"}) {
				continue
			}
			t.Fatalf("scoped image observer issued mutation/unexpected native command: %v", args)
		}
	}
}

func TestRBDNamespaceImageStatusUsesBoundScopesAndActualModes(t *testing.T) {
	h := newNamespaceImageHarness(t, 1)
	config, policies, peer := h.m.config, h.m.policyIdentities, h.m.receiverPeer
	for i, mapping := range [][2]string{{"ns-a", "ns-b"}, {"ns-c", "ns-d"}, {"ns-e", "ns-f"}} {
		view := namespaceBindingMustBind(t, h.receiverHarness, mapping[0], mapping[1])
		status, err := view.ImageStatus(t.Context(), "volume")
		mode := RBDMirrorModeSnapshot
		if i == 2 {
			mode = RBDMirrorModeJournal
		}
		if err != nil || !status.ReplayReady || status.Mode != mode || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] || status.GlobalID != fmt.Sprint("global-", i) || status.SourceImageID != fmt.Sprint("source-", i) || status.DestinationImageID != fmt.Sprint("destination-", i) || status.DaemonName != "a" || status.InstanceID != "100" {
			t.Fatalf("bound image report: %+v %v", status, err)
		}
		if len(h.imageCalls) < 5 || h.imageCalls[len(h.imageCalls)-5][2] != rbdMirrorImageSpec("images", mapping[0], "volume") || h.imageCalls[len(h.imageCalls)-3][4] != rbdMirrorImageSpec("images", mapping[1], "volume") {
			t.Fatal("image observer redirected its bound pair")
		}
	}
	// Image-scoped native policy can also contain a journal image.
	h.setImages([2]string{"ns-c", "ns-d"}, RBDMirrorModeJournal, "journal-image")
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
	if status, err := view.ImageStatus(t.Context(), "volume"); err != nil || !status.ReplayReady || status.Mode != RBDMirrorModeJournal {
		t.Fatalf("image policy native journal: %+v %v", status, err)
	}
	owner, err := h.m.ImageStatus(t.Context(), "volume")
	if err != nil || !owner.ReplayReady || owner.SourceNamespace != "ns-a" || owner.Mode != RBDMirrorModeSnapshot || h.m.config != config || h.m.policyIdentities != policies || h.m.receiverPeer != peer || len(h.m.daemons) != 1 {
		t.Fatal("view changed original owner contract", err)
	}
	namespaceImageReadOnly(t, h)
}

func TestRBDNamespaceImageStatusDefaultNamespaceSelections(t *testing.T) {
	for _, mapping := range [][2]string{{"", ""}, {"", "ns-b"}, {"ns-a", ""}} {
		t.Run(fmt.Sprint(mapping), func(t *testing.T) {
			h := namespaceImageHarnessForReceiver(t, newReceiverHarness(t, mapping[0], mapping[1], 1))
			h.setImages(mapping, RBDMirrorModeSnapshot, "default")
			view := namespaceBindingMustBind(t, h.receiverHarness, mapping[0], mapping[1])
			status, err := view.ImageStatus(t.Context(), "volume")
			if err != nil || !status.ReplayReady || status.SourceNamespace != mapping[0] || status.DestinationNamespace != mapping[1] {
				t.Fatalf("default namespace selection: %+v %v", status, err)
			}
			namespaceImageReadOnly(t, h)
		})
	}
}

func TestRBDNamespaceImageStatusModeAndNativeSchemaGuards(t *testing.T) {
	for _, fault := range []string{"source-missing-mode", "source-null-mode", "source-unknown-mode", "destination-mode", "pool-snapshot", "global-id", "info-duplicate", "info-trailing", "status-duplicate", "status-null-service", "status-incomplete-service", "source-second-mode", "destination-second-id"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-e", "ns-f")
			src, dst := "images/ns-e/volume", "images/ns-f/volume"
			calls := 0
			switch fault {
			case "source-missing-mode", "source-null-mode", "source-unknown-mode":
				h.sourceInfo[src] = namespaceImagePatch(t, h.sourceInfo[src], func(v map[string]any) {
					mirror := v["mirroring"].(map[string]any)
					if fault == "source-missing-mode" {
						delete(mirror, "mode")
					} else if fault == "source-null-mode" {
						mirror["mode"] = nil
					} else {
						mirror["mode"] = "unknown"
					}
				})
			case "destination-mode":
				h.destinationInfo[dst] = rbdImageStatusInfo("destination-2", "snapshot", "global-2", false)
			case "pool-snapshot":
				h.setImages([2]string{"ns-e", "ns-f"}, RBDMirrorModeSnapshot, "2")
			case "global-id":
				h.destinationInfo[dst] = rbdImageStatusInfo("destination-2", "journal", "foreign", false)
			case "info-duplicate":
				h.sourceInfo[src] = strings.Replace(h.sourceInfo[src], `"name":"volume"`, `"name":"foreign","name":"volume"`, 1)
			case "info-trailing":
				h.sourceInfo[src] += `{}`
			case "status-duplicate":
				h.statuses[dst] = strings.Replace(h.statuses[dst], `"name":"volume"`, `"name":"foreign","name":"volume"`, 1)
			case "status-null-service":
				h.statuses[dst] = namespaceImagePatch(t, h.statuses[dst], func(v map[string]any) { v["daemon_service"] = nil })
			case "status-incomplete-service":
				h.statuses[dst] = namespaceImagePatch(t, h.statuses[dst], func(v map[string]any) { delete(v["daemon_service"].(map[string]any), "instance_id") })
			case "source-second-mode":
				h.source.hook = func(_ context.Context, args []string) {
					if len(args) > 2 && args[1] == "info" {
						calls++
						if calls == 2 {
							h.sourceInfo[src] = rbdImageStatusInfo("source-2", "snapshot", "global-2", true)
						}
					}
				}
			case "destination-second-id":
				h.destination.hook = func(_ context.Context, args []string) {
					if len(args) > 2 && args[1] == "info" {
						calls++
						if calls == 2 {
							h.destinationInfo[dst] = rbdImageStatusInfo("replacement", "journal", "global-2", false)
						}
					}
				}
			}
			status, err := view.ImageStatus(t.Context(), "volume")
			if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady {
				t.Fatalf("accepted malformed/differing %s: %+v %v", fault, status, err)
			}
			namespaceImageReadOnly(t, h)
		})
	}
}

func TestRBDNamespaceImageStatusTransitionsAndMissingReports(t *testing.T) {
	for _, transition := range []string{"source-creating", "destination-disabling", "missing-local-report"} {
		t.Run(transition, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			if transition == "missing-local-report" {
				h.statuses["images/ns-d/volume"] = `{"name":"volume","global_id":"global-1"}`
			} else {
				dst, values, state := "images/ns-c/volume", h.sourceInfo, "creating"
				if transition == "destination-disabling" {
					dst, values, state = "images/ns-d/volume", h.destinationInfo, "disabling"
				}
				values[dst] = namespaceImagePatch(t, values[dst], func(v map[string]any) { v["mirroring"].(map[string]any)["state"] = state })
			}
			status, err := view.ImageStatus(t.Context(), "volume")
			if err != nil || status.ReplayReady || status.SourceImageID == "" || status.DestinationImageID == "" || status.Mode != RBDMirrorModeSnapshot {
				t.Fatalf("transition report: %+v %v", status, err)
			}
		})
	}
}

func TestRBDNamespaceImageStatusAttributedOwnedProcess(t *testing.T) {
	for _, fault := range []string{"stopped", "paused", "restarting", "dead", "terminated", "startup", "foreign-service", "wrong-instance", "foreign-peer", "wrong-namespace", "process-changed", "handle-swapped", "unrelated-stopped", "no-election"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 2)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			switch fault {
			case "stopped":
				h.daemons[0].state.Running = false
			case "paused":
				h.daemons[0].state.Paused = true
			case "restarting":
				h.daemons[0].state.Restarting = true
			case "dead":
				h.daemons[0].state.Dead = true
			case "terminated":
				h.m.daemons[0].terminated = true
			case "startup":
				h.m.daemons[0].receiverStartupConfirmed = false
			case "foreign-service":
				h.statuses["images/ns-d/volume"] = strings.ReplaceAll(h.statuses["images/ns-d/volume"], "tc-a", "tc-foreign")
			case "wrong-instance":
				h.statuses["images/ns-d/volume"] = strings.ReplaceAll(h.statuses["images/ns-d/volume"], `"instance_id":"100"`, `"instance_id":"999"`)
			case "foreign-peer":
				h.daemons[0].status = strings.ReplaceAll(h.daemons[0].status, receiverPeerUUID, receiverReplacementUUID)
			case "wrong-namespace":
				h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"other", "other"}})
			case "process-changed":
				h.daemons[0].hook = func(_ context.Context, _ int) { h.daemons[0].state.Pid++ }
			case "handle-swapped":
				h.daemons[0].hook = func(_ context.Context, _ int) { h.m.daemons[0].Container = h.daemons[1] }
			case "unrelated-stopped":
				h.daemons[1].state.Running = false
			case "no-election":
				h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) {
					row["leader"] = false
					delete(row, "instances")
					row["leader_instance_id"] = "999"
				})
			}
			status, err := view.ImageStatus(t.Context(), "volume")
			want := fault == "unrelated-stopped" || fault == "no-election"
			if status.ReplayReady != want || want && err != nil {
				t.Fatalf("attributed live process %s: %+v %v", fault, status, err)
			}
			if fault == "handle-swapped" && (err == nil || !scopedRBDImagePermanent(err)) {
				t.Fatal("late owned handle substitution was not rejected")
			}
		})
	}
}

func TestRBDNamespaceImageStatusReattestsAllOriginalAuthority(t *testing.T) {
	for _, fault := range []string{"base-before", "owner-selected-before", "bound-selected-before", "selected-during-status", "base-during-process", "fsid", "pool-id", "catalog", "setup-handle-late", "closed", "partial-setup"} {
		t.Run(fault, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			change := func(client *receiverTestClient, spec string) {
				p := client.policies[spec]
				p.MirrorUUID = receiverReplacementUUID
				client.policies[spec] = p
			}
			switch fault {
			case "base-before":
				change(h.source, "images")
			case "owner-selected-before":
				p := h.source.policies["images/ns-a"]
				p.RemoteNamespace = stringPointer("other")
				h.source.policies["images/ns-a"] = p
			case "bound-selected-before":
				change(h.source, "images/ns-c")
			case "selected-during-status":
				h.destination.hook = func(_ context.Context, args []string) {
					if len(args) > 4 && slices.Equal(args[:4], []string{"rbd", "mirror", "image", "status"}) {
						change(h.source, "images/ns-c")
					}
				}
			case "base-during-process":
				h.daemons[0].hook = func(_ context.Context, _ int) { change(h.source, "images") }
			case "fsid":
				h.source.fsid = receiverDestinationFSID
			case "pool-id":
				h.destination.poolID++
			case "catalog":
				delete(h.source.policies, "images/ns-c")
			case "setup-handle-late":
				h.daemons[0].hook = func(_ context.Context, _ int) { h.m.sourceClient = h.destination }
			case "closed":
				h.m.closed = true
			case "partial-setup":
				h.m.receiverSetupConfirmed = false
			}
			status, err := view.ImageStatus(t.Context(), "volume")
			if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady {
				t.Fatalf("original authority %s accepted: %+v %v", fault, status, err)
			}
		})
	}
}

func TestRBDNamespaceImageWaitPinsFirstCohortBeforeAnyImage(t *testing.T) {
	for _, state := range []string{"empty", "source-query-error", "destination-pending"} {
		t.Run(state, func(t *testing.T) {
			count := 1
			if state == "empty" {
				count = 0
			}
			h := newNamespaceImageHarness(t, count)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			fresh := newReceiverHarness(t, "ns-a", "ns-b", 1)
			calls := 0
			client := h.source
			if state == "destination-pending" {
				client = h.destination
			}
			client.failure = func(args []string) error {
				if len(args) > 2 && args[1] == "info" {
					calls++
					if calls == 1 {
						h.m.daemons = fresh.m.daemons
						return errors.New("private pending image query")
					}
				}
				return nil
			}
			status, err := view.waitReplayReady(t.Context(), time.Millisecond, "volume")
			if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady || calls != 1 {
				t.Fatalf("first admitted cohort adopted %s: %+v %v calls=%d", state, status, err, calls)
			}
			if state == "destination-pending" && status.SourceImageID != "source-1" {
				t.Fatal("lost first useful source report")
			}
		})
	}
}

func TestRBDNamespaceImageWaitPinsModeAndImageDuringDestinationPending(t *testing.T) {
	for _, change := range []string{"mode", "source-id", "global-id"} {
		t.Run(change, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			h.setImages([2]string{"ns-c", "ns-d"}, RBDMirrorModeJournal, "1")
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			calls := 0
			h.destination.failure = func(args []string) error {
				if len(args) > 2 && args[1] == "info" {
					calls++
					if calls == 1 {
						mode, id, global := "journal", "source-1", "global-1"
						if change == "mode" {
							mode = "snapshot"
							h.destinationInfo["images/ns-d/volume"] = rbdImageStatusInfo("destination-1", mode, global, false)
						}
						if change == "source-id" {
							id = "replacement"
						}
						if change == "global-id" {
							global = "replacement-global"
							h.destinationInfo["images/ns-d/volume"] = rbdImageStatusInfo("destination-1", mode, global, false)
							h.statuses["images/ns-d/volume"] = strings.ReplaceAll(h.statuses["images/ns-d/volume"], "global-1", global)
						}
						h.sourceInfo["images/ns-c/volume"] = rbdImageStatusInfo(id, mode, global, true)
						return errors.New("private destination not yet present")
					}
				}
				return nil
			}
			status, err := view.waitReplayReady(t.Context(), time.Millisecond, "volume")
			if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady || calls != 3 {
				t.Fatalf("adopted changed %s while pending: %+v %v calls=%d", change, status, err, calls)
			}
		})
	}
}

func TestRBDNamespaceImageWaitAllowsOriginalRestartAndRejectsReplacement(t *testing.T) {
	for _, change := range []string{"restart", "replacement", "remove", "add"} {
		t.Run(change, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			h.daemons[0].state.Running = false
			calls := 0
			h.source.hook = func(_ context.Context, args []string) {
				if len(args) > 2 && args[1] == "info" {
					calls++
					if calls == 3 {
						switch change {
						case "restart":
							h.daemons[0].state = container.State{Running: true, Status: container.StateRunning, Pid: 52, StartedAt: "2026-10-07T11:00:00Z"}
							h.daemons[0].status = strings.ReplaceAll(h.daemons[0].status, `"100"`, `"200"`)
							h.statuses["images/ns-d/volume"] = strings.ReplaceAll(h.statuses["images/ns-d/volume"], `"instance_id":"100"`, `"instance_id":"200"`)
						case "replacement":
							h.m.daemons = newReceiverHarness(t, "ns-a", "ns-b", 1).m.daemons
						case "remove":
							h.m.daemons = nil
						case "add":
							fresh := newReceiverHarness(t, "ns-a", "ns-b", 2)
							h.m.daemons = append(h.m.daemons, fresh.m.daemons[1])
						}
					}
				}
			}
			status, err := view.waitReplayReady(t.Context(), time.Millisecond, "volume")
			if change == "restart" {
				if err != nil || !status.ReplayReady || status.InstanceID != "200" {
					t.Fatalf("original same-CID restart rejected: %+v %v", status, err)
				}
			} else if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady {
				t.Fatalf("old wait adopted %s: %+v %v", change, status, err)
			}
		})
	}
}

func TestRBDNamespaceImageObserverPreservesCanonicalCausesAndGates(t *testing.T) {
	for _, gate := range []string{"owner", "member"} {
		t.Run(gate, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			unlock := h.m.mu.Unlock
			if gate == "owner" {
				h.m.mu.Lock()
			} else {
				h.m.daemons[0].mu.Lock()
				unlock = h.m.daemons[0].mu.Unlock
			}
			type observed struct {
				status RBDMirrorImageStatus
				err    error
			}
			got := receiverTestBoundedCall(t, cancel, unlock, func() observed { s, e := view.ImageStatus(ctx, "volume"); return observed{s, e} })
			if got.err == nil || !errors.Is(got.err, context.DeadlineExceeded) || got.status.ReplayReady {
				t.Fatalf("held %s gate: %+v %v", gate, got.status, got.err)
			}
		})
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			h.destination.failure = func(args []string) error {
				if len(args) > 2 && args[1] == "info" {
					return errors.Join(fmt.Errorf("native secret-sentinel: %w", cause), errors.New("private secret-sentinel transport"))
				}
				return nil
			}
			status, err := view.ImageStatus(t.Context(), "volume")
			if err == nil || !errors.Is(err, cause) || status.ReplayReady || status.SourceImageID != "source-1" {
				t.Fatalf("canonical partial query result: %+v %v", status, err)
			}
			var check func(error)
			check = func(e error) {
				if e == nil {
					return
				}
				if strings.Contains(e.Error(), "secret-sentinel") {
					t.Fatal("new observer retained private error body")
				}
				if j, ok := e.(interface{ Unwrap() []error }); ok {
					for _, child := range j.Unwrap() {
						check(child)
					}
				} else if w, ok := e.(interface{ Unwrap() error }); ok {
					check(w.Unwrap())
				}
			}
			check(err)
		})
	}
	// A final policy query that observes caller cancellation cannot return ready.
	h := newNamespaceImageHarness(t, 1)
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.daemons[0].hook = func(_ context.Context, _ int) { cancel() }
	status, err := view.ImageStatus(ctx, "volume")
	if err == nil || !errors.Is(err, context.Canceled) || status.ReplayReady {
		t.Fatalf("late cancellation succeeded: %+v %v", status, err)
	}
}

func TestRBDNamespaceImageWaitReleasesOwnerAndRetainsLastReport(t *testing.T) {
	h := newNamespaceImageHarness(t, 1)
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
	h.daemons[0].state.Running = false
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	h.source.failure = func(args []string) error {
		if len(args) > 2 && args[1] == "info" {
			calls++
			if calls == 3 {
				cancel()
				return errors.New("private after-useful-report")
			}
		}
		return nil
	}
	// The legacy loop callback boundary occurs after ImageStatus releases owner.
	var observed bool
	status, err := waitRBDMirrorReplay(ctx, time.Millisecond, func(attempt context.Context) (RBDMirrorImageStatus, error) {
		s, e := view.ImageStatus(attempt, "volume")
		if !h.m.mu.TryLock() {
			t.Fatal("observation held owner across poll boundary")
		}
		h.m.mu.Unlock()
		observed = true
		return s, e
	})
	if !observed || err == nil || !errors.Is(err, context.Canceled) || status.ReplayReady || status.SourceImageID != "source-1" {
		t.Fatalf("last useful report/canceled poll: %+v %v", status, err)
	}
}

func TestRBDNamespaceImageObserverInvalidatesActualBootstrapAttempt(t *testing.T) {
	for _, outcome := range []string{"success", "create-fault", "canceled", "preflight"} {
		t.Run(outcome, func(t *testing.T) {
			h := newNamespaceImageHarness(t, 1)
			view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
			peer := h.m.receiverPeer
			ctx := t.Context()
			if outcome == "canceled" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if outcome == "create-fault" {
				h.source.failure = func(args []string) error {
					if len(args) > 5 && args[5] == "create" {
						return errors.New("bootstrap reply lost")
					}
					return nil
				}
			}
			original := h.source.policies["images/ns-a"]
			if outcome == "preflight" {
				p := original
				p.RemoteNamespace = stringPointer("other")
				h.source.policies["images/ns-a"] = p
			}
			err := h.m.Rebootstrap(ctx)
			if outcome == "success" && err != nil || outcome != "success" && err == nil {
				t.Fatalf("bootstrap outcome %s: %v", outcome, err)
			}
			h.source.policies["images/ns-a"] = original
			h.source.failure = nil
			status, observeErr := view.ImageStatus(t.Context(), "volume")
			if outcome == "success" || outcome == "create-fault" {
				if observeErr == nil || !scopedRBDImagePermanent(observeErr) || status.ReplayReady || h.m.receiverPeer == peer {
					t.Fatal("old view adopted actual bootstrap attempt")
				}
			} else if observeErr != nil || !status.ReplayReady || h.m.receiverPeer != peer {
				t.Fatalf("native-zero rejection invalidated view: %+v %v", status, observeErr)
			}
			if outcome == "success" {
				fresh := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
				if s, e := fresh.ImageStatus(t.Context(), "volume"); e != nil || !s.ReplayReady {
					t.Fatalf("explicit fresh bind not usable: %+v %v", s, e)
				}
			}
		})
	}
}

func TestRBDNamespaceImageObserverRejectsUnavailableBeforeNative(t *testing.T) {
	for _, name := range []string{"", "--option", "pool/volume", "volume@snap", " volume", "volume\n"} {
		h := newNamespaceImageHarness(t, 1)
		view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
		before := len(h.source.calls) + len(h.destination.calls)
		if _, err := view.ImageStatus(t.Context(), name); err == nil {
			t.Fatalf("unsafe name %q accepted", name)
		}
		if len(h.source.calls)+len(h.destination.calls) != before {
			t.Fatal("unsafe name reached native CLI")
		}
	}
	for _, view := range []*RBDMirrorNamespace{nil, {}, {owner: &RBDMirror{}}} {
		if s, e := view.ImageStatus(t.Context(), "volume"); e == nil || s.ReplayReady {
			t.Fatal("unavailable image view accepted")
		}
		if s, e := view.WaitReplayReady(t.Context(), "volume"); e == nil || s.ReplayReady {
			t.Fatal("unavailable replay wait accepted")
		}
	}
}

func TestRBDNamespaceImageWaitAcceptsPendingDestinationAndCurrentOneShotCohort(t *testing.T) {
	h := newNamespaceImageHarness(t, 1)
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-e", "ns-f")
	calls := 0
	h.destination.failure = func(args []string) error {
		if len(args) > 2 && args[1] == "info" {
			calls++
			if calls == 1 {
				return errors.New("destination pending secret")
			}
		}
		return nil
	}
	status, err := view.waitReplayReady(t.Context(), time.Millisecond, "volume")
	if err != nil || !status.ReplayReady || status.Mode != RBDMirrorModeJournal || status.SourceImageID != "source-2" || status.DestinationImageID != "destination-2" || calls != 3 {
		t.Fatalf("legitimate pending destination failed: %+v %v calls=%d", status, err, calls)
	}
	zero := newNamespaceImageHarness(t, 0)
	bound := namespaceBindingMustBind(t, zero.receiverHarness, "ns-c", "ns-d")
	if s, e := bound.ImageStatus(t.Context(), "volume"); e != nil || s.ReplayReady || s.SourceImageID == "" {
		t.Fatalf("zero owner observation: %+v %v", s, e)
	}
	fresh := newNamespaceImageHarness(t, 1)
	zero.m.daemons = fresh.m.daemons
	if s, e := bound.ImageStatus(t.Context(), "volume"); e != nil || !s.ReplayReady {
		t.Fatalf("new one-shot did not use explicit current cohort: %+v %v", s, e)
	}
}

func TestRBDNamespaceImageTransitionSecondReadRejectsModeDrift(t *testing.T) {
	h := newNamespaceImageHarness(t, 1)
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
	spec := "images/ns-c/volume"
	h.sourceInfo[spec] = namespaceImagePatch(t, h.sourceInfo[spec], func(v map[string]any) { v["mirroring"].(map[string]any)["state"] = "creating" })
	calls := 0
	h.source.hook = func(_ context.Context, args []string) {
		if len(args) > 2 && args[1] == "info" {
			calls++
			if calls == 2 {
				h.sourceInfo[spec] = namespaceImagePatch(t, h.sourceInfo[spec], func(v map[string]any) { v["mirroring"].(map[string]any)["mode"] = "journal" })
			}
		}
	}
	status, err := view.ImageStatus(t.Context(), "volume")
	if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady || calls != 2 {
		t.Fatalf("creating state skipped source mode recheck: %+v %v", status, err)
	}
}

func TestRBDNamespaceImagePublicWaitDeadlineAndGuardAreSecretSafe(t *testing.T) {
	h := newNamespaceImageHarness(t, 1)
	view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
	h.destination.failure = func(args []string) error {
		if len(args) > 2 && args[1] == "info" {
			return errors.New("private-public-wait-secret")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	status, err := view.WaitReplayReady(ctx, "volume")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || status.ReplayReady || status.SourceImageID != "source-1" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("public bounded wait: %+v %v", status, err)
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), "private-public-wait-secret") {
			t.Fatal("public wait retained private body")
		}
	}
	// Translate a mixed receiver permanent guard/query once, then stop the shared
	// legacy loop rather than adopting another valid status on a later attempt.
	calls := 0
	safe := scopedRBDImageError(t.Context(), errors.Join(rbdReceiverQuery(t.Context(), "native query", errors.New("mixed-secret")), fmt.Errorf("wrapped: %w", rbdReceiverGuard("fixed guard"))))
	_, err = waitRBDMirrorReplay(t.Context(), time.Millisecond, func(context.Context) (RBDMirrorImageStatus, error) { calls++; return RBDMirrorImageStatus{}, safe })
	if err == nil || !scopedRBDImagePermanent(err) || calls != 1 || strings.Contains(err.Error(), "mixed-secret") {
		t.Fatalf("mixed guard did not stop immediately: calls=%d %v", calls, err)
	}
}

func TestRBDNamespaceImageStatusStateProgressRetainsIdentityGuards(t *testing.T) {
	for _, site := range []string{"source", "destination"} {
		for _, change := range []string{"progress", "disabling", "id", "global", "mode", "primary"} {
			t.Run(site+"/"+change, func(t *testing.T) {
				h := newNamespaceImageHarness(t, 1)
				view := namespaceBindingMustBind(t, h.receiverHarness, "ns-c", "ns-d")
				client, values, spec := h.source, h.sourceInfo, "images/ns-c/volume"
				if site == "destination" {
					client, values, spec = h.destination, h.destinationInfo, "images/ns-d/volume"
				}
				if change == "progress" {
					values[spec] = namespaceImagePatch(t, values[spec], func(v map[string]any) { v["mirroring"].(map[string]any)["state"] = "creating" })
				}
				calls := 0
				client.hook = func(_ context.Context, args []string) {
					if len(args) > 2 && args[1] == "info" {
						calls++
						if calls == 2 {
							values[spec] = namespaceImagePatch(t, values[spec], func(v map[string]any) {
								m := v["mirroring"].(map[string]any)
								switch change {
								case "progress":
									m["state"] = "enabled"
								case "disabling":
									m["state"] = "disabling"
								case "id":
									v["id"] = "new-local-id"
								case "global":
									m["global_id"] = "new-global-id"
								case "mode":
									m["mode"] = "journal"
								case "primary":
									m["primary"] = !m["primary"].(bool)
								}
							})
						}
					}
				}
				status, err := view.ImageStatus(t.Context(), "volume")
				if change == "progress" || change == "disabling" {
					if err != nil || status.ReplayReady || calls != 2 {
						t.Fatalf("valid state progress permanently rejected: %+v %v calls=%d", status, err, calls)
					}
					if change == "progress" {
						ready, e := view.waitReplayReady(t.Context(), time.Millisecond, "volume")
						if e != nil || !ready.ReplayReady || ready.SourceImageID != "source-1" || ready.DestinationImageID != "destination-1" {
							t.Fatalf("later stable state did not replay: %+v %v", ready, e)
						}
					}
				} else if err == nil || !scopedRBDImagePermanent(err) || status.ReplayReady {
					t.Fatalf("identity drift %s softened to state progress: %+v %v", change, status, err)
				}
			})
		}
	}
}
