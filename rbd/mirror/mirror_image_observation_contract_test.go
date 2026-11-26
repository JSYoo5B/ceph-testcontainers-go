package mirror

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// These controls substitute only CLI output and Docker process state. Both
// public observer paths retain their production admission, readers and loops.
type imageObserver interface {
	ImageStatus(context.Context, string) (ImageStatus, error)
	WaitReplayReady(context.Context, string) (ImageStatus, error)
	WatchImage(context.Context, ceph.WatchConfig, string) (*ImageWatch, error)
}

func contractImageObserver(t *testing.T, scoped bool) (*namespaceImageHarness, imageObserver) {
	t.Helper()
	h := newNamespaceImageHarness(t, 2)
	if scoped {
		return h, namespaceBindingMustBind(t, h.receiverHarness, "ns-a", "ns-b")
	}
	return h, h.m
}

func TestImageObserversRejectAmbiguousNativeReports(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "owner"
		if scoped {
			name = "namespace"
		}
		for _, fault := range []string{"duplicate-name", "duplicate-global", "duplicate-mirroring", "null-mirroring", "null-disabled-id", "null-disabled-primary", "null-service"} {
			t.Run(name+"/"+fault, func(t *testing.T) {
				h, observer := contractImageObserver(t, scoped)
				corrupt := func() {
					spec := "images/ns-b/volume"
					switch fault {
					case "duplicate-name":
						h.destinationInfo[spec] = strings.Replace(h.destinationInfo[spec], `"name":"volume"`, `"name":"other","name":"volume"`, 1)
					case "duplicate-global":
						h.statuses[spec] = strings.Replace(h.statuses[spec], `"global_id":`, `"global_id":"other","global_id":`, 1)
					case "duplicate-mirroring":
						h.destinationInfo[spec] = strings.Replace(h.destinationInfo[spec], `"mirroring":`, `"mirroring":null,"mirroring":`, 1)
					case "null-mirroring":
						h.destinationInfo[spec] = `{"name":"volume","id":"destination-0","mirroring":null}`
					case "null-disabled-id":
						h.destinationInfo[spec] = `{"name":"volume","id":"destination-0","mirroring":{"mode":"journal","state":"disabled","global_id":null}}`
					case "null-disabled-primary":
						h.destinationInfo[spec] = `{"name":"volume","id":"destination-0","mirroring":{"mode":"journal","state":"disabled","primary":null}}`
					case "null-service":
						h.statuses[spec] = namespaceImagePatch(t, h.statuses[spec], func(fields map[string]any) { fields["daemon_service"] = nil })
					}
				}
				// A valid baseline followed by invalid native output must terminate the
				// public watch, rather than publish an enrollment/non-ready change.
				w, err := observer.WatchImage(t.Context(), ceph.WatchConfig{Interval: 100 * time.Millisecond, Buffer: 8}, "volume")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = w.Stop() })
				if event := nextImageEvent(t, w); event.Kind != ceph.WatchBaseline {
					t.Fatal(event)
				}
				changeMirror(h.m, corrupt)
				drainImageWatch(t, w)
				if err := w.Stop(); err == nil {
					t.Fatal("later malformed report did not terminate watch")
				}
				if status, err := observer.ImageStatus(t.Context(), "volume"); err == nil || status.ReplayReady || !scopedRBDImagePermanent(err) {
					t.Fatalf("malformed observation accepted: %+v %v", status, err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if _, err := observer.WaitReplayReady(ctx, "volume"); err == nil || errors.Is(err, context.DeadlineExceeded) || !scopedRBDImagePermanent(err) {
					t.Fatalf("malformed wait was retried: %v", err)
				}
				if next, err := observer.WatchImage(t.Context(), ceph.WatchConfig{}, "volume"); err == nil {
					_ = next.Stop()
					t.Fatal("malformed baseline accepted")
				}
			})
		}
	}
}

func TestImageObserversRequireSelectedReceiverGeneration(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "owner"
		if scoped {
			name = "namespace"
		}
		for _, fault := range []string{"foreign-peer", "undiscovered", "wrong-mapping", "pid", "started", "unrelated-stopped", "no-election"} {
			t.Run(name+"/"+fault, func(t *testing.T) {
				h, observer := contractImageObserver(t, scoped)
				switch fault {
				case "foreign-peer":
					h.daemons[0].status = strings.ReplaceAll(h.daemons[0].status, receiverPeerUUID, receiverReplacementUUID)
				case "undiscovered":
					h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) { row["namespace_replayers"] = []any{} })
				case "wrong-mapping":
					h.daemons[0].status = namespaceBindingSocket(t, h.daemons[0].status, [][2]string{{"ns-b", "foreign"}})
				case "pid":
					h.daemons[0].hook = func(context.Context, int) { h.daemons[0].state.Pid++ }
				case "started":
					h.daemons[0].hook = func(context.Context, int) { h.daemons[0].state.StartedAt = "2026-10-07T10:00:00Z" }
				case "unrelated-stopped":
					h.daemons[1].state.Running = false
				case "no-election":
					h.daemons[0].status = receiverPatchStatus(t, h.daemons[0].status, func(row map[string]any) {
						row["leader"] = false
						row["leader_instance_id"] = "999"
						delete(row, "instances")
					})
				}
				status, err := observer.ImageStatus(t.Context(), "volume")
				want := fault == "unrelated-stopped" || fault == "no-election"
				if err != nil || status.ReplayReady != want {
					t.Fatalf("receiver attribution: %+v %v", status, err)
				}
			})
		}
	}
}

func TestImageObserversRequireOriginalReceiverAuthority(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		for _, fault := range []string{"handle substitution", "unconfirmed startup", "native peer replaced", "native cluster replaced", "peer changed during read"} {
			t.Run(fmt.Sprintf("scoped=%t/%s", scoped, fault), func(t *testing.T) {
				h, observer := contractImageObserver(t, scoped)
				status, err := observer.ImageStatus(t.Context(), "volume")
				if err != nil || !status.ReplayReady {
					t.Fatalf("positive control: %+v %v", status, err)
				}
				switch fault {
				case "handle substitution":
					replacement := *h.daemons[0]
					h.m.roster.daemons[0].Container = &replacement
				case "unconfirmed startup":
					h.m.roster.daemons[0].receiverStartupConfirmed = false
				case "native peer replaced":
					h.destination.peers[0].UUID = receiverReplacementUUID
				case "native cluster replaced":
					h.destination.fsid = receiverSourceFSID
				case "peer changed during read":
					h.daemons[0].hook = func(context.Context, int) { h.destination.peers[0].UUID = receiverReplacementUUID }
				}
				status, err = observer.ImageStatus(t.Context(), "volume")
				if status.ReplayReady || (fault != "unconfirmed startup" && !scopedRBDImagePermanent(err)) {
					t.Fatalf("unconfirmed original receiver became ready: %+v %v", status, err)
				}
			})
		}
	}
}

func TestImageObserversPreserveNativeOmittedForms(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		name := "owner"
		if scoped {
			name = "namespace"
		}
		for _, form := range []string{"disabled", "journal-disabled", "no-local-report", "no-service"} {
			t.Run(name+"/"+form, func(t *testing.T) {
				h, observer := contractImageObserver(t, scoped)
				spec := "images/ns-b/volume"
				switch form {
				case "disabled":
					h.destinationInfo[spec] = `{"name":"volume","id":"destination-0"}`
				case "journal-disabled":
					h.destinationInfo[spec] = `{"name":"volume","id":"destination-0","mirroring":{"mode":"journal","state":"disabled"}}`
				case "no-local-report":
					h.statuses[spec] = `{"name":"volume","global_id":"global-0"}`
				case "no-service":
					h.statuses[spec] = namespaceImagePatch(t, h.statuses[spec], func(fields map[string]any) { delete(fields, "daemon_service") })
				}
				status, err := observer.ImageStatus(t.Context(), "volume")
				if status.ReplayReady || err != nil && !rbdImagePending(err) {
					t.Fatalf("native omission rejected: %+v %v", status, err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
				defer cancel()
				if _, err := observer.WaitReplayReady(ctx, "volume"); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("pending wait became terminal: %v", err)
				}
				w, err := observer.WatchImage(t.Context(), ceph.WatchConfig{}, "volume")
				if err != nil {
					t.Fatal(err)
				}
				if event := nextImageEvent(t, w); event.Kind != ceph.WatchBaseline || event.Status.ReplayReady {
					t.Fatal(event)
				}
				if err := w.Stop(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
