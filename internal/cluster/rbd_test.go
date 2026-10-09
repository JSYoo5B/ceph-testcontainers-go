package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRBDResourceValidationDoesNotExecuteCommands(t *testing.T) {
	for _, name := range []string{"", "--option", ".private", "with space", "pool/ns", "ns\n", strings.Repeat("a", 129)} {
		control := newRBDTestControl()
		cluster := rbdTestCluster(control)
		if err := cluster.initRBDPool(t.Context(), name); err == nil {
			t.Errorf("invalid pool %q accepted", name)
		}
		if _, err := cluster.namespaceList(t.Context(), name); err == nil {
			t.Errorf("invalid listing pool %q accepted", name)
		}
		if _, err := cluster.createRBDNamespace(t.Context(), name, "blue"); err == nil {
			t.Errorf("invalid namespace pool %q accepted", name)
		}
		if _, err := cluster.createRBDNamespace(t.Context(), "images", name); err == nil {
			t.Errorf("invalid namespace %q accepted", name)
		}
		if len(control.calls) != 0 {
			t.Fatalf("invalid names executed commands: %v", control.calls)
		}
	}
}

func TestRBDPoolPreflightRejectsUnsafeMetadata(t *testing.T) {
	for _, scenario := range []string{"missing", "erasure", "foreign application", "mixed applications", "missing ID", "invalid map"} {
		t.Run(scenario, func(t *testing.T) {
			control := newRBDTestControl()
			switch scenario {
			case "missing":
				control.poolName = "other"
			case "erasure":
				control.poolType = 3
			case "foreign application":
				control.apps = map[string]json.RawMessage{"cephfs": json.RawMessage(`{}`)}
			case "mixed applications":
				control.apps["rados"] = json.RawMessage(`{}`)
			case "missing ID":
				control.poolID = nil
			case "invalid map":
				control.mapOverride = `{"pools":null}`
			}
			if err := rbdTestCluster(control).initRBDPool(t.Context(), "images"); err == nil {
				t.Fatal("unsafe metadata pool accepted")
			}
			if !slices.Equal(control.calls, []string{"ceph osd dump --format json"}) {
				t.Fatalf("unsafe metadata executed mutations: %v", control.calls)
			}
		})
	}
}

func TestRBDPoolInitVerifiesActualNativeInitialization(t *testing.T) {
	for _, scenario := range []string{"success", "CLI failure", "false CLI success", "missing native object"} {
		t.Run(scenario, func(t *testing.T) {
			control := newRBDTestControl()
			control.apps = map[string]json.RawMessage{}
			control.trash = false
			switch scenario {
			case "CLI failure":
				control.fail = "rbd pool init --pool images"
			case "false CLI success":
				control.ignoreInit = true
			case "missing native object":
				control.skipTrash = true
			}
			cluster := rbdTestCluster(control)
			err := cluster.initRBDPool(t.Context(), "images")
			if (err == nil) != (scenario == "success") {
				t.Fatalf("native init verification incorrect: %v", err)
			}
			if err == nil {
				if err := cluster.initRBDPool(t.Context(), "images"); err != nil {
					t.Fatalf("repeat native initialization failed: %v", err)
				}
			}
			for _, call := range control.calls {
				if strings.Contains(call, "--force") || strings.Contains(call, "delete") {
					t.Fatalf("initialization forced application or deleted data: %v", control.calls)
				}
			}
		})
	}
}

func TestRBDNamespaceCreationListingAndNonemptyRemoval(t *testing.T) {
	control := newRBDTestControl()
	control.namespaces["foreign"] = false
	cluster := rbdTestCluster(control)
	ns, err := cluster.createRBDNamespace(t.Context(), "images", "blue")
	if err != nil || ns.Name() != "blue" || ns.PoolName() != "images" {
		t.Fatalf("new namespace failed: ns=%+v err=%v", ns, err)
	}
	copyOfOldNamespace := *ns
	if _, err := cluster.createRBDNamespace(t.Context(), "images", "blue"); err == nil {
		t.Fatal("duplicate namespace accepted")
	}
	if _, err := cluster.createRBDNamespace(t.Context(), "images", "foreign"); err == nil {
		t.Fatal("native foreign namespace accepted")
	}
	names, err := cluster.namespaceList(t.Context(), "images")
	if err != nil || !slices.Equal(names, []string{"blue", "foreign"}) {
		t.Fatalf("listing lost native namespaces: %v %v", names, err)
	}
	control.namespaces["blue"] = true
	if err := cluster.removeRBDNamespace(t.Context(), ns); err == nil || ns.state.removed {
		t.Fatal("nonempty namespace removed or ownership lost")
	}
	control.namespaces["blue"] = false
	if err := cluster.removeRBDNamespace(t.Context(), ns); err != nil || !ns.state.removed {
		t.Fatal(err)
	}
	calls := len(control.calls)
	if err := cluster.removeRBDNamespace(t.Context(), ns); err != nil || len(control.calls) != calls {
		t.Fatal("successful removal is not idempotent")
	}
	// A copied descriptor still represents the original removed namespace,
	// even if a later API call creates another namespace with the same name.
	replacement, err := cluster.createRBDNamespace(t.Context(), "images", "blue")
	if err != nil {
		t.Fatal(err)
	}
	calls = len(control.calls)
	if err := cluster.removeRBDNamespace(t.Context(), &copyOfOldNamespace); err != nil || len(control.calls) != calls || replacement.state.removed {
		t.Fatal("old copied descriptor mutated a replacement namespace")
	}
	if err := cluster.removeRBDNamespace(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	if _, ok := control.namespaces["foreign"]; !ok {
		t.Fatal("foreign namespace was changed")
	}
}

func TestRBDNamespaceOwnershipAndPoolIdentity(t *testing.T) {
	control := newRBDTestControl()
	cluster := rbdTestCluster(control)
	ns, err := cluster.createRBDNamespace(t.Context(), "images", "blue")
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []*RBDNamespace{nil, {}, {owner: cluster, poolName: "images", name: "foreign"}, {owner: &Container{}, state: &rbdNamespaceState{created: true}}} {
		calls := len(control.calls)
		if err := cluster.removeRBDNamespace(t.Context(), invalid); err == nil || len(control.calls) != calls {
			t.Fatal("foreign or unconfirmed descriptor executed native removal")
		}
	}
	*control.poolID = 18
	if err := cluster.removeRBDNamespace(t.Context(), ns); err == nil {
		t.Fatal("replacement native pool identity accepted")
	}
	if _, ok := control.namespaces["blue"]; !ok {
		t.Fatal("replacement pool lost namespace")
	}
}

func TestRBDNamespacePartialCreationAndRemovalRetries(t *testing.T) {
	control := newRBDTestControl()
	cluster := rbdTestCluster(control)
	control.fail = "rbd namespace create --pool images --namespace blue"
	ns, err := cluster.createRBDNamespace(t.Context(), "images", "blue")
	if err == nil || ns == nil || ns.state.created {
		t.Fatal("uncertain namespace creation was reported as confirmed")
	}
	calls := len(control.calls)
	if err := cluster.removeRBDNamespace(t.Context(), ns); err == nil || len(control.calls) != calls {
		t.Fatal("uncertain creation authorized destructive cleanup")
	}
	control.fail = ""
	ns, err = cluster.createRBDNamespace(t.Context(), "images", "blue")
	if err != nil {
		t.Fatal(err)
	}
	control.fail = "rbd namespace remove --pool images --namespace blue"
	if err := cluster.removeRBDNamespace(t.Context(), ns); err == nil || ns.state.removed {
		t.Fatal("failed removal lost retryable ownership")
	}
	// The previous exec response may have failed after Ceph applied removal.
	delete(control.namespaces, "blue")
	control.fail = ""
	if err := cluster.removeRBDNamespace(t.Context(), ns); err != nil || !ns.state.removed {
		t.Fatalf("applied removal did not reconcile on retry: %v", err)
	}
}

func TestRBDNamespaceRejectsUninitializedAndMalformedListing(t *testing.T) {
	for _, scenario := range []string{"no application", "no initialization object", "null listing", "wrong listing type", "empty listing name", "duplicate listing name"} {
		t.Run(scenario, func(t *testing.T) {
			control := newRBDTestControl()
			switch scenario {
			case "no application":
				control.apps = map[string]json.RawMessage{}
			case "no initialization object":
				control.trash = false
			case "null listing":
				control.listOverride = "null"
			case "wrong listing type":
				control.listOverride = `["blue"]`
			case "empty listing name":
				control.listOverride = `[{}]`
			case "duplicate listing name":
				control.listOverride = `[{"name":"blue"},{"name":"blue"}]`
			}
			cluster := rbdTestCluster(control)
			if _, err := cluster.namespaceList(t.Context(), "images"); err == nil {
				t.Fatal("invalid namespace precondition accepted")
			}
			if ns, err := cluster.createRBDNamespace(t.Context(), "images", "blue"); err == nil || ns != nil {
				t.Fatal("invalid namespace precondition mutated Ceph")
			}
		})
	}
}

func TestRBDNamespaceStoppedClusterAndMissingControl(t *testing.T) {
	for _, cluster := range []*Container{{closed: true}, {}} {
		if err := cluster.initRBDPool(t.Context(), "images"); err == nil {
			t.Fatal("unavailable cluster accepted initialization")
		}
		if _, err := cluster.createRBDNamespace(t.Context(), "images", "blue"); err == nil {
			t.Fatal("unavailable cluster accepted namespace creation")
		}
		if _, err := cluster.namespaceList(t.Context(), "images"); err == nil {
			t.Fatal("unavailable cluster accepted namespace listing")
		}
		if err := cluster.removeRBDNamespace(t.Context(), &RBDNamespace{}); err == nil {
			t.Fatal("unavailable cluster accepted namespace removal")
		}
	}
}

func rbdTestCluster(control *rbdTestControl) *Container {
	return &Container{Container: control, settings: options{startupTimeout: time.Second}}
}

type rbdTestControl struct {
	testcontainers.Container
	calls                     []string
	poolID                    *int64
	poolName                  string
	poolType                  int
	apps                      map[string]json.RawMessage
	namespaces                map[string]bool // true means native nonempty guard.
	trash                     bool
	ignoreInit, skipTrash     bool
	fail                      string
	mapOverride, listOverride string
}

func newRBDTestControl() *rbdTestControl {
	id := int64(17)
	return &rbdTestControl{poolID: &id, poolName: "images", poolType: 1,
		apps: map[string]json.RawMessage{"rbd": json.RawMessage(`{}`)}, trash: true,
		namespaces: make(map[string]bool)}
}

func (control *rbdTestControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[0] == "ceph" {
		args = append([]string{"ceph"}, args[3:]...)
	}
	call := strings.Join(args, " ")
	control.calls = append(control.calls, call)
	output, code := "", 0
	if call == control.fail {
		output, code = "injected command failure", 1
	} else {
		switch call {
		case "ceph osd dump --format json":
			if control.mapOverride != "" {
				output = control.mapOverride
			} else {
				pool := map[string]any{"pool_name": control.poolName, "type": control.poolType, "application_metadata": control.apps}
				if control.poolID != nil {
					pool["pool"] = *control.poolID
				}
				data, _ := json.Marshal(map[string]any{"pools": []any{pool}})
				output = string(data)
			}
		case "rbd pool init --pool images":
			if !control.ignoreInit {
				control.apps["rbd"] = json.RawMessage(`{}`)
				control.trash = !control.skipTrash
			}
		case "rados --pool images stat rbd_trash":
			if !control.trash {
				output, code = "No such file or directory", 2
			}
		case "rbd namespace list --pool images --format json":
			if control.listOverride != "" {
				output = control.listOverride
			} else {
				entries := make([]map[string]string, 0, len(control.namespaces))
				for name := range control.namespaces {
					entries = append(entries, map[string]string{"name": name})
				}
				data, _ := json.Marshal(entries)
				output = string(data)
			}
		case "rbd namespace create --pool images --namespace blue":
			control.namespaces["blue"] = false
		case "rbd namespace remove --pool images --namespace blue":
			if control.namespaces["blue"] {
				output, code = "namespace contains images", 16
			} else {
				delete(control.namespaces, "blue")
			}
		default:
			output, code = fmt.Sprintf("unexpected test command: %s", call), 1
		}
	}
	var stream bytes.Buffer
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
