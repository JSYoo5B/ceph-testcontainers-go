package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// featuresFixture keeps the required_client_features of "app" by bit and
// answers fs get with them, and fs feature ls with supported; a nil required
// models an MDSMap without the field. lostReply applies the matching
// required_client_features command but fails its reply, and afterChange runs
// after every applied one.
type featuresFixture struct {
	*sessionFixture
	supported   map[string]int
	required    map[int]string
	lostReply   string
	afterChange func()
}

func (ctr *featuresFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	required := "null"
	if ctr.required != nil {
		byKey := map[string]string{}
		for bit, name := range ctr.required {
			byKey[fmt.Sprintf("feature_%d", bit)] = name
		}
		data, _ := json.Marshal(byKey)
		required = string(data)
	}
	mdsmap := fmt.Sprintf(`"session_timeout":%d,"session_autoclose":%d`, ctr.timeout, ctr.autoclose)
	if ctr.required != nil {
		mdsmap += `,"required_client_features":` + required
	}
	ctr.Output["fs get app --format json"] = fmt.Sprintf(`{"mdsmap":{%s},"id":%d}`, mdsmap, ctr.fsID)
	var listed []string
	for name, bit := range ctr.supported {
		listed = append(listed, fmt.Sprintf(`{"index":%d,"name":%q}`, bit, name))
	}
	ctr.Output["fs feature ls --format json"] = "[" + strings.Join(listed, ",") + "]"
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	if ctr.lostReply != "" {
		ctr.Fail = ""
		if command == ctr.lostReply {
			ctr.Fail = command
		}
	}
	code, reader, err := ctr.CannedCommandContainer.Exec(ctx, args, opts...)
	var seconds int
	if _, scanErr := fmt.Sscanf(command, "fs set app session_timeout %d", &seconds); scanErr == nil && code == 0 {
		ctr.timeout = seconds
	} else if _, scanErr := fmt.Sscanf(command, "fs set app session_autoclose %d", &seconds); scanErr == nil && code == 0 {
		ctr.autoclose = seconds
	}
	var op, name string
	if _, scanErr := fmt.Sscanf(command, "fs required_client_features app %s %s", &op, &name); scanErr == nil && (code == 0 || command == ctr.lostReply) {
		if bit, ok := ctr.supported[name]; ok {
			switch op {
			case "add":
				ctr.required[bit] = name
			case "rm":
				delete(ctr.required, bit)
			}
		}
		if ctr.afterChange != nil {
			ctr.afterChange()
		}
	}
	return code, reader, err
}

func (ctr *featuresFixture) featureMutations() []string {
	var commands []string
	for _, call := range ctr.Calls {
		if command := strings.Join(call, " "); strings.HasPrefix(command, "fs required_client_features") {
			commands = append(commands, strings.TrimPrefix(command, "fs required_client_features app "))
		}
	}
	return commands
}

// featuresFilesystem requires reply_encoding (bit 9) and an unnamed bit 40
// before any override.
func featuresFilesystem(t *testing.T) (*sessionCore, *featuresFixture) {
	t.Helper()
	fs, inner := sessionFilesystem(t)
	ctr := &featuresFixture{
		sessionFixture: inner,
		supported:      map[string]int{"reserved": 0, "jewel": 5, "reply_encoding": 9, "metric_collect": 14, "alternate_name": 15, "op_getvxattr": 17},
		required:       map[int]string{9: "reply_encoding", 40: "unknown"},
	}
	testhelper.TestSetControlPlane(fs.Cluster(), ctr)
	return fs, ctr
}

func TestOverrideRequiredFeaturesOwnsOnlyTheAddedBitsAndRestoresThem(t *testing.T) {
	fs, ctr := featuresFilesystem(t)
	change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect", "reply_encoding", "alternate_name"})
	if err != nil {
		t.Fatal(err)
	}
	want := []RequirementStep{
		{Feature: Feature{Bit: 14, Name: "metric_collect"}, Sent: true, Confirmed: true},
		{Feature: Feature{Bit: 9, Name: "reply_encoding"}, AlreadyRequired: true},
		{Feature: Feature{Bit: 15, Name: "alternate_name"}, Sent: true, Confirmed: true},
	}
	if got := change.Steps(); !slices.Equal(got, want) {
		t.Fatalf("steps = %+v", got)
	}
	observed, err := requiredFeatures(t.Context(), fs.Core)
	wantObserved := []Feature{{9, "reply_encoding"}, {14, "metric_collect"}, {15, "alternate_name"}, {40, "unknown"}}
	if err != nil || !slices.Equal(observed, wantObserved) {
		t.Fatalf("RequiredFeatures = %+v, %v", observed, err)
	}
	if second, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"jewel"}); second != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a second override of the same filesystem: handle=%v error=%v", second, err)
	}
	// The timeouts override of the same filesystem is independent.
	timeouts, err := overrideTimeouts(t.Context(), fs.Core, Limits{Timeout: 60 * time.Second, Autoclose: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil {
		t.Fatalf("a copy did not restore the shared override: %v", err)
	}
	if wantRequired := map[int]string{9: "reply_encoding", 40: "unknown"}; !maps.Equal(ctr.required, wantRequired) {
		t.Fatalf("restore left %v", ctr.required)
	}
	if got := ctr.featureMutations(); !slices.Equal(got, []string{"add metric_collect", "add alternate_name", "rm alternate_name", "rm metric_collect"}) {
		t.Fatalf("mutations = %q", got)
	}
	if overridesOf(fs.Core).features[fs.Core] != nil || overridesOf(fs.Core).byCore[fs.Core] != timeouts.state {
		t.Fatal("restoring the features override changed the tracked overrides")
	}
	if err := change.Restore(t.Context()); err != nil || len(ctr.featureMutations()) != 4 {
		t.Fatalf("a repeated restore: %v %q", err, ctr.featureMutations())
	}
	if got := change.Steps(); !slices.Equal(got, want) {
		t.Fatalf("Restore changed the apply evidence: %+v", got)
	}
	testhelper.TestSetClosed(fs.Cluster(), true)
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("a confirmed restore did not stay complete: %v", err)
	}
	if steps := (*RequiredFeaturesOverrideHandle)(nil).Steps(); steps != nil {
		t.Fatalf("nil handle steps = %+v", steps)
	}
}

// A feature that is already required gives the handle nothing to own: it
// sends no command, keeps the slot of the filesystem until Restore, and its
// Restore leaves the feature required.
func TestOverrideRequiredFeaturesOfAnAlreadyRequiredFeatureOwnsNothing(t *testing.T) {
	fs, ctr := featuresFilesystem(t)
	change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"reply_encoding"})
	if err != nil {
		t.Fatal(err)
	}
	if got := change.Steps(); len(got) != 1 || !got[0].AlreadyRequired || got[0].Sent || len(change.state.owned) != 0 {
		t.Fatalf("steps = %+v owned=%d", got, len(change.state.owned))
	}
	if second, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"jewel"}); second != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an override next to one that owns nothing: handle=%v error=%v", second, err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.featureMutations()) != 0 || ctr.required[9] != "reply_encoding" {
		t.Fatalf("a no-op override changed %q, required %v", ctr.featureMutations(), ctr.required)
	}
}

func TestOverrideRequiredFeaturesRefusesInvalidOrUnsupportedRequestsBeforeChanges(t *testing.T) {
	for _, names := range [][]string{nil, {}, {""}, {" metric_collect"}, {"metric_collect", "metric_collect"}} {
		fs, ctr := featuresFilesystem(t)
		if change, err := overrideRequiredFeatures(t.Context(), fs.Core, names); change != nil || !errors.Is(err, ceph.ErrInvalidRequest) || len(ctr.Calls) != 0 {
			t.Fatalf("%q: handle=%v error=%v calls=%q", names, change, err, ctr.Calls)
		}
	}
	// A name the image does not list, also a bit number, is unsupported.
	for _, name := range []string{"future_feature", "14", "reserved"} {
		fs, ctr := featuresFilesystem(t)
		change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect", name})
		var failure *ceph.OperationError
		if change != nil || !errors.Is(err, ceph.ErrUnsupported) || !errors.As(err, &failure) || !strings.Contains(failure.Capability, name) {
			t.Fatalf("%s: handle=%v error=%v", name, change, err)
		}
		if len(ctr.featureMutations()) != 0 || overridesOf(fs.Core).features[fs.Core] != nil {
			t.Fatalf("%s: an unsupported request changed %q", name, ctr.featureMutations())
		}
	}
	fs, ctr := featuresFilesystem(t)
	ctr.required = nil
	if change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"}); change != nil || !errors.Is(err, ceph.ErrUnsupported) {
		t.Fatalf("an MDSMap without the field: handle=%v error=%v", change, err)
	}
	if _, err := requiredFeatures(t.Context(), fs.Core); !errors.Is(err, ceph.ErrUnsupported) {
		t.Fatalf("observing an MDSMap without the field: %v", err)
	}
	fs, ctr = featuresFilesystem(t)
	ctr.Fail = "fs feature ls --format json"
	if change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"}); change != nil || err == nil || len(ctr.featureMutations()) != 0 {
		t.Fatalf("a failed feature listing: handle=%v error=%v", change, err)
	}
	fs, _ = featuresFilesystem(t)
	testhelper.TestSetClosed(fs.Cluster(), true)
	if change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"}); change != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a terminated cluster: handle=%v error=%v", change, err)
	}
	if _, err := requiredFeatures(t.Context(), fs.Core); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("observing a terminated cluster: %v", err)
	}
}

// An owned feature that was removed outside the handle is refused without a
// command, and the handle stays usable; a filesystem recreated under the same
// name is left alone.
func TestRequiredFeaturesRestoreRefusesOutsideEditsAndARecreatedFilesystem(t *testing.T) {
	fs, ctr := featuresFilesystem(t)
	change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect", "alternate_name"})
	if err != nil {
		t.Fatal(err)
	}
	delete(ctr.required, 14)
	if err := change.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || !strings.Contains(err.Error(), "metric_collect was removed outside") {
		t.Fatalf("Restore after an outside removal: %v", err)
	}
	if got := ctr.featureMutations(); len(got) != 2 || ctr.required[15] != "alternate_name" {
		t.Fatalf("a refused restore changed %q, required %v", got, ctr.required)
	}
	// The outside edit is undone; the same handle restores.
	ctr.required[14] = "metric_collect"
	if err := change.Restore(t.Context()); err != nil || len(ctr.required) != 2 {
		t.Fatalf("Restore after the edit was undone: %v, required %v", err, ctr.required)
	}

	fs, ctr = featuresFilesystem(t)
	change, err = overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fsID = 8
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("Restore on a recreated filesystem: %v", err)
	}
	if _, err := requiredFeatures(t.Context(), fs.Core); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("observing a recreated filesystem: %v", err)
	}
	fs.unregistered = true
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "active filesystem created by this cluster") {
		t.Fatalf("Restore on an unregistered filesystem: %v", err)
	}
	if got := ctr.featureMutations(); len(got) != 1 || ctr.required[14] != "metric_collect" {
		t.Fatalf("a recreated filesystem was changed: %q", got)
	}
	var missing *RequiredFeaturesOverrideHandle
	if err := missing.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("Restore of a nil handle: %v", err)
	}
}

// A failed later step stops the sequence and keeps a handle that owns the
// earlier step; a lost reply leaves the bit owned and confirmed by readback;
// a failed or lost removal keeps the handle usable.
func TestOverrideRequiredFeaturesKeepsAUsableHandleAfterPartialOrUncertainSteps(t *testing.T) {
	fs, ctr := featuresFilesystem(t)
	ctr.Fail = "fs required_client_features app add alternate_name"
	change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect", "alternate_name", "op_getvxattr"})
	if change == nil || !errors.Is(err, ceph.ErrNativeFailure) {
		t.Fatalf("a failed second step: handle=%v error=%v", change, err)
	}
	want := []RequirementStep{
		{Feature: Feature{Bit: 14, Name: "metric_collect"}, Sent: true, Confirmed: true},
		{Feature: Feature{Bit: 15, Name: "alternate_name"}, Sent: true},
	}
	if got := change.Steps(); !slices.Equal(got, want) {
		t.Fatalf("steps = %+v", got)
	}
	ctr.Fail = ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := ctr.featureMutations(); !slices.Equal(got, []string{"add metric_collect", "add alternate_name", "rm metric_collect"}) || len(ctr.required) != 2 {
		t.Fatalf("mutations = %q, required %v", got, ctr.required)
	}

	fs, ctr = featuresFilesystem(t)
	ctr.lostReply = "fs required_client_features app add metric_collect"
	change, err = overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect", "alternate_name"})
	if change == nil || err == nil {
		t.Fatalf("a lost reply: handle=%v error=%v", change, err)
	}
	if got := change.Steps(); len(got) != 1 || !got[0].Sent || !got[0].Confirmed || ctr.required[14] != "metric_collect" {
		t.Fatalf("a lost reply: steps %+v, required %v", got, ctr.required)
	}
	// The removal is lost too: its effect makes the next Restore a no-op
	// rather than an outside edit.
	ctr.lostReply = "fs required_client_features app rm metric_collect"
	if err := change.Restore(t.Context()); err == nil || ctr.required[14] != "" {
		t.Fatalf("a lost removal: %v, required %v", err, ctr.required)
	}
	ctr.lostReply = ""
	if err := change.Restore(t.Context()); err != nil || len(ctr.required) != 2 {
		t.Fatalf("Restore after a lost removal: %v, required %v", err, ctr.required)
	}

	// A removal refused before any change keeps the handle unrestored.
	fs, ctr = featuresFilesystem(t)
	change, err = overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Fail = "fs required_client_features app rm metric_collect"
	if err := change.Restore(t.Context()); !errors.Is(err, ceph.ErrNativeFailure) || ctr.required[14] != "metric_collect" {
		t.Fatalf("a refused removal: %v, required %v", err, ctr.required)
	}
	ctr.Fail = ""
	if err := change.Restore(t.Context()); err != nil || len(ctr.required) != 2 {
		t.Fatalf("Restore after a refused removal: %v, required %v", err, ctr.required)
	}
}

func TestOverrideRequiredFeaturesKeepsOwnershipAfterCancellation(t *testing.T) {
	fs, ctr := featuresFilesystem(t)
	ctx, cancel := context.WithCancel(t.Context())
	ctr.afterChange = cancel
	change, err := overrideRequiredFeatures(ctx, fs.Core, []string{"metric_collect", "alternate_name"})
	if change == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled after the first step: handle=%v error=%v", change, err)
	}
	if got := change.Steps(); len(got) != 2 || !got[0].Confirmed || !got[1].Sent || got[1].Confirmed || ctr.required[15] != "" {
		t.Fatalf("canceled after the first step: steps %+v, required %v", got, ctr.required)
	}
	ctr.afterChange = nil
	if err := change.Restore(ctx); !errors.Is(err, context.Canceled) || ctr.required[14] != "metric_collect" {
		t.Fatalf("Restore with a canceled context: %v", err)
	}
	if err := change.Restore(t.Context()); err != nil || len(ctr.required) != 2 {
		t.Fatalf("Restore with a fresh context: %v, required %v", err, ctr.required)
	}
	fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
}

func TestOverrideRequiredFeaturesAdmissionWaitsWithTheCallersContext(t *testing.T) {
	for _, gate := range []string{"setup", "owner"} {
		t.Run(gate, func(t *testing.T) {
			fs, ctr := featuresFilesystem(t)
			fstest.AssertAdmissionGate(t, fs.Cluster(), fs.setupMu, gate, func(ctx context.Context) error {
				_, err := overrideRequiredFeatures(ctx, fs.Core, []string{"metric_collect"})
				return err
			})
			fstest.AssertAdmissionGate(t, fs.Cluster(), fs.setupMu, gate, func(ctx context.Context) error {
				_, err := requiredFeatures(ctx, fs.Core)
				return err
			})
			change, err := overrideRequiredFeatures(t.Context(), fs.Core, []string{"metric_collect"})
			if err != nil {
				t.Fatal(err)
			}
			if gate == "setup" {
				clustertest.AssertContextLockQueued(t, fs.setupMu, change.Restore)
			} else {
				fstest.AssertAdmissionGate(t, fs.Cluster(), fs.setupMu, gate, change.Restore)
			}
			if len(ctr.featureMutations()) != 1 {
				t.Fatalf("a queued restore wrote: %q", ctr.featureMutations())
			}
			fstest.AssertAdmissionReleased(t, fs.Cluster(), fs.setupMu)
		})
	}
}

func TestParseRequiredFeatures(t *testing.T) {
	got, err := parseRequired([]byte(`{"mdsmap":{"required_client_features":{"feature_14":"metric_collect","feature_70":"unknown"}},"id":7}`), 7)
	if err != nil || !maps.Equal(got, map[int]string{14: "metric_collect", 70: "unknown"}) {
		t.Fatalf("required = %v, %v", got, err)
	}
	if got, err := parseRequired([]byte(`{"mdsmap":{"required_client_features":{}},"id":7}`), 7); err != nil || len(got) != 0 {
		t.Fatalf("empty = %v, %v", got, err)
	}
	if _, err := parseRequired([]byte(`{"mdsmap":{},"id":7}`), 7); !errors.Is(err, errNoRequiredFeatures) {
		t.Fatalf("missing field: %v", err)
	}
	if _, err := parseRequired([]byte(`{"mdsmap":{"required_client_features":{}},"id":8}`), 7); err == nil || !strings.Contains(err.Error(), "filesystem ID") {
		t.Fatalf("another filesystem ID: %v", err)
	}
	for _, bad := range []string{`[]`, `{"mdsmap":{"required_client_features":{"metric_collect":"x"}},"id":7}`,
		`{"mdsmap":{"required_client_features":{"feature_-1":"x"}},"id":7}`, `{"mdsmap":{"required_client_features":{"feature_07":"x"}},"id":7}`} {
		if _, err := parseRequired([]byte(bad), 7); err == nil || errors.Is(err, errNoRequiredFeatures) {
			t.Fatalf("%s: %v", bad, err)
		}
	}
	supported, err := parseSupported([]byte(`[{"index":0,"name":"reserved"},{"index":1,"name":"reserved"},{"index":14,"name":"metric_collect"}]`))
	if err != nil || !maps.Equal(supported, map[string]int{"metric_collect": 14}) {
		t.Fatalf("supported = %v, %v", supported, err)
	}
	for _, bad := range []string{`[]`, `{}`, `[{"name":"x"}]`, `[{"index":1,"name":""}]`, `[{"index":-1,"name":"x"}]`} {
		if _, err := parseSupported([]byte(bad)); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
}
