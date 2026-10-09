package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSPinValidationAndOwnershipBeforeCommands(t *testing.T) {
	for _, setting := range []CephFSPinSetting{{}, {Type: "other"}, {Type: CephFSPinExport, Value: -2}, {Type: CephFSPinExport, Value: 0.5}, {Type: CephFSPinExport, Value: math.Inf(1)}, {Type: CephFSPinDistributed, Value: 2}, {Type: CephFSPinRandom, Value: -0.01}, {Type: CephFSPinRandom, Value: 1.01}, {Type: CephFSPinRandom, Value: math.NaN()}} {
		fs, control, volume, _ := cephFSPinFixture()
		if change, err := fs.TemporarySubvolumePin(t.Context(), volume, setting); change != nil || err == nil || len(control.calls) != 0 {
			t.Fatalf("invalid pin setting executed commands: %+v handle=%v error=%v", setting, change, err)
		}
	}
	fs, control, volume, _ := cephFSPinFixture()
	foreign := *volume
	foreign.identity = &cephFSVolumeIdentity{filesystem: &CephFSContainer{}, ready: true}
	if change, err := fs.TemporarySubvolumePin(t.Context(), &foreign, CephFSPinSetting{Type: CephFSPinExport, Value: 1}); change != nil || err == nil || len(control.calls) != 0 {
		t.Fatal("foreign resource was adopted")
	}
	if _, err := fs.SubvolumePinPolicy(t.Context(), &foreign); err == nil || len(control.calls) != 0 {
		t.Fatal("foreign resource policy was queried")
	}
	fs.cluster.closed = true
	if change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: 1}); change != nil || err == nil || len(control.calls) != 0 {
		t.Fatal("closed cluster queried or changed directory pins")
	}
}

func TestCephFSPinNativeBasePrivateNamesAndSharedRestore(t *testing.T) {
	fs, control, volume, group := cephFSPinFixture()
	volume.Name, volume.GroupName, volume.Path, volume.FilesystemName = "foreign", "other", "/foreign", "other-fs"
	policy, err := fs.SubvolumePinPolicy(t.Context(), volume)
	if err != nil || policy.Path != "/volumes/group/volume" || policy.Inode != 103 || policy.ExportRank != 0 {
		t.Fatalf("subvolume read used data/public path instead of captured native base: %+v error=%v", policy, err)
	}
	change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(control.mutations[0], []string{"fs", "subvolume", "pin", "fixture", "volume", "export", "1", "--group_name", "group"}) {
		t.Fatalf("public handle edits redirected mutation: %v", control.mutations)
	}
	if _, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: -1}); err == nil || len(control.mutations) != 1 {
		t.Fatal("overlapping same-field override accepted")
	}
	copy := *change
	if err := copy.Restore(t.Context()); err != nil || len(fs.pinOverrides) != 0 || control.policies[change.state.path].ExportRank != 0 {
		t.Fatalf("copy did not restore shared override: %v", err)
	}
	fs.cluster.closed = true
	if err := change.Restore(t.Context()); err != nil || len(control.mutations) != 2 {
		t.Fatal("verified restore was not idempotent after cluster termination")
	}
	fs.cluster.closed = false
	group.Name, group.Path = "foreign", "/foreign"
	groupChange, err := fs.TemporarySubvolumeGroupPin(t.Context(), group, CephFSPinSetting{Type: CephFSPinExport, Value: 1})
	if err != nil || !slices.Equal(control.mutations[2], []string{"fs", "subvolumegroup", "pin", "fixture", "group", "export", "1"}) {
		t.Fatalf("group mutation changed target or used a subvolume command: %v error=%v", control.mutations, err)
	}
	if err := groupChange.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCephFSPinIndependentFieldsPreserveOutsidePolicies(t *testing.T) {
	fs, control, _, group := cephFSPinFixture()
	groupPath := group.identity.path
	control.policies[groupPath].Distributed = true
	control.policies[groupPath].RandomProbability = 0.01
	export, err := fs.TemporarySubvolumeGroupPin(t.Context(), group, CephFSPinSetting{Type: CephFSPinExport, Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	distributed, err := fs.TemporarySubvolumeGroupPin(t.Context(), group, CephFSPinSetting{Type: CephFSPinDistributed, Value: 0})
	if err != nil {
		t.Fatal(err)
	}
	// Another field changes outside this lease; restoring export preserves it.
	control.policies[groupPath].RandomProbability = 0.005
	if err := export.Restore(t.Context()); err != nil || control.policies[groupPath].ExportRank != 0 || control.policies[groupPath].Distributed || control.policies[groupPath].RandomProbability != 0.005 {
		t.Fatalf("restoring one policy overwrote another: %+v error=%v", control.policies[groupPath], err)
	}
	if err := distributed.Restore(t.Context()); err != nil || !control.policies[groupPath].Distributed || control.policies[groupPath].RandomProbability != 0.005 {
		t.Fatal("restoration did not preserve independent policy edits")
	}
}

func TestCephFSPinRestoreRejectsOutsideFieldOrResourceChanges(t *testing.T) {
	for _, scenario := range []string{"field", "inode", "subvolume path", "birth time", "filesystem", "pool", "removed"} {
		t.Run(scenario, func(t *testing.T) {
			fs, control, volume, _ := cephFSPinFixture()
			change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: 1})
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "field":
				control.policies[change.state.path].ExportRank = -1
			case "inode":
				control.policies[change.state.path].Inode++
			case "subvolume path":
				control.native.output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(control.native.output["fs subvolume info fixture volume --group_name group --format json"], "unique-id", "replacement")
			case "birth time":
				control.native.output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(control.native.output["fs subvolume info fixture volume --group_name group --format json"], ".123456", ".987654")
			case "filesystem":
				control.native.output["fs dump --format json"] = strings.ReplaceAll(control.native.output["fs dump --format json"], `"id":41`, `"id":42`)
			case "pool":
				control.native.output["osd pool ls detail --format json"] = strings.ReplaceAll(control.native.output["osd pool ls detail --format json"], `"pool_id":1`, `"pool_id":99`)
			case "removed":
				volume.identity.removed = true
			}
			if err := change.Restore(t.Context()); err == nil || len(control.mutations) != 1 || len(fs.pinOverrides) != 1 {
				t.Fatalf("outside %s change was overwritten: error=%v mutations=%v", scenario, err, control.mutations)
			}
		})
	}
}

func TestCephFSPinRankFeasibilityRequiresOwnedActiveNativeRank(t *testing.T) {
	for _, scenario := range []string{"outside topology", "foreign daemon", "recovering daemon"} {
		fs, control, volume, _ := cephFSPinFixture()
		rank := float64(1)
		switch scenario {
		case "outside topology":
			rank = 2
		case "foreign daemon":
			fs.mdss = fs.mdss[:1]
		case "recovering daemon":
			control.native.output["fs dump --format json"] = strings.Replace(control.native.output["fs dump --format json"], `"name":"b","rank":1,"gid":102,"state":"up:active"`, `"name":"b","rank":1,"gid":102,"state":"up:replay"`, 1)
		}
		if change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: rank}); change != nil || err == nil || len(control.mutations) != 0 {
			t.Fatalf("infeasible %s rank accepted: handle=%v error=%v", scenario, change, err)
		}
	}
}

func TestCephFSPinUncertainApplyAndRestoreRemainReconciliable(t *testing.T) {
	for _, scenario := range []string{"apply lost reply", "apply false success", "apply readback failure", "restore lost reply", "restore false success"} {
		t.Run(scenario, func(t *testing.T) {
			fs, control, volume, _ := cephFSPinFixture()
			control.lostReply = scenario == "apply lost reply"
			control.falseSuccess = scenario == "apply false success"
			control.failReadAfterMutation = scenario == "apply readback failure"
			change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinExport, Value: 1})
			if change == nil || (strings.HasPrefix(scenario, "apply") && err == nil) || (!strings.HasPrefix(scenario, "apply") && err != nil) {
				t.Fatalf("uncertain apply lost its handle/error: handle=%v error=%v", change, err)
			}
			control.lostReply, control.falseSuccess, control.failReadAfterMutation = false, false, false
			if scenario == "restore lost reply" || scenario == "restore false success" {
				control.lostReply = scenario == "restore lost reply"
				control.falseSuccess = scenario == "restore false success"
				if err := change.Restore(t.Context()); err == nil || change.state.restored || len(fs.pinOverrides) == 0 {
					t.Fatal("uncertain restore was marked complete before verified readback")
				}
				control.lostReply, control.falseSuccess = false, false
			}
			if err := change.Restore(t.Context()); err != nil || !change.state.restored || len(fs.pinOverrides) != 0 || control.policies[change.state.path].ExportRank != 0 {
				t.Fatalf("fresh-context restoration did not reconcile original policy: %v", err)
			}
			if scenario == "restore lost reply" && len(control.mutations) != 2 {
				t.Fatal("retry repeated a native restore that already persisted")
			}
		})
	}
}

func TestCephFSPinVirtualDefaultsAndNativeProbabilityPrecision(t *testing.T) {
	fs, control, volume, _ := cephFSPinFixture()
	base := "/volumes/group/volume"
	control.policies[base].ExportRank = -1
	policy, err := fs.SubvolumePinPolicy(t.Context(), volume)
	if err != nil || policy.ExportRank != -1 || policy.Distributed || policy.RandomProbability != 0 {
		t.Fatalf("virtual defaults treated as missing xattrs: %+v %v", policy, err)
	}
	change, err := fs.TemporarySubvolumePin(t.Context(), volume, CephFSPinSetting{Type: CephFSPinRandom, Value: 0.123456789})
	if err != nil || change.state.applied != 0.123457 {
		t.Fatalf("native probability canonicalization rejected successful value: %+v %v", change, err)
	}
	if err := change.Restore(t.Context()); err != nil || control.policies[base].RandomProbability != 0 {
		t.Fatal("random policy was not restored to its virtual default")
	}
	for _, malformed := range []string{`{`, `{}`, `{"path":"/volumes/group/volume","inode":1,"export":"-1","distributed":"2","random":"0"}`, `{"path":"/volumes/group/volume","inode":1,"export":"-1","distributed":"0","random":"NaN"}`} {
		control.override = malformed
		if _, err := fs.SubvolumePinPolicy(t.Context(), volume); err == nil {
			t.Errorf("malformed virtual policy accepted: %s", malformed)
		}
	}
}

type cephFSPinTestControl struct {
	testcontainers.Container
	native                  *poolFixtureContainer
	policies                map[string]*CephFSPinPolicy
	calls, mutations        [][]string
	lostReply, falseSuccess bool
	failReadAfterMutation   bool
	override                string
}

func cephFSPinFixture() (*CephFSContainer, *cephFSPinTestControl, *CephFSSubvolume, *CephFSSubvolumeGroup) {
	fs, native := subvolumeFixture()
	native.output["fs dump --format json"] = `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":2,"metadata_pool":1,"data_pools":[2,3],"info":{"a":{"name":"a","rank":0,"gid":101,"state":"up:active"},"b":{"name":"b","rank":1,"gid":102,"state":"up:active"}}}}],"standbys":[]}`
	fs.mdss = []*MDSContainer{{ID: "a", FilesystemName: "fixture"}, {ID: "b", FilesystemName: "fixture"}}
	control := &cephFSPinTestControl{native: native, policies: map[string]*CephFSPinPolicy{
		"/volumes/group":        {Path: "/volumes/group", Inode: 102, ExportRank: 0},
		"/volumes/group/volume": {Path: "/volumes/group/volume", Inode: 103, ExportRank: 0},
	}}
	fs.cluster.Container = control
	volume := &CephFSSubvolume{Name: "volume", GroupName: "group", FilesystemName: "fixture", Path: "/volumes/group/volume/unique-id", identity: &cephFSVolumeIdentity{
		filesystem: fs, filesystemID: 41, name: "volume", group: "group", path: "/volumes/group/volume/unique-id", createdAt: "2026-10-03 00:00:01.123456", ready: true,
	}}
	group := &CephFSSubvolumeGroup{Name: "group", FilesystemName: "fixture", Path: "/volumes/group", identity: &cephFSVolumeIdentity{
		filesystem: fs, filesystemID: 41, name: "group", path: "/volumes/group", createdAt: "2026-10-03 00:00:00.123456", ready: true,
	}}
	return fs, control, volume, group
}

func (c *cephFSPinTestControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls = append(c.calls, slices.Clone(args))
	if args[0] == "python3" {
		if c.failReadAfterMutation && len(c.mutations) > 0 {
			return 0, nil, errors.New("native readback unavailable")
		}
		policy := c.policies[args[len(args)-1]]
		data, _ := json.Marshal(map[string]any{"path": policy.Path, "inode": policy.Inode, "export": strconv.Itoa(policy.ExportRank), "distributed": strconv.Itoa(int(cephFSPinValue(policy, CephFSPinDistributed))), "random": strconv.FormatFloat(policy.RandomProbability, 'g', 6, 64)})
		if c.override != "" {
			data = []byte(c.override)
		}
		return 0, cephFSPinTestStream(data), nil
	}
	module := args[3:]
	if len(module) >= 7 && module[0] == "fs" && module[2] == "pin" {
		c.mutations = append(c.mutations, slices.Clone(module))
		base := "/volumes/group"
		if module[1] == "subvolume" {
			base += "/volume"
		}
		value, _ := strconv.ParseFloat(module[6], 64)
		if !c.falseSuccess {
			switch CephFSPinType(module[5]) {
			case CephFSPinExport:
				c.policies[base].ExportRank = int(value)
			case CephFSPinDistributed:
				c.policies[base].Distributed = value == 1
			case CephFSPinRandom:
				c.policies[base].RandomProbability = value
			}
		}
		if c.lostReply {
			return 0, nil, context.Canceled
		}
		return 0, cephFSPinTestStream(nil), nil
	}
	return c.native.Exec(ctx, args, opts...)
}

func cephFSPinTestStream(data []byte) io.Reader {
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.Write(data)
	return &stream
}
