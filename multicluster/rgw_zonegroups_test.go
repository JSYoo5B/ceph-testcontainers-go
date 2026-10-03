package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRGWZonegroupsNormalizeNamedMastersAndCloneInput(t *testing.T) {
	config := RGWTopologyConfig{MasterZonegroup: "us", Zonegroups: []RGWZonegroupConfig{
		{Name: "eu", MasterZone: "d", Zones: []RGWZoneConfig{{Name: "c"}, {Name: "d"}}},
		{Name: "us", MasterZone: "b", Zones: []RGWZoneConfig{{Name: "a"}, {Name: "b"}}},
	}}
	zones, err := normalizeRGWTopology(config)
	if err != nil {
		t.Fatal(err)
	}
	var names, groups []string
	for _, zone := range zones {
		names = append(names, zone.Name)
		groups = append(groups, zone.Zonegroup)
	}
	if !slices.Equal(names, []string{"b", "a", "d", "c"}) || !slices.Equal(groups, []string{"us", "us", "eu", "eu"}) {
		t.Fatalf("named master/group order lost: %v %v", names, groups)
	}
	zones[0].Name = "modified"
	if config.Zonegroups[1].Zones[0].Name != "a" || config.Zonegroups[1].Zones[1].Name != "b" || config.Zonegroups[1].Zones[1].Zonegroup != "" {
		t.Fatal("normalization mutated caller configuration")
	}
	for _, invalid := range []RGWTopologyConfig{
		{Zones: []RGWZoneConfig{{Name: "a"}, {Name: "b"}}, Zonegroups: config.Zonegroups},
		{Zonegroups: config.Zonegroups, MasterZonegroup: "unknown"},
		{Zonegroups: []RGWZonegroupConfig{{Name: "us", MasterZone: "elsewhere", Zones: []RGWZoneConfig{{Name: "a"}, {Name: "b"}}}}},
		{Zonegroups: []RGWZonegroupConfig{{Name: "us", Zones: []RGWZoneConfig{{Name: "a"}}}, {Name: "us", Zones: []RGWZoneConfig{{Name: "b"}}}}},
		{Zonegroups: []RGWZonegroupConfig{{Name: "us", Zones: []RGWZoneConfig{{Name: "same"}}}, {Name: "eu", Zones: []RGWZoneConfig{{Name: "same"}}}}},
		{Zonegroups: []RGWZonegroupConfig{{Name: "us", Zones: []RGWZoneConfig{{Name: "a", Zonegroup: "other"}, {Name: "b"}}}}},
		{Zones: []RGWZoneConfig{{Name: "a", Zonegroup: "us"}, {Name: "b", Zonegroup: "eu"}}, Zonegroup: "us"},
	} {
		if _, err := normalizeRGWTopology(invalid); err == nil {
			t.Fatalf("ambiguous topology accepted: %+v", invalid)
		}
	}
}

const rgwTwoGroups = `{"realm_id":"realm","master_zone":"a-id","master_zonegroup":"us-id","period_map":{"zonegroups":[{"id":"us-id","name":"us","master_zone":"a-id","endpoints":["http://a:7480"],"zones":[{"id":"a-id","name":"a","endpoints":["http://a:7480"]},{"id":"b-id","name":"b","endpoints":["http://b:7480"]}]},{"id":"eu-id","name":"eu","master_zone":"c-id","endpoints":["http://c:7480"],"zones":[{"id":"c-id","name":"c","endpoints":["http://c:7480"]}]}]}}`
const rgwAfterRemoveB = `{"realm_id":"realm","master_zone":"a-id","master_zonegroup":"us-id","period_map":{"zonegroups":[{"id":"us-id","name":"us","master_zone":"a-id","endpoints":["http://a:7480"],"zones":[{"id":"a-id","name":"a","endpoints":["http://a:7480"]}]},{"id":"eu-id","name":"eu","master_zone":"c-id","endpoints":["http://c:7480"],"zones":[{"id":"c-id","name":"c","endpoints":["http://c:7480"]}]}]}}`

func TestRGWPeriodAndRemovalGuardEveryLocalMasterAndMembership(t *testing.T) {
	f, _, _, _, _ := rgwRemovalFixture()
	period, err := decodeRGWTopologyPeriod([]byte(rgwTwoGroups))
	if err != nil {
		t.Fatal(err)
	}
	states := f.zoneStates()
	if err := f.checkZonePeriod(period, states[0], states); err != nil {
		t.Fatal(err)
	}
	for _, master := range []string{"a", "c", "foreign"} {
		if _, err := f.removableZone(period, states[0], states, master); err == nil {
			t.Fatalf("unsafe zone removal allowed: %s", master)
		}
	}
	if state, err := f.removableZone(period, states[0], states, "b"); err != nil || state.Name != "b" {
		t.Fatalf("secondary rejected: %+v %v", state, err)
	}
	for _, malformed := range []string{
		strings.Replace(rgwTwoGroups, `"master_zonegroup":"us-id"`, `"master_zonegroup":"eu-id"`, 1),
		strings.Replace(rgwTwoGroups, `"id":"eu-id"`, `"id":"foreign-id"`, 1),
		strings.Replace(rgwTwoGroups, `"name":"eu"`, `"name":"foreign"`, 1),
		strings.Replace(rgwTwoGroups, `"master_zone":"c-id"`, `"master_zone":"b-id"`, 1),
		strings.Replace(rgwTwoGroups, `http://c:7480`, `http://wrong:7480`, 1),
	} {
		p, err := decodeRGWTopologyPeriod([]byte(malformed))
		if err == nil && f.checkZonePeriod(p, states[0], states) == nil {
			t.Fatal("foreign/stale multi-group period accepted")
		}
	}
	groups := f.Zonegroups()
	if len(groups) != 2 || groups[0].Name != "eu" || groups[0].MasterZone != "c" || groups[1].Name != "us" || len(groups[1].Zones) != 2 {
		t.Fatalf("invalid group snapshot: %+v", groups)
	}
	groups[1].Zones[0].Name = "mutation"
	if f.Zones()[0].Name != "a" {
		t.Fatal("snapshot mutated fixture membership")
	}
}

type rgwLifecycleContainer struct {
	testcontainers.Container
	running                    bool
	startError, terminateError error
	starts, stops, terminates  int
}

func (c *rgwLifecycleContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: c.running}, nil
}
func (c *rgwLifecycleContainer) Stop(context.Context, *time.Duration) error {
	c.stops++
	c.running = false
	return nil
}
func (c *rgwLifecycleContainer) Start(context.Context) error {
	c.starts++
	if c.startError != nil {
		err := c.startError
		c.startError = nil
		return err
	}
	c.running = true
	return nil
}
func (c *rgwLifecycleContainer) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminates++
	if c.terminateError != nil {
		err := c.terminateError
		c.terminateError = nil
		return err
	}
	return nil
}

type rgwLifecycleCLI struct {
	testcontainers.Container
	removed, committed bool
	calls              [][]string
	terminates         int
	terminateError     error
}

func (c *rgwLifecycleCLI) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.calls = append(c.calls, slices.Clone(args))
	cmd := strings.Join(args, " ")
	data := ""
	switch {
	case strings.Contains(cmd, "period get"):
		data = rgwTwoGroups
		if c.committed {
			data = rgwAfterRemoveB
		}
	case strings.Contains(cmd, "zonegroup get"):
		data = `{"zones":[{"id":"a-id","name":"a"},{"id":"b-id","name":"b"}]}`
		if c.removed {
			data = `{"zones":[{"id":"a-id","name":"a"}]}`
		}
	case strings.Contains(cmd, "zonegroup remove"):
		c.removed = true
	case strings.Contains(cmd, "period update --commit"):
		c.committed = true
	}
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.WriteString(data)
	return 0, bytes.NewReader(stream.Bytes()), nil
}
func (c *rgwLifecycleCLI) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	c.terminates++
	if c.terminateError != nil {
		err := c.terminateError
		c.terminateError = nil
		return err
	}
	return nil
}

func rgwRemovalFixture() (*RGWMultisite, *rgwLifecycleCLI, *rgwLifecycleContainer, *rgwLifecycleContainer, *rgwLifecycleContainer) {
	cli := &rgwLifecycleCLI{}
	a, b, c := &rgwLifecycleContainer{running: true}, &rgwLifecycleContainer{running: true}, &rgwLifecycleContainer{}
	f := &RGWMultisite{RealmID: "realm", SourceZoneID: "a-id", DestinationZoneID: "b-id", config: RGWMultisiteConfig{SourceZone: "a", DestinationZone: "b", Zonegroup: "us"},
		Source: &ceph.RGWContainer{Container: a}, Destination: &ceph.RGWContainer{Container: b}, sourceClient: cli, destinationClient: cli, sourceURL: "http://a:7480", destinationURL: "http://b:7480",
		zoneGroups: map[string]string{"a": "us", "b": "us", "c": "eu"}, groupIDs: map[string]string{"us": "us-id", "eu": "eu-id"}, groupMasters: map[string]string{"us": "a", "eu": "c"},
		additionalZones: map[string]*rgwZoneState{"c": {RGWZone: RGWZone{Name: "c", ID: "c-id", Zonegroup: "eu", PeerEndpoint: "http://c:7480", Gateway: &ceph.RGWContainer{Container: c}}, client: cli}}}
	return f, cli, a, b, c
}

func TestRGWRemovalRetriesReloadAndCleanupWithoutDeletingData(t *testing.T) {
	f, cli, a, b, c := rgwRemovalFixture()
	a.startError = errors.New("temporary restart failure")
	b.terminateError = errors.New("temporary gateway removal failure")
	cli.terminateError = errors.New("temporary CLI removal failure")
	if err := f.RemoveZone(t.Context(), "b"); err == nil || !cli.committed || len(f.Zones()) != 2 || a.running {
		t.Fatalf("partial restart was not retained: %v", err)
	}
	if _, err := f.AddZone(t.Context(), "image", RGWZoneConfig{Name: "d"}); err == nil || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("addition ignored unfinished removal: %v", err)
	}
	if err := f.RemoveZone(t.Context(), "b"); err == nil || !a.running || a.starts != 2 || b.terminates != 1 {
		t.Fatalf("restart retry did not resume a stopped owned gateway: %v", err)
	}
	if err := f.RemoveZone(t.Context(), "b"); err == nil || b.terminates != 2 || cli.terminates != 1 {
		t.Fatalf("gateway cleanup retry failed: %v", err)
	}
	if err := f.RemoveZone(t.Context(), "b"); err != nil || b.terminates != 2 || cli.terminates != 2 {
		t.Fatalf("CLI cleanup retry repeated completed gateway removal: %v", err)
	}
	if err := f.RemoveZone(t.Context(), "b"); err != nil || b.terminates != 2 || cli.terminates != 2 {
		t.Fatalf("completed removal was not idempotent: %v", err)
	}
	if c.starts != 0 || c.stops != 0 {
		t.Fatal("pre-existing fenced gateway was restarted")
	}
	for _, args := range cli.calls {
		cmd := strings.Join(args, " ")
		if strings.Contains(cmd, "zone delete") || strings.Contains(cmd, "pool rm") {
			t.Fatalf("detach attempted destructive purge: %s", cmd)
		}
	}
	if _, err := f.AddZone(t.Context(), "image", RGWZoneConfig{Name: "b"}); err == nil || !strings.Contains(err.Error(), "retain configuration") {
		t.Fatalf("detached storage was adopted as fresh: %v", err)
	}
}
