package multisite

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rgwScriptCLI answers radosgw-admin by the operation words after the fixed
// connection prefix: replies prints a reply, fail exits 1 and anything else
// prints nothing. ops records every operation in order.
type rgwScriptCLI struct {
	testcontainers.Container
	replies map[string]string
	fail    map[string]bool
	ops     []string
}

func (c *rgwScriptCLI) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	op := strings.Join(args, " ")
	if i := slices.Index(args, "--osd-pool-default-pgp-num"); i >= 0 && i+2 <= len(args) {
		op = strings.Join(args[i+2:], " ")
	}
	c.ops = append(c.ops, op)
	code, data, stream := 0, c.replies[op], stdcopy.Stdout
	if c.fail[op] {
		code, data, stream = 1, "injected failure", stdcopy.Stderr
	}
	var out bytes.Buffer
	header := [8]byte{byte(stream)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	out.Write(header[:])
	out.WriteString(data)
	return code, bytes.NewReader(out.Bytes()), nil
}

func newPhaseFixture(source, destination *rgwScriptCLI) *Fixture {
	f := newPairFixture(Config{Realm: "realm", Zonegroup: "us", SourceZone: "a", DestinationZone: "b", destinationZonegroup: "us"})
	f.realm.sourceClient, f.realm.destinationClient = source, destination
	f.realm.systemAccess, f.realm.systemSecret = "ACCESS", "SECRET"
	return f
}

func TestRGWPairConfigFillsDefaultsAndRefusesMisreadNames(t *testing.T) {
	config, err := completePairConfig(Config{}, "abc")
	if err != nil {
		t.Fatal(err)
	}
	want := Config{Realm: "tc-multicluster-abc", Zonegroup: "us-east-1", SourceZone: "tc-primary-abc", DestinationZone: "tc-secondary-abc", destinationZonegroup: "us-east-1"}
	if config != want {
		t.Fatalf("defaults = %+v, want %+v", config, want)
	}
	kept, err := completePairConfig(Config{Realm: "r", Zonegroup: "g", SourceZone: "a", DestinationZone: "b", destinationZonegroup: "eu"}, "abc")
	if err != nil || kept.destinationZonegroup != "eu" || kept.Realm != "r" {
		t.Fatalf("explicit names were replaced: %+v, %v", kept, err)
	}
	for _, config := range []Config{
		{Realm: " realm"}, {Zonegroup: "-group"}, {SourceZone: "a\nb"}, {DestinationZone: "b\x00"},
		{SourceZone: "same", DestinationZone: "same"},
	} {
		if _, err := completePairConfig(config, "abc"); err == nil {
			t.Fatalf("%+v was accepted", config)
		}
	}
}

func TestRGWPairFixtureRecordsEachZonegroupMaster(t *testing.T) {
	same := newPairFixture(Config{Zonegroup: "us", SourceZone: "a", DestinationZone: "b", destinationZonegroup: "us"})
	if len(same.topology.groupMasters) != 1 || same.topology.groupMasters["us"] != "a" || same.realm.zoneGroupName("b") != "us" || !same.topology.bootstrapStagingPending {
		t.Fatalf("one zonegroup: masters %v", same.topology.groupMasters)
	}
	split := newPairFixture(Config{Zonegroup: "us", SourceZone: "a", DestinationZone: "b", destinationZonegroup: "eu"})
	if split.topology.groupMasters["us"] != "a" || split.topology.groupMasters["eu"] != "b" || split.realm.zoneGroupName("b") != "eu" {
		t.Fatalf("two zonegroups: masters %v", split.topology.groupMasters)
	}
}

func TestRGWJoinZoneCreatesAGroupOnlyForANewGroup(t *testing.T) {
	cli := &rgwScriptCLI{}
	f := newPhaseFixture(cli, cli)
	if err := f.realm.joinZone(t.Context(), zoneJoin{client: cli, zone: "b", masterURL: "http://a:7480", endpoint: "http://b:7480"}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"realm pull --url http://a:7480 --access-key ACCESS --secret SECRET",
		"realm default",
		"zone create --access-key ACCESS --secret SECRET --endpoints http://b:7480",
		"period update --commit",
	}
	if !slices.Equal(cli.ops, want) {
		t.Fatalf("existing group join:\n%q\nwant\n%q", cli.ops, want)
	}
	cli.ops = nil
	// Host mode commits no endpoint before the gateway listens.
	if err := f.realm.joinZone(t.Context(), zoneJoin{client: cli, zone: "c", masterURL: "http://a:7480", newGroup: "eu"}); err != nil {
		t.Fatal(err)
	}
	want = []string{
		"realm pull --url http://a:7480 --access-key ACCESS --secret SECRET",
		"realm default",
		"zonegroup create --api-name eu",
		"zone create --access-key ACCESS --secret SECRET --master",
		"period update --commit",
	}
	if !slices.Equal(cli.ops, want) {
		t.Fatalf("new group join:\n%q\nwant\n%q", cli.ops, want)
	}
}

func TestRGWZoneCommandsStopAtTheFirstFailure(t *testing.T) {
	cli := &rgwScriptCLI{fail: map[string]bool{"realm default": true}}
	f := newPhaseFixture(cli, cli)
	if err := f.realm.joinZone(t.Context(), zoneJoin{client: cli, zone: "b", masterURL: "http://a:7480"}); err == nil {
		t.Fatal("a failed realm default was ignored")
	}
	if len(cli.ops) != 2 {
		t.Fatalf("commands after the failure ran: %q", cli.ops)
	}
}

func TestRGWPublishZoneEndpointCommitsTheGroupEndpointOnlyForAGroupMaster(t *testing.T) {
	cli := &rgwScriptCLI{}
	f := newPhaseFixture(cli, cli)
	if err := f.realm.publishZoneEndpoint(t.Context(), cli, "b", "http://host:1", false); err != nil {
		t.Fatal(err)
	}
	if want := []string{"zone modify --endpoints http://host:1", "period update --commit"}; !slices.Equal(cli.ops, want) {
		t.Fatalf("member endpoint: %q", cli.ops)
	}
	cli.ops = nil
	if err := f.realm.publishZoneEndpoint(t.Context(), cli, "a", "http://host:2", true); err != nil {
		t.Fatal(err)
	}
	if want := []string{"zone modify --endpoints http://host:2", "zonegroup modify --endpoints http://host:2", "period update --commit"}; !slices.Equal(cli.ops, want) {
		t.Fatalf("group master endpoint: %q", cli.ops)
	}
}

func pairIdentityCLIs() (*rgwScriptCLI, *rgwScriptCLI) {
	source := &rgwScriptCLI{replies: map[string]string{"realm get": `{"id":"realm-id"}`, "zone get": `{"id":"a-id"}`, "zonegroup get": `{"id":"us-id"}`}}
	destination := &rgwScriptCLI{replies: map[string]string{"realm get": `{"id":"realm-id"}`, "zone get": `{"id":"b-id"}`, "zonegroup get": `{"id":"us-id"}`}}
	return source, destination
}

func TestRGWPairIdentitiesRequireOneRealmAndDistinctZones(t *testing.T) {
	source, destination := pairIdentityCLIs()
	f := newPhaseFixture(source, destination)
	if err := f.recordPairIdentities(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.RealmID != "realm-id" || f.SourceZoneID != "a-id" || f.DestinationZoneID != "b-id" || f.topology.groupIDs["us"] != "us-id" {
		t.Fatalf("identities: realm %q zones %q/%q groups %v", f.RealmID, f.SourceZoneID, f.DestinationZoneID, f.topology.groupIDs)
	}
	for name, change := range map[string]func(source, destination *rgwScriptCLI){
		"malformed zone":       func(_, d *rgwScriptCLI) { d.replies["zone get"] = `not json` },
		"empty realm":          func(s, _ *rgwScriptCLI) { s.replies["realm get"] = `{}` },
		"foreign realm":        func(_, d *rgwScriptCLI) { d.replies["realm get"] = `{"id":"other"}` },
		"same zone":            func(_, d *rgwScriptCLI) { d.replies["zone get"] = `{"id":"a-id"}` },
		"no zonegroup":         func(_, d *rgwScriptCLI) { d.replies["zonegroup get"] = `{}` },
		"failed realm command": func(s, _ *rgwScriptCLI) { s.fail = map[string]bool{"realm get": true} },
	} {
		source, destination := pairIdentityCLIs()
		change(source, destination)
		if err := newPhaseFixture(source, destination).recordPairIdentities(t.Context()); err == nil {
			t.Fatalf("%s was accepted", name)
		}
	}
}

func TestRGWS3CredentialsRequireExactlyOneCompleteKey(t *testing.T) {
	cli := &rgwScriptCLI{replies: map[string]string{
		"user create --uid tc-user-abc --display-name Testcontainers multicluster S3 user": `{"keys":[{"access_key":"AK","secret_key":"SK"}]}`,
	}}
	f := newPhaseFixture(cli, cli)
	f.Source, f.Destination = &rgw.Gateway{}, &rgw.Gateway{}
	if err := f.issueS3Credentials(t.Context(), "abc"); err != nil {
		t.Fatal(err)
	}
	if f.Source.AccessKey != "AK" || f.Destination.SecretKey != "SK" {
		t.Fatal("both gateways must expose the issued key")
	}
	for _, reply := range []string{`{"keys":[]}`, `{"keys":[{"access_key":"AK"}]}`, `{"keys":[{"access_key":"A","secret_key":"S"},{"access_key":"B","secret_key":"T"}]}`, `[]`} {
		cli.replies["user create --uid tc-user-abc --display-name Testcontainers multicluster S3 user"] = reply
		f.Source, f.Destination = &rgw.Gateway{}, &rgw.Gateway{}
		if err := f.issueS3Credentials(t.Context(), "abc"); err == nil || f.Source.AccessKey != "" {
			t.Fatalf("credentials %s were accepted", reply)
		}
	}
}

func TestRGWNativeIDTreatsAnUnreadableDocumentAsMissing(t *testing.T) {
	cli := &rgwScriptCLI{replies: map[string]string{"zone get": `{"name":"a"}`, "realm get": `[`}, fail: map[string]bool{"zonegroup get": true}}
	f := newPhaseFixture(cli, cli)
	for entity, wantErr := range map[string]bool{"zone": false, "realm": false, "zonegroup": true} {
		id, err := f.realm.nativeID(t.Context(), cli, "a", entity)
		if id != "" || (err != nil) != wantErr {
			t.Fatalf("%s: id %q err %v", entity, id, err)
		}
	}
}

func TestRGWZoneAdditionRefusesBeforeCreatingAnything(t *testing.T) {
	cluster := &ceph.Container{}
	cases := map[string]struct {
		config      ZoneConfig
		createGroup bool
		change      func(*Fixture)
		want        string
	}{
		"invalid name":    {config: ZoneConfig{Name: " d"}, want: "invalid RGW zone"},
		"invalid group":   {config: ZoneConfig{Name: "d", Zonegroup: "-g"}, want: "invalid RGW zone"},
		"detached name":   {config: ZoneConfig{Name: "b"}, change: func(f *Fixture) { f.topology.removed = map[string]*rgwZoneRemoval{"b": {state: &rgwZoneState{}}} }, want: "retain configuration"},
		"pending removal": {config: ZoneConfig{Name: "d"}, change: func(f *Fixture) { f.topology.removed = map[string]*rgwZoneRemoval{"x": {state: &rgwZoneState{}}} }, want: "pending"},
		"detached cluster": {config: ZoneConfig{Name: "d", Cluster: cluster}, change: func(f *Fixture) {
			f.topology.removed = map[string]*rgwZoneRemoval{"x": {state: &rgwZoneState{cluster: cluster}, converged: true, gatewayRemoved: true, clientRemoved: true}}
		}, want: "storage clusters retain"},
		"unknown group":   {config: ZoneConfig{Name: "d", Zonegroup: "asia"}, want: "requires an owned zonegroup"},
		"known new group": {config: ZoneConfig{Name: "d", Zonegroup: "eu"}, createGroup: true, want: "requires an owned zonegroup"},
		"closed":          {config: ZoneConfig{Name: "d"}, change: func(f *Fixture) { f.closed = true }, want: "initialized and running"},
		"no credentials":  {config: ZoneConfig{Name: "d"}, change: func(f *Fixture) { f.realm.systemSecret = "" }, want: "initialized and running"},
		"already owned":   {config: ZoneConfig{Name: "a"}, want: "already owned"},
	}
	for name, tc := range cases {
		f, cli, _, _, _ := rgwRemovalFixture()
		f.realm.systemAccess, f.realm.systemSecret = "ACCESS", "SECRET"
		if tc.change != nil {
			tc.change(f)
		}
		_, err := f.admitZoneAddition(t.Context(), "image", tc.config, tc.createGroup)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %v, want %q", name, err, tc.want)
		}
		if len(cli.calls) != 0 || len(f.topology.additional) != 1 {
			t.Fatalf("%s: a refused addition ran commands or registered a zone", name)
		}
	}
}

func TestRGWAdditionMasterRefusesANameInTheCommittedPeriod(t *testing.T) {
	f, _, _, _, _ := rgwRemovalFixture()
	master, err := f.additionMaster(t.Context(), "d")
	if err != nil || master.Name != "a" {
		t.Fatalf("master = %v, %v", master, err)
	}
	// An external zone the fixture does not own is still in the period.
	f.topology.removed = map[string]*rgwZoneRemoval{"b": {state: f.zoneStates()[1], detached: true}}
	if _, err := f.additionMaster(t.Context(), "b"); err == nil {
		t.Fatal("a zone name already in the committed period was accepted")
	}
	f.topology.removed = nil
	f.SourceZoneID = "renamed"
	if _, err := f.additionMaster(t.Context(), "d"); err == nil || !strings.Contains(err.Error(), "not an owned zone") {
		t.Fatalf("an unowned metadata master was accepted: %v", err)
	}
}

func TestRGWAddedZoneIsOwnedBeforeItsFirstContainer(t *testing.T) {
	f, _, _, _, _ := rgwRemovalFixture()
	state := f.registerAddedZone(ZoneConfig{Name: "d", Zonegroup: "eu"})
	if f.topology.additional["d"] != state || f.realm.zoneGroupName("d") != "eu" {
		t.Fatal("the added zone was not registered")
	}
	if !slices.ContainsFunc(f.Zones(), func(zone Zone) bool { return zone.Name == "d" && zone.ID == "" }) {
		t.Fatal("a partial zone must be visible through Zones")
	}
	empty := &Fixture{}
	empty.registerAddedZone(ZoneConfig{Name: "z", Zonegroup: "g"})
	if empty.realm.zoneGroupName("z") != "g" {
		t.Fatal("registration into an empty fixture lost the zonegroup")
	}
}

func TestRGWAddedZoneIdentityRecordsANewGroupMaster(t *testing.T) {
	cli := &rgwScriptCLI{replies: map[string]string{"zone get": `{"id":"d-id"}`, "zonegroup get": `{"id":"asia-id"}`}}
	f := newPhaseFixture(cli, cli)
	state := f.registerAddedZone(ZoneConfig{Name: "d", Zonegroup: "asia"})
	state.client = cli
	if err := f.recordAddedZoneIdentity(t.Context(), state, true); err != nil {
		t.Fatal(err)
	}
	if state.ID != "d-id" || f.topology.groupIDs["asia"] != "asia-id" || f.topology.groupMasters["asia"] != "d" {
		t.Fatalf("identity %q groups %v masters %v", state.ID, f.topology.groupIDs, f.topology.groupMasters)
	}
	member := f.registerAddedZone(ZoneConfig{Name: "e", Zonegroup: "us"})
	member.client = cli
	cli.ops = nil
	if err := f.recordAddedZoneIdentity(t.Context(), member, false); err != nil || slices.Contains(cli.ops, "zonegroup get") || f.topology.groupMasters["us"] != "a" {
		t.Fatalf("a member zone changed its group: %v %q", err, cli.ops)
	}
	cli.replies["zone get"] = `{}`
	if err := f.recordAddedZoneIdentity(t.Context(), member, false); err == nil {
		t.Fatal("an added zone without an ID was accepted")
	}
	cli.replies["zone get"], cli.replies["zonegroup get"] = `{"id":"d-id"}`, `{}`
	if err := f.recordAddedZoneIdentity(t.Context(), state, true); err == nil {
		t.Fatal("a new zonegroup without an ID was accepted")
	}
}

func TestRGWZoneRemovalRefusesAZoneThatBecameAMaster(t *testing.T) {
	f, _, _, _, _ := rgwRemovalFixture()
	states := f.zoneStates()
	period, err := decodeRGWTopologyPeriod([]byte(rgwTwoGroups))
	if err != nil {
		t.Fatal(err)
	}
	master := rgwPeriodMaster(period, states)
	removal, err := f.zoneRemoval(period, master, states, "b")
	if err != nil || removal.state.Name != "b" || f.topology.removed["b"] != removal {
		t.Fatalf("removal = %v, %v", removal, err)
	}
	again, err := f.zoneRemoval(period, master, states, "b")
	if err != nil || again != removal {
		t.Fatal("a recorded removal was not resumed")
	}
	promoted, err := decodeRGWTopologyPeriod([]byte(strings.Replace(rgwTwoGroups, `"master_zone":"a-id","endpoints"`, `"master_zone":"b-id","endpoints"`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.zoneRemoval(promoted, master, states, "b"); err == nil || !strings.Contains(err.Error(), "zonegroup master") {
		t.Fatalf("a recorded removal of a promoted zonegroup master continued: %v", err)
	}
	// Once detached, only the exact graph without the zone is accepted.
	removal.detached = true
	if _, err := f.zoneRemoval(period, master, slices.DeleteFunc(slices.Clone(states), func(s *rgwZoneState) bool { return s.Name == "c" }), "b"); err == nil {
		t.Fatal("a detached removal accepted an unrelated graph change")
	}
}

func TestRGWDetachZoneDoesNotRepeatALostRemoval(t *testing.T) {
	f, cli, _, b, _ := rgwRemovalFixture()
	cli.removed = true // the zone left the group, but the response was lost
	states := f.zoneStates()
	if err := f.detachZone(t.Context(), states[0], states[1]); err != nil {
		t.Fatal(err)
	}
	if b.running || b.stops != 1 {
		t.Fatal("the leaving gateway was not fenced")
	}
	for _, args := range cli.calls {
		if strings.Contains(strings.Join(args, " "), "zonegroup remove") {
			t.Fatal("an already removed zone was removed again")
		}
	}
	if !cli.committed {
		t.Fatal("the period was not committed")
	}
	if err := f.detachZone(t.Context(), states[0], &rgwZoneState{}); err == nil || !strings.Contains(err.Error(), "partially initialized") {
		t.Fatalf("a partial zone was detached: %v", err)
	}
}

func TestRGWRunRequiresExactlyOneRealmOption(t *testing.T) {
	if _, err := Run(t.Context(), "image"); err == nil {
		t.Fatal("Run without WithConfig or WithTopology was accepted")
	}
	if _, err := Run(t.Context(), "image", WithConfig(Config{}), WithTopology(TopologyConfig{})); err == nil {
		t.Fatal("Run with two realm options was accepted")
	}
	if _, err := Run(t.Context(), "image", Option{}); err == nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an empty Option was accepted: %v", err)
	}
	if err := (Option{}).Customize(nil); err != nil {
		t.Fatal("an Option must not customize a container")
	}
	groups := []ZonegroupConfig{{Name: "us", Zones: []ZoneConfig{{Name: "a"}}}}
	option := WithTopology(TopologyConfig{Zones: []ZoneConfig{{Name: "a"}}, Zonegroups: groups})
	groups[0].Zones[0].Name = "changed"
	if option.topology.Zonegroups[0].Zones[0].Name != "a" {
		t.Fatal("WithTopology kept the caller's zone slice")
	}
}

func TestRGWPeriodPullRequiresSystemCredentialsAndThePeerEndpoint(t *testing.T) {
	cli := &rgwScriptCLI{}
	f := newPhaseFixture(cli, cli)
	if err := f.PullSourcePeriod(t.Context()); err == nil {
		t.Fatal("a pull without the destination endpoint was accepted")
	}
	f.realm.sourceURL, f.realm.destinationURL = "http://a:7480", "http://b:7480"
	if err := f.PullSourcePeriod(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.PullDestinationPeriod(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"realm pull --url http://b:7480 --access-key ACCESS --secret SECRET",
		"realm pull --url http://a:7480 --access-key ACCESS --secret SECRET",
	}
	if !slices.Equal(cli.ops, want) {
		t.Fatalf("pulls: %q", cli.ops)
	}
	f.realm.systemSecret = ""
	if err := f.PullDestinationPeriod(t.Context()); err == nil {
		t.Fatal("a pull without system credentials was accepted")
	}
}

const rgwMetadataReady = `{"sync_status":{"info":{"status":"sync","num_shards":1},"markers":[{"key":0,"val":{"state":1}}]}}`

func TestRGWMetadataBootstrapWaitsOnlyForRunningSecondaries(t *testing.T) {
	master := &rgwScriptCLI{fail: map[string]bool{"metadata sync status --format json": true}}
	ready := &rgwScriptCLI{replies: map[string]string{"metadata sync status --format json": rgwMetadataReady}}
	stopped := &rgwScriptCLI{fail: map[string]bool{"metadata sync status --format json": true}}
	states := []*rgwZoneState{
		{Zone: Zone{Name: "a", ID: "a-id", Gateway: &rgw.Gateway{Container: &rgwLifecycleContainer{running: true}}}, client: master},
		{Zone: Zone{Name: "b", ID: "b-id", Gateway: &rgw.Gateway{Container: &rgwLifecycleContainer{running: true}}}, client: ready},
		{Zone: Zone{Name: "c", ID: "c-id", Gateway: &rgw.Gateway{Container: &rgwLifecycleContainer{}}}, client: stopped},
	}
	f := newPhaseFixture(master, ready)
	if err := f.realm.waitZoneMetadataBootstrap(t.Context(), states[0], states); err != nil {
		t.Fatal(err)
	}
	if len(master.ops) != 0 || len(stopped.ops) != 0 || len(ready.ops) != 1 {
		t.Fatalf("polled: master %d, running secondary %d, stopped secondary %d", len(master.ops), len(ready.ops), len(stopped.ops))
	}
	ready.replies["metadata sync status --format json"] = `{"sync_status":{"info":{"status":"building-full-sync-maps","num_shards":1}}}`
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.realm.waitZoneMetadataBootstrap(ctx, states[0], states); err == nil || !strings.Contains(err.Error(), "did not complete") {
		t.Fatalf("an unfinished bootstrap was accepted after cancellation: %v", err)
	}
}

func TestRGWZoneLinkInterruptionRequiresAnOwnedBridgeZone(t *testing.T) {
	probe := &clustertest.NativeCallCounter{}
	f := &Fixture{realm: rgwRealmAccess{sourceClient: probe, destinationClient: probe, config: Config{SourceZone: "a", DestinationZone: "b"}}}
	if _, err := f.InterruptZoneLink(t.Context(), "a"); err == nil || !strings.Contains(err.Error(), "bridge") {
		t.Fatalf("a host-mode fixture interrupted a zone link: %v", err)
	}
	f.httpNetwork = &testcontainers.DockerNetwork{Name: "bridge"}
	if _, err := f.InterruptZoneLink(t.Context(), "z"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an unknown zone link was interrupted: %v", err)
	}
	// Zone a has no gateway yet, so it has no link either.
	if _, err := f.InterruptZoneLink(t.Context(), "a"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a zone without a gateway was interrupted: %v", err)
	}
	loss := fault.NetworkProfile{HelperImage: "netem-helper:test", Loss: 100}
	if _, err := f.ApplyZoneLinkProfile(t.Context(), "a", "b", loss); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("zones without gateways were shaped: %v", err)
	}
	f.Source = &rgw.Gateway{Container: &rgwLifecycleContainer{running: true}}
	f.Destination = &rgw.Gateway{Container: &rgwLifecycleContainer{running: true}}
	f.realm.destinationURL = "http://b"
	if _, err := f.ApplyZoneLinkProfile(t.Context(), "a", "b", loss); err == nil || !strings.Contains(err.Error(), "port") {
		t.Fatalf("a peer endpoint without a port was accepted: %v", err)
	}
	f.closed = true
	if _, err := f.InterruptZoneLink(t.Context(), "a"); err == nil {
		t.Fatal("a terminated fixture interrupted a zone link")
	}
	if probe.Execs.Load() != 0 {
		t.Fatal("a refused zone link fault ran a native command")
	}
}

func TestRGWZoneAdminRunsOnlyInAnOwnedRunningZone(t *testing.T) {
	cli := &rgwScriptCLI{replies: map[string]string{"zone get": `{"id":"a-id"}`}}
	f := newPhaseFixture(cli, cli)
	if data, err := f.ZoneAdmin(t.Context(), "a", "zone", "get"); err != nil || string(data) != `{"id":"a-id"}` {
		t.Fatalf("owned zone: %q, %v", data, err)
	}
	if _, err := f.ZoneAdmin(t.Context(), "z", "zone", "get"); err == nil {
		t.Fatal("an unowned zone ran radosgw-admin")
	}
	f.closed = true
	if _, err := f.ZoneAdmin(t.Context(), "a", "zone", "get"); err == nil {
		t.Fatal("a terminated fixture ran radosgw-admin")
	}
	if len(cli.ops) != 1 {
		t.Fatalf("refused calls ran commands: %q", cli.ops)
	}
}

func TestRGWTopologyNormalizationRefusesAmbiguousLayouts(t *testing.T) {
	two := []ZoneConfig{{Name: "a"}, {Name: "b"}}
	for name, config := range map[string]TopologyConfig{
		"zones and zonegroups":     {Zones: two, Zonegroups: []ZonegroupConfig{{Name: "us", Zones: two}}},
		"master group without any": {Zones: two, MasterZonegroup: "us"},
		"invalid flat group":       {Zones: two, Zonegroup: "-us"},
		"conflicting flat group":   {Zones: []ZoneConfig{{Name: "a", Zonegroup: "eu"}, {Name: "b"}}, Zonegroup: "us"},
		"one zone":                 {Zones: two[:1]},
		"invalid zone":             {Zones: []ZoneConfig{{Name: "a"}, {Name: " b"}}},
		"duplicate zone":           {Zones: []ZoneConfig{{Name: "a"}, {Name: "a"}}},
		"unknown metadata master":  {Zones: two, MetadataMaster: "c"},
		"duplicate group":          {Zonegroups: []ZonegroupConfig{{Name: "us", Zones: two[:1]}, {Name: "us", Zones: two[1:]}}},
		"empty group":              {Zonegroups: []ZonegroupConfig{{Name: "us", Zones: two}, {Name: "eu"}}},
		"unknown group master":     {Zonegroups: []ZonegroupConfig{{Name: "us", MasterZone: "c", Zones: two}}},
		"zone in another group":    {Zonegroups: []ZonegroupConfig{{Name: "us", Zones: []ZoneConfig{{Name: "a", Zonegroup: "eu"}, {Name: "b"}}}}},
		"unknown master group":     {Zonegroups: []ZonegroupConfig{{Name: "us", Zones: two}}, MasterZonegroup: "eu"},
	} {
		if _, err := normalizeRGWTopology(config); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	zones, err := normalizeRGWTopology(TopologyConfig{Zones: []ZoneConfig{{Name: "a"}, {Name: "b"}, {Name: "c"}}, MetadataMaster: "c"})
	if err != nil || zones[0].Name != "c" || zones[1].Name != "a" || zones[2].Name != "b" || zones[0].Zonegroup != "us-east-1" {
		t.Fatalf("flat master ordering: %+v, %v", zones, err)
	}
}

func TestRGWPipeFilterSortsTagsAndRefusesUnrepresentableOnes(t *testing.T) {
	args, filter, err := syncPipeFilter(SyncPipeConfig{Prefix: "logs/", Tags: []SyncObjectTag{{Key: "b", Value: "2"}, {Key: "a", Value: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"--prefix", "logs/", "--tags-add", "a=1,b=2"}; !slices.Equal(args, want) {
		t.Fatalf("args %q, want %q", args, want)
	}
	tags := filter["tags"].([]any)
	if filter["prefix"] != "logs/" || len(tags) != 2 || tags[0].(map[string]any)["key"] != "a" {
		t.Fatalf("filter %v", filter)
	}
	if args, filter, err := syncPipeFilter(SyncPipeConfig{}); err != nil || len(args) != 0 || len(filter["tags"].([]any)) != 0 || filter["prefix"] != nil {
		t.Fatalf("an empty filter: %q %v %v", args, filter, err)
	}
	if _, _, err := syncPipeFilter(SyncPipeConfig{Tags: []SyncObjectTag{{Key: "a", Value: "1"}, {Key: "a", Value: "1"}}}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a repeated tag pair was accepted: %v", err)
	}
}

func TestRGWDestinationClassRequiresConcreteOwnedPlacements(t *testing.T) {
	f, _, _, _ := newRGWSyncTestFixture()
	g := &SyncGroup{owner: f, scope: SyncPolicyScope{Zonegroup: "group"}}
	info := &rgwBucketInstance{PlacementRule: nativeString{value: "tiered/COLD", set: true}}
	placement := map[string]*rgw.Placement{"destination": {}}
	for name, tc := range map[string]struct {
		config SyncPipeConfig
		info   *rgwBucketInstance
	}{
		"no bucket":         {SyncPipeConfig{DestinationZones: []string{"destination"}, DestinationPlacements: placement}, nil},
		"count mismatch":    {SyncPipeConfig{DestinationZones: []string{"destination", "source"}, DestinationPlacements: placement}, info},
		"wildcard zone":     {SyncPipeConfig{DestinationZones: []string{"*"}, DestinationPlacements: map[string]*rgw.Placement{"*": {}}}, info},
		"no placement rule": {SyncPipeConfig{DestinationZones: []string{"destination"}, DestinationPlacements: placement}, &rgwBucketInstance{}},
		"unattached zone":   {SyncPipeConfig{DestinationZones: []string{"elsewhere"}, DestinationPlacements: map[string]*rgw.Placement{"elsewhere": {}}}, info},
		"unowned placement": {SyncPipeConfig{DestinationZones: []string{"destination"}, DestinationPlacements: placement}, info},
	} {
		if err := g.destinationClass(t.Context(), tc.config, tc.info); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRGWBucketInstanceMatchesOnlyItsExactNativeIdentity(t *testing.T) {
	g := &SyncGroup{scope: SyncPolicyScope{Bucket: "input", Tenant: ""}, bucketID: "id-1", groupID: "group-id"}
	const doc = `{"key":"bucket.instance:input:id-1","data":{"bucket_info":{"bucket":{"name":"input","tenant":"","bucket_id":"id-1"},"zonegroup":"group-id","owner":"alice","placement_rule":"tiered/COLD","native_extra":{"kept":true},"sync_policy":{"groups":[{"id":"g1","status":"enabled","native_unknown":[1,2]}]}}}}`
	instance, matched, err := decodeBucketInstance([]byte(doc), "input:id-1", g)
	if err != nil || !matched {
		t.Fatalf("exact instance: matched %v, %v", matched, err)
	}
	if !instance.Owner.is("alice") || instance.PlacementRule.value != "tiered/COLD" {
		t.Fatalf("typed fields: %+v", instance)
	}
	// The policy is the exact native document, unknown members included, so
	// a later readback or write-back compares the whole group.
	group := syncNativeGroup(instance.policy, "g1")
	if group == nil || group["native_unknown"] == nil || group["status"] != "enabled" {
		t.Fatalf("policy lost native members: %v", instance.policy)
	}
	for name, edit := range map[string]func(string) string{
		"other key": func(s string) string {
			return strings.Replace(s, `"key":"bucket.instance:input:id-1"`, `"key":"bucket.instance:input:id-2"`, 1)
		},
		"missing tenant":     func(s string) string { return strings.Replace(s, `"tenant":"",`, ``, 1) },
		"null tenant":        func(s string) string { return strings.Replace(s, `"tenant":""`, `"tenant":null`, 1) },
		"numeric instance":   func(s string) string { return strings.Replace(s, `"bucket_id":"id-1"`, `"bucket_id":1`, 1) },
		"other zonegroup":    func(s string) string { return strings.Replace(s, `"zonegroup":"group-id"`, `"zonegroup":"other"`, 1) },
		"bucket_info string": func(string) string { return `{"key":"bucket.instance:input:id-1","data":{"bucket_info":"input"}}` },
		"no bucket_info":     func(string) string { return `{"key":"bucket.instance:input:id-1","data":{}}` },
		"data not an object": func(string) string { return `{"key":"bucket.instance:input:id-1","data":[]}` },
		// A later null for a repeated key wins, as in the native map; an
		// earlier valid value must not survive it.
		"repeated tenant then null": func(s string) string { return strings.Replace(s, `"tenant":""`, `"tenant":"","tenant":null`, 1) },
		// Only the exact native key names the bucket; a case alias is ignored.
		"case alias name": func(s string) string { return strings.Replace(s, `"name":"input"`, `"name":"other","NAME":"input"`, 1) },
		"case alias key": func(s string) string {
			return strings.Replace(s, `"key":"bucket.instance:input:id-1"`, `"key":"x","KEY":"bucket.instance:input:id-1"`, 1)
		},
	} {
		if _, matched, err := decodeBucketInstance([]byte(edit(doc)), "input:id-1", g); matched || err != nil {
			t.Errorf("%s: matched %v, err %v", name, matched, err)
		}
	}
	// A non-string owner leaves the member unset instead of failing the
	// document; a principal comparison with it then fails.
	odd, matched, err := decodeBucketInstance([]byte(strings.Replace(doc, `"owner":"alice"`, `"owner":{"id":"alice"}`, 1)), "input:id-1", g)
	if err != nil || !matched || odd.Owner.set || odd.Owner.is("") {
		t.Fatalf("object owner: %+v, %v, %v", odd, matched, err)
	}
	for name, edit := range map[string]string{
		"repeated owner then null": `"owner":"alice","owner":null`,
		"case alias owner":         `"owner":"mallory","OWNER":"alice"`,
	} {
		odd, matched, err := decodeBucketInstance([]byte(strings.Replace(doc, `"owner":"alice"`, edit, 1)), "input:id-1", g)
		if err != nil || !matched || odd.Owner.is("alice") {
			t.Errorf("%s: owner %+v, matched %v, err %v", name, odd.Owner, matched, err)
		}
	}
	if _, _, err := decodeBucketInstance([]byte(strings.Replace(doc, `"groups":[`, `"groups":{"x":[`, 1)+"}"), "input:id-1", g); err == nil {
		t.Fatal("a malformed instance document was accepted")
	}
	if _, matched, err := decodeBucketInstance([]byte(strings.Replace(doc, `"sync_policy":{"groups":[`, `"sync_policy":{"groups":7,"x":[`, 1)), "input:id-1", g); !matched || err == nil {
		t.Fatalf("a malformed policy was accepted: matched %v, %v", matched, err)
	}
	if _, _, err := decodeBucketInstance([]byte(doc+"{}"), "input:id-1", g); err == nil {
		t.Fatal("trailing native output was accepted")
	}
}
