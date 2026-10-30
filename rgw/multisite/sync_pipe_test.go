package multisite

import (
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestRGWSyncPipeCapturesDistinctBucketInstancesAndTagPriority(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.additionalBuckets = map[string]string{"input": "input-instance", "output": "output-instance"}
	g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{}, SyncGroupConfig{ID: "translate", Status: SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	config := SyncPipeConfig{ID: "filtered", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "input"}, DestinationBucket: &SyncBucketSelector{Name: "output"}, Tags: []SyncObjectTag{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}}, Prefix: "published/", Priority: 7}
	if err := g.CreatePipe(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	pipe := syncNativePipe(g.state.group, "filtered")
	if pipe["source"].(map[string]any)["bucket"] != "input:input-instance" || pipe["dest"].(map[string]any)["bucket"] != "output:output-instance" {
		t.Fatal("selector did not capture native bucket instance IDs")
	}
	filter := pipe["params"].(map[string]any)["source"].(map[string]any)["filter"].(map[string]any)
	if !reflect.DeepEqual(filter["tags"], []any{map[string]any{"key": "color", "value": "blue"}, map[string]any{"key": "color", "value": "red"}}) {
		t.Fatal("same-key OR tags were lost or reordered incorrectly")
	}
	config.Tags[0].Value = "mutated"
	if err := g.SetPipePrefix(t.Context(), "filtered", "reports/"); err != nil {
		t.Fatal(err)
	}
	if err := g.RemovePipe(t.Context(), "filtered"); err != nil {
		t.Fatal(err)
	}
}

func TestRGWSyncTagsRespectNativeRepresentability(t *testing.T) {
	for _, tags := range [][]SyncObjectTag{{{Key: "key", Value: "comma,value"}}, {{Key: "comma,key", Value: "v"}}, {{Key: "equals=key", Value: "v"}}, {{Key: "", Value: "v"}}, {{Key: "key", Value: "v"}, {Key: "key", Value: "v"}}, {{Key: "key", Value: strings.Repeat("x", 257)}}} {
		if _, err := normalizeSyncTags(tags); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("invalid or ambiguously encoded tags returned %v", err)
		}
	}
	if got, err := normalizeSyncTags([]SyncObjectTag{{Key: "empty", Value: ""}, {Key: "equal", Value: "a=b"}}); err != nil || len(got) != 2 {
		t.Fatal("representable native tag values were rejected")
	}
}

func TestRGWSyncTenantBucketScopeUsesCanonicalNativeKey(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.bucketTenant = "tenant-a"
	cli.additionalBuckets = map[string]string{"tenant-b/output": "output-instance"}
	g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{Bucket: "bucket", Tenant: "tenant-a"}, SyncGroupConfig{ID: "tenant-selector", Status: SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	config := SyncPipeConfig{ID: "tenant-pipe", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "bucket", Tenant: "tenant-a"}, DestinationBucket: &SyncBucketSelector{Name: "output", Tenant: "tenant-b"}}
	if err := g.CreatePipe(t.Context(), config); err != nil {
		t.Fatal(err)
	}
	pipe := syncNativePipe(g.state.group, config.ID)
	if pipe["source"].(map[string]any)["bucket"] != "tenant-a/bucket:bucket-original" || pipe["dest"].(map[string]any)["bucket"] != "tenant-b/output:output-instance" {
		t.Fatal("tenant bucket selector lost its exact native namespace/instance")
	}
	if err := g.RemovePipe(t.Context(), config.ID); err != nil {
		t.Fatal(err)
	}
	if err := g.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, args := range cli.calls {
		if syncTestFlag(args, "--tenant") != "" {
			t.Fatal("bucket operation used global --tenant without a native user ID")
		}
	}
}

func TestRGWSyncPipeRefusesUnconfirmedPrincipalsAndClassMappings(t *testing.T) {
	for _, config := range []SyncPipeConfig{
		{DestinationOwner: &rgw.User{}},
		{User: &rgw.User{}},
		{DestinationStorageClass: "STANDARD_IA"},
		{DestinationPlacements: map[string]*rgw.Placement{"destination": {}}},
		{SourceBucket: &SyncBucketSelector{Name: "*"}},
	} {
		f, cli, _, _ := newRGWSyncTestFixture()
		g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{}, SyncGroupConfig{ID: "guard", Status: SyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		config.ID = "unsafe"
		config.SourceZones = []string{"source"}
		config.DestinationZones = []string{"destination"}
		if err := g.CreatePipe(t.Context(), config); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("unconfirmed identity or destination mapping %+v returned %v", config, err)
		}
		for _, args := range cli.calls {
			if syncTestCommand(args, "pipe", "create") {
				t.Fatal("invalid pipe reached native mutation")
			}
		}
	}
}

func TestRGWSyncPrincipalRequiresExactSourceDestinationTenants(t *testing.T) {
	scope := SyncPolicyScope{Bucket: "input", Tenant: "tenant-a"}
	config := SyncPipeConfig{SourceBucket: &SyncBucketSelector{Name: "input", Tenant: "tenant-a"}, DestinationBucket: &SyncBucketSelector{Name: "output", Tenant: "tenant-a"}}
	if err := checkSyncPrincipalTenants(scope, config, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	if err := checkSyncPrincipalTenants(scope, config, ""); err == nil {
		t.Fatal("legacy principal accepted for a tenant bucket")
	}
	config.DestinationBucket.Tenant = "tenant-b"
	if err := checkSyncPrincipalTenants(scope, config, "tenant-a"); err == nil {
		t.Fatal("cross-tenant owner/user-mode translation accepted without proof")
	}
	if err := checkSyncPrincipalTenants(SyncPolicyScope{}, SyncPipeConfig{}, ""); err == nil {
		t.Fatal("unbounded global wildcard source accepted for a principal")
	}
	if err := checkSyncPrincipalTenants(SyncPolicyScope{Bucket: "input"}, SyncPipeConfig{}, ""); err != nil {
		t.Fatal("legacy current-bucket principal rejected")
	}
}

// Ceph 19 resolves no source for a user-mode pipe of an account principal,
// and no tool reveals that, so the release of every zone decides; the
// refusal names the zone that runs it.
func TestRGWAccountPipeRefusalNamesTheSquidZone(t *testing.T) {
	cluster := func(version string) *ceph.Container {
		c := testhelper.TestCluster(nil, testhelper.TestConfig{})
		testhelper.TestSetCephVersion(c, version)
		return c
	}
	f := &Fixture{config: Config{SourceZone: "source", DestinationZone: "destination", Source: cluster("20.2.4"), Destination: cluster("19.2.5")}}
	var refusal *ceph.OperationError
	if err := accountPipeUnsupported(f.zoneBefore(20)); !errors.As(err, &refusal) || !errors.Is(err, ceph.ErrUnsupported) ||
		refusal.Target != "zone destination" || refusal.Version != "19.2.5" || !strings.Contains(err.Error(), "needs Ceph 20 or later") {
		t.Fatalf("a Squid destination zone returned %v", err)
	}
	f.config.Destination = cluster("20.2.4")
	if zone := f.zoneBefore(20); zone != nil {
		t.Fatalf("Tentacle zones returned zone %s", zone.Name)
	}
}
