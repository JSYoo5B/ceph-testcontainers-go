package multicluster

import (
	"reflect"
	"strings"
	"testing"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestRGWSyncPipeCapturesDistinctBucketInstancesAndTagPriority(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.additionalBuckets = map[string]string{"input": "input-instance", "output": "output-instance"}
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "translate", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	config := RGWSyncPipeConfig{ID: "filtered", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &RGWSyncBucketSelector{Name: "input"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output"}, Tags: []RGWSyncObjectTag{{Key: "color", Value: "red"}, {Key: "color", Value: "blue"}}, Prefix: "published/", Priority: 7}
	if err := f.CreateSyncPipe(t.Context(), g, config); err != nil {
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
	if err := f.SetSyncPipePrefix(t.Context(), g, "filtered", "reports/"); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveSyncPipe(t.Context(), g, "filtered"); err != nil {
		t.Fatal(err)
	}
}

func TestRGWSyncTagsRespectNativeRepresentability(t *testing.T) {
	for _, tags := range [][]RGWSyncObjectTag{{{Key: "key", Value: "comma,value"}}, {{Key: "comma,key", Value: "v"}}, {{Key: "equals=key", Value: "v"}}, {{Key: "", Value: "v"}}, {{Key: "key", Value: "v"}, {Key: "key", Value: "v"}}, {{Key: "key", Value: strings.Repeat("x", 257)}}} {
		if _, err := normalizeSyncTags(tags); err == nil {
			t.Fatal("invalid or ambiguously encoded tags accepted")
		}
	}
	if got, err := normalizeSyncTags([]RGWSyncObjectTag{{Key: "empty", Value: ""}, {Key: "equal", Value: "a=b"}}); err != nil || len(got) != 2 {
		t.Fatal("representable native tag values were rejected")
	}
}

func TestRGWSyncTenantBucketScopeUsesCanonicalNativeKey(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.bucketTenant = "tenant-a"
	cli.additionalBuckets = map[string]string{"tenant-b/output": "output-instance"}
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{Bucket: "bucket", Tenant: "tenant-a"}, RGWSyncGroupConfig{ID: "tenant-selector", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	config := RGWSyncPipeConfig{ID: "tenant-pipe", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &RGWSyncBucketSelector{Name: "bucket", Tenant: "tenant-a"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output", Tenant: "tenant-b"}}
	if err := f.CreateSyncPipe(t.Context(), g, config); err != nil {
		t.Fatal(err)
	}
	pipe := syncNativePipe(g.state.group, config.ID)
	if pipe["source"].(map[string]any)["bucket"] != "tenant-a/bucket:bucket-original" || pipe["dest"].(map[string]any)["bucket"] != "tenant-b/output:output-instance" {
		t.Fatal("tenant bucket selector lost its exact native namespace/instance")
	}
	if err := f.RemoveSyncPipe(t.Context(), g, config.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.RemoveSyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	for _, args := range cli.calls {
		if syncTestFlag(args, "--tenant") != "" {
			t.Fatal("bucket operation used global --tenant without a native user ID")
		}
	}
}

func TestRGWSyncPipeRefusesUnconfirmedPrincipalsAndClassMappings(t *testing.T) {
	for _, config := range []RGWSyncPipeConfig{
		{DestinationOwner: &ceph.RGWUser{}},
		{User: &ceph.RGWUser{}},
		{DestinationStorageClass: "STANDARD_IA"},
		{DestinationPlacements: map[string]*ceph.RGWPlacement{"destination": {}}},
		{SourceBucket: &RGWSyncBucketSelector{Name: "*"}},
	} {
		f, cli, _, _ := newRGWSyncTestFixture()
		g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "guard", Status: RGWSyncEnabled})
		if err != nil {
			t.Fatal(err)
		}
		config.ID = "unsafe"
		config.SourceZones = []string{"source"}
		config.DestinationZones = []string{"destination"}
		if err := f.CreateSyncPipe(t.Context(), g, config); err == nil {
			t.Fatal("unconfirmed identity or destination mapping accepted")
		}
		for _, args := range cli.calls {
			if syncTestCommand(args, "pipe", "create") {
				t.Fatal("invalid pipe reached native mutation")
			}
		}
	}
}

func TestRGWSyncPrincipalRequiresExactSourceDestinationTenants(t *testing.T) {
	scope := RGWSyncPolicyScope{Bucket: "input", Tenant: "tenant-a"}
	config := RGWSyncPipeConfig{SourceBucket: &RGWSyncBucketSelector{Name: "input", Tenant: "tenant-a"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output", Tenant: "tenant-a"}}
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
	if err := checkSyncPrincipalTenants(RGWSyncPolicyScope{}, RGWSyncPipeConfig{}, ""); err == nil {
		t.Fatal("unbounded global wildcard source accepted for a principal")
	}
	if err := checkSyncPrincipalTenants(RGWSyncPolicyScope{Bucket: "input"}, RGWSyncPipeConfig{}, ""); err != nil {
		t.Fatal("legacy current-bucket principal rejected")
	}
}
