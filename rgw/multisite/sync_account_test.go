package multisite

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"maps"
	"testing"
)

func TestRGWSyncAccountRootRequiresCapturedAccountBucketsWithoutTranslation(t *testing.T) {
	info := rgw.UserInfo{ID: "tenant$root", Tenant: "tenant", LocalID: "root", Type: "root", AccountID: "RGW12345678901234567"}
	scope := SyncPolicyScope{Bucket: "input", Tenant: "tenant"}
	config := SyncPipeConfig{SourceBucket: &SyncBucketSelector{Name: "input", Tenant: "tenant"}, DestinationBucket: &SyncBucketSelector{Name: "output", Tenant: "tenant"}}
	bucket := map[string]any{"owner": info.AccountID}
	if err := checkSyncUserModePrincipal(scope, config, info, bucket, bucket); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"iam-nonroot", "root-without-account", "source-other-account", "destination-other-account", "owner-translation", "source-wildcard", "destination-wildcard", "cross-tenant", "missing-native-source"} {
		t.Run(name, func(t *testing.T) {
			principal, requested := info, config
			source, destination := maps.Clone(bucket), maps.Clone(bucket)
			switch name {
			case "iam-nonroot":
				principal.Type = "rgw"
			case "root-without-account":
				principal.AccountID = ""
			case "source-other-account":
				source["owner"] = "RGW00000000000000000"
			case "destination-other-account":
				destination["owner"] = "RGW00000000000000000"
			case "owner-translation":
				requested.DestinationOwner = &rgw.User{}
			case "source-wildcard":
				requested.SourceBucket = nil
			case "destination-wildcard":
				requested.DestinationBucket = nil
			case "cross-tenant":
				requested.DestinationBucket = &SyncBucketSelector{Name: "output", Tenant: "other"}
			case "missing-native-source":
				source = nil
			}
			if err := checkSyncUserModePrincipal(scope, requested, principal, source, destination); err == nil {
				t.Fatal("unproven or incomplete account-root identity was accepted")
			}
		})
	}
}

func TestRGWSyncOrdinaryUserModePreservesCurrentBucketBehavior(t *testing.T) {
	for _, kind := range []string{"", "rgw"} {
		info := rgw.UserInfo{ID: "ordinary", Type: kind}
		if err := checkSyncUserModePrincipal(SyncPolicyScope{Bucket: "bucket"}, SyncPipeConfig{}, info, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
}
