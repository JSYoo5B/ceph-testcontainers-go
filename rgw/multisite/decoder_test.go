package multisite

import (
	"strings"
	"testing"
)

// TestIdentityDecodersRefuseCaseAndDuplicateMembers feeds every decoder of an
// identity-bearing document a member spelled in another case and a repeated
// member. Each refuses both, so no check is decided by a member that the
// native tool did not write as read.
func TestIdentityDecodersRefuseCaseAndDuplicateMembers(t *testing.T) {
	want := BucketSyncStatus{RealmID: "realm", ZonegroupID: "group", ZoneID: "zone", DestinationBucket: SyncBucketIdentity{Name: "b", ID: "instance"}}
	syncStatus := `{"realm":"realm","zonegroup":"group","zone":"zone","bucket":"b","bucket_instance_id":"instance","sources":[]}`
	zonegroup := `{"id":"g","name":"us","master_zone":"a","endpoints":[],"zones":[{"id":"a","name":"a","endpoints":[]}]}`
	metadata := `{"sync_status":{"info":{"status":"sync","num_shards":1,"period":"p","realm_epoch":1},"markers":[{"key":0,"val":{"state":1,"marker":""}}]}}`
	decoders := map[string]struct {
		valid  string
		decode func(string) error
		cased  [2]string
		repeat [2]string
	}{
		"bucket sync status": {syncStatus, func(d string) error { _, err := decodeBucketSyncStatus([]byte(d), want); return err },
			[2]string{`"bucket_instance_id"`, `"Bucket_Instance_ID"`}, [2]string{`"bucket":"b"`, `"bucket":"a","bucket":"b"`}},
		"zonegroup get": {zonegroup, func(d string) error { _, err := decodeZonegroupDocument([]byte(d)); return err },
			[2]string{`{"id":"a"`, `{"ID":"a"`}, [2]string{`{"id":"a"`, `{"id":"x","id":"a"`}},
		"metadata sync status": {metadata, func(d string) error { _, _, err := decodeSyncMetadata([]byte(d)); return err },
			[2]string{`"key"`, `"Key"`}, [2]string{`"key":0`, `"key":0,"key":0`}},
	}
	for name, d := range decoders {
		if err := d.decode(d.valid); err != nil {
			t.Fatalf("%s: the valid document was refused: %v", name, err)
		}
		for variant, edit := range map[string][2]string{"cased": d.cased, "repeated": d.repeat} {
			document := strings.Replace(d.valid, edit[0], edit[1], 1)
			if document == d.valid {
				t.Fatalf("%s %s: the edit changed nothing", name, variant)
			}
			if err := d.decode(document); err == nil {
				t.Errorf("%s: a %s member was accepted: %s", name, variant, document)
			}
		}
	}
}

// TestReloadGateAndSyncStatusShareTheMetadataVerdict reads one metadata sync
// status through the reload gate and through the SyncStatus decoder: a
// missing shard key, a repeated one and fewer markers than shards are not
// bootstrapped for either.
func TestReloadGateAndSyncStatusShareTheMetadataVerdict(t *testing.T) {
	status := func(markers string) string {
		return `{"sync_status":{"info":{"status":"sync","num_shards":2,"period":"p","realm_epoch":1},"markers":[` + markers + `]}}`
	}
	const shard0, shard1 = `{"key":0,"val":{"state":1,"marker":""}}`, `{"key":1,"val":{"state":1,"marker":""}}`
	for name, c := range map[string]struct {
		document     string
		bootstrapped bool
	}{
		"every shard incremental": {status(shard0 + "," + shard1), true},
		"missing key":             {status(shard0 + `,{"val":{"state":1,"marker":""}}`), false},
		"duplicate key":           {status(shard0 + "," + shard0), false},
		"fewer markers":           {status(shard0), false},
		"shard in full sync":      {status(shard0 + `,{"key":1,"val":{"state":0,"marker":""}}`), false},
	} {
		gate := rgwMetadataBootstrapped([]byte(c.document)) == nil
		decoded, _, err := decodeSyncMetadata([]byte(c.document))
		syncStatus := err == nil && decoded.bootstrapped()
		if gate != c.bootstrapped || syncStatus != c.bootstrapped {
			t.Errorf("%s: reload gate %v, SyncStatus %v, want %v", name, gate, syncStatus, c.bootstrapped)
		}
	}
}
