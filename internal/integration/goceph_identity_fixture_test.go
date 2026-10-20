//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"encoding/json"
	"testing"
)

// A probe receipt names the go-ceph it was linked with, and only the pinned
// version passes: an old probe in a supplied client image, or a validator
// without an expected version, fails.
func TestGoCephIdentity(t *testing.T) {
	var receipt goCephIdentity
	if err := json.Unmarshal([]byte(`{"go_ceph": "v0.42.0", "fsid": "a", "cephx": true, "phase": "verify", "rados": {}}`), &receipt); err != nil {
		t.Fatal(err)
	}
	want := goCephIdentity{GoCeph: "v0.42.0", FSID: "a", Phase: "verify", CephX: true}
	if err := checkGoCephIdentity(receipt, want); err != nil {
		t.Fatalf("matching receipt: %v", err)
	}
	for name, test := range map[string]struct{ got, want goCephIdentity }{
		"older probe":         {goCephIdentity{GoCeph: "v0.41.0", FSID: "a", Phase: "verify", CephX: true}, want},
		"no version reported": {goCephIdentity{FSID: "a", Phase: "verify", CephX: true}, want},
		"no expected version": {receipt, goCephIdentity{FSID: "a", Phase: "verify", CephX: true}},
		"unreleased expected": {goCephIdentity{GoCeph: "(devel)", FSID: "a", Phase: "verify", CephX: true}, goCephIdentity{GoCeph: "(devel)", FSID: "a", Phase: "verify", CephX: true}},
		"another cluster":     {goCephIdentity{GoCeph: "v0.42.0", FSID: "b", Phase: "verify", CephX: true}, want},
		"another phase":       {goCephIdentity{GoCeph: "v0.42.0", FSID: "a", Phase: "seed", CephX: true}, want},
		"without cephx":       {goCephIdentity{GoCeph: "v0.42.0", FSID: "a", Phase: "verify"}, want},
	} {
		if err := checkGoCephIdentity(test.got, test.want); err == nil {
			t.Errorf("%s: accepted %+v for %+v", name, test.got, test.want)
		}
	}
}
