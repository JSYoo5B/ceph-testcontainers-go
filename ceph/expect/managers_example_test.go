package expect_test

import (
	"encoding/json"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
)

// A failover expectation names the standby that must take over and the GID
// of the active MGR it must replace, taken from an earlier observation.
func ExampleManagers_Evaluate() {
	var before, after ceph.ManagerStatus // as observer.ManagerStatus decodes them
	if err := json.Unmarshal([]byte(`{"available":true,"active_name":"a","active_gid":4100,
		"standbys":[{"name":"b","gid":4200}]}`), &before); err != nil {
		panic(err)
	}
	// ... fail the active MGR while the SUT runs ...
	if err := json.Unmarshal([]byte(`{"available":false,"active_name":"a","active_gid":4100,
		"standbys":[{"name":"b","gid":4200}]}`), &after); err != nil {
		panic(err)
	}
	want := expect.Managers{Available: true, Active: before.Standbys[0].Name, FormerGID: before.ActiveGID}
	result, err := want.Evaluate(after)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// mgrs want available=true active mgr.b gid other than 4100 in available=false active mgr.a gid 4100 standbys [b]: available: want true, observed false; active: want mgr.b, observed mgr.a; active gid: want other than 4100, observed 4100
}
