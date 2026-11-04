package expect_test

import (
	"encoding/json"
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
)

// A stopped MON leaves the quorum but stays in the monmap, and the
// evaluation names it.
func ExampleMonitors_Evaluate() {
	var observed ceph.QuorumStatus // as observer.QuorumStatus decodes it
	if err := json.Unmarshal([]byte(`{"quorum_names":["a","b"],"monmap":{"fsid":"44444444-4444-4444-8444-444444444444",
		"mons":[{"name":"a"},{"name":"b"},{"name":"c"}]}}`), &observed); err != nil {
		panic(err)
	}
	want := expect.Monitors{
		FSID:    "44444444-4444-4444-8444-444444444444",
		Members: []string{"a", "b", "c"},
		Quorum:  []string{"a", "b", "c"},
	}
	result, err := want.Evaluate(observed)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	fmt.Println(result)
	// Output:
	// false
	// mons want fsid 44444444-4444-4444-8444-444444444444 members [a b c] quorum [a b c] in monmap [a b c] quorum [a b]: mon.c out of quorum
}
