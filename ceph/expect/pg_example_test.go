package expect_test

import (
	"fmt"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
)

// A PG expectation pins the pool of a baseline, so a recreated pool of the
// same name does not pass, and names each PG and condition that differs.
func ExamplePGs_Evaluate() {
	pool := ceph.PoolState{ID: 3, Name: "orders", Type: "replicated", Size: 2, MinSize: 1, PGNum: 2}
	observed := ceph.PoolPGSnapshot{
		FSID:       "44444444-4444-4444-8444-444444444444",
		PoolBefore: pool, PoolAfter: pool,
		PGs: []ceph.PGState{
			{PGID: "3.0", State: "active+clean", Up: []int{0, 1}, Acting: []int{0, 1}, UpPrimary: 0, ActingPrimary: 0},
			{PGID: "3.1", State: "active+undersized+degraded", Up: []int{2}, Acting: []int{2}, UpPrimary: 2, ActingPrimary: 2},
		},
	}
	// Every PG of the baseline pool is clean on two OSDs, none on osd.2.
	want := expect.PGs{Pool: pool, Count: 2, State: "active+clean", ActingSize: 2, NotInActing: []int{2}}
	result, err := want.Evaluate(observed)
	if err != nil {
		panic(err) // invalid expectation or malformed observation: no verdict
	}
	fmt.Println(result.Matched)
	for _, m := range result.Mismatches {
		fmt.Println(m)
	}
	// Output:
	// false
	// pg 3.1 state: want active+clean, observed active+undersized+degraded
	// pg 3.1 acting size: want 2, observed [2]
	// pg 3.1 acting present osd.2: observed [2]
}
