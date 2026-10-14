//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_mutation_contracts))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// contractObject is an object as the reading client saw it.
type contractObject struct {
	Exists  bool    `json:"exists"`
	Size    int     `json:"size"`
	Data    string  `json:"data"`
	XAttr   *string `json:"xattr"`
	Version uint64  `json:"version"`
}

// The mutation contracts that a client implementation must reproduce, on the
// topology of a Messenger client suite: Messenger v2 only with CRC, AES256K
// keys where the image can make them, and a MemStore OSD with the legacy CRUSH
// map and a pool without hashpspool. A second client reads every result back.
func TestRADOSMutationContracts(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	base := append(slices.Clone(roles), ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithOSDObjectStore(ceph.OSDMemStore), ceph.WithOSDBlockSize(256<<20), legacyPoolFlags(t),
		ceph.WithMessengerMode(ceph.MessengerV2Only), ceph.WithMessengerPolicy(ceph.MessengerPolicy{Monitors: modeCRC, Managers: modeCRC, OSDs: modeCRC}))
	auth := profileAuth(t, ctx, image, base, ceph.AuthConfig{KeyType: ceph.CipherAES256K})
	cluster, client := newServiceClusterWithOptions(t, image, append(base, ceph.WithAuth(auth))...)
	installCRUSHMap(t, ctx, cluster, legacyCRUSHMap)
	const pool, namespace = "tc-contracts", "suite"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	checkMemStoreLegacyPool(t, ctx, cluster, pool)
	phase := func(out any, args ...string) {
		t.Helper()
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv: append([]string{"python3", "-c", radosMutationContractScript}, args...), Timeout: 3 * time.Minute,
			MaxStdout: 1 << 20, MaxStderr: 64 << 10, Label: "mutation contract " + args[0],
		})
		if err != nil || result.ExitCode != 0 || json.Unmarshal(result.Stdout, out) != nil {
			t.Fatalf("%s: %v, exit %d: %s %s", args[0], err, result.ExitCode, result.Stdout, result.Stderr)
		}
	}

	var contracts struct {
		Zeroed               contractObject `json:"zeroed"`
		ZeroedExpected       string         `json:"zeroed_expected"`
		CreateExclusive      int            `json:"create_exclusive"`
		CreateExclusiveAgain int            `json:"create_exclusive_again"`
		CreateIdempotent     int            `json:"create_idempotent"`
		CreateExclusiveWrite int            `json:"create_exclusive_write"`
		Created              contractObject `json:"created"`
		CmpextMismatch       int            `json:"cmpext_mismatch"`
		AfterCmpext          contractObject `json:"after_cmpext_mismatch"`
		OmapMismatch         int            `json:"omap_mismatch"`
		AfterOmap            contractObject `json:"after_omap_mismatch"`
		CmpxattrMismatch     int            `json:"cmpxattr_mismatch"`
		AfterCmpxattr        contractObject `json:"after_cmpxattr_mismatch"`
		CmpxattrMatch        int            `json:"cmpxattr_match"`
		AfterCmpxattrMatch   contractObject `json:"after_cmpxattr_match"`
		MaxErrno             int            `json:"max_errno"`
	}
	phase(&contracts, "contracts", pool, namespace)
	// zero keeps the size and clears only the range.
	if contracts.Zeroed.Size != 8192 || contracts.Zeroed.Data != contracts.ZeroedExpected {
		t.Fatalf("zero: %+v", contracts.Zeroed)
	}
	// An exclusive create of an existing object is refused with EEXIST, also
	// as part of a compound write, which then changes nothing; an idempotent
	// create succeeds.
	if contracts.CreateExclusive != 0 || contracts.CreateExclusiveAgain != 17 || contracts.CreateIdempotent != 0 ||
		contracts.CreateExclusiveWrite != 17 || !contracts.Created.Exists || contracts.Created.Size != 0 {
		t.Fatalf("create: %+v", contracts)
	}
	// A failed guard refuses the whole compound operation. cmpext reports
	// MAX_ERRNO plus the offset of the first differing byte (3); omap_cmp and
	// cmpxattr report ECANCELED.
	for name, got := range map[string]contractObject{"cmpext": contracts.AfterCmpext, "omap_cmp": contracts.AfterOmap, "cmpxattr": contracts.AfterCmpxattr} {
		if !got.Exists || got.Size != 8 || got.Data != "6162636465666768" || got.XAttr == nil || *got.XAttr != "v1" {
			t.Fatalf("a refused %s guard changed the object: %+v", name, got)
		}
	}
	if contracts.CmpextMismatch != contracts.MaxErrno+3 || contracts.OmapMismatch != 125 || contracts.CmpxattrMismatch != -125 {
		t.Fatalf("guard refusals: cmpext %d (MAX_ERRNO %d), omap_cmp %d, cmpxattr %d", contracts.CmpextMismatch, contracts.MaxErrno, contracts.OmapMismatch, contracts.CmpxattrMismatch)
	}
	if contracts.CmpxattrMatch != 0 || contracts.AfterCmpxattrMatch.Data != "7265706c61636564" {
		t.Fatalf("a matching cmpxattr guard did not apply the write: %d, %+v", contracts.CmpxattrMatch, contracts.AfterCmpxattrMatch)
	}

	// The client gives up on a truncate, a remove and a guarded compound write
	// while the OSD delays them; the OSD then applies each exactly once.
	var before, after map[string]contractObject
	phase(&before, "prepare", pool, namespace)
	restore := holdDispatchDelay(t, ctx, cluster, "osd.0", 2*time.Second)
	var late struct {
		Client    string    `json:"client"`
		Completed []float64 `json:"completed"`
		Truncate  int       `json:"truncate"`
		Remove    int       `json:"remove"`
		Compound  int       `json:"compound"`
	}
	phase(&late, "late", pool, namespace, "2", "90")
	restore()
	if late.Truncate != 110 || late.Remove != 110 || late.Compound != 110 || len(late.Completed) != 3 {
		t.Fatalf("the client's timeout did not end each operation with ETIMEDOUT: %+v", late)
	}
	// The OSD's records show that it started and committed each operation
	// after the client had given up on it.
	poolState, err := cluster.PoolStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	pg := fmt.Sprintf("%d.0", poolState.ID)
	var records []osdTrackedOp
	for deadline := time.Now().Add(30 * time.Second); ; {
		records = records[:0]
		for _, op := range osdClientWrites(t, ctx, cluster, "osd.0", "dump_historic_ops_by_duration", pg) {
			if trackedClient(op.Description) == late.Client {
				records = append(records, op)
			}
		}
		if len(records) == 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if len(records) != 3 {
		t.Fatalf("osd.0 recorded %d finished operations of %s, want 3", len(records), late.Client)
	}
	slices.SortFunc(records, func(x, y osdTrackedOp) int {
		return strings.Compare(trackedRequest(x.Description), trackedRequest(y.Description))
	})
	for i, record := range records {
		started, ok := record.event("started")
		_, committed := record.event("commit_sent")
		gaveUp := time.Unix(0, int64(late.Completed[i]*1e9))
		if !ok || !committed || !started.After(gaveUp) {
			t.Fatalf("osd.0 did not start and commit operation %d of %s after the client gave up at %v: %+v", i+1, late.Client, gaveUp, record)
		}
	}
	phase(&after, "state", pool, namespace)
	// The pool has one PG, whose version every applied write advances by one,
	// and the compound object was the last one prepared. The three late
	// operations therefore take the next three versions, in order, if each
	// ran exactly once.
	last := before["late-compound"].Version
	truncated, removed, compound := after["late-truncate"], after["late-remove"], after["late-compound"]
	if truncated.Size != 10 || truncated.Data != "30313233343536373839" || truncated.Version != last+1 {
		t.Fatalf("the late truncate was not applied once at version %d: %+v", last+1, truncated)
	}
	if removed.Exists {
		t.Fatalf("the late remove was not applied: %+v", removed)
	}
	if compound.Data != "636f6d706f756e64" || compound.XAttr == nil || *compound.XAttr != "v2" || compound.Version != last+3 {
		t.Fatalf("the late compound write was not applied once and whole at version %d: %+v", last+3, compound)
	}
	t.Logf("contracts: zero, create, guarded compound refusals (cmpext %d, omap_cmp %d, cmpxattr %d); osd.0 started the late truncate, remove and compound of %s after its ETIMEDOUT and applied each once",
		contracts.CmpextMismatch, contracts.OmapMismatch, contracts.CmpxattrMismatch, late.Client)
}

// trackedRequest returns the request ID of an operation description such as
// "osd_op(client.4199.0:12 2.0 ...)", "client.4199.0:12", padded so that
// request IDs of one client sort in order.
func trackedRequest(description string) string {
	request, _, _ := strings.Cut(strings.TrimPrefix(description, "osd_op("), " ")
	client, tid, _ := strings.Cut(request, ":")
	return fmt.Sprintf("%s:%020s", client, tid)
}
