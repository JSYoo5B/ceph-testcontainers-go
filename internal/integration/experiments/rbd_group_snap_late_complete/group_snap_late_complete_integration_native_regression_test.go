//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_group_snap_late_complete)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package rbd_group_snap_late_complete_test

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// groupSnapScript reads the header object of a group, and in its "set" mode
// first calls the class method rbd.group_snap_set on it with the payload it
// is given in hex. The payload of that method is an encoded group snapshot,
// which is also the value the method stores under snapshot_<id>, so a stored
// value replays the update that wrote it. The script reports the result of
// the call (0, or the negative errno of a refusal), the omap keys of the
// header and the ID, name, state and stored value of every snapshot record.
const groupSnapScript = `import json,struct,sys
import rados
mode,pool,group=sys.argv[1:4]
def text(buf,at):
    n,=struct.unpack_from('<I',buf,at)
    return buf[at+4:at+4+n].decode(),at+4+n
def record(value):
    # struct_v, struct_compat and a 32-bit length, then the ID, the name and
    # the state as one byte.
    ident,at=text(value,6)
    name,at=text(value,at)
    return {'id':ident,'name':name,'state':value[at],'value':value.hex()}
def omap(ioctx,oid):
    with rados.ReadOpCtx() as op:
        values,_=ioctx.get_omap_vals(op,'','',1000)
        ioctx.operate_read_op(op,oid)
        return dict(values)
with rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'}) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        ident,_=text(omap(ioctx,'rbd_group_directory')['name_'+group],0)
        oid='rbd_group_header.'+ident
        out={'object':oid}
        if mode=='set':
            try:
                out['return'],_=ioctx.execute(oid,'rbd','group_snap_set',bytes.fromhex(sys.argv[4]))
            except rados.Error as e:
                out['return'],out['error']=-abs(e.errno),type(e).__name__+': '+str(e)
        values=omap(ioctx,oid)
        out['keys']=sorted(values)
        out['snapshots']=[record(values[k]) for k in sorted(values) if k.startswith('snapshot_')]
        print(json.dumps(out))
`

// groupSnapRecord is one snapshot record of a group header object. State is
// the stored state byte, groupSnapComplete for a complete snapshot, and
// Value the stored encoded record in hex.
type groupSnapRecord struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	State int    `json:"state"`
	Value string `json:"value"`
}

const groupSnapComplete = 1

// groupSnapHeader is what groupSnapScript reports.
type groupSnapHeader struct {
	Object    string            `json:"object"`
	Return    *int              `json:"return"`
	Error     string            `json:"error"`
	Keys      []string          `json:"keys"`
	Snapshots []groupSnapRecord `json:"snapshots"`
}

// record returns the record named name.
func (header groupSnapHeader) record(name string) (groupSnapRecord, bool) {
	for _, record := range header.Snapshots {
		if record.Name == name {
			return record, true
		}
	}
	return groupSnapRecord{}, false
}

const groupSnapPool = "tc-group-snap"

// image is the Ceph image the case runs on: Tentacle 20.2.4, which
// reproduces the defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/rbd_group_snap_late_complete_test.image=<image>"
//
// No release fixes it: Umbrella 21.2.0 accepts the late update too.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// TestRBDGroupSnapshotLateCompleteAfterRemoval reports whether a COMPLETE
// update that arrives for an already removed group snapshot brings the
// snapshot back into the group's listing.
//
// Given two groups set up the same way, each with one image and the
// snapshots healthy and victim, the stored record of each victim, an update
// of the existing healthy record that the server accepts and that changes
// nothing (the client and the method work), and the removal of victim from
// both groups, after which neither listing nor header object names it, when
// the stored COMPLETE record of the removed victim is sent again to one
// group, then the defect is REPRODUCED if that group's listing names victim
// again, or fails, with the record back in the header object without its
// order entry, and NOT_REPRODUCED if the listing succeeds with only healthy,
// whether the server accepted the update or refused it as invalid. The other
// group gets no late update and must keep listing only healthy.
//
// A failure of the given or when steps fails the test; the result does not.
func TestRBDGroupSnapshotLateCompleteAfterRemoval(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("image: %s", image)
	cluster, client := harness.NewSingleImageCluster(t, ceph.Run, image, harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: groupSnapPool, PGNum: 1, Application: "rbd"}); err != nil {
		t.Fatal(err)
	}
	header := func(group string, args ...string) groupSnapHeader {
		t.Helper()
		out := harness.ExecOutput(t, ctx, client, append([]string{"python3", "-c", groupSnapScript}, args...)...)
		var got groupSnapHeader
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("header of %s: %v: %s", group, err, out)
		}
		return got
	}
	read := func(group string) groupSnapHeader { t.Helper(); return header(group, "read", groupSnapPool, group) }
	// set sends payload to the group's header object as a group_snap_set
	// and returns its result with the header after it.
	set := func(group, payload string) (int, groupSnapHeader) {
		t.Helper()
		got := header(group, "set", groupSnapPool, group, payload)
		if got.Return == nil {
			t.Fatalf("the update of %s reported no result: %+v", group, got)
		}
		t.Logf("group_snap_set on %s (%s): return %d %s", got.Object, group, *got.Return, got.Error)
		return *got.Return, got
	}
	// listed requires the listing of group to succeed with exactly names.
	listed := func(group string, names ...string) {
		t.Helper()
		if exit, output, got := groupSnapList(t, ctx, client, group); exit != 0 || !slices.Equal(got, names) {
			t.Fatalf("condition not established: the listing of %s is %v (exit %d), want %v: %s", group, got, exit, names, output)
		}
	}

	// Given: two groups with the same contents, made by the ordinary
	// commands, so each snapshot went through INCOMPLETE and COMPLETE.
	const late, control = "late", "control"
	victims := map[string]groupSnapRecord{}
	for _, group := range []string{late, control} {
		spec := groupSnapPool + "/" + group
		harness.ExecOutput(t, ctx, client, "rbd", "create", "--size", "4M", "--image-feature", "layering", spec+"-image")
		harness.ExecOutput(t, ctx, client, "rbd", "group", "create", spec)
		harness.ExecOutput(t, ctx, client, "rbd", "group", "image", "add", spec, spec+"-image")
		for _, name := range []string{groupSnapHealthy, groupSnapVictim} {
			harness.ExecOutput(t, ctx, client, "rbd", "group", "snap", "create", spec+"@"+name)
		}
		listed(group, groupSnapHealthy, groupSnapVictim)
		got := read(group)
		for _, name := range []string{groupSnapHealthy, groupSnapVictim} {
			record, ok := got.record(name)
			if !ok || record.ID == "" || record.State != groupSnapComplete || !slices.Contains(got.Keys, "snapshot_"+record.ID) || !slices.Contains(got.Keys, "snap_order_"+record.ID) {
				t.Fatalf("condition not established: %s of %s is not one complete record with an order entry: %+v", name, group, got)
			}
		}
		victims[group], _ = got.record(groupSnapVictim)
	}
	// The mechanism control: the stored record of the existing sibling,
	// sent again, is accepted and changes nothing.
	before := read(late)
	healthy, _ := before.record(groupSnapHealthy)
	if result, after := set(late, healthy.Value); result != 0 || !slices.Equal(after.Keys, before.Keys) || !slices.Equal(after.Snapshots, before.Snapshots) {
		t.Fatalf("condition not established: the update of the existing record of %s returned %d and left %+v, want 0 and %+v", groupSnapHealthy, result, after, before)
	}
	listed(late, groupSnapHealthy, groupSnapVictim)
	// The victim is removed from both groups by the ordinary command.
	for _, group := range []string{late, control} {
		harness.ExecOutput(t, ctx, client, "rbd", "group", "snap", "rm", groupSnapPool+"/"+group+"@"+groupSnapVictim)
		listed(group, groupSnapHealthy)
		got, victim := read(group), victims[group]
		if _, ok := got.record(groupSnapVictim); ok || slices.Contains(got.Keys, "snapshot_"+victim.ID) || slices.Contains(got.Keys, "snap_order_"+victim.ID) {
			t.Fatalf("condition not established: the header object of %s still has the removed snapshot %s: %+v", group, victim.ID, got)
		}
	}

	// When: the COMPLETE record of the removed victim arrives again.
	victim := victims[late]
	result, after := set(late, victim.Value)

	// Then: the control group still lists only the sibling, and the group
	// that got the late update is judged by its listing.
	listed(control, groupSnapHealthy)
	exit, output, names := groupSnapList(t, ctx, client, late)
	t.Logf("RBD_GROUP_SNAP_LATE_COMPLETE_RECEIPT removed snapshot %s (%s): late update returned %d; header keys %v; listing exit %d: %s", groupSnapVictim, victim.ID, result, after.Keys, exit, strings.TrimSpace(output))
	symptoms, err := judgeLateComplete(victim.ID, groupSnapOutcome{Return: result, Keys: after.Keys, ListExit: exit, ListOutput: output, Listed: names})
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}
	verdict := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		verdict = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=rbd_group_snap_late_complete result=%s image=%s symptoms=%q", verdict, image, symptoms)
}

// groupSnapList runs the ordinary listing command for group and returns its
// exit code, its output and, when it succeeded, the snapshot names in its
// order. The command failing is an outcome; only a failure to run it, or
// output that is not the listing, fails the test.
func groupSnapList(t *testing.T, ctx context.Context, client testcontainers.Container, group string) (int, string, []string) {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	exit, reader, err := client.Exec(execCtx, []string{"rbd", "group", "snap", "ls", groupSnapPool + "/" + group, "--format", "json"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if exit != 0 {
		return exit, string(output), nil
	}
	var snapshots []struct {
		Snapshot string `json:"snapshot"`
	}
	if err := json.Unmarshal(output, &snapshots); err != nil {
		t.Fatalf("listing of %s: %v: %s", group, err, output)
	}
	var names []string
	for _, snapshot := range snapshots {
		if snapshot.Snapshot == "" {
			t.Fatalf("listing of %s has an entry without a name: %s", group, output)
		}
		names = append(names, snapshot.Snapshot)
	}
	return exit, string(output), names
}
