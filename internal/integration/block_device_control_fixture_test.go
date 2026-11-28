//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/blockio"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	ctlLinear = "0 131072 linear 7:3 0"
	ctlError  = "0 131072 error"
)

func ctlView(state string, live string, inactive *string) dmView {
	l := live
	return dmView{Action: "observe", Exists: true, UUID: "TC-CTL-x", Major: 253, Minor: 4, State: state, Live: &l, Inactive: inactive, InFlight: []int64{0, 0}}
}

func strp(s string) *string { return &s }

func TestClassifyDMExplainsOnlyWhatTheRecordExplains(t *testing.T) {
	owned := dmRecord{Name: "x", UUID: "TC-CTL-x", Major: 253, Minor: 4, Live: ctlLinear}
	loaded := owned
	loaded.Loaded = ctlError
	unsettled := loaded
	unsettled.Unsettled = "suspend"
	for name, test := range map[string]struct {
		rec  dmRecord
		want string
		view dmView
		is   string
	}{
		"gone":                       {owned, ctlError, dmView{Action: "observe"}, dmGone},
		"another UUID":               {owned, ctlError, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }(), dmIdentityChanged},
		"another minor":              {owned, ctlError, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.Minor = 5; return v }(), dmIdentityChanged},
		"unsettled step first":       {unsettled, ctlError, ctlView("SUSPENDED", ctlLinear, strp(ctlError)), dmStepUnsettled},
		"identity before unsettled":  {unsettled, ctlError, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }(), dmIdentityChanged},
		"unchanged":                  {owned, ctlError, ctlView("ACTIVE", ctlLinear, nil), dmUnchanged},
		"trailing space":             {owned, ctlError, ctlView("ACTIVE", ctlError+" ", nil), dmApplied},
		"loaded":                     {loaded, ctlError, ctlView("ACTIVE", ctlLinear, strp(ctlError+" ")), dmLoaded},
		"suspended and loaded":       {loaded, ctlError, ctlView("SUSPENDED", ctlLinear, strp(ctlError)), dmSuspendedLoaded},
		"suspended":                  {owned, ctlError, ctlView("SUSPENDED", ctlLinear, nil), dmSuspended},
		"swapped on hold":            {owned, ctlError, ctlView("SUSPENDED", ctlError, nil), dmSwappedOnHold},
		"inactive not loaded by us":  {owned, ctlError, ctlView("ACTIVE", ctlLinear, strp(ctlError)), dmForeign},
		"another inactive table":     {loaded, ctlError, ctlView("ACTIVE", ctlLinear, strp("0 65536 linear 7:3 0")), dmForeign},
		"wanted table replaced ours": {loaded, "0 65536 linear 7:3 0", ctlView("ACTIVE", ctlLinear, strp("0 65536 linear 7:3 0")), dmForeign},
		"replaced ours, suspended":   {loaded, "0 65536 linear 7:3 0", ctlView("SUSPENDED", ctlLinear, strp("0 65536 linear 7:3 0")), dmForeign},
		"another live table":         {owned, ctlError, ctlView("ACTIVE", "0 65536 linear 7:3 0", nil), dmForeign},
		"unknown state":              {owned, ctlError, ctlView("", ctlLinear, nil), dmForeign},
		"no live table":              {owned, ctlError, func() dmView { v := ctlView("ACTIVE", "", nil); v.Live = nil; return v }(), dmForeign},
	} {
		if got := classifyDM(test.rec, test.want, test.view); got != test.is {
			t.Errorf("%s: %s, want %s", name, got, test.is)
		}
	}
}

func TestDMRecordKeepsAStepUntilItsStatusSaysSo(t *testing.T) {
	owned := dmRecord{Name: "x", UUID: "TC-CTL-x", Major: 253, Minor: 4, Live: ctlLinear}
	view := ctlView("ACTIVE", ctlLinear, strp(ctlError))

	// The reply of a dispatched load is lost: the record still holds it.
	lost := owned
	lost.Begin("load", ctlError, "load-1")
	if got := classifyDM(lost, ctlError, view); got != dmStepUnsettled {
		t.Errorf("a load whose reply was lost: %s", got)
	}
	if verdict, _ := judgeDMRelease(lost, ctlView("ACTIVE", ctlLinear, nil)); verdict != dmReleaseHeld {
		t.Error("a node with a load of unknown outcome was releasable")
	}

	// The load finishes after its wait: the retained table is the load.
	late := owned
	late.Begin("load", ctlError, "load-1")
	late.Settle(dmView{Done: false})
	if late.Unsettled != "load" || late.UnsettledTable != ctlError || late.UnsettledID != "load-1" {
		t.Fatalf("a load still running was not kept: %+v", late)
	}
	late.Settle(dmView{Done: true})
	if late.Unsettled != "" || late.Loaded != ctlError {
		t.Fatalf("a late load was not recorded as the control's: %+v", late)
	}
	if got := classifyDM(late, ctlError, view); got != dmLoaded {
		t.Errorf("the control's late load: %s", got)
	}

	refused := owned
	refused.Begin("load", ctlError, "load-1")
	refused.Settle(dmView{Done: true, RC: 1})
	if refused.Unsettled != "" || refused.Loaded != "" {
		t.Errorf("a refused load: %+v", refused)
	}

	swapped := late
	swapped.Begin("resume", "", "resume-1")
	swapped.Settle(dmView{Done: true})
	if swapped.Live != ctlError || swapped.Loaded != "" {
		t.Errorf("a resume of the loaded table: %+v", swapped)
	}
}

func TestDMForwardActsOnlyOnExplainedStates(t *testing.T) {
	steps := map[string]string{dmUnchanged: "load", dmSuspended: "load", dmLoaded: "suspend", dmSuspendedLoaded: "resume", dmSwappedOnHold: "resume"}
	for _, state := range []string{dmGone, dmIdentityChanged, dmForeign, dmStepUnsettled, dmApplied, dmUnchanged, dmSuspended, dmLoaded, dmSuspendedLoaded, dmSwappedOnHold} {
		if got := dmForward(state); got != steps[state] {
			t.Errorf("%s: step %q, want %q", state, got, steps[state])
		}
		if dmRefuses(state) && dmForward(state) != "" {
			t.Errorf("%s refuses action but has a step", state)
		}
	}
}

func TestJudgeDMReleaseNeedsTheRecordedIdleNode(t *testing.T) {
	owned := dmRecord{Name: "x", UUID: "TC-CTL-x", Major: 253, Minor: 4, Live: ctlLinear}
	if verdict, why := judgeDMRelease(owned, ctlView("ACTIVE", ctlLinear, nil)); verdict != dmReleasable {
		t.Fatalf("an idle recorded node was held: %s", why)
	}
	unsettled := owned
	unsettled.Unsettled = "suspend"
	loaded := owned
	loaded.Loaded = ctlError
	for name, test := range map[string]struct {
		rec  dmRecord
		view dmView
	}{
		"opener":        {owned, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.Open = 1; return v }()},
		"write":         {owned, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = []int64{0, 1}; return v }()},
		"read":          {owned, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = []int64{1, 0}; return v }()},
		"no counters":   {owned, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = nil; return v }()},
		"suspended":     {owned, ctlView("SUSPENDED", ctlLinear, nil)},
		"loaded":        {loaded, ctlView("ACTIVE", ctlLinear, strp(ctlError))},
		"unsettled":     {unsettled, ctlView("ACTIVE", ctlLinear, nil)},
		"fault live":    {owned, ctlView("ACTIVE", ctlError, nil)},
		"reused name":   {owned, func() dmView { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }()},
		"already gone":  {owned, dmView{Action: "observe"}},
		"foreign table": {owned, ctlView("ACTIVE", "0 65536 linear 7:3 0", nil)},
	} {
		if verdict, why := judgeDMRelease(test.rec, test.view); verdict != dmReleaseHeld {
			t.Errorf("%s: released (%s)", name, why)
		}
	}
}

func TestJudgeLoopReleaseNeedsTheAttachedUnusedFile(t *testing.T) {
	own := loopIdentity{Loop: "/dev/loop3", File: "/tc-blk/a", Inode: 41, Dev: "7:3"}
	if ok, why := judgeLoopRelease(own, dmView{File: "/tc-blk/a", Inode: 41}, nil); !ok {
		t.Fatalf("the attached loop was held: %s", why)
	}
	for name, test := range map[string]struct {
		seen dmView
		kept []string
	}{
		"another file":        {dmView{File: "/tc-blk/n", Inode: 42}, nil},
		"same path, new file": {dmView{File: "/tc-blk/a", Inode: 43}, nil},
		"detached":            {dmView{Error: "No such file or directory"}, nil},
		"kept mapping above":  {dmView{File: "/tc-blk/a", Inode: 41}, []string{"tcctl-pl"}},
		"held by a mapping":   {dmView{File: "/tc-blk/a", Inode: 41, Holders: []string{"dm-0"}}, nil},
	} {
		if ok, _ := judgeLoopRelease(own, test.seen, test.kept); ok {
			t.Errorf("%s: released", name)
		}
	}
}

func TestBlockControlScriptsCompile(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required to compile the keeper scripts")
	}
	for name, script := range map[string]string{"control": blockio.Script, "test actions": blockTestScript} {
		if out, err := exec.Command(python, "-I", "-c", "import sys; compile(sys.argv[1], 'control', 'exec')", script).CombinedOutput(); err != nil {
			t.Fatalf("%s: %v: %s", name, err, out)
		}
	}
}

// dmCleanupReceipts replaces only keeper exec replies to exercise the same
// cleanup caller used by the two native block fixtures.
type dmCleanupReceipts struct {
	testcontainers.Container
	replies []string
	calls   [][]string
}

func (r *dmCleanupReceipts) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	r.calls = append(r.calls, args[4:])
	if len(r.replies) == 0 || r.replies[0] == "" {
		if len(r.replies) != 0 {
			r.replies = r.replies[1:]
		}
		return -1, nil, errors.New("keeper reply unavailable")
	}
	reply := r.replies[0]
	r.replies = r.replies[1:]
	// Step IDs are transport correlation tokens chosen by the caller.
	if args[4] == "step" {
		reply = strings.ReplaceAll(reply, "step-token", args[8])
	}
	return 0, strings.NewReader(reply), nil
}

const (
	dmAbsentReceipt  = `{"action":"observe","name":"x","exists":false}`
	dmLookupFailed   = `{"action":"observe","name":"x","exists":false,"error":"permission denied"}`
	dmOwnedReceipt   = `{"action":"observe","name":"x","exists":true,"uuid":"TC-CTL-x","major":253,"minor":4,"state":"ACTIVE","live":"0 131072 linear 7:3 0","inactive":null,"open":0,"event":1,"in_flight":[0,0]}`
	dmRemovalReceipt = `{"action":"step","name":"x","id":"step-token","op":"remove","done":true,"rc":0,"error":""}`
	dmLookupLoop     = `{"action":"loopid","name":"-","file":"/tc-blk/a","inode":41,"holders":[]}`
	dmLoopUnlink     = `{"action":"unloop","name":"-","holders":[],"detached":true,"file_removed":true,"error":""}`
)

func TestBlockControlReleaseUsesObservedIdentityThroughCleanup(t *testing.T) {
	for _, test := range []struct {
		name, pending, want string
		replies             []string
		calls               int
	}{
		{"owned", "", dmReleasable, []string{dmOwnedReceipt, dmRemovalReceipt, dmAbsentReceipt}, 3},
		{"reused name", "", dmReleaseHeld, []string{strings.ReplaceAll(dmOwnedReceipt, "TC-CTL-x", "TC-OTHER-x")}, 1},
		{"unreadable existence", "", dmReleaseHeld, []string{dmLookupFailed}, 1},
		{"unreadable after remove", "", dmReleaseHeld, []string{dmOwnedReceipt, dmRemovalReceipt, dmLookupFailed}, 3},
		{"unsettled table change", "load", dmReleaseHeld, []string{`{"action":"settled","name":"x","id":"load-a","done":false}`}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &dmCleanupReceipts{replies: test.replies}
			c := dmControl{keeper: dmKeeper{ctr: ctr}, rec: dmRecord{Name: "x", UUID: "TC-CTL-x", Major: 253, Minor: 4,
				Live: ctlLinear, Unsettled: test.pending, UnsettledID: "load-a"}}
			got, why := c.releaseOwned(t.Context())
			if got != test.want || len(ctr.calls) != test.calls {
				t.Fatalf("release %s (%s), keeper calls %v", got, why, ctr.calls)
			}
			if test.calls == 3 && !reflect.DeepEqual(ctr.calls[1][:3], []string{"step", "x", "remove"}) {
				t.Fatalf("expected one removal, got %v", ctr.calls[1])
			}
			if test.calls == 3 && ctr.calls[1][5] != c.rec.UUID {
				t.Fatalf("removal was not selected by the owned UUID: %v", ctr.calls[1])
			}
		})
	}
}

func TestBlockControlReleasePreservesBackingOnReusedLoop(t *testing.T) {
	ctr := &dmCleanupReceipts{replies: []string{strings.ReplaceAll(dmLookupLoop, "41", "42")}}
	keeper := dmKeeper{ctr: ctr}
	backing := &dmBacking{name: "a", loopIdentity: loopIdentity{Loop: "/dev/loop3", File: "/tc-blk/a", Inode: 41}}
	if _, err := backing.release(t.Context(), keeper, nil); err == nil || len(ctr.calls) != 1 || ctr.calls[0][0] != "loopid" {
		t.Fatalf("foreign loop reached detach: err=%v calls=%v", err, ctr.calls)
	}
}

func TestBlockControlCleanupSettlesLostAcquisitionReplies(t *testing.T) {
	t.Run("mapping receipt", func(t *testing.T) {
		ctr := &dmCleanupReceipts{replies: []string{
			"",
			`{"action":"creation","name":"x","done":false}`,
			`{"action":"creation","name":"x","done":true,"created":true}`,
			dmOwnedReceipt, dmRemovalReceipt, dmAbsentReceipt,
		}}
		f := dmFixture{keeper: dmKeeper{ctr: ctr}}
		_, err := f.createNode(t.Context(), "x", "TC-CTL-x", "7:3", dmControlSectors)
		if err == nil || len(f.controls) != 1 {
			t.Fatalf("dispatched mapping lost its cleanup record: %v %+v", err, f.controls)
		}
		c := f.controls[0]
		if got, _ := c.releaseOwned(t.Context()); got != dmReleaseHeld || len(ctr.calls) != 2 {
			t.Fatalf("pending mapping was released: %s %v", got, ctr.calls)
		}
		if got, why := c.releaseOwned(t.Context()); got != dmReleasable || c.rec.Major != 253 || len(ctr.calls) != 6 {
			t.Fatalf("created mapping not safely released: %s %s %v", got, why, ctr.calls)
		}
	})
	t.Run("loop receipt", func(t *testing.T) {
		ctr := &dmCleanupReceipts{replies: []string{
			"",
			`{"action":"creation","name":"a","done":false}`,
			`{"action":"creation","name":"a","done":true,"file":"/tc-blk/a","inode":41,"loop":"/dev/loop3","major":7,"minor":3}`,
			dmLookupLoop, dmLoopUnlink,
		}}
		f := dmFixture{keeper: dmKeeper{ctr: ctr}}
		_, err := f.createLoop(t.Context(), "a", dmControlBytes)
		if err == nil || len(f.loops) != 1 {
			t.Fatalf("dispatched loop lost its cleanup record: %v %+v", err, f.loops)
		}
		l := f.loops[0]
		if _, err := l.release(t.Context(), f.keeper, nil); err == nil || len(ctr.calls) != 2 {
			t.Fatalf("pending loop was released: %v %v", err, ctr.calls)
		}
		if r, err := l.release(t.Context(), f.keeper, nil); err != nil || !r.FileRemoved || len(ctr.calls) != 5 {
			t.Fatalf("created loop not safely released: %+v %v %v", r, err, ctr.calls)
		}
	})
}
