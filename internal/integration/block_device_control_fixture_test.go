//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"os/exec"
	"testing"
)

const (
	ctlLinear = "0 131072 linear 7:3 0"
	ctlError  = "0 131072 error"
)

func ctlView(state string, live string, inactive *string) dmControlReply {
	l := live
	return dmControlReply{Action: "observe", Exists: true, UUID: "TC-CTL-x", Major: 253, Minor: 4, State: state, Live: &l, Inactive: inactive, InFlight: []int64{0, 0}}
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
		view dmControlReply
		is   string
	}{
		"gone":                       {owned, ctlError, dmControlReply{Action: "observe"}, dmGone},
		"another UUID":               {owned, ctlError, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }(), dmIdentityChanged},
		"another minor":              {owned, ctlError, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.Minor = 5; return v }(), dmIdentityChanged},
		"unsettled step first":       {unsettled, ctlError, ctlView("SUSPENDED", ctlLinear, strp(ctlError)), dmStepUnsettled},
		"identity before unsettled":  {unsettled, ctlError, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }(), dmIdentityChanged},
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
		"no live table":              {owned, ctlError, func() dmControlReply { v := ctlView("ACTIVE", "", nil); v.Live = nil; return v }(), dmForeign},
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
	lost.begin("load", ctlError, "load-1")
	if got := classifyDM(lost, ctlError, view); got != dmStepUnsettled {
		t.Errorf("a load whose reply was lost: %s", got)
	}
	if verdict, _ := judgeDMRelease(lost, ctlView("ACTIVE", ctlLinear, nil)); verdict != dmReleaseHeld {
		t.Error("a node with a load of unknown outcome was releasable")
	}

	// The load finishes after its wait: the retained table is the load.
	late := owned
	late.begin("load", ctlError, "load-1")
	late.settle(dmControlReply{Done: false})
	if late.Unsettled != "load" || late.UnsettledTable != ctlError || late.UnsettledID != "load-1" {
		t.Fatalf("a load still running was not kept: %+v", late)
	}
	late.settle(dmControlReply{Done: true})
	if late.Unsettled != "" || late.Loaded != ctlError {
		t.Fatalf("a late load was not recorded as the control's: %+v", late)
	}
	if got := classifyDM(late, ctlError, view); got != dmLoaded {
		t.Errorf("the control's late load: %s", got)
	}

	refused := owned
	refused.begin("load", ctlError, "load-1")
	refused.settle(dmControlReply{Done: true, RC: 1})
	if refused.Unsettled != "" || refused.Loaded != "" {
		t.Errorf("a refused load: %+v", refused)
	}

	swapped := late
	swapped.begin("resume", "", "resume-1")
	swapped.settle(dmControlReply{Done: true})
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
		view dmControlReply
	}{
		"opener":        {owned, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.Open = 1; return v }()},
		"write":         {owned, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = []int64{0, 1}; return v }()},
		"read":          {owned, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = []int64{1, 0}; return v }()},
		"no counters":   {owned, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.InFlight = nil; return v }()},
		"suspended":     {owned, ctlView("SUSPENDED", ctlLinear, nil)},
		"loaded":        {loaded, ctlView("ACTIVE", ctlLinear, strp(ctlError))},
		"unsettled":     {unsettled, ctlView("ACTIVE", ctlLinear, nil)},
		"fault live":    {owned, ctlView("ACTIVE", ctlError, nil)},
		"reused name":   {owned, func() dmControlReply { v := ctlView("ACTIVE", ctlLinear, nil); v.UUID = "TC-OTHER"; return v }()},
		"already gone":  {owned, dmControlReply{Action: "observe"}},
		"foreign table": {owned, ctlView("ACTIVE", "0 65536 linear 7:3 0", nil)},
	} {
		if verdict, why := judgeDMRelease(test.rec, test.view); verdict != dmReleaseHeld {
			t.Errorf("%s: released (%s)", name, why)
		}
	}
}

func TestJudgeLoopReleaseNeedsTheAttachedUnusedFile(t *testing.T) {
	own := loopIdentity{Loop: "/dev/loop3", File: "/tc-blk/a", Inode: 41, Dev: "7:3"}
	if ok, why := judgeLoopRelease(own, dmControlReply{File: "/tc-blk/a", Inode: 41}, nil); !ok {
		t.Fatalf("the attached loop was held: %s", why)
	}
	for name, test := range map[string]struct {
		seen dmControlReply
		kept []string
	}{
		"another file":        {dmControlReply{File: "/tc-blk/n", Inode: 42}, nil},
		"same path, new file": {dmControlReply{File: "/tc-blk/a", Inode: 43}, nil},
		"detached":            {dmControlReply{Error: "No such file or directory"}, nil},
		"kept mapping above":  {dmControlReply{File: "/tc-blk/a", Inode: 41}, []string{"tcctl-pl"}},
		"held by a mapping":   {dmControlReply{File: "/tc-blk/a", Inode: 41, Holders: []string{"dm-0"}}, nil},
	} {
		if ok, _ := judgeLoopRelease(own, test.seen, test.kept); ok {
			t.Errorf("%s: released", name)
		}
	}
}

func TestBlockControlScriptCompiles(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python 3 is required to compile the keeper script")
	}
	if out, err := exec.Command(python, "-I", "-c", "import sys; compile(sys.argv[1], 'control', 'exec')", blockControlScript).CombinedOutput(); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}
