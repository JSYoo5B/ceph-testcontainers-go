package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const osdPolicyUUID = "f7e51c0d-b1c5-4b02-9cf3-07e980da36f1"

func TestOSDStatesAndMembershipVerifyOwnedUUID(t *testing.T) {
	ctr := &osdPolicyFixture{osds: []OSDState{{ID: 3, UUID: osdPolicyUUID, Up: true, In: true, Weight: 0.75}}}
	c := osdPolicyCluster(ctr)
	states, err := c.osdStates(t.Context())
	if err != nil || len(states) != 1 || states[0] != ctr.osds[0] {
		t.Fatalf("authoritative owned state: %v %v", states, err)
	}
	if err := osdHandle(t, t.Context(), c, 3).SetIn(t.Context(), false); err != nil || ctr.osds[0].In || ctr.osds[0].Up == false || ctr.osds[0].UUID != osdPolicyUUID {
		t.Fatalf("out changed unrelated native identity/daemon state: %v", err)
	}
	if err := osdHandle(t, t.Context(), c, 3).SetIn(t.Context(), true); err != nil || !ctr.osds[0].In || ctr.osds[0].Weight != 0.75 {
		t.Fatalf("in did not recover original map reweight: %v", err)
	}
	before := len(ctr.calls)
	if err := osdHandle(t, t.Context(), c, 99).SetIn(t.Context(), false); err == nil || len(ctr.calls) != before {
		t.Fatal("a handle that the cluster does not own reached the native membership")
	}
	ctr.osds[0].UUID = "c09ab9b3-3f50-4169-a6a4-4a58ab1c0d6c"
	before = len(ctr.calls)
	if _, err := c.osdStates(t.Context()); err == nil {
		t.Fatal("outside replacement accepted for native state")
	}
	if err := osdHandle(t, t.Context(), c, 3).SetIn(t.Context(), false); err == nil || len(ctr.calls) != before+2 || !ctr.osds[0].In {
		t.Fatal("outside replacement was mutated")
	}
}

func TestOSDMembershipRejectsAbsentAndPostMutationReplacement(t *testing.T) {
	for _, phase := range []string{"before", "after"} {
		ctr := &osdPolicyFixture{osds: []OSDState{{ID: 3, UUID: osdPolicyUUID, Up: true, In: true, Weight: 1}}}
		if phase == "before" {
			ctr.osds = nil
		} else {
			ctr.replaceAfter = true
		}
		c := osdPolicyCluster(ctr)
		if err := osdHandle(t, t.Context(), c, 3).SetIn(t.Context(), false); err == nil {
			t.Fatalf("%s missing/replaced registration accepted", phase)
		}
	}
	ctr := &osdPolicyFixture{osds: []OSDState{{ID: 3, UUID: osdPolicyUUID, Up: true, In: true, Weight: 1}}}
	c := osdPolicyCluster(ctr)
	c.fleet.osdHandles.get(3).nativeUUID = ""
	if err := osdHandle(t, t.Context(), c, 3).SetIn(t.Context(), false); err == nil || len(ctr.calls) != 1 {
		t.Fatal("missing captured UUID adopted from current native state")
	}
	c.fleet.osdHandles.get(3).nativeUUID = osdPolicyUUID
	c.fleet.osdHandles.get(3).purged = true
	if _, err := c.osdStates(t.Context()); err == nil {
		t.Fatal("purged registration treated as active")
	}
}

func TestOSDFlagMalformedDump(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `{"flags":[],"osds":[]}`, `{"flags":"noout,noout","osds":[]}`, `{"flags":"noout,","osds":[]}`, `{"flags":"","osds":[{"osd":3,"up":1,"in":2,"weight":1}]}`, `{"flags":"","osds":[{"osd":3,"up":1,"in":1,"weight":1},{"osd":3,"up":1,"in":1,"weight":1}]}`} {
		ctr := &osdPolicyFixture{raw: raw}
		if _, err := osdPolicyCluster(ctr).osdFlags(t.Context()); err == nil {
			t.Fatal("malformed native dump accepted", raw)
		}
	}
}

func TestWaitForPGCleanAllowsOutOSDsButRequiresAllPGsClean(t *testing.T) {
	ctr := &osdPolicyFixture{}
	c := osdPolicyCluster(ctr)
	ctr.status = `{"mgrmap":{"available":true},"osdmap":{"num_osds":4,"num_up_osds":4,"num_in_osds":3},"pgmap":{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean","count":8}]}}`
	if err := c.WaitForPGClean(t.Context()); err != nil {
		t.Fatal("out OSD prevented a PG recovery barrier", err)
	}
	for _, state := range []string{`{"num_pgs":0}`, `{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean","count":7}]}`, `{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean+remapped","count":8}]}`} {
		ctr.status = `{"mgrmap":{"available":true},"pgmap":` + state + `}`
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		if err := c.WaitForPGClean(ctx); err == nil {
			t.Fatal("incomplete/remapped PGs accepted as clean")
		}
		cancel()
	}
}

func TestWaitForPGCleanRejectsClosedOrUnavailableControlImmediately(t *testing.T) {
	ctr := &osdPolicyFixture{status: `{"mgrmap":{"available":true},"pgmap":{"num_pgs":8,"pgs_by_state":[{"state_name":"active+clean","count":8}]}}`}
	c := osdPolicyCluster(ctr)
	c.closed = true
	if err := c.WaitForPGClean(t.Context()); err == nil || len(ctr.calls) != 0 {
		t.Fatal("closed cluster accepted or queried readiness")
	}
	c.closed = false
	c.Container = nil
	if err := c.WaitForPGClean(t.Context()); err == nil || len(ctr.calls) != 0 {
		t.Fatal("missing control container waited for readiness")
	}
}

func osdPolicyCluster(ctr *osdPolicyFixture) *Container {
	return &Container{Container: ctr, settings: options{startupTimeout: time.Second}, fleet: osdFleetState{osdHandles: osdRegistry{owned: map[int]*OSDContainer{3: {ID: 3, nativeUUID: osdPolicyUUID}}}}}
}

type osdPolicyFixture struct {
	testcontainers.Container
	flags                  []string
	osds                   []OSDState
	calls                  [][]string
	raw, status, failAfter string
	replaceAfter           bool
	oldWeight              float64
}

func (ctr *osdPolicyFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args = slices.Clone(args[3:])
	ctr.calls = append(ctr.calls, args)
	output, code := "", 0
	if slices.Equal(args, []string{"osd", "dump", "--format", "json"}) {
		rows := make([]map[string]any, 0, len(ctr.osds))
		for _, osd := range ctr.osds {
			up, in := 0, 0
			if osd.Up {
				up = 1
			}
			if osd.In {
				in = 1
			}
			rows = append(rows, map[string]any{"osd": osd.ID, "uuid": osd.UUID, "up": up, "in": in, "weight": osd.Weight})
		}
		raw, _ := json.Marshal(map[string]any{"flags": strings.Join(ctr.flags, ","), "osds": rows})
		output = string(raw)
		if ctr.raw != "" {
			output = ctr.raw
		}
	} else if slices.Equal(args, []string{"status", "--format", "json"}) {
		output = ctr.status
	} else if len(args) == 3 && args[0] == "osd" && (args[1] == "set" || args[1] == "unset") {
		ctr.flags = slices.DeleteFunc(ctr.flags, func(flag string) bool { return flag == args[2] })
		if args[1] == "set" {
			ctr.flags = append(ctr.flags, args[2])
		}
		if ctr.failAfter == args[1] {
			code, output = 1, "uncertain native reply"
		}
	} else if len(args) == 3 && args[0] == "osd" && (args[1] == "in" || args[1] == "out") {
		id, _ := strconv.Atoi(args[2])
		for i := range ctr.osds {
			if ctr.osds[i].ID != id {
				continue
			}
			ctr.osds[i].In = args[1] == "in"
			if args[1] == "out" {
				ctr.oldWeight, ctr.osds[i].Weight = ctr.osds[i].Weight, 0
			} else {
				ctr.osds[i].Weight = ctr.oldWeight
			}
			if ctr.replaceAfter {
				ctr.osds[i].UUID = "c09ab9b3-3f50-4169-a6a4-4a58ab1c0d6c"
			}
		}
	} else {
		return 0, nil, fmt.Errorf("unexpected command %v: %w", args, errors.New("test fixture"))
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
