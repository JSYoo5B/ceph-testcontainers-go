package override

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const fullRatiosFSID = "35e00c14-bc10-4357-aed3-107912b9bead"

// fullRatiosFixture is a fake control container that answers the full ratios
// commands of the Ceph CLI the way the native OSDMap does: it stores float32
// values and records whether every observed tuple stayed ascending.
type fullRatiosFixture struct {
	testcontainers.Container
	ratios          ceph.FullRatios
	epoch           uint32
	fsid            string
	calls           [][]string
	mutations       int
	failBefore      int
	failAfter       int
	failDump        bool
	raw             string
	hook            func(context.Context, []string) error
	afterMutation   func()
	observedOrdered bool
}

func newFullRatiosFixture(t *testing.T) (*ceph.Container, *fullRatiosFixture) {
	t.Helper()
	ratios, _ := normalizeFullRatios(ceph.FullRatios{NearFull: .85, BackfillFull: .9, Full: .95})
	ctr := &fullRatiosFixture{ratios: ratios, epoch: 7, fsid: fullRatiosFSID, observedOrdered: true}
	c := testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Minute})
	testhelper.TestSetConfig(c, []byte("[global]\nfsid = "+fullRatiosFSID+"\nmon_host = mon\n"))
	testhelper.TestSetKeyring(c, []byte("confirmed"))
	return c, ctr
}

// heldFullRatios returns the full ratios handles that the cluster's registry
// holds.
func heldFullRatios(c *ceph.Container) map[string]any {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return testhelper.TestHeld(c, overridehook.FullRatios)
}

func fullRatiosStream(output string) io.Reader {
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return io.MultiReader(bytes.NewReader(header[:]), strings.NewReader(output))
}

func (ctr *fullRatiosFixture) Exec(ctx context.Context, command []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args := slices.Clone(command)
	ctr.calls = append(ctr.calls, args)
	if !slices.Equal(args[:min(3, len(args))], []string{"ceph", "--connect-timeout", "5"}) {
		return 0, nil, errors.New("unexpected fixture CLI prefix")
	}
	args = args[3:]
	if ctr.hook != nil {
		if err := ctr.hook(ctx, args); err != nil {
			return 0, nil, err
		}
	}
	output := ""
	switch {
	case slices.Equal(args, []string{"fsid"}):
		output = ctr.fsid + "\n"
	case slices.Equal(args, []string{"osd", "dump", "--format", "json"}):
		if ctr.failDump {
			return 2, fullRatiosStream("PRIVATE-NATIVE-OUTPUT"), nil
		}
		if ctr.raw != "" {
			output = ctr.raw
		} else {
			data, _ := json.Marshal(map[string]any{"fsid": ctr.fsid, "epoch": ctr.epoch,
				"nearfull_ratio": ctr.ratios.NearFull, "backfillfull_ratio": ctr.ratios.BackfillFull, "full_ratio": ctr.ratios.Full,
				"flags": "noout,sortbitwise", "pools": []any{}})
			output = string(data)
		}
	case len(args) == 3 && args[0] == "osd" && strings.HasPrefix(args[1], "set-"):
		ctr.mutations++
		if ctr.failBefore == ctr.mutations {
			return 0, nil, errors.New("PRIVATE-TRANSPORT-ERROR")
		}
		value, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return 0, nil, err
		}
		value = float64(float32(value))
		switch args[1] {
		case "set-nearfull-ratio":
			ctr.ratios.NearFull = value
		case "set-backfillfull-ratio":
			ctr.ratios.BackfillFull = value
		case "set-full-ratio":
			ctr.ratios.Full = value
		default:
			return 0, nil, errors.New("unexpected native mutation")
		}
		ctr.epoch++
		ctr.observedOrdered = ctr.observedOrdered && orderedFullRatios(ctr.ratios)
		if ctr.afterMutation != nil {
			ctr.afterMutation()
		}
		if ctr.failAfter == ctr.mutations {
			return 0, nil, errors.New("PRIVATE-LOST-REPLY")
		}
	default:
		return 0, nil, errors.New("unexpected full ratios fixture command")
	}
	return 0, fullRatiosStream(output), nil
}
