package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type quiesceTestControl struct {
	*poolFixtureContainer
	id, state               string
	version                 uint64
	members                 map[string]any
	timeout, expiration     float64
	mutations               int
	lostCreate, lostRelease bool
}

func (f *quiesceTestControl) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	args := argv[3:]
	if len(args) < 2 || args[0] != "fs" || args[1] != "quiesce" {
		return f.poolFixtureContainer.Exec(ctx, argv, opts...)
	}
	flag := func(name string) string {
		for i, arg := range args {
			if arg == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	f.calls = append(f.calls, slices.Clone(args))
	if slices.Contains(args, "--release") {
		f.mutations++
		if flag("--if-version") != strconv.FormatUint(f.version, 10) {
			return 0, nil, errors.New("ESTALE")
		}
		f.state = "RELEASED"
		f.version++
		if f.lostRelease {
			f.lostRelease = false
			return 0, nil, errors.New("lost release reply")
		}
	} else if !slices.Contains(args, "--query") {
		f.mutations++
		if flag("--if-version") != "0" {
			return 0, nil, errors.New("exclusive create missing")
		}
		f.id = flag("--set-id")
		f.state = "QUIESCED"
		f.version = 12
		f.timeout, _ = strconv.ParseFloat(flag("--timeout"), 64)
		f.expiration, _ = strconv.ParseFloat(flag("--expiration"), 64)
		f.members = map[string]any{"file:/volumes/group/volume/unique-id": map[string]any{"excluded": false}}
		if f.lostCreate {
			f.lostCreate = false
			return 0, nil, errors.New("lost create reply")
		}
	}
	data, _ := json.Marshal(map[string]any{"sets": map[string]any{f.id: map[string]any{"version": f.version, "state": map[string]any{"name": f.state}, "timeout": f.timeout, "expiration": f.expiration, "members": f.members}}})
	var header [8]byte
	header[0] = 1
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return 0, bytes.NewReader(append(header[:], data...)), nil
}

func quiesceFixture(t *testing.T) (*CephFSContainer, *quiesceTestControl, *CephFSSubvolume) {
	t.Helper()
	fs, control := subvolumeFixture()
	volume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", DataPool: "additional", SizeBytes: 32768, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	fake := &quiesceTestControl{poolFixtureContainer: control}
	fs.cluster.Container = fake
	return fs, fake, volume
}

func TestCephFSQuiesceVersionReleaseAndCopiedHandle(t *testing.T) {
	fs, control, volume := quiesceFixture(t)
	q, err := fs.QuiesceSubvolumes(t.Context(), []*CephFSSubvolume{volume}, CephFSQuiesceConfig{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if state, err := q.Status(t.Context()); err != nil || state.State != "QUIESCED" || state.Version != 12 {
		t.Fatal(state, err)
	}
	if control.mutations != 1 {
		t.Fatal("query changed native expiration")
	}
	copy := *q
	if err := copy.Release(t.Context()); err != nil {
		t.Fatal(err)
	}
	control.version++
	control.state = "QUIESCED" // An outside later set is preserved.
	if err := q.Release(t.Context()); err != nil || control.mutations != 2 {
		t.Fatal("stale copied handle changed a later native set", err)
	}
}

func TestCephFSQuiesceOutsideEditsAndExpiryRefused(t *testing.T) {
	for _, change := range []string{"version", "members", "expiry"} {
		t.Run(change, func(t *testing.T) {
			fs, control, volume := quiesceFixture(t)
			q, err := fs.QuiesceSubvolumes(t.Context(), []*CephFSSubvolume{volume}, CephFSQuiesceConfig{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "version":
				control.version++
			case "members":
				control.members["file:/volumes/foreign"] = map[string]any{"excluded": false}
			case "expiry":
				control.state = "EXPIRED"
			}
			if err := q.Release(t.Context()); err == nil || control.mutations != 1 {
				t.Fatal("changed checkpoint state accepted release", err)
			}
		})
	}
}

func TestCephFSQuiesceUncertainRepliesRetainRecoveryHandle(t *testing.T) {
	fs, control, volume := quiesceFixture(t)
	control.lostCreate = true
	q, err := fs.QuiesceSubvolumes(t.Context(), []*CephFSSubvolume{volume}, CephFSQuiesceConfig{Timeout: 10 * time.Second, Expiration: 30 * time.Second})
	if err == nil || q == nil || !q.state.confirmed {
		t.Fatal("lost persisted create lost its lease", err)
	}
	control.lostRelease = true
	if err := q.Release(t.Context()); err == nil {
		t.Fatal("lost release reply suppressed")
	}
	if err := q.Release(t.Context()); err != nil || control.mutations != 2 {
		t.Fatal("release retry duplicated mutation", err)
	}
}

func TestCephFSQuiesceValidationAndDecoder(t *testing.T) {
	fs, control, volume := quiesceFixture(t)
	for _, config := range []CephFSQuiesceConfig{{}, {Timeout: time.Second}, {Expiration: time.Second}} {
		if _, err := fs.QuiesceSubvolumes(t.Context(), []*CephFSSubvolume{volume}, config); err == nil || control.mutations != 0 {
			t.Fatal("unbounded quiesce accepted")
		}
	}
	for _, volumes := range [][]*CephFSSubvolume{nil, {nil}, {&CephFSSubvolume{}}, {volume, volume}} {
		if _, err := fs.QuiesceSubvolumes(t.Context(), volumes, CephFSQuiesceConfig{Timeout: time.Second, Expiration: time.Second}); err == nil || control.mutations != 0 {
			t.Fatal("invalid quiesce member accepted")
		}
	}
	base := `{"sets":{"id":{"version":12,"state":{"name":"QUIESCED"},"timeout":10.0,"expiration":30.0,"members":{"file:/volumes/owned":{"excluded":false}}}}}`
	for _, data := range []string{"null", `{"sets":{}}`, strings.Replace(base, `"version":12`, `"version":0`, 1), strings.Replace(base, "QUIESCED", "unknown", 1), strings.Replace(base, `"excluded":false`, `"excluded":true`, 1), strings.Replace(base, "file:/", "inode:", 1)} {
		if _, err := decodeCephFSQuiesce([]byte(data), "id"); err == nil {
			t.Fatal("invalid native quiesce state accepted", data)
		}
	}
}
