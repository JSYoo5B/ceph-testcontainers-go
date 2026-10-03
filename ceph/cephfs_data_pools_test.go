package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type cephFSDataPoolsFixture struct {
	testcontainers.Container
	fsID, metadataID, defaultID, targetID int64
	attached                              bool
	apps                                  map[string]map[string]string
	poolType                              int
	flags, objects                        string
	otherRole                             bool
	failAdd, failRemove, failReadback     bool
	output                                map[string]string
	calls                                 []string
}

func dataPoolsFixture(t *testing.T) (*CephFSContainer, *cephFSDataPoolsFixture) {
	t.Helper()
	ctr := &cephFSDataPoolsFixture{fsID: 41, metadataID: 1, defaultID: 2, targetID: 3, poolType: 1,
		apps: map[string]map[string]string{"cephfs": {}}, output: make(map[string]string)}
	c := &Container{Container: ctr, settings: options{startupTimeout: time.Second}, filesystems: make(map[string]*CephFSContainer)}
	fs := &CephFSContainer{FilesystemName: "fixture", MetadataPool: "metadata", DataPool: "data", cluster: c,
		config: CephFSConfig{Name: "fixture", MetadataPool: PoolConfig{Name: "metadata"}, DataPool: PoolConfig{Name: "data"}}}
	c.filesystems["fixture"] = fs
	if err := fs.captureNativePoolIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.calls = nil
	return fs, ctr
}

func (ctr *cephFSDataPoolsFixture) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	key := strings.Join(args, " ")
	if args[0] == "ceph" {
		key = strings.Join(args[3:], " ")
	}
	ctr.calls = append(ctr.calls, key)
	output, code := "", 0
	marshal := func(value any) string { data, _ := json.Marshal(value); return string(data) }
	switch key {
	case "fs dump --format json":
		data := []int64{ctr.defaultID}
		if ctr.attached {
			data = append(data, ctr.targetID)
		}
		filesystems := []any{map[string]any{"id": ctr.fsID, "mdsmap": map[string]any{"fs_name": "fixture", "metadata_pool": ctr.metadataID, "data_pools": data, "info": map[string]any{}}}}
		if ctr.otherRole {
			filesystems = append(filesystems, map[string]any{"id": 42, "mdsmap": map[string]any{"fs_name": "other", "metadata_pool": ctr.targetID, "data_pools": []int64{9}}})
		}
		output = marshal(map[string]any{"filesystems": filesystems, "standbys": []any{}})
		if ctr.failReadback && ctr.attached {
			code, output = 1, "lost readback"
		}
	case "osd pool ls detail --format json":
		pools := []any{}
		for _, p := range []struct {
			id    int64
			name  string
			kind  int
			flags string
		}{{ctr.metadataID, "metadata", 1, ""}, {ctr.defaultID, "data", 1, ""}, {ctr.targetID, "extra", ctr.poolType, ctr.flags}} {
			pools = append(pools, map[string]any{"pool_id": p.id, "pool_name": p.name, "type": p.kind, "flags_names": p.flags, "size": 2, "min_size": 1, "pg_num": 8})
		}
		output = marshal(pools)
	case "osd pool application get extra --format json":
		output = marshal(ctr.apps)
	case "fs add_data_pool fixture 3":
		ctr.attached = true
		ctr.apps["cephfs"]["data"] = "fixture"
		if ctr.failAdd {
			code, output = 1, "lost add reply"
		}
	case "fs rm_data_pool fixture 3":
		ctr.attached = false
		if ctr.failRemove {
			code, output = 1, "lost remove reply"
		}
	case "mgr module ls --format json":
		output = `{"enabled_modules":["volumes"],"always_on_modules":[]}`
	case "fs volume ls --format json":
		output = `[{"name":"fixture"}]`
	case "fs subvolumegroup ls fixture --format json", "fs subvolume ls fixture --format json":
		output = "[]"
	case "rados -p extra --all ls":
		output = ctr.objects
	default:
		if strings.Contains(key, " ls ") {
			output = "[]"
		}
	}
	if override, ok := ctr.output[key]; ok {
		output = override
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}

func TestCephFSDataPoolAttachmentIdentityAndLiveSelection(t *testing.T) {
	fs, ctr := dataPoolsFixture(t)
	pool, err := fs.AddDataPool(t.Context(), "extra")
	if err != nil || pool == nil || pool.ID != 3 {
		t.Fatalf("attach=%+v error=%v", pool, err)
	}
	states, err := fs.DataPools(t.Context())
	if err != nil || !slices.Equal(states, []CephFSDataPoolState{{Name: "data", ID: 2, Default: true}, {Name: "extra", ID: 3}}) {
		t.Fatalf("native registration=%+v error=%v", states, err)
	}
	if _, err := fs.AddDataPool(t.Context(), "extra"); err == nil {
		t.Fatal("confirmed repeated attachment accepted")
	}
	if err := fs.validateSubvolumeConfig("volume", "", "extra", 0); err != nil {
		t.Fatal("dynamic pool rejected by old static whitelist", err)
	}
	ctr.attached = false
	if err := fs.checkSubvolumeDataPool(t.Context(), "extra"); err == nil {
		t.Fatal("externally detached pool accepted for volume creation")
	}
}

type cephFSDataPoolWaitingContext struct {
	context.Context
	firstCheck chan struct{}
	once       sync.Once
}

func (ctx *cephFSDataPoolWaitingContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.firstCheck) })
	return err
}

func TestCephFSDataPoolCanceledWhileWaitingForSetupNeverStartsOperation(t *testing.T) {
	fs, ctr := dataPoolsFixture(t)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &cephFSDataPoolWaitingContext{Context: base, firstCheck: make(chan struct{})}
	fs.cluster.cephfsSetupMu.Lock()
	locked := true
	defer func() {
		if locked {
			fs.cluster.cephfsSetupMu.Unlock()
		}
	}()
	result := make(chan error, 1)
	go func() {
		_, done, err := fs.beginDataPoolOperation(ctx)
		done()
		result <- err
	}()
	select {
	case <-ctx.firstCheck:
	case <-time.After(time.Second):
		t.Fatal("data pool operation did not reach its pre-lock context check")
	}
	cancel()
	fs.cluster.cephfsSetupMu.Unlock()
	locked = false
	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("canceled waiter started a native-operation lease: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter retained the filesystem setup lock")
	}
	if len(ctr.calls) != 0 {
		t.Fatal("canceled setup waiter sent native commands", ctr.calls)
	}
	// Cancellation releases the shared lock; a subsequent live query still works.
	if _, err := fs.DataPools(t.Context()); err != nil {
		t.Fatal("canceled setup waiter blocked the next operation", err)
	}
}

func TestCephFSDataPoolGuardsBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*CephFSContainer, *cephFSDataPoolsFixture)
	}{
		{"changed filesystem", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.fsID++ }},
		{"changed default pool", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.defaultID++ }},
		{"changed metadata pool", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.metadataID++ }},
		{"cross filesystem metadata", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.otherRole = true }},
		{"EC without overwrites", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.poolType = 3 }},
		{"other application", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.apps = map[string]map[string]string{"rbd": {}} }},
		{"retained native role", func(_ *CephFSContainer, c *cephFSDataPoolsFixture) { c.apps["cephfs"]["data"] = "fixture" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, c := dataPoolsFixture(t)
			test.edit(fs, c)
			if p, err := fs.AddDataPool(t.Context(), "extra"); err == nil || p != nil || c.attached {
				t.Fatalf("unsafe attachment=%+v err=%v calls=%v", p, err, c.calls)
			}
		})
	}
	fs, c := dataPoolsFixture(t)
	c.poolType, c.flags = 3, "hashpspool,ec_overwrites"
	if _, err := fs.AddDataPool(t.Context(), "extra"); err != nil {
		t.Fatal("overwrite-enabled EC refused", err)
	}
	fs, c = dataPoolsFixture(t)
	fs.cluster.closed = true
	if _, err := fs.DataPools(t.Context()); err == nil || len(c.calls) != 0 {
		t.Fatal("terminated cluster queried")
	}
	fs, c = dataPoolsFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fs.AddDataPool(ctx, "extra"); err == nil || len(c.calls) != 0 {
		t.Fatal("canceled caller reached CLI")
	}
	if _, err := (*CephFSContainer)(nil).DataPools(t.Context()); err == nil {
		t.Fatal("nil filesystem accepted")
	}
	fs, c = dataPoolsFixture(t)
	fs.nativeIdentity = nil // Start failed before confirming its created FSMap.
	if _, err := fs.DataPools(t.Context()); err == nil || len(c.calls) != 0 {
		t.Fatal("partial filesystem adopted a native identity on first use")
	}
}

func TestCephFSDataPoolLostReplyRecoveryAndReplacedID(t *testing.T) {
	for _, readback := range []bool{false, true} {
		fs, c := dataPoolsFixture(t)
		c.failAdd, c.failReadback = !readback, readback
		attempt, err := fs.AddDataPool(t.Context(), "extra")
		if err == nil || attempt == nil || !c.attached || attempt.identity.confirmed {
			t.Fatalf("partial attach=%+v err=%v", attempt, err)
		}
		c.failAdd, c.failReadback = false, false
		confirmed, err := fs.AddDataPool(t.Context(), "extra")
		if err != nil || confirmed.identity != attempt.identity {
			t.Fatalf("unknown success did not reconcile: %+v %v", confirmed, err)
		}
		if count := slices.Index(c.calls, "fs add_data_pool fixture 3"); count < 0 {
			t.Fatal("attachment was never attempted")
		}
	}
	fs, c := dataPoolsFixture(t)
	c.failAdd = true
	_, _ = fs.AddDataPool(t.Context(), "extra")
	c.failAdd = false
	c.targetID = 8
	before := len(c.calls)
	if p, err := fs.AddDataPool(t.Context(), "extra"); err == nil || p != nil {
		t.Fatal("replaced pool adopted")
	}
	for _, call := range c.calls[before:] {
		if strings.Contains(call, "add_data_pool") {
			t.Fatal("replaced pool mutated")
		}
	}
}

func TestCephFSUnusedDataPoolRemovalPreservesDataAndSharedState(t *testing.T) {
	fs, c := dataPoolsFixture(t)
	attachment, err := fs.AddDataPool(t.Context(), "extra")
	if err != nil {
		t.Fatal(err)
	}
	copy := *attachment
	copy.Name, copy.ID, copy.FilesystemName = "metadata", 1, "other"
	c.failRemove = true
	if err := fs.RemoveUnusedDataPool(t.Context(), &copy); err == nil || c.attached {
		t.Fatal("lost removal reply was not retained")
	}
	c.failRemove = false
	if err := fs.RemoveUnusedDataPool(t.Context(), attachment); err != nil {
		t.Fatal("unknown successful detach retry", err)
	}
	if err := fs.RemoveUnusedDataPool(t.Context(), &copy); err != nil {
		t.Fatal("copied removal state not shared", err)
	}
	if c.apps["cephfs"]["data"] != "fixture" {
		t.Fatal("native role tag was deleted")
	}
	if _, err := fs.AddDataPool(t.Context(), "extra"); err == nil {
		t.Fatal("retained tag silently changed for reattachment")
	}
	for _, call := range c.calls {
		if strings.Contains(call, "delete") || strings.Contains(call, "application rm") {
			t.Fatal("detachment deleted resource", call)
		}
	}
}

func TestCephFSUnusedDataPoolRemovalRefusesReferencesAndExternalEdits(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*CephFSContainer, *cephFSDataPoolsFixture, *CephFSDataPool)
	}{
		{"managed use", func(fs *CephFSContainer, _ *cephFSDataPoolsFixture, p *CephFSDataPool) {
			if err := fs.checkSubvolumeDataPool(t.Context(), p.Name); err != nil {
				t.Fatal(err)
			}
		}},
		{"all namespace objects", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) {
			c.objects = "namespace\tobject\n"
		}},
		{"native group layout", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) {
			c.output["fs subvolumegroup ls fixture --format json"] = `[{"name":"group"}]`
			c.output["fs subvolumegroup info fixture group --format json"] = `{"data_pool":"extra","created_at":"now","bytes_quota":"infinite","bytes_used":0}`
			c.output["fs subvolumegroup getpath fixture group"] = "/volumes/group"
		}},
		{"unknown pending clone", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) {
			c.output["fs subvolume ls fixture --format json"] = `[{"name":"pending"}]`
			c.output["fs subvolume info fixture pending --format json"] = `{}`
		}},
		{"retained snapshots", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) {
			c.output["fs subvolume ls fixture --format json"] = `[{"name":"retained"}]`
			c.output["fs subvolume info fixture retained --format json"] = `{"state":"snapshot-retained","type":"subvolume"}`
		}},
		{"changed role", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) {
			c.apps["cephfs"]["data"] = "other"
		}},
		{"replaced pool", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) { c.targetID = 8 }},
		{"externally detached", func(_ *CephFSContainer, c *cephFSDataPoolsFixture, _ *CephFSDataPool) { c.attached = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, c := dataPoolsFixture(t)
			p, err := fs.AddDataPool(t.Context(), "extra")
			if err != nil {
				t.Fatal(err)
			}
			test.edit(fs, c, p)
			before := len(c.calls)
			if err := fs.RemoveUnusedDataPool(t.Context(), p); err == nil {
				t.Fatal("unsafe detach accepted")
			}
			for _, call := range c.calls[before:] {
				if strings.Contains(call, "rm_data_pool") {
					t.Fatal("guard mutated registration", call)
				}
			}
		})
	}
	fs, _ := dataPoolsFixture(t)
	if err := fs.RemoveUnusedDataPool(t.Context(), &CephFSDataPool{Name: "data", ID: 2}); err == nil {
		t.Fatal("unowned default pool accepted")
	}
}

func TestCephFSDataPoolInheritedGroupSelectionIsStickyBeforeCreate(t *testing.T) {
	fs, c := dataPoolsFixture(t)
	attachment, err := fs.AddDataPool(t.Context(), "extra")
	if err != nil {
		t.Fatal(err)
	}
	c.output["fs subvolumegroup info fixture group --format json"] = `{"data_pool":"extra","created_at":"now","bytes_quota":"infinite","bytes_used":0}`
	c.output["fs subvolumegroup getpath fixture group"] = "/volumes/group"
	if err := fs.checkSubvolumeCreationPool(t.Context(), "", "group"); err != nil {
		t.Fatal(err)
	}
	if !attachment.identity.used {
		t.Fatal("implicit pool selection was not retained before native create")
	}
	// External removal of the group and eventual object purge cannot turn a
	// previously selected pool into a fresh, never-used attachment.
	if err := fs.RemoveUnusedDataPool(t.Context(), attachment); err == nil {
		t.Fatal("implicit use was forgotten")
	}
}
