package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestInvalidPoolConfigDoesNotExecuteCeph(t *testing.T) {
	for _, config := range []PoolConfig{
		{}, {Name: ".reserved"}, {Name: "--option"}, {Name: "with space"},
		{Name: "fixture", PGNum: -1}, {Name: "fixture", Replicas: -1},
		{Name: "fixture", Replicas: 11}, {Name: "fixture", MinSize: -1},
		{Name: "fixture", Replicas: 2, MinSize: 3}, {Name: "fixture", Application: "bad application"},
		{Name: "fixture", FailureDomain: "zone"}, {Name: "fixture", FailureDomain: "Host"},
		{Name: "fixture", CRUSHRoot: "bad root"}, {Name: "fixture", CRUSHRoot: "--option"},
		{Name: "fixture", DeviceClass: "bad class"}, {Name: "fixture", DeviceClass: "ssd\n"},
		{Name: "fixture", ErasureCode: &ErasureCodeConfig{}},
		{Name: "fixture", ErasureCode: &ErasureCodeConfig{K: 1, M: 1}},
		{Name: "fixture", ErasureCode: &ErasureCodeConfig{K: 2, M: -1}},
		{Name: "fixture", ErasureCode: &ErasureCodeConfig{K: 127, M: 2}},
		{Name: "fixture", Replicas: 2, ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
		{Name: "fixture", MinSize: 1, ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
		{Name: "fixture", MinSize: 4, ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
		{Name: "fixture", Application: "rbd", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
		{Name: "fixture", Application: "cephfs", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
	} {
		ctr := &poolFixtureContainer{}
		cluster := poolFixtureCluster(ctr, 3)
		if pool, err := cluster.CreatePool(t.Context(), config); err == nil || pool != nil || len(ctr.calls) != 0 {
			t.Errorf("invalid config executed Ceph: config=%+v pool=%+v err=%v calls=%v", config, pool, err, ctr.calls)
		}
	}
}

func TestPoolPlacementRequiresOwnedOSDsBeforeCommands(t *testing.T) {
	for _, config := range []PoolConfig{
		{Name: "replicated", Replicas: 3},
		{Name: "erasure", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}},
	} {
		ctr := &poolFixtureContainer{}
		cluster := poolFixtureCluster(ctr, 2)
		if pool, err := cluster.CreatePool(t.Context(), config); err == nil || pool != nil || len(ctr.calls) != 0 {
			t.Fatalf("insufficient OSDs mutated Ceph: pool=%+v error=%v calls=%v", pool, err, ctr.calls)
		}
	}
	ctr := &poolFixtureContainer{}
	cluster := poolFixtureCluster(ctr, 3)
	cluster.osds[2].purged = true
	if _, err := cluster.CreatePool(t.Context(), PoolConfig{Name: "fixture", Replicas: 3}); err == nil || len(ctr.calls) != 0 {
		t.Fatal("a purged OSD counted toward pool placement")
	}
}

func TestPoolDefaultsAndECConfigAreIndependent(t *testing.T) {
	config, err := normalizePoolConfig(PoolConfig{Name: "replicated"})
	if err != nil || config.PGNum != 8 || config.Replicas != 2 || config.MinSize != 1 || config.Application != "" || config.FailureDomain != "osd" || config.CRUSHRoot != "default" || config.DeviceClass != "" {
		t.Fatalf("unexpected replicated defaults: %+v error=%v", config, err)
	}
	ec := &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}
	config, err = normalizePoolConfig(PoolConfig{Name: "erasure", Application: "rbd", ErasureCode: ec})
	if err != nil || config.MinSize != 3 || config.Replicas != 0 {
		t.Fatalf("unexpected EC defaults: %+v error=%v", config, err)
	}
	ec.K = 7
	if config.ErasureCode.K != 2 {
		t.Fatal("resolved EC configuration aliases caller memory")
	}
}

func TestPoolPlacementCountsDistinctEligibleDomains(t *testing.T) {
	for _, test := range []struct {
		name      string
		config    PoolConfig
		placement []OSDConfig
	}{
		{"hosts are distinct", PoolConfig{Name: "fixture", Replicas: 3, FailureDomain: "host"}, []OSDConfig{
			{Host: "a"}, {Host: "a"}, {Host: "b"},
		}},
		{"racks are distinct", PoolConfig{Name: "fixture", Replicas: 3, FailureDomain: "rack"}, []OSDConfig{
			{Host: "a", Rack: "one"}, {Host: "b", Rack: "one"}, {Host: "c", Rack: "two"},
		}},
		{"missing rack is ineligible", PoolConfig{Name: "fixture", Replicas: 2, FailureDomain: "rack"}, []OSDConfig{
			{Host: "a", Rack: "one"}, {Host: "b"}, {Host: "c"},
		}},
		{"root filter", PoolConfig{Name: "fixture", Replicas: 3, CRUSHRoot: "data"}, []OSDConfig{
			{Host: "a", Root: "data"}, {Host: "b", Root: "data"}, {Host: "c", Root: "default"},
		}},
		{"class filter", PoolConfig{Name: "fixture", Replicas: 3, DeviceClass: "ssd"}, []OSDConfig{
			{Host: "a", DeviceClass: "ssd"}, {Host: "b", DeviceClass: "ssd"}, {Host: "c", DeviceClass: "hdd"},
		}},
		{"EC needs K plus M domains", PoolConfig{Name: "fixture", FailureDomain: "host", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}, []OSDConfig{
			{Host: "a"}, {Host: "a"}, {Host: "b"},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctr := &poolFixtureContainer{}
			cluster := poolFixtureCluster(ctr, len(test.placement))
			for id, placement := range test.placement {
				cluster.osds[id].placement = placement
			}
			if pool, err := cluster.CreatePool(t.Context(), test.config); err == nil || pool != nil || len(ctr.calls) != 0 {
				t.Fatalf("ineligible domains mutated Ceph: pool=%+v error=%v calls=%v", pool, err, ctr.calls)
			}
		})
	}
}

func TestPoolPlacementSelectsReplicatedAndECConstraints(t *testing.T) {
	for _, erasure := range []bool{false, true} {
		ctr := &poolFixtureContainer{}
		cluster := poolFixtureCluster(ctr, 4)
		for id := range 3 {
			cluster.osds[id].placement = OSDConfig{Host: "host-" + string(rune('a'+id)), Rack: "rack-" + string(rune('a'+id)), Root: "data", DeviceClass: "fast"}
		}
		cluster.osds[3].placement = OSDConfig{Host: "excluded", Rack: "excluded", Root: "other", DeviceClass: "fast"}
		config := PoolConfig{Name: "fixture", Replicas: 3, MinSize: 2, FailureDomain: "rack", CRUSHRoot: "data", DeviceClass: "fast"}
		if erasure {
			config.Replicas, config.MinSize = 0, 3
			config.ErasureCode = &ErasureCodeConfig{K: 2, M: 1}
		}
		pool, err := cluster.CreatePool(t.Context(), config)
		if err != nil || pool == nil || pool.FailureDomain != "rack" || pool.CRUSHRoot != "data" || pool.DeviceClass != "fast" {
			t.Fatalf("eligible pool rejected: pool=%+v error=%v", pool, err)
		}
		found := false
		for _, call := range ctr.calls {
			if erasure && slices.Contains(call, "plugin=jerasure") {
				found = slices.Contains(call, "crush-root=data") && slices.Contains(call, "crush-failure-domain=rack") && slices.Contains(call, "crush-device-class=fast")
			}
			if !erasure && slices.Contains(call, "create-replicated") {
				found = slices.Equal(call[len(call)-3:], []string{"data", "rack", "fast"})
			}
		}
		if !found {
			t.Fatalf("placement constraints did not reach CRUSH: %v", ctr.calls)
		}
	}
}

func TestReplicatedPoolUsesClusterDefaults(t *testing.T) {
	ctr := &poolFixtureContainer{}
	cluster := poolFixtureCluster(ctr, 3)
	cluster.settings.poolReplicas, cluster.settings.poolMinSize = 3, 2
	pool, err := cluster.CreatePool(t.Context(), PoolConfig{Name: "fixture"})
	if err != nil || pool.Replicas != 3 || pool.MinSize != 2 {
		t.Fatalf("cluster defaults lost: pool=%+v error=%v", pool, err)
	}
	ctr = &poolFixtureContainer{}
	cluster = poolFixtureCluster(ctr, 3)
	cluster.settings.poolReplicas, cluster.settings.poolMinSize = 3, 2
	pool, err = cluster.CreatePool(t.Context(), PoolConfig{Name: "fixture", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}})
	if err != nil || pool.Replicas != 0 || pool.MinSize != 3 {
		t.Fatalf("replicated defaults changed EC sizing: pool=%+v error=%v", pool, err)
	}
}

func TestExistingPoolResourcesCannotBeReconfigured(t *testing.T) {
	config := PoolConfig{Name: "fixture", ErasureCode: &ErasureCodeConfig{K: 2, M: 1}}
	for _, test := range []struct{ command, names string }{
		{"osd pool ls --format json", `["fixture"]`},
		{"osd crush rule ls --format json", `["tc-fixture-ec"]`},
		{"osd erasure-code-profile ls --format json", `["tc-fixture-ec"]`},
	} {
		ctr := &poolFixtureContainer{output: map[string]string{test.command: test.names}}
		if pool, err := poolFixtureCluster(ctr, 3).CreatePool(t.Context(), config); err == nil || pool != nil {
			t.Fatalf("existing resource was accepted: %+v error=%v", pool, err)
		}
		for _, call := range ctr.calls {
			if !slices.Contains(call, "ls") {
				t.Fatalf("resource preflight mutated Ceph: %v", call)
			}
		}
	}
}

func TestPoolCreationFailurePreservesPartialResources(t *testing.T) {
	failure := "osd pool set fixture allow_ec_overwrites true"
	ctr := &poolFixtureContainer{fail: failure}
	config := PoolConfig{Name: "fixture", Application: "rbd", ErasureCode: &ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}}
	pool, err := poolFixtureCluster(ctr, 3).CreatePool(t.Context(), config)
	if err == nil || pool == nil || pool.Name != "fixture" || pool.ErasureCodeProfile != "tc-fixture-ec" || pool.CRUSHRule != "tc-fixture-ec" {
		t.Fatalf("partial resources became unidentifiable: pool=%+v error=%v", pool, err)
	}
	if strings.Join(ctr.calls[len(ctr.calls)-1], " ") != failure {
		t.Fatal("pool creation continued after the failed mutation")
	}
	foundProfile := false
	for _, call := range ctr.calls {
		if slices.Contains(call, "delete") || slices.Contains(call, "rm") {
			t.Fatal("uncertain pool creation failure deleted resources")
		}
		if slices.Contains(call, "plugin=jerasure") && slices.Contains(call, "crush-failure-domain=osd") {
			foundProfile = true
		}
		if slices.Contains(call, "size") {
			t.Fatal("EC pool attempted to set its immutable replica size")
		}
	}
	if !foundProfile {
		t.Fatal("EC profile did not select portable coding and per-OSD placement")
	}
}

func TestSingleReplicaPoolExplicitlyAcknowledgesFixtureSetting(t *testing.T) {
	ctr := &poolFixtureContainer{}
	pool, err := poolFixtureCluster(ctr, 1).CreatePool(t.Context(), PoolConfig{Name: "fixture", Replicas: 1, Application: "rados"})
	if err != nil || pool == nil {
		t.Fatal(err)
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "size") {
			if !slices.Contains(call, "--yes-i-really-mean-it") {
				t.Fatal("size=1 omitted Ceph's required acknowledgement")
			}
			return
		}
	}
	t.Fatal("single replica pool never configured its size")
}

func poolFixtureCluster(ctr testcontainers.Container, count int) *Container {
	cluster := &Container{Container: ctr, osds: make(map[int]*OSDContainer), settings: options{startupTimeout: time.Second}}
	for id := range count {
		cluster.osds[id] = &OSDContainer{ID: id, Container: ctr}
	}
	return cluster
}

type poolFixtureContainer struct {
	testcontainers.Container
	calls  [][]string
	output map[string]string
	fail   string
}

func (ctr *poolFixtureContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args = slices.Clone(args[3:]) // ceph --connect-timeout 5 precedes module arguments.
	ctr.calls = append(ctr.calls, args)
	command := strings.Join(args, " ")
	output := ""
	if slices.Contains(args, "ls") {
		output = "[]"
	}
	if value, ok := ctr.output[command]; ok {
		output = value
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	code := 0
	if command == ctr.fail {
		header[0], code, output = byte(stdcopy.Stderr), 1, "injected command failure"
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
