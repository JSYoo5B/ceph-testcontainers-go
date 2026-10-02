package ceph

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestDefaultCRUSHRuleConfigPreservesSectionsAndReplacesAliases(t *testing.T) {
	original := []byte("[global]\nfsid = fixture\nosd_pool_default_crush_rule = 0\n[osd]\ncache = retained\n[global]\nosd pool default crush rule = 9\n")
	config, err := defaultCRUSHRuleConfig(original, "4")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(config), "osd pool default crush rule = 4") != 1 || bytes.Contains(config, []byte("rule = 0")) || bytes.Contains(config, []byte("rule = 9")) || !bytes.Contains(config, []byte("[osd]\ncache = retained")) || !bytes.Contains(original, []byte("rule = 0")) {
		t.Fatalf("global default update damaged config or preserved stale overrides: %s", config)
	}
	if _, err := defaultCRUSHRuleConfig([]byte("[osd]\n"), "4"); err == nil {
		t.Fatal("missing bootstrap global section was silently accepted")
	}
}

func TestCustomDefaultRootConfiguresNumericRuleAndCopiesClientConfig(t *testing.T) {
	ctr := &defaultRootFixtureContainer{poolFixtureContainer: poolFixtureContainer{output: map[string]string{
		"osd crush rule dump tc-default-placement --format json": `{"rule_id":4,"rule_name":"tc-default-placement"}`,
	}}}
	cluster := &Container{Container: ctr, config: []byte("[global]\nfsid = fixture\n[osd]\n"), settings: options{defaultCRUSHRoot: "storage", hostNetwork: true, publicAddress: "127.0.0.1"}}
	if err := cluster.configureDefaultCRUSHRoot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(ctr.calls[0], []string{"osd", "crush", "add-bucket", "storage", "root"}) || !slices.Equal(ctr.calls[1], replicatedCRUSHRuleCommand(PoolConfig{CRUSHRoot: "storage", FailureDomain: "osd"}, "tc-default-placement")) || !slices.Equal(ctr.calls[3], []string{"config", "set", "global", "osd_pool_default_crush_rule", "4"}) {
		t.Fatalf("custom default did not use the actual numeric CRUSH rule: %v", ctr.calls)
	}
	if ctr.path != "/etc/ceph/ceph.conf" || ctr.mode != 0o644 || !bytes.Contains(ctr.copied, []byte("[global]\nosd pool default crush rule = 4")) || !bytes.Equal(cluster.config, ctr.copied) {
		t.Fatal("daemon/client config did not retain the selected numeric default")
	}
	var request testcontainers.GenericContainerRequest
	if err := cluster.WithClient()(&request); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(request.Files[0].Reader)
	if err != nil || !bytes.Equal(data, cluster.config) {
		t.Fatal("attached client did not receive the selected default rule")
	}
}

func TestDefaultCRUSHRootRejectsMissingRuleIdentityBeforeSettingConfig(t *testing.T) {
	for _, output := range []string{`{}`, `{"rule_id":-1,"rule_name":"tc-default-placement"}`, `{"rule_id":4,"rule_name":"other"}`, `[]`} {
		ctr := &defaultRootFixtureContainer{poolFixtureContainer: poolFixtureContainer{output: map[string]string{"osd crush rule dump tc-default-placement --format json": output}}}
		cluster := &Container{Container: ctr, config: []byte("[global]\n"), settings: options{defaultCRUSHRoot: "storage"}}
		if err := cluster.configureDefaultCRUSHRoot(t.Context()); err == nil {
			t.Fatal("malformed rule identity became the default")
		}
		for _, call := range ctr.calls {
			if len(call) != 0 && call[0] == "config" {
				t.Fatalf("unknown default rule changed global config: %v", call)
			}
		}
		if len(ctr.copied) != 0 {
			t.Fatal("unknown default rule changed bootstrap config")
		}
	}
}

type defaultRootFixtureContainer struct {
	poolFixtureContainer
	copied []byte
	path   string
	mode   int64
}

func (ctr *defaultRootFixtureContainer) CopyToContainer(_ context.Context, data []byte, path string, mode int64) error {
	ctr.copied, ctr.path, ctr.mode = bytes.Clone(data), path, mode
	return nil
}
