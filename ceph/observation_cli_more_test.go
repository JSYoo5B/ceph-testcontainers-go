package ceph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	mgrServicesCmd  = "mgr services --format json"
	blocklistCmd    = "osd blocklist ls --format json"
	configDumpCmd   = "config dump --format json"
	osdDumpCmd      = "osd dump --format json"
	mgrModuleLsCmd  = "mgr module ls --format json"
	mgrDumpCmd      = "mgr dump --format json"
	mgrModuleLsBody = `{"enabled_modules":["dashboard"],"always_on_modules":["balancer"],"force_disabled_modules":[],"disabled_modules":[{"name":"zabbix"}]}`
	mgrDumpBody     = `{"available":true,"modules":["dashboard"],"available_modules":[{"name":"dashboard","can_run":true,"error_string":""},{"name":"balancer","can_run":true,"error_string":""},{"name":"zabbix","can_run":false,"error_string":"missing"}]}`
)

func TestReadManagerServicesCommandAndDecode(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{mgrServicesCmd: []byte(`{"prometheus":"http://10.0.0.1:9283/"}`)}}
	got, err := readManagerServices(context.Background(), cli.run)
	if err != nil || !reflect.DeepEqual(got, map[string]string{"prometheus": "http://10.0.0.1:9283/"}) {
		t.Fatalf("services = %v, %v", got, err)
	}
	if !reflect.DeepEqual(cli.calls, []string{mgrServicesCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
}

func TestReadManagerServicesErrors(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeCLI{errs: map[string]error{mgrServicesCmd: boom}}
	if got, err := readManagerServices(context.Background(), cli.run); got != nil || err != boom {
		t.Fatalf("command error = %v, %v", got, err)
	}
	for name, reply := range map[string]string{"null": `null`, "malformed": `{`, "empty address": `{"dashboard":""}`, "bad scheme": `{"dashboard":"ftp://h/"}`} {
		cli := &fakeCLI{replies: map[string][]byte{mgrServicesCmd: []byte(reply)}}
		if got, err := readManagerServices(context.Background(), cli.run); got != nil || err == nil {
			t.Fatalf("%s: got %v, %v", name, got, err)
		}
	}
	cli = &fakeCLI{replies: map[string][]byte{mgrServicesCmd: []byte(`{}`)}}
	if got, err := readManagerServices(context.Background(), cli.run); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty services = %v, %v", got, err)
	}
}

func TestReadBlocklistEntriesCommandAndDecode(t *testing.T) {
	reply := `[{"addr":"10.0.0.2:0/123","until":"2026-01-02T03:04:05.123456+0000"}]
[{"range":"10.0.1.0/24","until":"2026-01-02T03:04:05.123456+0000"}]`
	cli := &fakeCLI{replies: map[string][]byte{blocklistCmd: []byte(reply)}}
	got, err := readBlocklistEntries(context.Background(), cli.run)
	if err != nil || len(got) != 2 || got[0].Range || !got[1].Range || got[1].Address != "10.0.1.0/24" {
		t.Fatalf("entries = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(cli.calls, []string{blocklistCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
}

func TestReadBlocklistEntriesErrors(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeCLI{errs: map[string]error{blocklistCmd: boom}}
	if got, err := readBlocklistEntries(context.Background(), cli.run); got != nil || err != boom {
		t.Fatalf("command error = %v, %v", got, err)
	}
	// A reply with no array, or only one, is evidence of nothing.
	for name, reply := range map[string]string{"empty": ``, "null": `null`, "malformed": `[{`} {
		cli := &fakeCLI{replies: map[string][]byte{blocklistCmd: []byte(reply)}}
		if got, err := readBlocklistEntries(context.Background(), cli.run); got != nil || err == nil {
			t.Fatalf("%s: got %v, %v", name, got, err)
		}
	}
}

func TestReadConfigurationCommandAndDecode(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{configDumpCmd: []byte(`[{"section":"global","name":"osd_pool_default_size","value":"1","mask":"","level":"advanced"}]`)}}
	got, err := readConfiguration(context.Background(), cli.run)
	if err != nil || len(got) != 1 || got[0].Name != "osd_pool_default_size" {
		t.Fatalf("configuration = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(cli.calls, []string{configDumpCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
	for name, reply := range map[string]string{"null": `null`, "malformed": `[`} {
		cli := &fakeCLI{replies: map[string][]byte{configDumpCmd: []byte(reply)}}
		if got, err := readConfiguration(context.Background(), cli.run); got != nil || err == nil {
			t.Fatalf("%s: got %v, %v", name, got, err)
		}
	}
}

func TestReadConfigurationRedactsNativeFailure(t *testing.T) {
	native := errors.New("secret-value-from-native-output")
	cli := &fakeCLI{errs: map[string]error{configDumpCmd: native}}
	_, err := readConfiguration(context.Background(), cli.run)
	if err == nil || strings.Contains(err.Error(), "secret-value") || err.Error() != "Ceph configuration command failed; native output is redacted" {
		t.Fatalf("error = %v", err)
	}
}

func TestRunConfigCommandPrefixAndCancellation(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{"config set global k v": []byte("ok")}}
	data, err := runConfigCommand(context.Background(), cli.run, "set", "global", "k", "v")
	if err != nil || string(data) != "ok" || !reflect.DeepEqual(cli.calls, []string{"config set global k v"}) {
		t.Fatalf("data = %q, %v, calls = %q", data, err, cli.calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cli = &fakeCLI{errs: map[string]error{"config rm global k": errors.New("native")}}
	if _, err := runConfigCommand(ctx, cli.run, "rm", "global", "k"); !errors.Is(err, context.Canceled) || !strings.HasPrefix(err.Error(), "Ceph configuration command: ") {
		t.Fatalf("cancelled error = %v", err)
	}
}

const osdDumpReply = `{"flags":"sortbitwise,noout,noup","osds":[{"osd":1,"uuid":"u1","up":1,"in":0,"weight":0.5},{"osd":0,"uuid":"u0","up":1,"in":1,"weight":1}]}`

func TestReadOSDFlagsSortedFromDump(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(osdDumpReply)}}
	got, err := readOSDFlags(context.Background(), cli.run)
	if err != nil || !reflect.DeepEqual(got, []OSDFlag{"noout", "noup", "sortbitwise"}) {
		t.Fatalf("flags = %v, %v", got, err)
	}
	if !reflect.DeepEqual(cli.calls, []string{osdDumpCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
	cli = &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(`{"flags":"","osds":[]}`)}}
	if got, err := readOSDFlags(context.Background(), cli.run); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no flags = %#v, %v", got, err)
	}
}

func TestReadOSDFlagsErrors(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeCLI{errs: map[string]error{osdDumpCmd: boom}}
	if got, err := readOSDFlags(context.Background(), cli.run); got != nil || err != boom {
		t.Fatalf("command error = %v, %v", got, err)
	}
	// A missing flags field must not read as "no flags set".
	for name, reply := range map[string]string{"missing flags": `{"osds":[]}`, "null flags": `{"flags":null,"osds":[]}`, "missing osds": `{"flags":""}`, "empty token": `{"flags":"noout,,noup","osds":[]}`, "duplicate": `{"flags":"noout,noout","osds":[]}`} {
		cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(reply)}}
		if got, err := readOSDFlags(context.Background(), cli.run); got != nil || err == nil {
			t.Fatalf("%s: got %v, %v", name, got, err)
		}
	}
}

func TestReadOSDPolicyDumpSnapshotAndErrorShape(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(osdDumpReply)}}
	snap, err := readOSDPolicyDump(context.Background(), cli.run)
	if err != nil || len(snap.osds) != 2 || snap.osds[1].In || !snap.osds[0].In || snap.osds[1].Weight != 0.5 {
		t.Fatalf("snapshot = %+v, %v", snap, err)
	}
	// Even on failure the snapshot map is usable, as callers index into it.
	boom := errors.New("boom")
	cli = &fakeCLI{errs: map[string]error{osdDumpCmd: boom}}
	snap, err = readOSDPolicyDump(context.Background(), cli.run)
	if err != boom || snap.osds == nil || len(snap.osds) != 0 {
		t.Fatalf("failed snapshot = %+v, %v", snap, err)
	}
}

func TestReadMGRModulesCommandOrderAndMerge(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{mgrModuleLsCmd: []byte(mgrModuleLsBody), mgrDumpCmd: []byte(mgrDumpBody)}}
	got, err := readMGRModules(context.Background(), cli.run)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cli.calls, []string{mgrModuleLsCmd, mgrDumpCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
	want := []MGRModuleState{
		{Name: "balancer", Enabled: true, AlwaysOn: true, Available: true, CanRun: true},
		{Name: "dashboard", Enabled: true, Available: true, CanRun: true},
		{Name: "zabbix", Available: true, ErrorString: "missing"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("modules = %+v", got)
	}
}

func TestReadMGRModulesFailureAttribution(t *testing.T) {
	boom := errors.New("boom")
	cli := &fakeCLI{errs: map[string]error{mgrModuleLsCmd: boom}, replies: map[string][]byte{mgrDumpCmd: []byte(mgrDumpBody)}}
	if got, err := readMGRModules(context.Background(), cli.run); got != nil || err != boom || len(cli.calls) != 1 {
		t.Fatalf("module ls error = %v, %v, calls %q", got, err, cli.calls)
	}
	cli = &fakeCLI{errs: map[string]error{mgrDumpCmd: boom}, replies: map[string][]byte{mgrModuleLsCmd: []byte(mgrModuleLsBody)}}
	if got, err := readMGRModules(context.Background(), cli.run); got != nil || err != boom || len(cli.calls) != 2 {
		t.Fatalf("mgr dump error = %v, %v, calls %q", got, err, cli.calls)
	}
}

func TestReadMGRModulesRejectsIncompleteEvidence(t *testing.T) {
	cases := map[string][2]string{
		"ls missing enabled":             {`{"always_on_modules":[],"disabled_modules":[]}`, mgrDumpBody},
		"ls missing always_on":           {`{"enabled_modules":[],"disabled_modules":[]}`, mgrDumpBody},
		"ls missing disabled":            {`{"enabled_modules":[],"always_on_modules":[]}`, mgrDumpBody},
		"ls null enabled":                {`{"enabled_modules":null,"always_on_modules":[],"disabled_modules":[]}`, mgrDumpBody},
		"ls malformed":                   {`{`, mgrDumpBody},
		"ls bad module name":             {`{"enabled_modules":["Bad Name"],"always_on_modules":[],"disabled_modules":[]}`, mgrDumpBody},
		"dump missing available":         {mgrModuleLsBody, `{"modules":[],"available_modules":[]}`},
		"dump missing modules":           {mgrModuleLsBody, `{"available":true,"available_modules":[]}`},
		"dump missing available_modules": {mgrModuleLsBody, `{"available":true,"modules":[]}`},
		"dump unavailable":               {mgrModuleLsBody, `{"available":false,"modules":[],"available_modules":[]}`},
		"dump missing can_run":           {mgrModuleLsBody, `{"available":true,"modules":["dashboard"],"available_modules":[{"name":"dashboard"}]}`},
		"dump unknown module":            {mgrModuleLsBody, `{"available":true,"modules":["dashboard"],"available_modules":[{"name":"other","can_run":true}]}`},
		"dump enabled not loaded":        {mgrModuleLsBody, `{"available":true,"modules":[],"available_modules":[]}`},
	}
	for name, c := range cases {
		cli := &fakeCLI{replies: map[string][]byte{mgrModuleLsCmd: []byte(c[0]), mgrDumpCmd: []byte(c[1])}}
		if got, err := readMGRModules(context.Background(), cli.run); got != nil || err == nil {
			t.Errorf("%s: got %+v, %v", name, got, err)
		}
	}
}
