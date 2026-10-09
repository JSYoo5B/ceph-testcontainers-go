package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestTemporaryConfigRestoresAbsenceAndKeepsMaskedInheritedValues(t *testing.T) {
	ctr := &configFixture{entries: []ConfigEntry{{Section: "global", Name: "osd_max_backfills", Value: "3"}, {Section: "osd", Mask: "class:ssd", Name: "osd_max_backfills", Value: "4"}}}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Name: "osd-max-backfills", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	copy := *change
	if _, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Name: "osd_max_backfills", Value: "5"}); err == nil {
		t.Fatal("overlapping override accepted")
	}
	if err := copy.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.entries) != 2 || ctr.entries[0].Value != "3" || ctr.entries[1].Value != "4" || len(c.configOverrides) != 0 {
		t.Fatal("restoration changed inherited/masked state")
	}
}

func TestTemporaryConfigUsesNativeNormalizedValueAndExistingEntry(t *testing.T) {
	ctr := &configFixture{entries: []ConfigEntry{{Section: "osd", Mask: "class:ssd", Name: "osd_recovery_sleep", Value: "0"}}, normalized: "1"}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Mask: "class:ssd", Name: "osd_recovery_sleep", Value: "1.000"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.normalized = ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.entries) != 1 || ctr.entries[0].Value != "0" {
		t.Fatal("native canonical value wasn't restored")
	}
}

func TestTemporaryConfigPreservesModuleOptionsAndStoredStrings(t *testing.T) {
	module := ConfigEntry{Section: "mgr", Name: "mgr/volumes/pause_cloning", Value: "false"}
	foreign := ConfigEntry{Section: "client.example", Name: "key", Value: "stored\nstring"}
	ctr := &configFixture{entries: []ConfigEntry{module, foreign}}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "mgr", Name: module.Name, Value: "true"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	entries, err := c.Configuration(t.Context())
	if err != nil || len(entries) != 2 || !slices.Contains(entries, module) || !slices.Contains(entries, foreign) {
		t.Fatal("module setting or arbitrary foreign stored string lost")
	}
}

func TestTemporaryConfigRefusesOutsideEditsAndSanitizesValues(t *testing.T) {
	ctr := &configFixture{}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "client.example", Name: "debug_ms", Value: "PRIVATE-VALUE"})
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		for _, v := range []any{change, *change} {
			if strings.Contains(fmt.Sprintf(format, v), "PRIVATE-VALUE") {
				t.Fatal("override formatting leaked value")
			}
		}
	}
	ctr.entries[0].Value = "external"
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err == nil || len(ctr.calls) != before+1 || ctr.entries[0].Value != "external" {
		t.Fatal("outside edit overwritten")
	}
	ctr.entries[0].Value = "PRIVATE-VALUE"
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTemporaryConfigLostApplyAndRestoreRepliesConverge(t *testing.T) {
	ctr := &configFixture{failAfter: "set"}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Name: "debug_osd", Value: "2"})
	if change == nil || err == nil || strings.Contains(err.Error(), "PRIVATE-ERROR") {
		t.Fatal("lost reply lost handle or leaked values")
	}
	ctr.failAfter = "rm"
	if err := change.Restore(t.Context()); err == nil {
		t.Fatal("lost restore reply hidden")
	}
	ctr.failAfter = ""
	before := len(ctr.calls)
	if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before+1 || len(c.configOverrides) != 0 {
		t.Fatal("applied restoration was not reconciled")
	}
}

func TestTemporaryConfigLostNormalizedReadbackRequiresInspection(t *testing.T) {
	ctr := &configFixture{normalized: "7200", failDumpAt: 2}
	c := configCluster(ctr)
	change, err := c.TemporaryConfig(t.Context(), ConfigSetting{Section: "osd", Name: "osd_scrub_min_interval", Value: "7200.0000"})
	if err == nil || change == nil || change.state.known {
		t.Fatal("lost readback didn't preserve an uncertain handle")
	}
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "readback was unavailable") || ctr.entries[0].Value != "7200" {
		t.Fatal("unknown normalized value adopted for mutation")
	}
	// Explicit outside cleanup can resolve an uncertain lease without another set.
	ctr.entries = nil
	if err := change.Restore(t.Context()); err != nil || len(c.configOverrides) != 0 {
		t.Fatal("previous native state was not reconciled")
	}
}

func TestTemporaryConfigValidationAndMaskCanonicalization(t *testing.T) {
	for _, setting := range []ConfigSetting{{Section: "global|mon", Name: "debug_ms"}, {Section: "global.a", Name: "debug_ms"}, {Section: "osd", Name: "--option"}, {Section: "osd", Name: "debug_ms", Value: "bad\nvalue"}, {Section: "osd", Name: "debug_ms", Mask: "host:a/rack:b"}, {Section: "osd", Name: "debug_ms", Mask: "class:ssd/class:hdd"}} {
		ctr := &configFixture{}
		if _, err := configCluster(ctr).TemporaryConfig(t.Context(), setting); err == nil || len(ctr.calls) != 0 {
			t.Fatal("invalid setting reached Ceph")
		}
	}
	setting, err := normalizeConfigSetting(ConfigSetting{Section: "osd.0", Mask: "class:ssd/host:a", Name: "debug-ms"})
	if err != nil || configWho(setting) != "osd.0/host:a/class:ssd" || setting.Name != "debug_ms" {
		t.Fatal("mask/name canonicalization failed")
	}
}

func TestConfigurationRejectsMalformedDuplicateDump(t *testing.T) {
	for _, raw := range []string{"null", `[{"section":"osd","name":"debug_osd","value":"2"},{"section":"osd","name":"debug_osd","value":"3"}]`, `[{"section":"osd","mask":{},"name":"debug_osd"}]`} {
		if _, err := configCluster(&configFixture{raw: raw}).Configuration(t.Context()); err == nil {
			t.Fatal("invalid configuration dump accepted")
		}
	}
}

func configCluster(ctr testcontainers.Container) *Container {
	return &Container{Container: ctr, settings: options{startupTimeout: time.Second}}
}

type configFixture struct {
	testcontainers.Container
	entries                    []ConfigEntry
	calls                      [][]string
	normalized, failAfter, raw string
	dumps, failDumpAt          int
}

func (ctr *configFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args = slices.Clone(args[3:])
	ctr.calls = append(ctr.calls, args)
	output := ""
	code := 0
	if slices.Equal(args, []string{"config", "dump", "--format", "json"}) {
		ctr.dumps++
		if ctr.dumps == ctr.failDumpAt {
			return 1, strings.NewReader("PRIVATE-ERROR"), nil
		}
		data := ctr.entries
		if data == nil {
			data = []ConfigEntry{}
		}
		raw, _ := json.Marshal(data)
		output = string(raw)
		if ctr.raw != "" {
			output = ctr.raw
		}
	} else if len(args) >= 4 && args[0] == "config" && (args[1] == "set" || args[1] == "rm") {
		section, mask, _ := strings.Cut(args[2], "/")
		ctr.entries = slices.DeleteFunc(ctr.entries, func(entry ConfigEntry) bool {
			return entry.Section == section && entry.Mask == mask && entry.Name == args[3]
		})
		if args[1] == "set" {
			value := args[4]
			if ctr.normalized != "" {
				value = ctr.normalized
			}
			ctr.entries = append(ctr.entries, ConfigEntry{Section: section, Mask: mask, Name: args[3], Value: value})
		}
		if args[1] == ctr.failAfter {
			code = 1
			output = "PRIVATE-ERROR injected uncertain reply"
		}
	} else {
		return 0, nil, fmt.Errorf("unexpected command %v", args)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
