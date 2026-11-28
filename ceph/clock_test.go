package ceph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// clockTestELF builds a minimal little-endian ELF64 shared object for machine
// whose dynamic symbol table holds the named symbols, each in the given
// section (elf.SHN_UNDEF for an undefined reference).
func clockTestELF(machine elf.Machine, symbols map[string]elf.SectionIndex) []byte {
	le := binary.LittleEndian
	dynstr := []byte{0}
	dynsym := make([]byte, 24) // the null symbol
	for _, name := range []string{"clock_gettime", "syscall"} {
		section, ok := symbols[name]
		if !ok {
			continue
		}
		entry := make([]byte, 24)
		le.PutUint32(entry[0:], uint32(len(dynstr)))
		entry[4] = byte(elf.STB_GLOBAL)<<4 | byte(elf.STT_FUNC)
		le.PutUint16(entry[6:], uint16(section))
		dynstr = append(dynstr, append([]byte(name), 0)...)
		dynsym = append(dynsym, entry...)
	}
	shstrtab := []byte("\x00.dynsym\x00.dynstr\x00.shstrtab\x00")
	offDynstr := 64
	offDynsym := offDynstr + len(dynstr)
	offShstr := offDynsym + len(dynsym)
	offSh := offShstr + len(shstrtab)
	var b bytes.Buffer
	header := make([]byte, 64)
	copy(header, []byte{0x7f, 'E', 'L', 'F', byte(elf.ELFCLASS64), byte(elf.ELFDATA2LSB), byte(elf.EV_CURRENT)})
	le.PutUint16(header[16:], uint16(elf.ET_DYN))
	le.PutUint16(header[18:], uint16(machine))
	le.PutUint32(header[20:], uint32(elf.EV_CURRENT))
	le.PutUint64(header[40:], uint64(offSh))
	le.PutUint16(header[52:], 64)
	le.PutUint16(header[54:], 56)
	le.PutUint16(header[58:], 64)
	le.PutUint16(header[60:], 4)
	le.PutUint16(header[62:], 3)
	b.Write(header)
	b.Write(dynstr)
	b.Write(dynsym)
	b.Write(shstrtab)
	section := func(name uint32, typ elf.SectionType, off, size int, link uint32, entsize uint64) {
		sh := make([]byte, 64)
		le.PutUint32(sh[0:], name)
		le.PutUint32(sh[4:], uint32(typ))
		le.PutUint64(sh[24:], uint64(off))
		le.PutUint64(sh[32:], uint64(size))
		le.PutUint32(sh[40:], link)
		le.PutUint64(sh[48:], 1)
		le.PutUint64(sh[56:], entsize)
		b.Write(sh)
	}
	section(0, elf.SHT_NULL, 0, 0, 0, 0)
	section(1, elf.SHT_DYNSYM, offDynsym, len(dynsym), 2, 24)
	section(9, elf.SHT_STRTAB, offDynstr, len(dynstr), 0, 0)
	section(17, elf.SHT_STRTAB, offShstr, len(shstrtab), 0, 0)
	return b.Bytes()
}

// clockTestManifest is a contract-1 manifest of interposer, with edit applied
// to its decoded form.
func clockTestManifest(t *testing.T, interposer []byte, edit func(map[string]any)) []byte {
	t.Helper()
	sum := sha256.Sum256(interposer)
	m := map[string]any{
		"schema": 1,
		"source": map[string]any{"repository": "https://github.com/wolfcw/libfaketime", "tag": "v0.9.12",
			"commit": "3ccdd344aadf1e317156fa5fb7c881f2c4404778", "tree": "26a197ec0000000000000000000000000000000a"},
		"toolchain":    map[string]any{"compiler": "gcc 12.2.0", "libc": "glibc 2.36"},
		"architecture": "x86_64",
		"interposer": map[string]any{"path": clockHelperInterposerPath, "sha256": hex.EncodeToString(sum[:]),
			"build": "default", "intercept_syscall": false, "compile_flags": ""},
	}
	if edit != nil {
		edit(m)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestClockInterposerAdmission(t *testing.T) {
	good := clockTestELF(elf.EM_X86_64, map[string]elf.SectionIndex{"clock_gettime": 1, "syscall": elf.SHN_UNDEF})
	identity, err := admitClockInterposer("helper:v1", clockTestManifest(t, good, nil), good)
	sum := sha256.Sum256(good)
	if err != nil || identity.HelperImage != "helper:v1" || identity.Build != "default" || identity.Architecture != "x86_64" ||
		identity.SHA256 != hex.EncodeToString(sum[:]) || identity.SourceCommit != "3ccdd344aadf1e317156fa5fb7c881f2c4404778" {
		t.Fatalf("default build refused or misrecorded: %+v, %v", identity, err)
	}
	intercept := clockTestELF(elf.EM_X86_64, map[string]elf.SectionIndex{"clock_gettime": 1, "syscall": 1})
	interposer := func(m map[string]any) map[string]any { return m["interposer"].(map[string]any) }
	cases := map[string]struct {
		interposer []byte
		edit       func(map[string]any)
		manifest   []byte
	}{
		"intercept-syscall build declared": {good, func(m map[string]any) { interposer(m)["intercept_syscall"] = true }, nil},
		"other build name":                 {good, func(m map[string]any) { interposer(m)["build"] = "intercept-syscall" }, nil},
		"intercept flag missing":           {good, func(m map[string]any) { delete(interposer(m), "intercept_syscall") }, nil},
		"extra compile flags":              {good, func(m map[string]any) { interposer(m)["compile_flags"] = "-DINTERCEPT_SYSCALL" }, nil},
		"syscall exported anyway":          {intercept, nil, nil},
		"hash differs":                     {good, func(m map[string]any) { interposer(m)["sha256"] = strings.Repeat("0", 64) }, nil},
		"other interposer path":            {good, func(m map[string]any) { interposer(m)["path"] = "/lib/libfaketime.so.1" }, nil},
		"other schema":                     {good, func(m map[string]any) { m["schema"] = 2 }, nil},
		"short commit":                     {good, func(m map[string]any) { m["source"].(map[string]any)["commit"] = "3ccdd344" }, nil},
		"unknown field":                    {good, func(m map[string]any) { m["extra"] = true }, nil},
		"unknown architecture":             {good, func(m map[string]any) { m["architecture"] = "riscv64" }, nil},
		"machine differs from manifest":    {good, func(m map[string]any) { m["architecture"] = "aarch64" }, nil},
		"not an ELF file":                  {[]byte("not elf"), nil, nil},
		"two JSON values":                  {good, nil, append(clockTestManifest(t, good, nil), []byte(" {}")...)},
	}
	for name, tc := range cases {
		manifest := tc.manifest
		if manifest == nil {
			manifest = clockTestManifest(t, tc.interposer, tc.edit)
		}
		if _, err := admitClockInterposer("helper:v1", manifest, tc.interposer); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "helper:v1") {
			t.Errorf("%s: admitted or unclassified: %v", name, err)
		}
	}
	arm := clockTestELF(elf.EM_AARCH64, map[string]elf.SectionIndex{"clock_gettime": 1})
	if _, err := admitClockInterposer("helper:v1", clockTestManifest(t, arm, func(m map[string]any) { m["architecture"] = "aarch64" }), arm); err != nil {
		t.Fatalf("aarch64 default build refused: %v", err)
	}
}

func TestClockConfigValidation(t *testing.T) {
	resolve := func(opts ...testcontainers.ContainerCustomizer) error {
		_, err := resolveOptions(DefaultImage, opts)
		return err
	}
	three := WithClusterConfig(ClusterConfig{}.WithMonitorCount(3))
	if err := resolve(three, WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("a", "c"))); err != nil {
		t.Fatalf("valid clock config refused: %v", err)
	}
	if err := resolve(WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1"))); err != nil {
		t.Fatalf("a helper without initial MONs is refused: %v", err)
	}
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"no helper":            {three, WithClockConfig(ClockConfig{}.WithMonitors("a"))},
		"not an initial MON":   {three, WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("d"))},
		"MON named twice":      {three, WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("a", "a"))},
		"empty MON list":       {three, WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors())},
		"empty helper":         {WithClockConfig(ClockConfig{}.WithHelperImage(""))},
		"helper set twice":     {WithClockConfig(ClockConfig{}.WithHelperImage("x")), WithClockConfig(ClockConfig{}.WithHelperImage("x"))},
		"monitors set twice":   {three, WithClockConfig(ClockConfig{}.WithHelperImage("x").WithMonitors("a")), WithClockConfig(ClockConfig{}.WithMonitors("b"))},
		"second of one MON":    {WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("b"))},
		"whitespace in helper": {WithClockConfig(ClockConfig{}.WithHelperImage("helper :v1"))},
	} {
		if err := resolve(opts...); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestClockHelperIsAdmittedBeforeAnythingStarts(t *testing.T) {
	restore := clockHelperFiles
	t.Cleanup(func() { clockHelperFiles = restore })
	good := clockTestELF(elf.EM_X86_64, map[string]elf.SectionIndex{"clock_gettime": 1})
	clockHelperFiles = func(context.Context, string) ([]byte, []byte, error) {
		return clockTestManifest(t, good, func(m map[string]any) { m["interposer"].(map[string]any)["intercept_syscall"] = true }), good, nil
	}
	c, err := Run(t.Context(), DefaultImage, WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("a")))
	if c != nil || !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "default build") {
		t.Fatalf("an intercept-syscall helper reached the cluster: %v, %v", c, err)
	}
	settings, err := resolveOptions(DefaultImage, []testcontainers.ContainerCustomizer{WithClockConfig(ClockConfig{}.WithHelperImage("helper:v1").WithMonitors("a"))})
	if err != nil {
		t.Fatal(err)
	}
	clockHelperFiles = func(context.Context, string) ([]byte, []byte, error) {
		return clockTestManifest(t, good, nil), good, nil
	}
	state, err := loadClockInterposer(t.Context(), settings)
	if err != nil || !state.armed["a"] || !bytes.Equal(state.interposer, good) || state.identity.HelperImage != "helper:v1" {
		t.Fatalf("admitted helper: %+v, %v", state.identity, err)
	}
	none, err := loadClockInterposer(t.Context(), options{})
	if err != nil || none.interposer != nil || len(none.armed) != 0 || none.identity != (ClockInterposer{}) {
		t.Fatalf("a cluster without a helper loaded one: %+v, %v", none, err)
	}
}

func TestClockFilesReachOnlyArmedMonitors(t *testing.T) {
	c := &Container{clock: clockState{interposer: []byte("interposer"), armed: map[string]bool{"b": true}}}
	if opts := c.clockFiles("a", false); opts != nil {
		t.Fatalf("an unarmed MON gets clock files: %v", opts)
	}
	req := testcontainers.GenericContainerRequest{}
	for _, opt := range c.clockFiles("b", false) {
		if err := opt.Customize(&req); err != nil {
			t.Fatal(err)
		}
	}
	got := map[string]int64{}
	for _, file := range req.Files {
		got[file.ContainerFilePath] = file.FileMode
	}
	if len(got) != 2 || got[clockInterposerPath] != 0o644 || got[clockOffsetPath] != 0o644 || len(req.Env) != 0 {
		t.Fatalf("armed MON files = %v, env = %v", got, req.Env)
	}
	if err := clockOnlyForMonitors(DaemonOptions{}.WithClockInstrumentation(), "AddManager"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a MGR accepted clock instrumentation: %v", err)
	}
}

// TestMonClockBlockPreloadsOnlyTheDaemon runs both scripts with stubbed
// tools that record LD_PRELOAD and the faketime variables of every call.
func TestMonClockBlockPreloadsOnlyTheDaemon(t *testing.T) {
	const record = "#!/bin/sh\nprintf '%s %s|%s|%s|%s|%s|%s\\n' \"${0##*/}\" \"$1\" \"${LD_PRELOAD-unset}\" \"${FAKETIME_TIMESTAMP_FILE-unset}\" \"${FAKETIME_CACHE_DURATION-unset}\" \"${FAKETIME_DONT_FAKE_MONOTONIC-unset}\" \"${FAKETIME-unset}\" >> \"$TC_CALLS\"\n"
	for _, script := range requireStoreScripts {
		for _, armed := range []bool{false, true} {
			f := newRequireStoreFixture(t, script, nil)
			bin := strings.SplitN(f.env["PATH"], string(os.PathListSeparator), 2)[0]
			for _, tool := range []string{"ceph-mon", "monmaptool", "ceph-authtool", "ceph"} {
				if err := os.WriteFile(filepath.Join(bin, tool), []byte(record), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			clock := filepath.Join(f.root, "tc/clock")
			if armed {
				if err := os.MkdirAll(clock, 0o700); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"libfaketime.so.1", "offset"} {
					if err := os.WriteFile(filepath.Join(clock, name), []byte("+0\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			// A caller's FAKETIME must not reach the daemon either.
			f.env["FAKETIME"] = "+0"
			result := f.run(t, nil)
			if result.exitCode != 0 || len(result.calls) < 2 {
				t.Fatalf("%s armed=%v: exit %d, %q, %s", script, armed, result.exitCode, result.calls, result.output)
			}
			daemon := result.calls[len(result.calls)-1]
			if !strings.HasPrefix(daemon, "ceph-mon -f|") {
				t.Fatalf("%s: last call is %q, not the daemon", script, daemon)
			}
			for _, call := range result.calls[:len(result.calls)-1] {
				if !strings.Contains(call, "|unset|unset|unset|unset|+0") {
					t.Errorf("%s armed=%v: a step before the daemon was instrumented: %q", script, armed, call)
				}
			}
			want := "ceph-mon -f|unset|unset|unset|unset|+0"
			if armed {
				want = "ceph-mon -f|" + clock + "/libfaketime.so.1|" + clock + "/offset|1|1|unset"
			}
			if daemon != want {
				t.Errorf("%s armed=%v: daemon %q, want %q", script, armed, daemon, want)
			}
		}
		// One of the two files alone is a broken arming, not an unarmed MON.
		f := newRequireStoreFixture(t, script, nil)
		if err := os.MkdirAll(filepath.Join(f.root, "tc/clock"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(f.root, "tc/clock/offset"), []byte("+0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if result := f.run(t, nil); result.exitCode != 2 || !strings.Contains(result.output, "tc-ceph: clock") {
			t.Errorf("%s: half-armed MON started: exit %d %s", script, result.exitCode, result.output)
		}
	}
}

// TestAddMonitorArmsOnlyAnOwnedMonitor fails AddMonitor before it owns the
// MON, with a quorum reply it cannot read, and requires the name to stay
// unarmed, so that a later AddMonitor of that name without
// WithClockInstrumentation gets no interposer.
func TestAddMonitorArmsOnlyAnOwnedMonitor(t *testing.T) {
	c, control := newMonitorConfigFixture()
	c.clock = clockState{interposer: []byte("interposer"), armed: map[string]bool{}}
	control.native = []byte("{not a quorum reply")
	mon, err := c.AddMonitor(t.Context(), "d", DaemonOptions{}.WithClockInstrumentation())
	if err == nil || mon != nil || c.members.roster.monitor("d") != nil {
		t.Fatalf("AddMonitor = %v, %v; want a failure before registration", mon, err)
	}
	if c.clockArmed("d") || c.clockFiles("d", false) != nil {
		t.Fatal("a MON that was never owned left its name armed")
	}
}

func TestClockArmingNeedsTheClusterHelper(t *testing.T) {
	c := &Container{clock: clockState{armed: map[string]bool{}}}
	if err := c.checkClockArming(DaemonOptions{}.WithClockInstrumentation()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("arming without a helper: %v", err)
	}
	if err := c.checkClockArming(DaemonOptions{}); err != nil {
		t.Fatalf("an unarmed request was refused: %v", err)
	}
	if len(c.clock.armed) != 0 {
		t.Fatal("the check recorded an arming")
	}
	c.settings.monitorVolume = true
	mon := &MonitorContainer{DaemonName: "b", owner: c}
	if _, err := mon.Replace(t.Context(), DaemonOptions{}.WithClockInstrumentation()); !errors.Is(err, ErrInvalidRequest) || c.clockArmed("b") {
		t.Fatalf("Replace armed without a helper: %v, armed=%v", err, c.clockArmed("b"))
	}
}

// TestJoiningRecipeTakesThisOperationsArming checks that a replacement asked
// to arm gets the files before the name is recorded as armed, and that
// neither an unarmed name nor an unrequested join does.
func TestJoiningRecipeTakesThisOperationsArming(t *testing.T) {
	c := &Container{clock: clockState{interposer: []byte("interposer"), armed: map[string]bool{}}}
	c.nets.public = &testcontainers.DockerNetwork{Name: "tc-public"}
	files := func(join monitorJoin) map[string]bool {
		req := testcontainers.GenericContainerRequest{}
		for _, opt := range c.monitorJoinOptions(join, nil) {
			if err := opt.Customize(&req); err != nil {
				t.Fatal(err)
			}
		}
		got := map[string]bool{}
		for _, file := range req.Files {
			got[file.ContainerFilePath] = true
		}
		return got
	}
	if got := files(monitorJoin{name: "b"}); got[clockInterposerPath] || got[clockOffsetPath] {
		t.Fatalf("an unarmed join got clock files: %v", got)
	}
	if got := files(monitorJoin{name: "b", clock: true}); !got[clockInterposerPath] || !got[clockOffsetPath] || c.clockArmed("b") {
		t.Fatalf("a requested join: files %v, armed %v", got, c.clockArmed("b"))
	}
	c.recordClockArming("b")
	if got := files(monitorJoin{name: "b"}); !got[clockInterposerPath] {
		t.Fatal("an armed name lost its files on a later generation")
	}
}

func TestDaemonOptionsRefuseTwoClockArmings(t *testing.T) {
	armed := DaemonOptions{}.WithClockInstrumentation()
	if _, err := resolveDaemonOptions([]DaemonOptions{armed, armed}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("two values arming: %v", err)
	}
	if resolved, err := resolveDaemonOptions([]DaemonOptions{armed.WithClockInstrumentation()}); err != nil || !resolved.clock {
		t.Fatalf("one value arming twice: %+v, %v", resolved, err)
	}
	if resolved, err := resolveDaemonOptions([]DaemonOptions{armed, DaemonOptions{}.WithImage("other:1")}); err != nil || !resolved.clock || resolved.image != "other:1" {
		t.Fatalf("arming with an image from another value: %+v, %v", resolved, err)
	}
}
