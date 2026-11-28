package ceph

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// The clock helper's files (contract 1 of the clock extension of
// ceph-testcontainers-images) and where an armed MON container holds them.
// mon.sh and mon-join.sh read the two container paths in their mon-clock
// block.
const (
	clockHelperInterposerPath = "/opt/ceph-testcontainers/clock/libfaketime.so.1"
	clockHelperManifestPath   = "/opt/ceph-testcontainers/clock/build.json"
	clockInterposerPath       = "/tc/clock/libfaketime.so.1"
	clockOffsetPath           = "/tc/clock/offset"
	// clockHelperFileLimit bounds each file read from the helper.
	clockHelperFileLimit = 16 << 20
)

// clockField names one setting of ClockConfig.
type clockField uint8

const (
	clockHelperImage clockField = 1 << iota
	clockMonitors
)

// ClockConfig arms MONs for per-daemon wall-clock offsets, which
// fault.ApplyClockOffset applies. It is a plain value with chainable,
// value-returning methods; pass it to Run through WithClockConfig. The zero
// value arms nothing and starts every daemon unchanged.
//
//	clock := ceph.ClockConfig{}.WithHelperImage(fault.ClockHelperImage).WithMonitors("a", "b", "c")
//	c, err := ceph.Run(ctx, ceph.DefaultImage,
//		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3)), ceph.WithClockConfig(clock))
//
// An armed MON container receives the helper's libfaketime interposer and a
// timestamp file at +0 that the cluster owns, both copied in before the
// container starts. Its startup script preloads them into the final exec of
// ceph-mon only, with FAKETIME_DONT_FAKE_MONOTONIC=1 and a cache duration of
// one second: --mkfs, the key and map tools before it and every command
// executed in the container later run without the interposer, and
// CLOCK_MONOTONIC is not faked. An interposer cannot be attached to a running
// process, so a MON must be armed when its container is created. The
// interposer covers the public clock_gettime and related libc calls only; a
// clock read through the libc syscall() function, the vDSO symbol or a raw
// system call bypasses it, and the host clock is never changed.
type ClockConfig struct {
	helperImage string
	monitors    []string
	set         clockField
}

// WithHelperImage selects the clock helper: fault.ClockHelperImage, or another
// image that meets contract 1 of the clock extension. Run pulls it, copies the
// interposer and its build manifest out of it without starting it, and
// refuses it before any daemon starts unless the manifest describes the
// libfaketime default build (no INTERCEPT_SYSCALL, no extra compile flags),
// its hash matches the interposer, and the interposer is a 64-bit
// little-endian shared object for the manifest's architecture that does not
// export syscall. The intercept-syscall build hangs every process that loads
// tcmalloc, which every Ceph daemon does. ClusterFacts.ClockInterposer records
// the accepted identity. The helper is never selected implicitly.
//
//purpose:construction
func (c ClockConfig) WithHelperImage(image string) ClockConfig {
	c.helperImage = image
	c.set |= clockHelperImage
	return c
}

// WithMonitors arms the initial MONs of these names (a, b, c, ... up to
// ClusterConfig.WithMonitorCount). It needs WithHelperImage. A MON that
// AddMonitor adds later is armed with DaemonOptions.WithClockInstrumentation.
// An armed MON stays armed for its lifetime: Stop and Start keep its container
// and its timestamp file, and MonitorContainer.Replace arms the replacement.
//
//purpose:construction
func (c ClockConfig) WithMonitors(names ...string) ClockConfig {
	c.monitors = slices.Clone(names)
	c.set |= clockMonitors
	return c
}

// WithClockConfig carries a ClockConfig to Run. Run refuses a field that two
// WithClockConfig options both set. Like every Option it changes the cluster
// settings only and never reaches a container request.
//
//purpose:construction
func WithClockConfig(clock ClockConfig) Option {
	return func(o *options) error { return clock.apply(o) }
}

// apply validates every field c sets and then merges them into the cluster
// settings, so that a refused value writes nothing.
func (c ClockConfig) apply(o *options) error {
	if err := c.validate(o.clock); err != nil {
		return err
	}
	if c.set&clockHelperImage != 0 {
		o.clock.helperImage = c.helperImage
	}
	if c.set&clockMonitors != 0 {
		o.clock.monitors = slices.Clone(c.monitors)
	}
	o.clock.set |= c.set
	return nil
}

// validate checks the fields c sets on their own and against held, the
// ClockConfig fields already set. It changes nothing.
func (c ClockConfig) validate(held ClockConfig) error {
	if clash := c.set & held.set; clash != 0 {
		name := "helper image"
		if clash&clockMonitors != 0 {
			name = "monitors"
		}
		return fmt.Errorf("clock %s set by more than one WithClockConfig", name)
	}
	if c.set&clockHelperImage != 0 {
		if err := validateImageReference(c.helperImage); err != nil {
			return fmt.Errorf("clock helper image: %w", err)
		}
	}
	if c.set&clockMonitors != 0 {
		if len(c.monitors) == 0 {
			return errors.New("ClockConfig.WithMonitors needs at least one MON name")
		}
		for i, name := range c.monitors {
			if slices.Contains(c.monitors[:i], name) {
				return fmt.Errorf("ClockConfig.WithMonitors names MON %q twice", name)
			}
		}
	}
	return nil
}

// validateClock checks the clock settings against the assembled settings,
// before anything is created.
func validateClock(settings options) error {
	clock := settings.clock
	if clock.set&clockMonitors != 0 && clock.set&clockHelperImage == 0 {
		return errors.New("ClockConfig.WithMonitors needs ClockConfig.WithHelperImage: the clock helper is never selected implicitly")
	}
	for _, name := range clock.monitors {
		if !slices.Contains(initialMonitorNames(settings.monitors), name) {
			return fmt.Errorf("ClockConfig.WithMonitors names MON %q, which is not one of the %d initial MONs %v", name, settings.monitors, initialMonitorNames(settings.monitors))
		}
	}
	return nil
}

// initialMonitorNames are the names Run gives its initial MONs.
func initialMonitorNames(count int) []string {
	names := make([]string, 0, count)
	for i := range count {
		names = append(names, daemonName(i))
	}
	return names
}

// ClockInterposer is the identity of the clock interposer that a cluster
// copies into its armed MONs, read from the helper's build manifest and the
// interposer itself. The zero value means the cluster has no clock helper.
type ClockInterposer struct {
	// HelperImage is the reference given to ClockConfig.WithHelperImage.
	HelperImage string
	// SourceRepository, SourceTag, SourceCommit and SourceTree identify the
	// libfaketime source the helper built.
	SourceRepository, SourceTag, SourceCommit, SourceTree string
	// Compiler and Libc are the helper's build toolchain.
	Compiler, Libc string
	// Architecture is the machine the interposer is built for (x86_64 or
	// aarch64).
	Architecture string
	// SHA256 is the hash of the interposer, checked against the manifest.
	SHA256 string
	// Build is the libfaketime build, always "default".
	Build string
}

// clockManifest is build.json of contract 1 of the clock helper.
type clockManifest struct {
	Schema int `json:"schema"`
	Source struct {
		Repository string `json:"repository"`
		Tag        string `json:"tag"`
		Commit     string `json:"commit"`
		Tree       string `json:"tree"`
	} `json:"source"`
	Toolchain struct {
		Compiler string `json:"compiler"`
		Libc     string `json:"libc"`
	} `json:"toolchain"`
	Architecture string `json:"architecture"`
	Interposer   struct {
		Path             string  `json:"path"`
		SHA256           string  `json:"sha256"`
		Build            string  `json:"build"`
		InterceptSyscall *bool   `json:"intercept_syscall"`
		CompileFlags     *string `json:"compile_flags"`
	} `json:"interposer"`
}

var (
	clockGitObject = regexp.MustCompile(`^[0-9a-f]{40}$`)
	clockSHA256    = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// clockMachines maps the manifest architecture to the ELF machine of the
// interposer.
var clockMachines = map[string]elf.Machine{"x86_64": elf.EM_X86_64, "aarch64": elf.EM_AARCH64}

// admitClockInterposer validates the helper's manifest and interposer and
// returns their identity. Any refusal names the helper image.
func admitClockInterposer(image string, manifestData, interposer []byte) (ClockInterposer, error) {
	refuse := func(format string, args ...any) (ClockInterposer, error) {
		return ClockInterposer{}, invalidRequest("clock helper %s: "+format, append([]any{image}, args...)...)
	}
	var m clockManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return refuse("build manifest is not one contract-1 JSON object: %v", err)
	}
	if decoder.More() {
		return refuse("build manifest holds more than one JSON value")
	}
	switch {
	case m.Schema != 1:
		return refuse("build manifest schema %d is not 1", m.Schema)
	case m.Source.Repository == "" || m.Source.Tag == "":
		return refuse("build manifest names no source repository or tag")
	case !clockGitObject.MatchString(m.Source.Commit) || !clockGitObject.MatchString(m.Source.Tree):
		return refuse("build manifest source commit and tree must be 40 lowercase hexadecimal digits")
	case m.Interposer.Path != clockHelperInterposerPath:
		return refuse("build manifest names interposer %q, not %s", m.Interposer.Path, clockHelperInterposerPath)
	case m.Interposer.InterceptSyscall == nil || *m.Interposer.InterceptSyscall || m.Interposer.Build != "default":
		return refuse("the interposer is not the libfaketime default build: the intercept-syscall build hangs every process that loads tcmalloc, as Ceph daemons do")
	case m.Interposer.CompileFlags == nil || *m.Interposer.CompileFlags != "":
		return refuse("the interposer was built with extra compile flags; only the upstream default flags are accepted")
	case !clockSHA256.MatchString(m.Interposer.SHA256):
		return refuse("build manifest interposer hash is not a lowercase SHA-256")
	}
	machine, ok := clockMachines[m.Architecture]
	if !ok {
		return refuse("build manifest architecture %q is not x86_64 or aarch64", m.Architecture)
	}
	sum := sha256.Sum256(interposer)
	if hex.EncodeToString(sum[:]) != m.Interposer.SHA256 {
		return refuse("interposer hash differs from its build manifest")
	}
	if err := checkClockInterposerELF(interposer, machine); err != nil {
		return refuse("interposer: %v", err)
	}
	return ClockInterposer{
		HelperImage:      image,
		SourceRepository: m.Source.Repository, SourceTag: m.Source.Tag, SourceCommit: m.Source.Commit, SourceTree: m.Source.Tree,
		Compiler: m.Toolchain.Compiler, Libc: m.Toolchain.Libc,
		Architecture: m.Architecture, SHA256: m.Interposer.SHA256, Build: m.Interposer.Build,
	}, nil
}

// checkClockInterposerELF requires a little-endian ELF64 shared object for
// machine that defines no syscall symbol: the intercept-syscall build exports
// one, whatever its manifest says.
func checkClockInterposerELF(data []byte, machine elf.Machine) error {
	file, err := elf.NewFile(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("not an ELF file: %w", err)
	}
	defer func() { _ = file.Close() }()
	switch {
	case file.Class != elf.ELFCLASS64 || file.Data != elf.ELFDATA2LSB:
		return errors.New("not a little-endian ELF64 file")
	case file.Type != elf.ET_DYN:
		return errors.New("not a shared object")
	case file.Machine != machine:
		return fmt.Errorf("built for %s, not %s", file.Machine, machine)
	}
	symbols, err := file.DynamicSymbols()
	if err != nil {
		return fmt.Errorf("read dynamic symbols: %w", err)
	}
	for _, symbol := range symbols {
		if symbol.Name == "syscall" && symbol.Section != elf.SHN_UNDEF {
			return errors.New("exports syscall, as the intercept-syscall build does")
		}
	}
	return nil
}

// clockHelperRequest creates the helper container of the cluster with owner
// ID owner. The helper has no executable; its container is created only so
// that its files can be copied, and never starts.
func clockHelperRequest(owner string) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/clock-helper-is-not-started"), testcontainers.WithCmd(),
		testcontainers.WithNoStart(), testcontainers.WithLabels(map[string]string{clusterOwnerLabel: owner}),
	}
}

// clockHelperFiles copies the interposer and the build manifest out of a
// created, never started, container of the helper image, labelled for the
// cluster with owner ID owner, and removes it.
//
//testdouble:dependency-port
var clockHelperFiles = func(ctx context.Context, image, owner string) (manifest, interposer []byte, err error) {
	ctr, err := runContainer(ctx, image, clockHelperRequest(owner)...)
	if ctr != nil {
		defer func() {
			if terminateErr := ctr.Terminate(context.WithoutCancel(ctx)); terminateErr != nil {
				err = errors.Join(err, fmt.Errorf("remove clock helper container: %w", terminateErr))
			}
		}()
	}
	if err != nil {
		return nil, nil, classifyDocker(ctx, fmt.Errorf("create clock helper container from %s: %w", image, err))
	}
	read := func(path string) ([]byte, error) {
		r, err := ctr.CopyFileFromContainer(ctx, path)
		if err != nil {
			return nil, classify(ErrImageCapability, fmt.Errorf("clock helper %s has no %s: %w", image, path, err))
		}
		defer func() { _ = r.Close() }()
		data, err := io.ReadAll(io.LimitReader(r, clockHelperFileLimit+1))
		if err != nil {
			return nil, classifyDocker(ctx, fmt.Errorf("read %s from clock helper %s: %w", path, image, err))
		}
		if len(data) > clockHelperFileLimit {
			return nil, invalidRequest("clock helper %s: %s is larger than %d bytes", image, path, clockHelperFileLimit)
		}
		return data, nil
	}
	if manifest, err = read(clockHelperManifestPath); err != nil {
		return nil, nil, err
	}
	if interposer, err = read(clockHelperInterposerPath); err != nil {
		return nil, nil, err
	}
	return manifest, interposer, nil
}

// clockState is the clock instrumentation of a cluster: the admitted
// interposer, recorded by Run before any daemon starts, and the names of the
// armed MONs, guarded by c.mu.
type clockState struct {
	identity   ClockInterposer
	interposer []byte
	armed      map[string]bool
}

// loadClockInterposer admits the helper of settings, when it names one,
// before Run creates anything else; owner is the ID that labels the
// cluster's containers.
func loadClockInterposer(ctx context.Context, settings options, owner string) (clockState, error) {
	state := clockState{armed: map[string]bool{}}
	if settings.clock.set&clockHelperImage == 0 {
		return state, nil
	}
	manifest, interposer, err := clockHelperFiles(ctx, settings.clock.helperImage, owner)
	if err != nil {
		return clockState{}, err
	}
	state.identity, err = admitClockInterposer(settings.clock.helperImage, manifest, interposer)
	if err != nil {
		return clockState{}, err
	}
	state.interposer = interposer
	for _, name := range settings.clock.monitors {
		state.armed[name] = true
	}
	return state, nil
}

// clockArmed reports whether MON name is armed. The caller holds c.mu or
// runs before the cluster is shared.
func (c *Container) clockArmed(name string) bool { return c.clock.armed[name] }

// checkClockArming refuses WithClockInstrumentation on a cluster without an
// admitted interposer. It changes nothing, so it runs before any work.
func (c *Container) checkClockArming(daemon DaemonOptions) error {
	if daemon.clock && c.clock.interposer == nil {
		return invalidRequest("DaemonOptions.WithClockInstrumentation needs ClockConfig.WithHelperImage on the cluster")
	}
	return nil
}

// recordClockArming records that MON name is armed once the cluster owns a
// MON, or a new container of one, that was created armed. An operation that
// fails before that leaves the name as it was. The caller holds c.mu and has
// passed checkClockArming.
func (c *Container) recordClockArming(name string) { c.clock.armed[name] = true }

// disarmClock forgets MON name after its removal, so that a later MON of the
// same name is armed only when asked. The caller holds c.mu.
func (c *Container) disarmClock(name string) { delete(c.clock.armed, name) }

// clockFiles copies the interposer and the owned timestamp file at +0 into
// the container of MON name when the name is armed or this operation asks
// for it (requested).
func (c *Container) clockFiles(name string, requested bool) []testcontainers.ContainerCustomizer {
	if !requested && !c.clockArmed(name) {
		return nil
	}
	return []testcontainers.ContainerCustomizer{testcontainers.WithFiles(
		textFile(clockInterposerPath, c.clock.interposer, 0o644),
		textFile(clockOffsetPath, []byte("+0\n"), 0o644))}
}

// clockTarget resolves container id to an owned MON. The caller holds c.mu.
func (c *Container) clockTarget(id string) (daemon string, armed, owned bool) {
	for _, mon := range c.monitorTopology().monitorsLocked() {
		if mon.Container != nil && mon.GetContainerID() == id {
			return "mon." + mon.DaemonName, c.clockArmed(mon.DaemonName), true
		}
	}
	return "", false, false
}

// clockDaemonOption returns the DaemonOptions of initial MON name.
func (o options) clockDaemonOption(name string) []DaemonOptions {
	if slices.Contains(o.clock.monitors, name) {
		return []DaemonOptions{DaemonOptions{}.WithClockInstrumentation()}
	}
	return nil
}

// WithClockInstrumentation arms the one new MON for clock offsets, as
// ClockConfig.WithMonitors does for the initial MONs; see ClockConfig. The
// cluster needs ClockConfig.WithHelperImage. Only AddMonitor and
// MonitorContainer.Replace accept it: an interposer reaches only MON daemons,
// the one kind with native evidence for it. A refusal matches
// ErrInvalidRequest and happens before anything is created.
//
//purpose:construction
func (o DaemonOptions) WithClockInstrumentation() DaemonOptions {
	o.clock = true
	return o
}

// clockOnlyForMonitors refuses clock instrumentation of another daemon kind.
func clockOnlyForMonitors(daemon DaemonOptions, operation string) error {
	if daemon.clock {
		return invalidRequest("%s: DaemonOptions.WithClockInstrumentation arms MONs only", operation)
	}
	return nil
}
