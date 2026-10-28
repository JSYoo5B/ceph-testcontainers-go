package ceph

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	dockercontainer "github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// crashEntry is one entry of a fake crash directory archive.
type crashEntry struct {
	name, body, link string
	kind             byte
	// size, when set, is the header's size, to model a large file without
	// its bytes.
	size int64
}

// crashArchive builds the tar stream that Docker's archive API returns for
// /var/lib/ceph/crash: entries named below "crash".
func crashArchive(t *testing.T, entries ...crashEntry) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, entry := range entries {
		header := &tar.Header{Name: entry.name, Mode: 0o644, Typeflag: entry.kind, Linkname: entry.link, ModTime: time.Unix(1, 0)}
		switch entry.kind {
		case 0:
			header.Typeflag = tar.TypeReg
			header.Size = int64(len(entry.body))
		case tar.TypeDir:
			header.Mode = 0o755
		}
		if entry.size > 0 {
			header.Size = entry.size
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		body := []byte(entry.body)
		if entry.size > 0 {
			body = bytes.Repeat([]byte("x"), int(entry.size))
		}
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// fakeCrashContainer is one container as Docker sees it: whether it exists,
// its state, and its crash directory, or the error that reading fails with.
type fakeCrashContainer struct {
	removed bool
	// removedAfterInspect removes the container between the inspection and
	// the archive request.
	removedAfterInspect bool
	inspectErr          error
	// inspectErrAfter fails every inspection after that many.
	inspectErrAfter int
	inspections     int
	noCrashDir      bool
	archive         []byte
	copyErr         error
	state           dockercontainer.State
	copies          int
}

type fakeCrashDocker struct {
	containers map[string]*fakeCrashContainer
}

func (d *fakeCrashDocker) ContainerInspect(ctx context.Context, id string, _ mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.ContainerInspectResult{}, err
	}
	ctr := d.containers[id]
	switch {
	case ctr == nil || ctr.removed:
		return mobycl.ContainerInspectResult{}, fmt.Errorf("no such container %s: %w", id, errdefs.ErrNotFound)
	case ctr.inspectErr != nil:
		return mobycl.ContainerInspectResult{}, ctr.inspectErr
	case ctr.inspectErrAfter > 0 && ctr.inspections >= ctr.inspectErrAfter:
		return mobycl.ContainerInspectResult{}, errors.New("engine stopped answering")
	}
	ctr.inspections++
	state := ctr.state
	return mobycl.ContainerInspectResult{Container: dockercontainer.InspectResponse{ID: id, Image: "sha256:image-" + id, State: &state}}, nil
}

func (d *fakeCrashDocker) CopyFromContainer(ctx context.Context, id string, options mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.CopyFromContainerResult{}, err
	}
	ctr := d.containers[id]
	if ctr != nil && ctr.removedAfterInspect {
		ctr.removed = true
	}
	switch {
	case options.SourcePath != CrashDir:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("unexpected path %s", options.SourcePath)
	case ctr == nil || ctr.removed || ctr.noCrashDir:
		return mobycl.CopyFromContainerResult{}, fmt.Errorf("could not find the file %s in container %s: %w", options.SourcePath, id, errdefs.ErrNotFound)
	case ctr.copyErr != nil:
		return mobycl.CopyFromContainerResult{}, ctr.copyErr
	}
	ctr.copies++
	return mobycl.CopyFromContainerResult{Content: io.NopCloser(bytes.NewReader(ctr.archive))}, nil
}

type crashTarget struct {
	testcontainers.Container
	id string
}

func (c crashTarget) GetContainerID() string { return c.id }

func crashSnapshot(targets ...DiagnosticsContainer) diagnosticsSnapshot {
	return diagnosticsSnapshot{containers: targets, secrets: []string{"AQBadminsecretkey=="}}
}

func crashContainer(role, name, id string) DiagnosticsContainer {
	return DiagnosticsContainer{Role: role, Name: name, Container: crashTarget{id: id}}
}

const crashMeta = `{
  "crash_id": "2026-10-12T10:06:08.307241Z_f9710ec6",
  "timestamp": "2026-10-12T10:06:08.307241Z",
  "process_name": "ceph-mgr",
  "entity_name": "mgr.a",
  "ceph_version": "20.2.4",
  "utsname_hostname": "f419bd0a74bd",
  "assert_line": 42,
  "assert_msg": "key AQBadminsecretkey== leaked into an assertion",
  "backtrace": ["__kernel_rt_sigreturn()", "main()"],
  "mempool": {"bytes": 1},
  "unknown_field": "value"
}`

func collectCrashes(t *testing.T, ctx context.Context, config CrashMetadataConfig, docker *fakeCrashDocker, snapshot diagnosticsSnapshot) (*CrashMetadataReport, error) {
	t.Helper()
	config, err := normalizeCrashMetadataConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	return collectCrashMetadata(ctx, config, snapshot, docker, &CrashMetadataReport{SchemaVersion: 1, Containers: []CrashMetadataContainer{}})
}

// Only regular meta files of crash report directories are read; their known
// fields are kept and redacted, and crash logs, dumps, links, escaping paths
// and non-objects stay out of the report.
func TestCrashMetadataSelectsDaemonRecords(t *testing.T) {
	archive := crashArchive(t,
		crashEntry{name: "crash/", kind: tar.TypeDir},
		crashEntry{name: "crash/posted/", kind: tar.TypeDir},
		crashEntry{name: "crash/2026-10-12_a/", kind: tar.TypeDir},
		crashEntry{name: "crash/2026-10-12_a/meta", body: crashMeta},
		crashEntry{name: "crash/2026-10-12_a/log", body: "log line with keyring AQBlogsecret== and dump"},
		crashEntry{name: "crash/2026-10-12_a/done"},
		crashEntry{name: "crash/2026-10-12_a/core.1234", body: "process memory"},
		crashEntry{name: "crash/posted/2026-10-11_b/meta", body: `{"crash_id":"2026-10-11_b","entity_name":"mgr.a"}`},
		crashEntry{name: "crash/2026-10-12_c/meta", kind: tar.TypeSymlink, link: "/etc/ceph/ceph.client.admin.keyring"},
		crashEntry{name: "crash/2026-10-12_d/meta", body: `["not", "an", "object"]`},
		crashEntry{name: "crash/2026-10-12_e/meta", body: `{"a":1} {"b":2}`},
		crashEntry{name: "crash/2026-10-12_f/meta", size: 2048},
		crashEntry{name: "crash/../etc/shadow", body: "root:x"},
		crashEntry{name: "/etc/passwd", body: "root:x"},
		crashEntry{name: "crash/2026-10-12_x/../2026-10-12_y/meta", body: crashMeta},
		crashEntry{name: "crash/2026-10-12_h/meta", body: `{"entity_name":"mgr.a"}]`},
		crashEntry{name: "crash/2026-10-12_i/meta", body: `{"entity_name":"mgr.a"}}garbage`},
		crashEntry{name: "crash/2026-10-12_j/meta", kind: tar.TypeDir},
		crashEntry{name: "crash/2026-10-12_g/sub/meta", body: crashMeta},
	)
	docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{"mgr-a": {archive: archive,
		state: dockercontainer.State{Status: "exited", ExitCode: 139, StartedAt: "start", FinishedAt: "finish"}}}}
	report, err := collectCrashes(t, t.Context(), CrashMetadataConfig{MaxRecordBytes: 1024}, docker, crashSnapshot(crashContainer("mgr", "a", "mgr-a")))
	if err != nil {
		t.Fatal(err)
	}
	if report.Complete || len(report.Containers) != 1 {
		t.Fatalf("report %+v", report)
	}
	result := report.Containers[0]
	if result.Outcome != CrashRecordsFound || result.State != "exited" || result.ExitCode != 139 || result.ImageID != "sha256:image-mgr-a" || result.CrashDir != CrashDir {
		t.Fatalf("container result %+v", result)
	}
	if len(result.Records) != 2 {
		t.Fatalf("records %+v", result.Records)
	}
	record, posted := result.Records[0], result.Records[1]
	if record.CrashID != "2026-10-12_a" || !record.Done || record.Posted || record.Entity != "mgr.a" || record.Process != "ceph-mgr" ||
		record.CephVersion != "20.2.4" || record.Timestamp != "2026-10-12T10:06:08.307241Z" || record.Path != "2026-10-12_a/meta" {
		t.Fatalf("record %+v", record)
	}
	if record.Fields["assert_msg"] != "key [REDACTED] leaked into an assertion" || !slices.Equal(record.Fields["backtrace"].([]string), []string{"__kernel_rt_sigreturn()", "main()"}) ||
		record.Fields["assert_line"] != json.Number("42") || !slices.Equal(record.OmittedFields, []string{"mempool", "unknown_field"}) {
		t.Fatalf("record fields %+v, omitted %v", record.Fields, record.OmittedFields)
	}
	if posted.CrashID != "2026-10-11_b" || !posted.Posted || posted.Done {
		t.Fatalf("posted record %+v", posted)
	}
	reasons := map[string]string{}
	for _, omission := range result.Omitted {
		reasons[omission.Path] = omission.Reason
	}
	for path, reason := range map[string]string{
		"2026-10-12_c/meta": "not a regular file", "2026-10-12_d/meta": "not a JSON object", "2026-10-12_e/meta": "not a JSON object",
		"2026-10-12_f/meta": "larger than 1024 bytes", "crash/../etc/shadow": "unexpected path", "/etc/passwd": "unexpected path",
		"crash/2026-10-12_x/../2026-10-12_y/meta": "unexpected path",
		"2026-10-12_h/meta":                       "not a JSON object", "2026-10-12_i/meta": "not a JSON object", "2026-10-12_j/meta": "not a regular file",
	} {
		if reasons[path] != reason {
			t.Fatalf("omission of %s = %q, want %q; all %v", path, reasons[path], reason, reasons)
		}
	}
	// The log, the dump and the nested meta are skipped, not read.
	if len(result.Omitted) != 10 || result.IgnoredEntries != 3 {
		t.Fatalf("omitted %v, ignored %d", result.Omitted, result.IgnoredEntries)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, leaked := range []string{"AQBadminsecretkey", "AQBlogsecret", "log line", "process memory", "root:x", "admin.keyring", `"bytes"`} {
		if strings.Contains(string(data), leaked) {
			t.Fatalf("the report contains %q: %s", leaked, data)
		}
	}
}

// Each container's outcome is its own: records, an empty or missing
// directory and a removed container are told apart, a failure or an
// incomplete archive is an error, and the other containers' records stay.
func TestCrashMetadataTellsOutcomesApart(t *testing.T) {
	meta := func(id string) crashEntry {
		return crashEntry{name: "crash/" + id + "/meta", body: `{"crash_id":"` + id + `"}`}
	}
	records := crashArchive(t, crashEntry{name: "crash/", kind: tar.TypeDir}, meta("one"))
	empty := crashArchive(t, crashEntry{name: "crash/", kind: tar.TypeDir}, crashEntry{name: "crash/posted/", kind: tar.TypeDir})
	two := crashArchive(t, meta("first"), crashEntry{name: "crash/second/log", body: strings.Repeat("l", 4096)}, meta("third"))
	many := crashArchive(t, meta("1"), meta("2"), meta("3"), meta("4"), meta("5"))
	docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
		"records":         {archive: records},
		"empty":           {archive: empty},
		"absent":          {noCrashDir: true},
		"removed":         {removed: true},
		"removed-midway":  {archive: records, removedAfterInspect: true},
		"denied":          {copyErr: errors.New("permission denied")},
		"engine":          {inspectErr: errors.New("engine down")},
		"truncated":       {archive: two[:len(two)/2+700]},
		"oversized":       {archive: two},
		"too-many":        {archive: many},
		"rejected":        {archive: crashArchive(t, crashEntry{name: "crash/x/meta", body: "not json"})},
		"records-limited": {archive: many},
		"control":         {archive: records},
	}}
	snapshot := crashSnapshot(
		crashContainer("mon", "a", "records"), crashContainer("mgr", "a", "empty"), crashContainer("osd", "0", "absent"),
		crashContainer("osd", "1", "removed"), crashContainer("osd", "2", "removed-midway"), crashContainer("osd", "3", "denied"),
		crashContainer("osd", "4", "engine"), crashContainer("osd", "5", "truncated"), crashContainer("osd", "6", "oversized"),
		crashContainer("osd", "7", "too-many"), crashContainer("osd", "8", "rejected"),
		DiagnosticsContainer{Role: "osd", Name: "9"}, crashContainer("control", "cli", "control"),
	)
	snapshot.controlPlane = crashTarget{id: "control"}
	config := CrashMetadataConfig{MaxArchiveBytes: int64(len(two) - 1024), MaxEntries: 4}
	report, err := collectCrashes(t, t.Context(), config, docker, snapshot)
	want := map[string]CrashOutcome{
		"mon a": CrashRecordsFound, "mgr a": CrashDirectoryEmpty, "osd 0": CrashDirectoryAbsent, "osd 1": CrashContainerUnavailable,
		"osd 2": CrashContainerUnavailable, "osd 3": CrashReadFailed, "osd 4": CrashReadFailed, "osd 5": CrashArchiveIncomplete,
		"osd 6": CrashArchiveIncomplete, "osd 7": CrashArchiveIncomplete, "osd 8": CrashEntriesRejected, "osd 9": CrashContainerUnavailable,
	}
	got := map[string]CrashMetadataContainer{}
	for _, result := range report.Containers {
		got[result.Role+" "+result.Name] = result
	}
	if len(got) != len(want) || report.Complete {
		t.Fatalf("report %+v", report)
	}
	for name, outcome := range want {
		if got[name].Outcome != outcome {
			t.Fatalf("%s: outcome %s, want %s; %+v", name, got[name].Outcome, outcome, got[name])
		}
	}
	if len(got["mon a"].Records) != 1 || got["osd 3"].Error == "" || !strings.Contains(got["osd 7"].Error, "more than 4 entries") ||
		!strings.Contains(got["osd 6"].Error, "exceeds") || len(got["osd 5"].Records) != 1 || len(got["osd 7"].Records) != 4 {
		t.Fatalf("partial results lost: %+v", got)
	}
	if docker.containers["control"].copies != 0 {
		t.Fatal("the control container was read")
	}
	for _, name := range []string{"osd 3", "osd 4", "osd 5", "osd 6", "osd 7"} {
		if err == nil || !strings.Contains(err.Error(), name+":") {
			t.Fatalf("the error does not report %s: %v", name, err)
		}
	}
	for _, name := range []string{"mon a", "mgr a", "osd 0", "osd 1", "osd 8"} {
		if strings.Contains(err.Error(), name+":") {
			t.Fatalf("the error reports %s: %v", name, err)
		}
	}

	config = CrashMetadataConfig{MaxRecords: 3}
	report, err = collectCrashes(t, t.Context(), config, docker, crashSnapshot(crashContainer("osd", "0", "records-limited")))
	if err != nil || report.Complete || len(report.Containers[0].Records) != 3 || len(report.Containers[0].Omitted) != 2 {
		t.Fatalf("record limit: %+v, %v", report.Containers[0], err)
	}
}

// Records, empty directories and absent directories make a complete report.
func TestCrashMetadataCompleteReport(t *testing.T) {
	docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
		"mon": {archive: crashArchive(t, crashEntry{name: "crash/r/meta", body: `{"crash_id":"r"}`}, crashEntry{name: "crash/r/done"})},
		"mgr": {archive: crashArchive(t, crashEntry{name: "crash/", kind: tar.TypeDir})},
		"osd": {noCrashDir: true},
	}}
	report, err := collectCrashes(t, t.Context(), CrashMetadataConfig{}, docker,
		crashSnapshot(crashContainer("mon", "a", "mon"), crashContainer("mgr", "a", "mgr"), crashContainer("osd", "0", "osd")))
	if err != nil || !report.Complete || !report.Containers[0].Records[0].Done {
		t.Fatalf("report %+v, %v", report, err)
	}
}

// A canceled context fails every read without losing the report, and a
// fresh context reads again.
func TestCrashMetadataCanceledContext(t *testing.T) {
	docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
		"mgr": {archive: crashArchive(t, crashEntry{name: "crash/r/meta", body: `{"crash_id":"r"}`})},
	}}
	snapshot := crashSnapshot(crashContainer("mgr", "a", "mgr"))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := collectCrashes(t, ctx, CrashMetadataConfig{}, docker, snapshot)
	if !errors.Is(err, context.Canceled) && (err == nil || !strings.Contains(err.Error(), "context canceled")) {
		t.Fatalf("canceled collection returned %v", err)
	}
	if report == nil || report.Complete || report.Containers[0].Outcome != CrashReadFailed || docker.containers["mgr"].copies != 0 {
		t.Fatalf("canceled report %+v", report)
	}
	report, err = collectCrashes(t, t.Context(), CrashMetadataConfig{}, docker, snapshot)
	if err != nil || len(report.Containers[0].Records) != 1 {
		t.Fatalf("fresh collection: %+v, %v", report, err)
	}
}

func TestCrashMetadataRefusesInvalidLimits(t *testing.T) {
	for name, config := range map[string]CrashMetadataConfig{
		"negative timeout":  {Timeout: -1},
		"long operation":    {OperationTimeout: 2 * time.Minute},
		"huge archive":      {MaxArchiveBytes: 1 << 30},
		"negative entries":  {MaxEntries: -1},
		"huge record":       {MaxRecordBytes: 2 << 20},
		"too many records":  {MaxRecords: 2048},
		"bad container":     {AdditionalContainers: []DiagnosticsContainer{{Role: "x\ny"}}},
		"long redact value": {RedactValues: []string{strings.Repeat("s", 4097)}},
	} {
		if _, err := (&Container{}).CollectCrashMetadata(t.Context(), config); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

// The redactor replaces values in one pass, so that a value that occurs in
// the replacement text, given many times, does not rewrite its own output.
func TestDiagnosticsRedactorDoesNotRewriteItsOutput(t *testing.T) {
	redact := diagnosticsRedactor(slices.Repeat([]string{"E"}, 32))
	if got := redact("E and E"); got != "[REDACTED] and [REDACTED]" {
		t.Fatalf("redacted %q", got)
	}
	redact = diagnosticsRedactor([]string{"secret", "secret-longer", ""})
	if got := redact("secret-longer secret"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redacted %q", got)
	}
}

// A damaged or unexpected archive, a failed re-inspection and caller-given
// names are handled without a false complete result or a leaked secret.
func TestCrashMetadataRejectsDamagedArchives(t *testing.T) {
	record := crashArchive(t, crashEntry{name: "crash/", kind: tar.TypeDir}, crashEntry{name: "crash/r/meta", body: `{"crash_id":"r"}`}, crashEntry{name: "crash/r/done"})
	// Cut right after the last entry, without the two zero blocks.
	boundary := record[:len(record)-1024]
	// A record and its done marker, then a broken header.
	broken := append(slices.Clone(boundary), bytes.Repeat([]byte{0xff}, 512)...)
	// An extended header with its payload, which the tar reader consumes,
	// and then the end of the stream.
	var extended bytes.Buffer
	writer := tar.NewWriter(&extended)
	if err := writer.WriteHeader(&tar.Header{Name: "crash/p/meta", Typeflag: tar.TypeReg, PAXRecords: map[string]string{"comment": "x"}}); err != nil {
		t.Fatal(err)
	}
	pax := append(slices.Clone(boundary), extended.Bytes()[:1024]...)
	// A GNU long name header whose payload ends with zero blocks, and then
	// the end of the stream.
	longName := make([]byte, 512)
	copy(longName, "././@LongLink")
	copy(longName[124:], fmt.Sprintf("%011o", 1536))
	longName[156] = tar.TypeGNULongName
	copy(longName[257:], "ustar  \x00")
	copy(longName[148:156], "        ")
	sum := 0
	for _, b := range longName {
		sum += int(b)
	}
	copy(longName[148:], fmt.Sprintf("%06o\x00 ", sum))
	payload := make([]byte, 1536)
	copy(payload, "crash/p/meta\x00")
	gnu := append(append(slices.Clone(boundary), longName...), payload...)
	docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
		"boundary":  {archive: boundary},
		"stream":    {archive: nil},
		"broken":    {archive: broken},
		"pax":       {archive: pax},
		"gnu":       {archive: gnu},
		"link-root": {archive: crashArchive(t, crashEntry{name: "crash", kind: tar.TypeSymlink, link: "/etc"})},
		"vanishing": {noCrashDir: true, inspectErrAfter: 1},
		"secret":    {copyErr: errors.New("permission denied")},
	}}
	snapshot := crashSnapshot(crashContainer("osd", "boundary", "boundary"), crashContainer("osd", "stream", "stream"),
		crashContainer("osd", "broken", "broken"), crashContainer("osd", "pax", "pax"), crashContainer("osd", "gnu", "gnu"), crashContainer("osd", "link", "link-root"), crashContainer("osd", "vanishing", "vanishing"),
		crashContainer("client", "app-hunter2", "secret"))
	// CollectCrashMetadata adds RedactValues to the snapshot's secrets.
	snapshot.secrets = append(snapshot.secrets, "hunter2")
	report, err := collectCrashes(t, t.Context(), CrashMetadataConfig{}, docker, snapshot)
	got := map[string]CrashMetadataContainer{}
	for _, result := range report.Containers {
		got[result.Name] = result
	}
	for name, outcome := range map[string]CrashOutcome{
		"boundary": CrashArchiveIncomplete, "stream": CrashArchiveIncomplete, "broken": CrashArchiveIncomplete, "pax": CrashArchiveIncomplete, "gnu": CrashArchiveIncomplete,
		"link": CrashEntriesRejected, "vanishing": CrashReadFailed, "app-[REDACTED]": CrashReadFailed,
	} {
		if got[name].Outcome != outcome {
			t.Fatalf("%s: outcome %s, want %s; %+v", name, got[name].Outcome, outcome, got[name])
		}
	}
	if report.Complete || !strings.Contains(got["boundary"].Error, "end marker") || len(got["broken"].Records) != 1 || !got["broken"].Records[0].Done ||
		got["link"].Omitted[0].Reason != "the crash directory is not a directory" {
		t.Fatalf("report %+v", got)
	}
	data, _ := json.Marshal(report)
	if strings.Contains(string(data), "hunter2") || err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("a redaction value leaked: %v; %s", err, data)
	}
}

// The cluster's own control container is skipped by its identity. A caller's
// container is read whatever it is called, also one described like the
// control container, and so is the control container when the caller asks
// for it.
func TestCrashMetadataSelectsTargetsByIdentity(t *testing.T) {
	records := crashArchive(t, crashEntry{name: "crash/", kind: tar.TypeDir}, crashEntry{name: "crash/one/meta", body: `{"crash_id":"one"}`})
	control := crashTarget{id: "control"}
	for name, tc := range map[string]struct {
		additional []DiagnosticsContainer
		read       []string
	}{
		"helper described as a helper":   {[]DiagnosticsContainer{crashContainer("helper", "collector", "helper")}, []string{"helper"}},
		"helper described as control":    {[]DiagnosticsContainer{crashContainer("control", "cli", "helper")}, []string{"helper"}},
		"no additional containers":       {nil, nil},
		"control container asked for":    {[]DiagnosticsContainer{{Role: "control", Name: "mine", Container: control}}, []string{"control"}},
		"control and a helper asked for": {[]DiagnosticsContainer{crashContainer("control", "cli", "helper"), {Container: control}}, []string{"control", "helper"}},
	} {
		t.Run(name, func(t *testing.T) {
			docker := &fakeCrashDocker{containers: map[string]*fakeCrashContainer{
				"control": {archive: records}, "helper": {archive: records},
			}}
			cluster := &Container{controlPlane: controlPlaneState{handle: control}}
			config, err := normalizeCrashMetadataConfig(CrashMetadataConfig{AdditionalContainers: tc.additional})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := cluster.diagnosticsSnapshot(t.Context(), DiagnosticsConfig{MaxContainers: 128, AdditionalContainers: config.AdditionalContainers})
			if err != nil {
				t.Fatal(err)
			}
			report, err := collectCrashMetadata(t.Context(), config, snapshot, docker, &CrashMetadataReport{SchemaVersion: 1, Containers: []CrashMetadataContainer{}})
			if err != nil || !report.Complete || len(report.Containers) != len(tc.read) {
				t.Fatalf("report %+v, %v", report, err)
			}
			for _, id := range []string{"control", "helper"} {
				if read := docker.containers[id].copies == 1; read != slices.Contains(tc.read, id) {
					t.Fatalf("%s read: %t", id, read)
				}
			}
			for _, result := range report.Containers {
				if result.Outcome != CrashRecordsFound || len(result.Records) != 1 {
					t.Fatalf("result %+v", result)
				}
			}
		})
	}
}
