package ceph

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// CrashDir is the crash directory of every daemon of the cluster. The
// bootstrap ceph.conf sets it, and WithConfigFile refuses to change it.
const CrashDir = "/var/lib/ceph/crash"

// CrashMetadataConfig bounds CollectCrashMetadata. Zero fields select a one
// minute total timeout, 20 seconds per container, 16 MiB of archive and 4096
// archive entries per container, 64 KiB per metadata file and 64 records per
// container. AdditionalContainers are caller-owned containers, such as
// mirror daemons, whose CrashDir is read too; collection never adopts them.
// RedactValues removes additional opaque secrets from the collected fields.
type CrashMetadataConfig struct {
	Timeout              time.Duration
	OperationTimeout     time.Duration
	MaxArchiveBytes      int64
	MaxEntries           int
	MaxRecordBytes       int
	MaxRecords           int
	AdditionalContainers []DiagnosticsContainer
	RedactValues         []string
}

// CrashOutcome says what reading one container's crash directory found.
type CrashOutcome string

const (
	// CrashRecordsFound means that the directory was read and held at least
	// one accepted record.
	CrashRecordsFound CrashOutcome = "records"
	// CrashEntriesRejected means that the directory was read, but every
	// metadata file in it was rejected; Omitted says why.
	CrashEntriesRejected CrashOutcome = "rejected"
	// CrashDirectoryEmpty means that the directory was read and held no
	// metadata file. It is not proof that the daemon never crashed.
	CrashDirectoryEmpty CrashOutcome = "empty"
	// CrashDirectoryAbsent means that the container has no crash directory.
	CrashDirectoryAbsent CrashOutcome = "absent"
	// CrashContainerUnavailable means that the container was never created
	// or was removed, with its crash directory.
	CrashContainerUnavailable CrashOutcome = "unavailable"
	// CrashReadFailed means that Docker or the container implementation
	// failed, for example without permission or after the context ended.
	CrashReadFailed CrashOutcome = "failed"
	// CrashArchiveIncomplete means that the archive was malformed, cut off or
	// over a limit; Records holds what was read before.
	CrashArchiveIncomplete CrashOutcome = "incomplete"
)

// CrashMetadataReport is an independent, JSON-serializable report of the Ceph
// crash metadata in the cluster's daemon containers. Complete means that
// every container's crash directory was read in full and every metadata file
// in it was accepted, or that the directory is absent. It does not mean that
// every crash in the cluster's history was captured: a removed container takes
// its records along.
type CrashMetadataReport struct {
	SchemaVersion int                      `json:"schema_version"`
	StartedAt     time.Time                `json:"started_at"`
	FinishedAt    time.Time                `json:"finished_at"`
	Closed        bool                     `json:"closed"`
	Complete      bool                     `json:"complete"`
	Containers    []CrashMetadataContainer `json:"containers"`
}

// CrashMetadataContainer is what one container's crash directory held. The
// state fields are Docker's view at collection, for correlation with the
// records.
type CrashMetadataContainer struct {
	Role        string `json:"role"`
	Name        string `json:"name"`
	ContainerID string `json:"container_id,omitempty"`
	ImageID     string `json:"image_id,omitempty"`
	CrashDir    string `json:"crash_dir"`
	State       string `json:"state,omitempty"`
	ExitCode    int    `json:"exit_code"`
	OOMKilled   bool   `json:"oom_killed,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	FinishedAt  string `json:"finished_at,omitempty"`

	Outcome CrashOutcome  `json:"outcome"`
	Error   string        `json:"error,omitempty"`
	Records []CrashRecord `json:"records"`
	// Omitted lists the metadata files that were rejected or not read.
	Omitted []CrashOmission `json:"omitted,omitempty"`
	// IgnoredEntries counts the other files, such as crash logs, whose
	// content was skipped.
	IgnoredEntries int `json:"ignored_entries"`
	// ArchiveBytes counts the archive bytes read.
	ArchiveBytes int64 `json:"archive_bytes"`
}

// CrashRecord is one daemon-written crash metadata file. Fields holds the
// known Ceph crash fields, redacted; OmittedFields names the others, whose
// values are not kept. Done reports the done marker with which Ceph
// completes a crash report. Posted means that the report was in the posted
// directory, where ceph-crash moves the reports that it sent.
type CrashRecord struct {
	CrashID       string         `json:"crash_id"`
	Path          string         `json:"path"`
	Entity        string         `json:"entity,omitempty"`
	Process       string         `json:"process,omitempty"`
	CephVersion   string         `json:"ceph_version,omitempty"`
	Timestamp     string         `json:"timestamp,omitempty"`
	Done          bool           `json:"done"`
	Posted        bool           `json:"posted,omitempty"`
	Fields        map[string]any `json:"fields"`
	OmittedFields []string       `json:"omitted_fields,omitempty"`
	// identity is the crash directory's own name, before redaction, for
	// comparing crashes; it is never reported.
	identity string
}

// CrashOmission names a metadata file that was not accepted, and why.
type CrashOmission struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// crashFields are the fields that Ceph writes into crash metadata. Strings,
// numbers, booleans and lists of strings are kept.
var crashFields = map[string]bool{
	"crash_id": true, "timestamp": true, "process_name": true, "entity_name": true, "ceph_version": true,
	"utsname_hostname": true, "utsname_sysname": true, "utsname_release": true, "utsname_version": true, "utsname_machine": true,
	"os_name": true, "os_id": true, "os_version_id": true, "os_version": true, "backtrace": true,
	"assert_condition": true, "assert_func": true, "assert_file": true, "assert_line": true, "assert_thread_name": true, "assert_msg": true,
	"io_error": true, "io_error_devname": true, "io_error_path": true, "io_error_code": true, "io_error_optype": true,
	"io_error_offset": true, "io_error_length": true, "archived": true, "stack_sig": true,
}

type crashDocker interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	CopyFromContainer(context.Context, string, mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error)
}

func normalizeCrashMetadataConfig(config CrashMetadataConfig) (CrashMetadataConfig, error) {
	if config.Timeout == 0 {
		config.Timeout = time.Minute
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 20 * time.Second
	}
	if config.MaxArchiveBytes == 0 {
		config.MaxArchiveBytes = 16 << 20
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = 4096
	}
	if config.MaxRecordBytes == 0 {
		config.MaxRecordBytes = 64 << 10
	}
	if config.MaxRecords == 0 {
		config.MaxRecords = 64
	}
	if config.Timeout < 0 || config.Timeout > 5*time.Minute || config.OperationTimeout < 0 || config.OperationTimeout > time.Minute ||
		config.MaxArchiveBytes < 1 || config.MaxArchiveBytes > 256<<20 || config.MaxEntries < 1 || config.MaxEntries > 65536 ||
		config.MaxRecordBytes < 1 || config.MaxRecordBytes > 1<<20 || config.MaxRecords < 1 || config.MaxRecords > 1024 ||
		len(config.AdditionalContainers) > 64 || len(config.RedactValues) > 128 {
		return config, invalid(errors.New("invalid crash metadata limits"))
	}
	for _, target := range config.AdditionalContainers {
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return config, invalid(errors.New("invalid crash metadata container description"))
		}
	}
	for _, value := range config.RedactValues {
		if len(value) > 4096 {
			return config, invalid(errors.New("crash metadata redaction value exceeds 4096 bytes"))
		}
	}
	return config, nil
}

// CollectCrashMetadata reads the Ceph crash metadata files in CrashDir of the
// cluster's MON, MGR, OSD, MDS and RGW containers, and of
// config.AdditionalContainers, through Docker's archive API. It works on
// stopped containers and on a partially bootstrapped or terminated cluster,
// and starts, stops or reconfigures nothing. It reads only regular files named
// meta in a crash report directory, keeps their known fields and redacts them
// like CollectDiagnostics; it skips crash logs and every other file, and
// rejects links, unexpected paths and files that are not JSON objects. Each
// container's outcome, rejected files and errors stay in the report, which is
// returned also with an error for failed or incomplete reads. Crash metadata
// can still hold host names and paths; review a report before sharing it.
func (c *Container) CollectCrashMetadata(ctx context.Context, config CrashMetadataConfig) (*CrashMetadataReport, error) {
	config, err := normalizeCrashMetadataConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	report := &CrashMetadataReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Containers: []CrashMetadataContainer{}}
	snapshot, err := c.diagnosticsSnapshot(ctx, DiagnosticsConfig{MaxContainers: 128, AdditionalContainers: config.AdditionalContainers, RedactValues: config.RedactValues})
	report.Closed = snapshot.closed
	if err != nil {
		report.FinishedAt = time.Now().UTC()
		return report, err
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		report.FinishedAt = time.Now().UTC()
		return report, fmt.Errorf("connect to Docker: %w", err)
	}
	defer func() { _ = docker.Close() }()
	return collectCrashMetadata(ctx, config, snapshot, docker, report)
}

func collectCrashMetadata(ctx context.Context, config CrashMetadataConfig, snapshot diagnosticsSnapshot, docker crashDocker, report *CrashMetadataReport) (*CrashMetadataReport, error) {
	redact := diagnosticsRedactor(snapshot.secrets)
	var errs []error
	report.Complete = true
	for _, target := range snapshot.containers {
		// The separate control container runs no daemon. It is recognized
		// by its identity, not by a description, which a caller's container
		// may share, and it is read when the caller asked for it.
		if sameContainer(target.Container, snapshot.controlPlane) && !slices.ContainsFunc(config.AdditionalContainers,
			func(requested DiagnosticsContainer) bool {
				return sameContainer(requested.Container, snapshot.controlPlane)
			}) {
			continue
		}
		result := readCrashDirectory(ctx, config, target, docker, redact)
		switch result.Outcome {
		case CrashRecordsFound, CrashDirectoryEmpty, CrashDirectoryAbsent:
			report.Complete = report.Complete && len(result.Omitted) == 0
		case CrashContainerUnavailable, CrashEntriesRejected:
			report.Complete = false
		default:
			report.Complete = false
			errs = append(errs, fmt.Errorf("%s %s: %s", result.Role, result.Name, result.Error))
		}
		report.Containers = append(report.Containers, result)
	}
	if snapshot.omitted > 0 {
		report.Complete = false
		errs = append(errs, fmt.Errorf("%d containers exceed the container limit", snapshot.omitted))
	}
	report.FinishedAt = time.Now().UTC()
	return report, errors.Join(errs...)
}

// readCrashDirectory reads the crash directory of one container.
func readCrashDirectory(ctx context.Context, config CrashMetadataConfig, target DiagnosticsContainer, docker crashDocker, redact func(string) string) CrashMetadataContainer {
	result := CrashMetadataContainer{Role: redact(target.Role), Name: redact(target.Name), CrashDir: CrashDir, Records: []CrashRecord{}}
	fail := func(outcome CrashOutcome, err error) CrashMetadataContainer {
		result.Outcome = outcome
		result.Error, _ = diagnosticErrorText(err, redact, 1024)
		return result
	}
	if target.Container == nil || target.Container.GetContainerID() == "" {
		return fail(CrashContainerUnavailable, errors.New("container was not created"))
	}
	result.ContainerID = target.Container.GetContainerID()
	opCtx, cancel := context.WithTimeout(ctx, config.OperationTimeout)
	defer cancel()
	inspection, err := docker.ContainerInspect(opCtx, result.ContainerID, mobycl.ContainerInspectOptions{})
	switch {
	case errdefs.IsNotFound(err):
		return fail(CrashContainerUnavailable, errors.New("container was removed"))
	case err != nil:
		return fail(CrashReadFailed, errors.Join(err, opCtx.Err()))
	}
	result.ImageID = inspection.Container.Image
	if state := inspection.Container.State; state != nil {
		result.State, result.ExitCode, result.OOMKilled = string(state.Status), state.ExitCode, state.OOMKilled
		result.StartedAt, result.FinishedAt = state.StartedAt, state.FinishedAt
	}
	archive, err := docker.CopyFromContainer(opCtx, result.ContainerID, mobycl.CopyFromContainerOptions{SourcePath: CrashDir})
	if errdefs.IsNotFound(err) {
		// The archive API reports a missing path and a missing container
		// alike, so only a container that still exists lacks the directory.
		switch _, inspectErr := docker.ContainerInspect(opCtx, result.ContainerID, mobycl.ContainerInspectOptions{}); {
		case errdefs.IsNotFound(inspectErr):
			return fail(CrashContainerUnavailable, errors.New("container was removed"))
		case inspectErr != nil:
			return fail(CrashReadFailed, errors.Join(inspectErr, opCtx.Err()))
		}
		result.Outcome = CrashDirectoryAbsent
		return result
	}
	if err != nil {
		return fail(CrashReadFailed, errors.Join(err, opCtx.Err()))
	}
	defer func() { _ = archive.Content.Close() }()
	if err := readCrashArchive(archive.Content, config, &result, redact); err != nil {
		return fail(CrashArchiveIncomplete, errors.Join(err, opCtx.Err()))
	}
	switch {
	case len(result.Records) > 0:
		result.Outcome = CrashRecordsFound
	case len(result.Omitted) > 0:
		result.Outcome = CrashEntriesRejected
	default:
		result.Outcome = CrashDirectoryEmpty
	}
	return result
}

// countingReader counts the bytes read through it and records whether any
// byte from the position after, set with follow, was not zero.
type countingReader struct {
	r       io.Reader
	n       int64
	after   int64
	nonZero bool
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	start := r.n
	r.n += int64(n)
	if from := max(r.after-start, 0); from < int64(n) {
		r.nonZero = r.nonZero || slices.ContainsFunc(p[from:n], func(b byte) bool { return b != 0 })
	}
	return n, err
}

// follow starts watching the bytes from position after.
func (r *countingReader) follow(after int64) {
	r.after, r.nonZero = after, false
}

// terminated reports whether the stream ended with two zero blocks, and
// nothing but zero bytes, after the last entry's content.
func (r *countingReader) terminated() bool {
	return r.n-r.after >= 1024 && !r.nonZero
}

// readCrashArchive streams the tar archive of the crash directory into
// result. It returns an error when the archive is malformed or exceeds a
// limit; result then holds what was read before.
func readCrashArchive(content io.Reader, config CrashMetadataConfig, result *CrashMetadataContainer, redact func(string) string) error {
	counted := &countingReader{r: io.LimitReader(content, config.MaxArchiveBytes+1)}
	defer func() { result.ArchiveBytes = min(counted.n, config.MaxArchiveBytes) }()
	overLimit := func() error {
		if counted.n > config.MaxArchiveBytes {
			return fmt.Errorf("the crash directory archive exceeds %d bytes", config.MaxArchiveBytes)
		}
		return nil
	}
	archive := crashArchiveReader{config: config, result: result, redact: redact, reader: tar.NewReader(counted), done: map[string]bool{}}
	defer archive.markDone()
	entries := 0
	// The archive ends with two zero blocks after the last entry's content.
	// The tar reader also reports a normal end for a stream cut at an entry
	// boundary, or after an extended header (PAX or GNU) and its payload,
	// which it consumes itself.
	for {
		header, err := archive.reader.Next()
		if errors.Is(err, io.EOF) {
			if !counted.terminated() {
				return errors.Join(overLimit(), errors.New("the crash directory archive ends without its end marker"))
			}
			break
		}
		if err != nil {
			return errors.Join(overLimit(), fmt.Errorf("read the crash directory archive: %w", err))
		}
		if entries++; entries > config.MaxEntries {
			return fmt.Errorf("the crash directory archive has more than %d entries", config.MaxEntries)
		}
		counted.follow(counted.n + (max(header.Size, 0)+511)/512*512)
		if err := archive.entry(header); err != nil {
			return errors.Join(overLimit(), err)
		}
	}
	return overLimit()
}

// crashArchiveReader reads the entries of one crash directory archive into result.
type crashArchiveReader struct {
	config CrashMetadataConfig
	result *CrashMetadataContainer
	redact func(string) string
	reader *tar.Reader
	// done holds the report directories with a done marker, and directories
	// each record's report directory, both unredacted.
	done        map[string]bool
	directories []string
}

// markDone marks each record whose report directory has a done marker.
func (a *crashArchiveReader) markDone() {
	for i, directory := range a.directories {
		a.result.Records[i].Done = a.done[directory]
	}
}

func (a *crashArchiveReader) omit(path, reason string) {
	a.result.Omitted = append(a.result.Omitted, CrashOmission{Path: a.redact(path), Reason: reason})
}

// entry classifies one archive entry: a path outside the crash directory, a
// report's metadata file or done marker, or an ignored entry. Only a failed
// read of a metadata file's content is an error.
func (a *crashArchiveReader) entry(header *tar.Header) error {
	base := path.Base(CrashDir)
	name := header.Name
	relative, inside := strings.CutPrefix(path.Clean(name), base+"/")
	if path.IsAbs(name) || slices.Contains(strings.Split(name, "/"), "..") || (!inside && path.Clean(name) != base) {
		a.omit(name, "unexpected path")
		return nil
	}
	parts := strings.Split(relative, "/")
	posted := len(parts) == 3 && parts[0] == "posted"
	if posted {
		parts = parts[1:]
	}
	report := len(parts) == 2 && parts[0] != "" && parts[0] != "posted"
	switch {
	case !inside && header.Typeflag != tar.TypeDir:
		a.omit(name, "the crash directory is not a directory")
	case report && parts[1] == "meta" && header.Typeflag != tar.TypeReg:
		a.omit(relative, "not a regular file")
	case !inside || header.Typeflag == tar.TypeDir:
	case report && parts[1] == "meta":
		return a.meta(header, relative, posted)
	case report && parts[1] == "done" && header.Typeflag == tar.TypeReg:
		a.done[relative[:len(relative)-len("/done")]] = true
	default:
		a.result.IgnoredEntries++
	}
	return nil
}

// meta reads one metadata file into a record, or omits it with its reason.
func (a *crashArchiveReader) meta(header *tar.Header, relative string, posted bool) error {
	if len(a.result.Records) >= a.config.MaxRecords {
		a.omit(relative, fmt.Sprintf("over the limit of %d records", a.config.MaxRecords))
		return nil
	}
	if header.Size > int64(a.config.MaxRecordBytes) {
		a.omit(relative, fmt.Sprintf("larger than %d bytes", a.config.MaxRecordBytes))
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(a.reader, int64(a.config.MaxRecordBytes)+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", a.redact(relative), err)
	}
	record, reason := crashRecord(data, relative, posted, a.redact)
	if reason != "" {
		a.omit(relative, reason)
		return nil
	}
	a.result.Records = append(a.result.Records, record)
	a.directories = append(a.directories, path.Dir(relative))
	return nil
}

// crashRecord parses one metadata file, or returns why it is rejected.
func crashRecord(data []byte, relative string, posted bool, redact func(string) string) (CrashRecord, string) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var object map[string]any
	var rest any
	if err := decoder.Decode(&object); err != nil || object == nil || !errors.Is(decoder.Decode(&rest), io.EOF) {
		return CrashRecord{}, "not a JSON object"
	}
	record := CrashRecord{CrashID: redact(path.Base(path.Dir(relative))), Path: redact(relative), Posted: posted, Fields: map[string]any{},
		identity: path.Base(path.Dir(relative))}
	for key, value := range object {
		kept, ok := crashFieldValue(value, redact)
		if !crashFields[key] || !ok {
			record.OmittedFields = append(record.OmittedFields, redact(key))
			continue
		}
		record.Fields[key] = kept
	}
	slices.Sort(record.OmittedFields)
	text := func(key string) string {
		value, _ := record.Fields[key].(string)
		return value
	}
	record.Entity, record.Process, record.CephVersion, record.Timestamp = text("entity_name"), text("process_name"), text("ceph_version"), text("timestamp")
	return record, ""
}

// crashFieldValue redacts a kept field value: a string, number, boolean or
// list of strings.
func crashFieldValue(value any, redact func(string) string) (any, bool) {
	switch value := value.(type) {
	case string:
		return redact(value), true
	case json.Number, bool:
		return value, true
	case []any:
		lines := make([]string, 0, len(value))
		for _, item := range value {
			line, ok := item.(string)
			if !ok {
				return nil, false
			}
			lines = append(lines, redact(line))
		}
		return lines, true
	}
	return nil, false
}
