package diagnostics

import (
	"context"
	"fmt"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/internal/evidence"
	"github.com/testcontainers/testcontainers-go"
)

// CrashMetadataConfig bounds CollectCrashMetadata. Zero fields select a one
// minute total timeout, 20 seconds per container, 16 MiB of archive and 4096
// archive entries per container, 64 KiB per metadata file and 64 records per
// container. AdditionalContainers are caller-owned containers, such as
// mirror daemons, whose [ceph.CrashDir] is read too; collection never adopts them.
// RedactValues removes additional opaque secrets from the collected fields.
//
// The limits are 5 minutes total, 1 minute per container, 256 MiB of archive,
// 65536 entries, 1 MiB per metadata file, 1024 records per container (later
// ones are listed in Omitted), 64 additional containers and 128 redaction
// values of up to 4096 bytes. An additional container is read whatever its
// Role and Name; the control container, which runs no daemon, is read only
// when listed here.
type CrashMetadataConfig struct {
	Timeout              time.Duration
	OperationTimeout     time.Duration
	MaxArchiveBytes      int64
	MaxEntries           int
	MaxRecordBytes       int
	MaxRecords           int
	AdditionalContainers []Target
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
}

// CrashOmission names a metadata file that was not accepted, and why.
type CrashOmission struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func normalizeCrashMetadataConfig(config CrashMetadataConfig) (CrashMetadataConfig, error) {
	normalized, err := evidence.NormalizeCrashConfig(evidence.CrashConfig[Target](config))
	if err != nil {
		return config, invalid(err)
	}
	return CrashMetadataConfig(normalized), nil
}

// CollectCrashMetadata reads the Ceph crash metadata files in [ceph.CrashDir]
// of the MON, MGR, OSD, MDS and RGW containers and the client toolkit of
// cluster, and of config.AdditionalContainers, through Docker's archive API. It works on
// stopped containers and on a partially bootstrapped or terminated cluster,
// and starts, stops or reconfigures nothing. It reads only regular files named
// meta in a crash report directory, keeps their known fields and redacts them
// like Collect; it skips crash logs and every other file, and
// rejects links, unexpected paths and files that are not JSON objects. Each
// container's outcome, rejected files and errors stay in the report, which is
// returned also with an error for failed or incomplete reads. Crash metadata
// can still hold host names and paths; review a report before sharing it.
//
// Ceph writes a report directory (a meta JSON file, a log file and a done
// marker) into [ceph.CrashDir] when a daemon dies on a fatal signal or a failed
// assertion; only meta files, also in posted/, are read, other entries are
// counted in IgnoredEntries, and rejected files are listed in Omitted with
// their reason. A record keeps the known crash fields (crash_id, timestamp,
// process_name, entity_name, ceph_version, utsname_*, os_*, backtrace,
// assert_*, io_error*, archived, stack_sig) and names other fields in
// OmittedFields without values. A failed or incomplete container makes the
// call return an error with the report and the other containers' records. A
// container's exit code alone does not tell a crash: a daemon that is PID 1,
// as here, exits 1 after writing its report. It is not called automatically
// outside the failure archive of finalize.TerminateOnCleanup.
//
//purpose:evidence
func CollectCrashMetadata(ctx context.Context, cluster *ceph.Container, config CrashMetadataConfig) (*CrashMetadataReport, error) {
	config, err := normalizeCrashMetadataConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	report := &CrashMetadataReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Containers: []CrashMetadataContainer{}}
	snapshot, err := takeSnapshot(ctx, cluster, Config{MaxContainers: 128, AdditionalContainers: config.AdditionalContainers, RedactValues: config.RedactValues})
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

// collectCrashMetadata reads the crash directories of the snapshot's
// containers into report with config normalized.
func collectCrashMetadata(ctx context.Context, config CrashMetadataConfig, snapshot diagnosticsSnapshot, docker evidence.CrashDocker, report *CrashMetadataReport) (*CrashMetadataReport, error) {
	collected := &evidence.CrashReport{SchemaVersion: report.SchemaVersion, StartedAt: report.StartedAt, Closed: report.Closed,
		Containers: []evidence.CrashContainer{}}
	collected, err := evidence.CollectCrashes(ctx, evidence.MemberConfig(evidence.CrashConfig[Target](config)), evidence.Members(snapshot.containers),
		snapshot.controlPlane, snapshot.omitted, snapshot.secrets, docker, collected)
	*report = crashMetadataReport(collected)
	return report, err
}

// crashMetadataReport is the public form of a collected report.
func crashMetadataReport(collected *evidence.CrashReport) CrashMetadataReport {
	report := CrashMetadataReport{SchemaVersion: collected.SchemaVersion, StartedAt: collected.StartedAt, FinishedAt: collected.FinishedAt,
		Closed: collected.Closed, Complete: collected.Complete, Containers: make([]CrashMetadataContainer, 0, len(collected.Containers))}
	for _, c := range collected.Containers {
		container := CrashMetadataContainer{Role: c.Role, Name: c.Name, ContainerID: c.ContainerID, ImageID: c.ImageID, CrashDir: c.CrashDir,
			State: c.State, ExitCode: c.ExitCode, OOMKilled: c.OOMKilled, StartedAt: c.StartedAt, FinishedAt: c.FinishedAt,
			Outcome: CrashOutcome(c.Outcome), Error: c.Error, Records: make([]CrashRecord, 0, len(c.Records)),
			IgnoredEntries: c.IgnoredEntries, ArchiveBytes: c.ArchiveBytes}
		for _, r := range c.Records {
			container.Records = append(container.Records, CrashRecord{CrashID: r.CrashID, Path: r.Path, Entity: r.Entity, Process: r.Process,
				CephVersion: r.CephVersion, Timestamp: r.Timestamp, Done: r.Done, Posted: r.Posted, Fields: r.Fields, OmittedFields: r.OmittedFields})
		}
		for _, o := range c.Omitted {
			container.Omitted = append(container.Omitted, CrashOmission(o))
		}
		report.Containers = append(report.Containers, container)
	}
	return report
}
