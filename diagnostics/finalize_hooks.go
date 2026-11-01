package diagnostics

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// finalizeHooks gives package finalize the parts of the collectors that are
// not public API: the validation of a configuration before anything is
// registered, the crash directory reader over chosen containers, and a crash
// record's unredacted identity. Its methods are exported only so that
// internal/diagnosticshook can call them through an interface; the type
// itself is unexported and never appears in the public packages.
type finalizeHooks struct{}

func init() {
	hookreg.Diagnostics = finalizeHooks{}
}

// ValidateConfig refuses the limits that Collect refuses.
func (finalizeHooks) ValidateConfig(config Config) error {
	_, err := normalizeDiagnosticsConfig(config)
	return err
}

// ValidateCrashMetadataConfig refuses the limits that CollectCrashMetadata
// refuses.
func (finalizeHooks) ValidateCrashMetadataConfig(config CrashMetadataConfig) error {
	_, err := normalizeCrashMetadataConfig(config)
	return err
}

// ReadCrashes reads the crash directories of targets with the default
// limits, one result per target in order, and redacts with secrets.
func (finalizeHooks) ReadCrashes(ctx context.Context, operationTimeout time.Duration, targets []Target, secrets []string, docker evidence.CrashDocker) (*CrashMetadataReport, error) {
	config, _ := normalizeCrashMetadataConfig(CrashMetadataConfig{OperationTimeout: operationTimeout})
	snapshot := diagnosticsSnapshot{containers: targets, secrets: secrets}
	report := &CrashMetadataReport{SchemaVersion: 1, StartedAt: time.Now().UTC(), Containers: []CrashMetadataContainer{}}
	return collectCrashMetadata(ctx, config, snapshot, docker, report)
}

// CrashIdentity returns the crash directory's own name, before redaction.
func (finalizeHooks) CrashIdentity(record CrashRecord) string { return record.identity }
