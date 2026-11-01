// Package diagnosticshook gives package finalize the parts of the diagnostics
// collectors that are not public API: configuration validation, the crash
// directory reader over chosen containers and a crash record's unredacted
// identity. Package diagnostics registers them in internal/hookreg when it is
// initialized.
package diagnosticshook

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/diagnostics"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

type hooks interface {
	ValidateConfig(config diagnostics.Config) error
	ValidateCrashMetadataConfig(config diagnostics.CrashMetadataConfig) error
	ReadCrashes(ctx context.Context, operationTimeout time.Duration, targets []diagnostics.Target, secrets []string, docker evidence.CrashDocker) (*diagnostics.CrashMetadataReport, error)
	CrashIdentity(record diagnostics.CrashRecord) string
}

func get() hooks {
	h, ok := hookreg.Diagnostics.(hooks)
	if !ok {
		panic("diagnosticshook: package diagnostics did not register its hooks")
	}
	return h
}

// ValidateConfig refuses the limits that diagnostics.Collect refuses, before
// any collection.
func ValidateConfig(config diagnostics.Config) error { return get().ValidateConfig(config) }

// ValidateCrashMetadataConfig refuses the limits that
// diagnostics.CollectCrashMetadata refuses, before any collection.
func ValidateCrashMetadataConfig(config diagnostics.CrashMetadataConfig) error {
	return get().ValidateCrashMetadataConfig(config)
}

// ReadCrashes reads the crash directories of targets with the default limits
// and operationTimeout per container, one result per target in order, and
// redacts the records with secrets. It reads nothing else of a cluster.
func ReadCrashes(ctx context.Context, operationTimeout time.Duration, targets []diagnostics.Target, secrets []string, docker evidence.CrashDocker) (*diagnostics.CrashMetadataReport, error) {
	return get().ReadCrashes(ctx, operationTimeout, targets, secrets, docker)
}

// CrashIdentity returns the crash directory's own name, before redaction, for
// telling crashes apart; it is never reported.
func CrashIdentity(record diagnostics.CrashRecord) string { return get().CrashIdentity(record) }
