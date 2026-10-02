package ceph

import (
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	osds           int
	blockSize      int64
	startupTimeout time.Duration
}

// Option transfers cluster settings outside the monitor's container request.
type Option func(*options) error

// Customize implements testcontainers.ContainerCustomizer.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithOSDCount sets the initial number of OSD containers (default: 2).
func WithOSDCount(count int) Option {
	return func(o *options) error {
		if count < 1 {
			return fmt.Errorf("OSD count must be at least 1")
		}
		o.osds = count
		return nil
	}
}

// WithOSDBlockSize sets each sparse BlueStore file's logical size in bytes.
// The default is 1 GiB; these are disposable files, not real disks.
func WithOSDBlockSize(size int64) Option {
	return func(o *options) error {
		if size < 1<<30 {
			return fmt.Errorf("OSD block size must be at least 1 GiB")
		}
		o.blockSize = size
		return nil
	}
}

// WithStartupTimeout bounds bootstrap, readiness and topology operations.
// Each operation is also bounded by the caller's context. Default: 3 minutes.
func WithStartupTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		if timeout <= 0 {
			return fmt.Errorf("startup timeout must be positive")
		}
		o.startupTimeout = timeout
		return nil
	}
}
