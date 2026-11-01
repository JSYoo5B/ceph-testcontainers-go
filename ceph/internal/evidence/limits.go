package evidence

import (
	"errors"
	"strings"
	"time"
)

// DiagnosticsConfig is the diagnostics configuration of package
// diagnostics, with its additional containers of type T, so that a public
// configuration converts to it as a whole.
type DiagnosticsConfig[T Described] struct {
	Timeout              time.Duration
	OperationTimeout     time.Duration
	MaxOutputBytes       int
	LogTail              int
	Concurrency          int
	MaxContainers        int
	AdditionalContainers []T
	RedactValues         []string
}

// NormalizeDiagnosticsConfig fills the zero fields of config and refuses
// limits outside the documented bounds; the error is not classified.
func NormalizeDiagnosticsConfig[T Described](config DiagnosticsConfig[T]) (DiagnosticsConfig[T], error) {
	if config.Timeout == 0 {
		config.Timeout = time.Minute
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 10 * time.Second
	}
	if config.MaxOutputBytes == 0 {
		config.MaxOutputBytes = 64 << 10
	}
	if config.LogTail == 0 {
		config.LogTail = 1000
	}
	if config.Concurrency == 0 {
		config.Concurrency = 4
	}
	if config.MaxContainers == 0 {
		config.MaxContainers = 128
	}
	if config.Timeout < 0 || config.Timeout > 5*time.Minute || config.OperationTimeout < 0 || config.OperationTimeout > time.Minute ||
		config.MaxOutputBytes < 1 || config.MaxOutputBytes > 1<<20 || config.LogTail < 1 || config.LogTail > 10000 ||
		config.Concurrency < 1 || config.Concurrency > 8 || config.MaxContainers < 1 || config.MaxContainers > 128 ||
		len(config.AdditionalContainers) > 64 || len(config.RedactValues) > 128 {
		return config, errors.New("invalid diagnostics limits")
	}
	for _, described := range config.AdditionalContainers {
		target := Member(described)
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return config, errors.New("invalid diagnostics container description")
		}
	}
	for _, value := range config.RedactValues {
		if len(value) > 4096 {
			return config, errors.New("diagnostics redaction value exceeds 4096 bytes")
		}
	}
	return config, nil
}
