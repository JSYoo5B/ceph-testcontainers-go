package evidence

import (
	"errors"
	"fmt"
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
		config.Concurrency < 1 || config.Concurrency > 8 || config.MaxContainers < 1 || config.MaxContainers > 128 {
		return config, errors.New("invalid diagnostics limits")
	}
	if err := CheckCollectionInputs(config.AdditionalContainers, config.RedactValues); err != nil {
		return config, fmt.Errorf("diagnostics: %w", err)
	}
	return config, nil
}

// CheckCollectionInputs refuses the additional containers and redaction
// values of a collection outside their bounds: at most 64 containers, each
// with a role of at most 64 bytes and a name of at most 128 bytes without a
// line break or NUL, and at most 128 redaction values of at most 4096 bytes
// each. The error names no value.
func CheckCollectionInputs[T Described](additional []T, redactValues []string) error {
	if len(additional) > 64 || len(redactValues) > 128 {
		return errors.New("too many additional containers or redaction values")
	}
	for _, described := range additional {
		target := Member(described)
		if len(target.Role) > 64 || len(target.Name) > 128 || strings.ContainsAny(target.Role+target.Name, "\x00\r\n") {
			return errors.New("invalid container description")
		}
	}
	for _, value := range redactValues {
		if len(value) > 4096 {
			return errors.New("redaction value exceeds 4096 bytes")
		}
	}
	return nil
}
