package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

var daemonVersionPattern = regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)(?:-\S+)?$`)

// parseDaemonVersion decodes the output of a daemon's admin socket "version"
// command, {"version":"20.2.4","release":"tentacle","release_type":"stable"},
// and returns the release as major.minor.patch, the form CephVersion uses. A
// missing, null, non-string or unrecognized "version" is an error: it never
// becomes an empty or defaulted release.
func parseDaemonVersion(output []byte) (string, error) {
	var reply map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(output))
	if err := decoder.Decode(&reply); err != nil || reply == nil {
		return "", errors.New("daemon version reply is not a JSON object")
	}
	// Require end of input: More would accept a stray closing delimiter.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return "", errors.New("daemon version reply has trailing data")
	}
	raw, ok := reply["version"]
	if !ok {
		return "", errors.New("daemon version reply has no version")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return "", errors.New("daemon version reply version is not a string")
	}
	match := daemonVersionPattern.FindStringSubmatch(text)
	if match == nil {
		return "", fmt.Errorf("unrecognized daemon version %q", text)
	}
	return match[1] + "." + match[2] + "." + match[3], nil
}

func (c *Container) daemonVersion(ctx context.Context, handle daemonHandle) (string, error) {
	output, err := c.adminSocket(ctx, handle, "version")
	if err != nil {
		return "", err
	}
	kind, id := handle.daemonRef()
	version, err := parseDaemonVersion(output)
	if err != nil {
		return "", fmt.Errorf("version of %s.%s: %w", kind, id, err)
	}
	return version, nil
}

// Version returns the release that this running OSD reports itself, such as
// "20.2.4", read from its own admin socket. It does not use CephVersion, which
// describes the first MON and the CLI, so it is the way to see the release of an
// OSD that OSDOptions.WithImage started from another image. It works under the
// same conditions as AdminSocket, and a stopped daemon, a stale handle or an
// unrecognized reply returns an error.
//
//purpose:observation
func (o *OSDContainer) Version(ctx context.Context) (string, error) {
	c, err := o.cluster("OSDContainer.Version")
	if err != nil {
		return "", err
	}
	return c.daemonVersion(ctx, o)
}

// Version returns the release that this running MON reports itself, such as
// "20.2.4". See OSDContainer.Version.
//
//purpose:observation
func (m *MonitorContainer) Version(ctx context.Context) (string, error) {
	c, err := m.cluster("MonitorContainer.Version")
	if err != nil {
		return "", err
	}
	return c.daemonVersion(ctx, m)
}

// Version returns the release that this running MGR reports itself, such as
// "20.2.4". See OSDContainer.Version.
//
//purpose:observation
func (m *ManagerContainer) Version(ctx context.Context) (string, error) {
	c, err := m.cluster("ManagerContainer.Version")
	if err != nil {
		return "", err
	}
	return c.daemonVersion(ctx, m)
}
