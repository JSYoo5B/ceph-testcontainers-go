package cluster

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var cephVersionPattern = regexp.MustCompile(`^ceph version (\d+)\.(\d+)\.(\d+)\S* `)

// CephVersion reports the release of the image that runs the first MON and
// the fixture's Ceph CLI, such as "20.2.4". Run records it once after the MON
// starts; daemons started from other role images may run another release.
// The fixture uses it to choose commands that differ between releases, and
// tests can use it to skip checks for features a release does not have.
func (c *Container) CephVersion() string {
	if c == nil {
		return ""
	}
	return c.cephVersion
}

// cephBefore reports whether the recorded release is older than major. An
// unknown version keeps the default image's command forms.
func (c *Container) cephBefore(major int) bool {
	recorded, _, _ := strings.Cut(c.CephVersion(), ".")
	value, err := strconv.Atoi(recorded)
	return err == nil && value < major
}

func parseCephVersion(output string) (string, error) {
	match := cephVersionPattern.FindStringSubmatch(strings.TrimSpace(output) + " ")
	if match == nil {
		return "", fmt.Errorf("unrecognized Ceph version output %q", strings.TrimSpace(output))
	}
	return match[1] + "." + match[2] + "." + match[3], nil
}

func (c *Container) recordCephVersion(ctx context.Context) error {
	output, err := command(ctx, c.Container, "ceph", "--version")
	if err != nil {
		return fmt.Errorf("read Ceph version: %w", err)
	}
	version, err := parseCephVersion(string(output))
	if err != nil {
		return err
	}
	c.cephVersion = version
	return nil
}
