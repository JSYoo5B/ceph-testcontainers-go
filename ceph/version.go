package ceph

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var cephVersionPattern = regexp.MustCompile(`^ceph version (\d+)\.(\d+)\.(\d+)\S* `)

// cephBefore reports whether the recorded release is older than major. An
// unknown version keeps the default image's command forms.
func (c *Container) cephBefore(major int) bool {
	recorded, _, _ := strings.Cut(c.staticFacts().CephVersion, ".")
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
	c.bootstrap.recordVersion(version)
	return nil
}
