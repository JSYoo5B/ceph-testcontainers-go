package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// OSDConfig places a container's OSD in the logical CRUSH hierarchy.
// Root defaults to the cluster's selected default root, Host defaults to osd-ID, and Rack/DeviceClass
// are optional. Multiple OSDs may share a host or rack for domain-failure tests.
type OSDConfig struct {
	Host        string
	Rack        string
	Root        string
	DeviceClass string
}

// Placement returns a copy of this OSD's resolved logical CRUSH location.
func (o *OSDContainer) Placement() OSDConfig { return o.placement }

var crushLocationName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
var crushDeviceName = regexp.MustCompile(`^osd\.[0-9]+$`)

// CRUSH bucket names form one namespace across types. Reusing a bucket with a
// different parent would move the OSDs already below it into that location.
func validateCRUSHHierarchy(defaultRoot string, locations []OSDConfig) error {
	types := make(map[string]string)
	declare := func(name, kind string) error {
		if name == "" {
			return nil
		}
		if crushDeviceName.MatchString(name) {
			return fmt.Errorf("CRUSH %s %q conflicts with the OSD device namespace", kind, name)
		}
		if previous := types[name]; previous != "" && previous != kind {
			return fmt.Errorf("CRUSH bucket %q cannot be both %s and %s", name, previous, kind)
		}
		types[name] = kind
		return nil
	}
	// The initial map always contains default; the selected root may also have
	// been created before any OSD joined it.
	for _, root := range []string{"default", defaultRoot} {
		if err := declare(root, "root"); err != nil {
			return err
		}
	}
	rackRoots := make(map[string]string)
	hostLocations := make(map[string]OSDConfig)
	for _, location := range locations {
		for _, bucket := range []struct{ name, kind string }{
			{location.Root, "root"}, {location.Rack, "rack"}, {location.Host, "host"},
		} {
			if err := declare(bucket.name, bucket.kind); err != nil {
				return err
			}
		}
		if location.Rack != "" {
			if root, exists := rackRoots[location.Rack]; exists && root != location.Root {
				return fmt.Errorf("CRUSH rack %q already belongs to root %q, cannot move it to %q", location.Rack, root, location.Root)
			}
			rackRoots[location.Rack] = location.Root
		}
		if location.Host != "" {
			if previous, exists := hostLocations[location.Host]; exists && (previous.Root != location.Root || previous.Rack != location.Rack) {
				return fmt.Errorf("CRUSH host %q already belongs to root %q rack %q, cannot change its location", location.Host, previous.Root, previous.Rack)
			}
			hostLocations[location.Host] = location
		}
	}
	return nil
}

// Caller holds c.mu while checking the candidate against immutable owned OSDs.
func (c *Container) validateOSDPlacement(candidate OSDConfig) error {
	locations := make([]OSDConfig, 0, len(c.osds)+1)
	for _, osd := range c.osds {
		locations = append(locations, osd.Placement())
	}
	locations = append(locations, candidate)
	return validateCRUSHHierarchy(clusterCRUSHRoot(c.settings), locations)
}

func clusterCRUSHRoot(settings options) string {
	if settings.defaultCRUSHRoot == "" {
		return "default"
	}
	return settings.defaultCRUSHRoot
}

func resolveOSDDefaults(settings options, config OSDConfig) OSDConfig {
	if config.Root == "" {
		config.Root = clusterCRUSHRoot(settings)
	}
	return config
}

// Called after the first MON is ready and before starting other daemons. A
// numeric default is necessary: Ceph otherwise chooses the lowest-ID rule,
// whose root may have no OSDs in a custom initial layout.
func (c *Container) configureDefaultCRUSHRoot(ctx context.Context) error {
	root := clusterCRUSHRoot(c.settings)
	if root == "default" {
		return nil
	}
	if _, err := c.Ceph(ctx, "osd", "crush", "add-bucket", root, "root"); err != nil {
		return err
	}
	// This name cannot collide with CreatePool's -replicated/-ec rule names.
	const ruleName = "tc-default-placement"
	config := PoolConfig{CRUSHRoot: root, FailureDomain: "osd"}
	if _, err := c.Ceph(ctx, replicatedCRUSHRuleCommand(config, ruleName)...); err != nil {
		return err
	}
	data, err := c.Ceph(ctx, "osd", "crush", "rule", "dump", ruleName, "--format", "json")
	if err != nil {
		return err
	}
	var rule struct {
		ID   *int   `json:"rule_id"`
		Name string `json:"rule_name"`
	}
	if err := json.Unmarshal(data, &rule); err != nil {
		return fmt.Errorf("decode default CRUSH rule: %w", err)
	}
	if rule.ID == nil || *rule.ID < 0 || rule.Name != ruleName {
		return fmt.Errorf("default CRUSH rule response is incomplete")
	}
	value := strconv.Itoa(*rule.ID)
	if _, err := c.Ceph(ctx, "config", "set", "global", "osd_pool_default_crush_rule", value); err != nil {
		return err
	}
	c.configMu.Lock()
	configBytes, err := defaultCRUSHRuleConfig(c.config, value)
	if err == nil {
		c.config = configBytes
	}
	c.configMu.Unlock()
	if err != nil {
		return err
	}
	return c.Container.CopyToContainer(ctx, bytes.Clone(configBytes), "/etc/ceph/ceph.conf", 0o644)
}

func defaultCRUSHRuleConfig(config []byte, value string) ([]byte, error) {
	lines := strings.Split(string(config), "\n")
	global, found := false, false
	var result []string
	for _, line := range lines {
		section := strings.TrimSpace(line)
		if strings.HasPrefix(section, "[") && strings.HasSuffix(section, "]") {
			global = section == "[global]"
			if global && !found {
				result = append(result, line, "osd pool default crush rule = "+value)
				found = true
				continue
			}
		}
		if key, _, ok := strings.Cut(line, "="); global && ok && strings.ReplaceAll(strings.TrimSpace(key), " ", "_") == "osd_pool_default_crush_rule" {
			continue
		}
		result = append(result, line)
	}
	if !found {
		return nil, fmt.Errorf("bootstrap config has no global section")
	}
	return []byte(strings.Join(result, "\n")), nil
}

func normalizeOSDConfig(config OSDConfig) (OSDConfig, error) {
	if config.Root == "" {
		config.Root = "default"
	}
	for _, field := range []struct{ name, value string }{
		{"host", config.Host}, {"rack", config.Rack}, {"root", config.Root}, {"device class", config.DeviceClass},
	} {
		if field.value != "" && (len(field.value) > 128 || !crushLocationName.MatchString(field.value)) {
			return config, fmt.Errorf("CRUSH %s must use letters, digits, underscores, dots or dashes", field.name)
		}
		if field.name != "device class" && crushDeviceName.MatchString(field.value) {
			return config, fmt.Errorf("CRUSH %s %q conflicts with the OSD device namespace", field.name, field.value)
		}
	}
	return config, nil
}
