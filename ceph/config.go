package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

// ConfigSetting addresses one entry in the MON configuration database. Section
// is global, mon, mgr, osd, mds, client or a specific identity such as osd.0.
// Mask is optional: class:ssd, host:node-a or host:node-a/class:ssd. These stored
// settings have lower precedence than local files, argv and runtime overrides.
type ConfigSetting struct {
	Section, Mask, Name, Value string
}

// ConfigEntry reports an exact stored entry rather than an inherited/default
// value. Configuration may contain sensitive values; log selected fields only.
type ConfigEntry struct {
	Section            string `json:"section"`
	Mask               string `json:"mask"`
	Name               string `json:"name"`
	Value              string `json:"value"`
	Level              string `json:"level"`
	CanUpdateAtRuntime bool   `json:"can_update_at_runtime"`
}

// observedConfiguration implements Observer.Configuration. It takes the topology lock and refuses
// a cluster without pool policy readiness before it reads.
func (c *Container) observedConfiguration(ctx context.Context) ([]ConfigEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.configuration(ctx)
}

var configNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}(/[a-z][a-z0-9_]{0,127})*$`)
var configMaskKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)
var configIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func normalizeConfigSetting(setting ConfigSetting) (ConfigSetting, error) {
	section, id, hasID := strings.Cut(setting.Section, ".")
	if !slices.Contains([]string{"global", "mon", "mgr", "osd", "mds", "client"}, section) || (hasID && (section == "global" || !configIDPattern.MatchString(id))) {
		return setting, invalid(errors.New("invalid Ceph configuration section"))
	}
	setting.Name = strings.ReplaceAll(setting.Name, "-", "_")
	if !configNamePattern.MatchString(setting.Name) || strings.IndexFunc(setting.Value, unicode.IsControl) != -1 {
		return setting, invalid(errors.New("invalid Ceph configuration name or value"))
	}
	if setting.Mask != "" {
		parts := strings.Split(setting.Mask, "/")
		if len(parts) > 2 {
			return setting, invalid(errors.New("configuration mask accepts one location and one device class"))
		}
		var location, class string
		for _, part := range parts {
			kind, value, ok := strings.Cut(part, ":")
			if !ok || !configMaskKindPattern.MatchString(kind) || !crushLocationName.MatchString(value) {
				return setting, invalid(errors.New("invalid Ceph configuration mask"))
			}
			if kind == "class" {
				if class != "" {
					return setting, invalid(errors.New("duplicate configuration class mask"))
				}
				class = part
			} else {
				if location != "" {
					return setting, invalid(errors.New("duplicate configuration location mask"))
				}
				location = part
			}
		}
		if location != "" && class != "" {
			setting.Mask = location + "/" + class
		} else if location != "" {
			setting.Mask = location
		} else {
			setting.Mask = class
		}
	}
	return setting, nil
}

func configWho(setting ConfigSetting) string {
	if setting.Mask != "" {
		return setting.Section + "/" + setting.Mask
	}
	return setting.Section
}
func configOverrideKey(setting ConfigSetting) string { return configWho(setting) + "/" + setting.Name }
func findConfigEntry(entries []ConfigEntry, setting ConfigSetting) *ConfigEntry {
	for _, entry := range entries {
		if entry.Section == setting.Section && entry.Mask == setting.Mask && entry.Name == setting.Name {
			copy := entry
			return &copy
		}
	}
	return nil
}

func (c *Container) configuration(ctx context.Context) ([]ConfigEntry, error) {
	return readConfiguration(ctx, c.cephCLI)
}

// decodeConfiguration decodes the output of config dump.
func decodeConfiguration(data []byte) ([]ConfigEntry, error) {
	var entries []ConfigEntry
	if err := json.Unmarshal(data, &entries); err != nil || entries == nil {
		return nil, errors.New("decode central Ceph configuration")
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		// Native module options contain paths, and arbitrary stored string values
		// need not satisfy the input restrictions of this fixture's setters.
		setting, err := normalizeConfigSetting(ConfigSetting{Section: entry.Section, Mask: entry.Mask, Name: entry.Name})
		if err != nil || setting.Mask != entry.Mask || setting.Name != entry.Name {
			return nil, errors.New("invalid stored Ceph configuration entry")
		}
		key := configOverrideKey(setting)
		if seen[key] {
			return nil, errors.New("duplicate stored Ceph configuration entry")
		}
		seen[key] = true
	}
	return entries, nil
}

func (c *Container) configCommand(ctx context.Context, args ...string) ([]byte, error) {
	return runConfigCommand(ctx, c.cephCLI, args...)
}
