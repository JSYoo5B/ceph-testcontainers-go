package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
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

// ConfigOverride restores one exact database entry. Copies share lifecycle
// state. Restore explicitly when the test condition should end; cluster
// termination disposes of the entire configuration database. A non-nil handle
// returned with an error must also be restored or the cluster terminated.
type ConfigOverride struct {
	owner   *Container
	setting ConfigSetting
	state   *configOverrideState
}

type configOverrideState struct {
	previous *ConfigEntry
	applied  string
	known    bool
	restored bool
}

func (change ConfigOverride) String() string {
	return "Ceph config override " + configWho(change.setting) + " " + change.setting.Name
}
func (change ConfigOverride) GoString() string { return change.String() }

// Configuration lists exact central database entries, including masked entries.
// Use native config show/daemon config get to inspect a running daemon's effective
// value; this method does not claim a stored setting has overridden local config.
func (c *Container) Configuration(ctx context.Context) ([]ConfigEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return c.configuration(ctx)
}

// TemporaryConfig sets one central database entry and records its native
// normalized value. It preserves absence separately from inherited defaults,
// and rejects overlapping handles for the same section/mask/name. On command or
// readback failure, the handle remains tracked for Restore. Never race external
// writes to this key with application/restoration; Ceph has no config CAS.
// MessengerV2Secure reserves its six ms_*_mode options and ms_bind_msgr1/2 in
// every section and mask: central overrides cannot change the fixed local
// bootstrap policy. Select a MessengerMode when creating a new cluster instead.
func (c *Container) TemporaryConfig(ctx context.Context, setting ConfigSetting) (*ConfigOverride, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	setting, err := normalizeConfigSetting(setting)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("ceph cluster is unavailable")
	}
	if c.settings.messengerMode == MessengerV2Secure && messengerBootstrapSetting(setting.Name) {
		return nil, fmt.Errorf("%s is fixed in the MessengerV2Secure bootstrap configuration for every section and mask; choose WithMessengerMode when creating a new cluster", setting.Name)
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	key := configOverrideKey(setting)
	if previous := c.configOverrides[key]; previous != nil && !previous.state.restored {
		return nil, errors.New("restore the existing configuration override first")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	entries, err := c.configuration(ctx)
	if err != nil {
		return nil, err
	}
	change := &ConfigOverride{owner: c, setting: setting, state: &configOverrideState{previous: findConfigEntry(entries, setting)}}
	if c.configOverrides == nil {
		c.configOverrides = make(map[string]*ConfigOverride)
	}
	c.configOverrides[key] = change
	if change.state.previous != nil && change.state.previous.Value == setting.Value {
		change.state.applied, change.state.known = setting.Value, true
		return change, nil
	}
	_, setErr := c.configCommand(ctx, "set", configWho(setting), setting.Name, setting.Value)
	// A lost reply or cancellation cannot prove config set did not persist.
	// Record readback using a bounded fresh context, preserving the original
	// error. External writes must not race this native mutation/readback pair.
	readCtx, readCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer readCancel()
	entries, readErr := c.configuration(readCtx)
	if readErr == nil {
		if current := findConfigEntry(entries, setting); current != nil {
			change.state.applied, change.state.known = current.Value, true
		}
	}
	if setErr != nil || readErr != nil {
		return change, errors.Join(setErr, readErr)
	}
	if !change.state.known {
		return change, errors.New("configuration set returned success without a stored entry")
	}
	return change, nil
}

// Restore returns the exact entry to its prior value, or removes it if it was
// absent. It refuses a different current value to preserve outside edits. After
// an uncertain restore reply, retrying reconciles the native state first. Same
// value outside writes cannot be distinguished; external edits must not race.
func (change *ConfigOverride) Restore(ctx context.Context) error {
	if change == nil || change.owner == nil || change.state == nil {
		return errors.New("configuration override is unavailable")
	}
	c := change.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if change.state.restored {
		return nil
	}
	if err := c.poolPolicyReady(); err != nil {
		return err
	}
	if c.configOverrides[configOverrideKey(change.setting)] == nil || c.configOverrides[configOverrideKey(change.setting)].state != change.state {
		return errors.New("configuration override is not tracked by this cluster")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	entries, err := c.configuration(ctx)
	if err != nil {
		return err
	}
	current := findConfigEntry(entries, change.setting)
	if sameConfigValue(current, change.state.previous) {
		change.finish()
		return nil
	}
	expected := change.state.applied
	if !change.state.known {
		expected = change.setting.Value
		if current != nil && current.Value != expected {
			return errors.New("configuration apply readback was unavailable and the current value differs from the requested text; inspect native state or terminate the cluster")
		}
	}
	if current == nil || current.Value != expected {
		return errors.New("configuration entry changed outside this override; refusing restoration")
	}
	var restoreErr error
	if change.state.previous == nil {
		_, restoreErr = c.configCommand(ctx, "rm", configWho(change.setting), change.setting.Name)
	} else {
		_, restoreErr = c.configCommand(ctx, "set", configWho(change.setting), change.setting.Name, change.state.previous.Value)
	}
	if restoreErr != nil {
		return restoreErr
	}
	entries, err = c.configuration(ctx)
	if err != nil {
		return err
	}
	if !sameConfigValue(findConfigEntry(entries, change.setting), change.state.previous) {
		return errors.New("configuration restoration readback differs from previous entry")
	}
	change.finish()
	return nil
}

func (change *ConfigOverride) finish() {
	change.state.restored = true
	delete(change.owner.configOverrides, configOverrideKey(change.setting))
}

var configNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}(/[a-z][a-z0-9_]{0,127})*$`)
var configMaskKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)
var configIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func normalizeConfigSetting(setting ConfigSetting) (ConfigSetting, error) {
	section, id, hasID := strings.Cut(setting.Section, ".")
	if !slices.Contains([]string{"global", "mon", "mgr", "osd", "mds", "client"}, section) || (hasID && (section == "global" || !configIDPattern.MatchString(id))) {
		return setting, errors.New("invalid Ceph configuration section")
	}
	setting.Name = strings.ReplaceAll(setting.Name, "-", "_")
	if !configNamePattern.MatchString(setting.Name) || strings.IndexFunc(setting.Value, unicode.IsControl) != -1 {
		return setting, errors.New("invalid Ceph configuration name or value")
	}
	if setting.Mask != "" {
		parts := strings.Split(setting.Mask, "/")
		if len(parts) > 2 {
			return setting, errors.New("configuration mask accepts one location and one device class")
		}
		var location, class string
		for _, part := range parts {
			kind, value, ok := strings.Cut(part, ":")
			if !ok || !configMaskKindPattern.MatchString(kind) || !crushLocationName.MatchString(value) {
				return setting, errors.New("invalid Ceph configuration mask")
			}
			if kind == "class" {
				if class != "" {
					return setting, errors.New("duplicate configuration class mask")
				}
				class = part
			} else {
				if location != "" {
					return setting, errors.New("duplicate configuration location mask")
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
func sameConfigValue(a, b *ConfigEntry) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Value == b.Value
}
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
	data, err := c.configCommand(ctx, "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
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
	data, err := c.Ceph(ctx, append([]string{"config"}, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("Ceph configuration command: %w", ctx.Err())
		}
		return nil, errors.New("Ceph configuration command failed; native output is redacted")
	}
	return data, nil
}
