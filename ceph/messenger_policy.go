package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// MessengerConnectionMode is one Messenger v2 connection mode.
type MessengerConnectionMode string

const (
	// MessengerCRC checks frame integrity with CRC32C and does not encrypt.
	MessengerCRC MessengerConnectionMode = "crc"
	// MessengerSecure encrypts frames with AES-GCM.
	MessengerSecure MessengerConnectionMode = "secure"
)

// messengerModeList returns the native value of a mode list, keeping its
// order, or "" for nil, which keeps Ceph's default.
func messengerModeList(field string, modes []MessengerConnectionMode) (string, error) {
	if modes == nil {
		return "", nil
	}
	if len(modes) == 0 {
		return "", fmt.Errorf("%s lists no Messenger connection mode; use nil for the default", field)
	}
	values := make([]string, len(modes))
	for i, mode := range modes {
		if mode != MessengerCRC && mode != MessengerSecure {
			return "", fmt.Errorf("%s has unknown Messenger connection mode %q; use crc or secure", field, mode)
		}
		if slices.Contains(modes[:i], mode) {
			return "", fmt.Errorf("%s lists Messenger connection mode %s twice", field, mode)
		}
		values[i] = string(mode)
	}
	return strings.Join(values, " "), nil
}

// MessengerPolicy selects, for each daemon type, the Messenger v2 connection
// modes that it accepts from clients. The order of a list does not matter:
// the connection uses the first mode in the client's order of preference that
// the daemon accepts. A nil field keeps Ceph's default, which accepts both
// modes. Connections between daemons keep their defaults, so every policy
// leaves the cluster itself working.
type MessengerPolicy struct {
	// Monitors sets ms_mon_service_mode for the MONs.
	Monitors []MessengerConnectionMode
	// Managers sets ms_mon_service_mode for the MGRs: Ceph applies the MON
	// option to connections from clients to MGRs.
	Managers []MessengerConnectionMode
	// OSDs sets ms_service_mode for the OSDs.
	OSDs []MessengerConnectionMode
}

// settings returns the central configuration entries of the policy.
func (policy MessengerPolicy) settings() ([]ConfigSetting, error) {
	var settings []ConfigSetting
	for _, entry := range []struct {
		field, section, name string
		modes                []MessengerConnectionMode
	}{
		{"Monitors", "mon", "ms_mon_service_mode", policy.Monitors},
		{"Managers", "mgr", "ms_mon_service_mode", policy.Managers},
		{"OSDs", "osd", "ms_service_mode", policy.OSDs},
	} {
		value, err := messengerModeList(entry.field, entry.modes)
		if err != nil {
			return nil, err
		}
		if value != "" {
			settings = append(settings, ConfigSetting{Section: entry.section, Name: entry.name, Value: value})
		}
	}
	return settings, nil
}

// WithMessengerPolicy selects the connection modes that MONs, MGRs and OSDs
// accept from clients. Run stores the policy in the central configuration
// right after the first MON starts, before any other daemon, so every daemon
// starts with it. OverrideMessengerPolicy changes it on a running cluster.
// It cannot be combined with MessengerV2Secure, whose local configuration
// fixes every mode.
func WithMessengerPolicy(policy MessengerPolicy) Option {
	return func(o *options) error {
		if _, err := policy.settings(); err != nil {
			return fmt.Errorf("messenger policy: %w", err)
		}
		o.messengerPolicy = MessengerPolicy{Monitors: slices.Clone(policy.Monitors), Managers: slices.Clone(policy.Managers), OSDs: slices.Clone(policy.OSDs)}
		return nil
	}
}

// applyMessengerPolicy stores the bootstrap policy in the central
// configuration.
func (c *Container) applyMessengerPolicy(ctx context.Context) error {
	settings, err := c.settings.messengerPolicy.settings()
	if err != nil {
		return err
	}
	for _, setting := range settings {
		if _, err := c.configCommand(ctx, "set", setting.Section, setting.Name, setting.Value); err != nil {
			return fmt.Errorf("set %s %s: %w", setting.Section, setting.Name, err)
		}
	}
	return nil
}

// MessengerPolicyOverride holds the configuration overrides of one
// OverrideMessengerPolicy call.
type MessengerPolicyOverride struct {
	overrides []*ConfigOverride
}

// OverrideMessengerPolicy changes the connection modes that the running
// daemons accept from clients. Each non-nil field becomes a OverrideConfig
// override of the policy's option; nil fields stay as they are. The daemons
// apply the change to new connections without a restart, and established
// connections keep their mode. A non-nil result returned with an error holds
// the overrides that were made and must be restored. A policy that selects no
// valid mode, and any policy with MessengerV2Secure, is refused with
// ErrInvalidRequest before any change.
func (c *Container) OverrideMessengerPolicy(ctx context.Context, policy MessengerPolicy) (*MessengerPolicyOverride, error) {
	if gateErr := c.sharedGate("OverrideMessengerPolicy"); gateErr != nil {
		return nil, gateErr
	}
	settings, err := policy.settings()
	if err != nil {
		return nil, invalid(fmt.Errorf("messenger policy: %w", err))
	}
	if len(settings) == 0 {
		return nil, invalidRequest("messenger policy selects no mode")
	}
	if c != nil && c.settings.messengerMode == MessengerV2Secure {
		return nil, invalidRequest("MessengerV2Secure fixes every connection mode in the local configuration")
	}
	result := &MessengerPolicyOverride{}
	for _, setting := range settings {
		override, err := c.OverrideConfig(ctx, setting)
		if override != nil {
			result.overrides = append(result.overrides, override)
		}
		if err != nil {
			if len(result.overrides) == 0 {
				return nil, err
			}
			return result, err
		}
	}
	return result, nil
}

// Restore returns every option of the policy to its previous value, in
// reverse order. It is idempotent and can be retried after a failure.
func (override *MessengerPolicyOverride) Restore(ctx context.Context) error {
	if override == nil {
		return errors.New("messenger policy override is unavailable")
	}
	var errs []error
	for _, change := range slices.Backward(override.overrides) {
		errs = append(errs, change.Restore(ctx))
	}
	return errors.Join(errs...)
}

// ClientMessengerPolicy selects the Messenger v2 connection modes that a
// native client offers, in order of preference. A nil field keeps Ceph's
// default, which offers both modes.
type ClientMessengerPolicy struct {
	// MonitorsAndManagers sets ms_mon_client_mode, which Ceph uses for
	// connections to MONs and MGRs.
	MonitorsAndManagers []MessengerConnectionMode
	// OtherDaemons sets ms_client_mode, which Ceph uses for connections to
	// the other daemons, such as OSDs.
	OtherDaemons []MessengerConnectionMode
}

// configLines returns the [client] section lines of the policy.
func (policy ClientMessengerPolicy) configLines() ([]string, error) {
	var lines []string
	for _, entry := range []struct {
		field, name string
		modes       []MessengerConnectionMode
	}{
		{"MonitorsAndManagers", "ms mon client mode", policy.MonitorsAndManagers},
		{"OtherDaemons", "ms client mode", policy.OtherDaemons},
	} {
		value, err := messengerModeList(entry.field, entry.modes)
		if err != nil {
			return nil, err
		}
		if value != "" {
			lines = append(lines, entry.name+" = "+value)
		}
	}
	return lines, nil
}

// ClientContainerOption customizes the configuration that WithClient and
// WithClientIdentity write into a client container.
type ClientContainerOption func(*clientContainerOptions) error

type clientContainerOptions struct {
	messengerLines []string
}

// WithClientMessengerPolicy writes the policy into the [client] section of the
// client container's ceph.conf. It applies only to the Ceph clients in that
// container, overrides the central configuration and MessengerV2Secure's
// global modes, and leaves the cluster's own control CLI alone.
func WithClientMessengerPolicy(policy ClientMessengerPolicy) ClientContainerOption {
	return func(o *clientContainerOptions) error {
		lines, err := policy.configLines()
		if err != nil {
			return invalid(fmt.Errorf("client messenger policy: %w", err))
		}
		o.messengerLines = lines
		return nil
	}
}

// clientContainerConfig applies the options to a client container's config.
func clientContainerConfig(config []byte, opts []ClientContainerOption) ([]byte, error) {
	var settings clientContainerOptions
	for _, opt := range opts {
		if opt == nil {
			return nil, invalidRequest("client container option is nil")
		}
		if err := opt(&settings); err != nil {
			return nil, err
		}
	}
	return withConfigSectionValues(config, "client", settings.messengerLines), nil
}
