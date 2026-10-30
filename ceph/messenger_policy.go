package ceph

import (
	"context"
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
//
// A client that offers no accepted mode fails at once against a MON (EACCES;
// the ceph CLI prints RADOS permission denied and exits with 13), but gets no
// error from a MGR or an OSD and keeps retrying until its own timeout, so bound
// such calls. The MON client's handle_auth_bad_method log line is not reliable
// in the ceph CLI's output, which exits without shutting librados down.
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

// applyMessengerPolicy stores the bootstrap policy in the central
// configuration.
func (c *Container) applyMessengerPolicy(ctx context.Context) error {
	settings, err := c.settings.messengerPolicy.settings()
	if err != nil {
		return err
	}
	for _, setting := range settings {
		if _, err := runConfigCommand(ctx, c.conn().ceph, "set", setting.Section, setting.Name, setting.Value); err != nil {
			return fmt.Errorf("set %s %s: %w", setting.Section, setting.Name, err)
		}
	}
	return nil
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

// ClientContainerOptions is the configuration that WithClient and
// ClientConfig.WithIdentity write into a client container. It is a plain value with
// chainable, value-returning methods; the zero value changes nothing. Within
// one value a later call of the same method replaces the earlier one. A field
// set by two values is refused with ErrInvalidRequest, even when they agree.
//
//	cluster.WithClient(ceph.ClientContainerOptions{}.WithMessengerPolicy(policy))
type ClientContainerOptions struct {
	messengerPolicy    ClientMessengerPolicy
	messengerPolicySet bool
}

// WithMessengerPolicy writes the policy into the [client] section of the
// client container's ceph.conf. It applies only to the Ceph clients in that
// container, overrides the central configuration and MessengerV2Secure's
// global modes, and leaves the cluster's own control CLI alone. The value keeps
// its own copy of the lists; a nil list keeps Ceph's default and a non-nil
// empty list is refused.
func (o ClientContainerOptions) WithMessengerPolicy(policy ClientMessengerPolicy) ClientContainerOptions {
	o.messengerPolicy = ClientMessengerPolicy{MonitorsAndManagers: slices.Clone(policy.MonitorsAndManagers), OtherDaemons: slices.Clone(policy.OtherDaemons)}
	o.messengerPolicySet = true
	return o
}

// clientContainerConfig merges and validates configs and applies them to a
// client container's config.
func clientContainerConfig(config []byte, configs []ClientContainerOptions) ([]byte, error) {
	var lines []string
	set := false
	for _, c := range configs {
		if !c.messengerPolicySet {
			continue
		}
		if set {
			return nil, invalidRequest("client Messenger policy set by more than one ClientContainerOptions")
		}
		var err error
		if lines, err = c.messengerPolicy.configLines(); err != nil {
			return nil, invalid(fmt.Errorf("client messenger policy: %w", err))
		}
		set = true
	}
	return withConfigSectionValues(config, "client", lines), nil
}
