package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

// userConfigMaxBytes bounds each WithConfigFile input. The merged bootstrap
// file must stay well below monitorConfigMaxBytes for later MON refreshes.
const userConfigMaxBytes = 64 << 10

// userConfigPath is read by mon.sh before the first MON creates its store.
const userConfigPath = "/tc/ceph-user.conf"

var (
	userConfigSectionName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	userConfigKeyName     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_]{0,127}$`)
)

type userConfigEntry struct {
	key, value string
}

type userConfigSection struct {
	name    string
	entries []userConfigEntry
}

// WithConfigFile merges a native ceph.conf file from the test host into the
// bootstrap configuration of every daemon and WithClient container, like the
// WithConfigFile options of testcontainers-go modules. Entries replace the
// fixture's defaults for the same section and key, and later files override
// earlier files key by key. Ceph gives local files precedence over the MON
// configuration database, so TemporaryConfig cannot change a key set here.
// Keys that the fixture owns or passes on a daemon command line are rejected:
// identity, addresses, networks, Cephx, Messenger modes, logging, data paths,
// pool size defaults and BlueStore file layout. Use the dedicated options for
// those. RGW runs as client.admin, so RGW settings belong in [client] or
// [client.admin]. Includes, continuations and keys outside a section are
// rejected.
func WithConfigFile(path string) Option {
	return func(o *options) error {
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("read Ceph config file: %w", err)
		}
		data, readErr := readLimited(file, userConfigMaxBytes)
		if err := errors.Join(readErr, file.Close()); err != nil {
			return fmt.Errorf("read Ceph config file %s: %w", path, err)
		}
		sections, err := parseUserConfig(data)
		if err != nil {
			return fmt.Errorf("Ceph config file %s: %w", path, err)
		}
		merged := mergeUserConfig(o.userConfig, sections)
		if size := len(renderUserConfig(merged)); size > userConfigMaxBytes {
			return fmt.Errorf("merged Ceph config files exceed %d bytes", userConfigMaxBytes)
		}
		o.userConfig = merged
		return nil
	}
}

func readLimited(reader io.Reader, limit int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, fmt.Errorf("file exceeds %d bytes", limit)
	}
	return data, nil
}

func parseUserConfig(data []byte) ([]userConfigSection, error) {
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return nil, errors.New("file must be NUL-free UTF-8 text")
	}
	var sections []userConfigSection
	current := -1
	for number, line := range strings.Split(string(data), "\n") {
		number++
		line = strings.TrimSuffix(line, "\r")
		comment, err := cephConfigComment(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", number, err)
		}
		if comment >= 0 {
			line = line[:comment]
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			name, ok := strings.CutSuffix(trimmed[1:], "]")
			name = strings.TrimSpace(name)
			if !ok || !userConfigSectionName.MatchString(name) {
				return nil, fmt.Errorf("line %d: invalid section header %q", number, trimmed)
			}
			current = slices.IndexFunc(sections, func(section userConfigSection) bool { return section.name == name })
			if current < 0 {
				sections = append(sections, userConfigSection{name: name})
				current = len(sections) - 1
			}
			continue
		}
		if strings.HasPrefix(trimmed, "!") {
			return nil, fmt.Errorf("line %d: include directives are not supported", number)
		}
		rawKey, value, hasValue := strings.Cut(trimmed, "=")
		if !hasValue {
			return nil, fmt.Errorf("line %d: expected key = value", number)
		}
		if current < 0 {
			return nil, fmt.Errorf("line %d: setting appears before any [section]", number)
		}
		key := normalizeConfigKey(rawKey)
		if !userConfigKeyName.MatchString(key) {
			return nil, fmt.Errorf("line %d: invalid key %q", number, strings.TrimSpace(rawKey))
		}
		if reason := reservedConfigKey(key); reason != "" {
			return nil, fmt.Errorf("line %d: [%s] %s %s", number, sections[current].name, key, reason)
		}
		section := &sections[current]
		if slices.ContainsFunc(section.entries, func(entry userConfigEntry) bool { return entry.key == key }) {
			return nil, fmt.Errorf("line %d: duplicate [%s] %s", number, section.name, key)
		}
		entry := userConfigEntry{key: key, value: strings.TrimSpace(value)}
		// Ceph ends a value at # or ; even inside quotes. Refuse quoting that
		// the fixture's own config rewriters would read differently.
		if comment, err := monitorConfigComment(entry.render()); err != nil || comment >= 0 {
			return nil, fmt.Errorf("line %d: ambiguous quoting in [%s] %s", number, section.name, key)
		}
		section.entries = append(section.entries, entry)
	}
	return slices.DeleteFunc(sections, func(section userConfigSection) bool { return len(section.entries) == 0 }), nil
}

// cephConfigComment finds the first unescaped # or ;. Unlike
// monitorConfigComment it ignores quotes, matching ceph-conf.
func cephConfigComment(line string) (int, error) {
	escaped := false
	for index := range len(line) {
		switch {
		case escaped:
			escaped = false
		case line[index] == '\\':
			escaped = true
		case line[index] == '#' || line[index] == ';':
			return index, nil
		}
	}
	if escaped {
		return -1, errors.New("line continuations are not supported")
	}
	return -1, nil
}

func (entry userConfigEntry) render() string { return entry.key + " = " + entry.value }

// normalizeConfigKey applies Ceph's equivalence of spaces, dashes and
// underscores in option names.
func normalizeConfigKey(key string) string {
	return strings.Join(strings.Fields(strings.NewReplacer("_", " ", "-", " ").Replace(key)), "_")
}

var reservedConfigKeys = map[string]string{
	"fsid":                        "is generated by the fixture",
	"mon_host":                    "is managed by the fixture",
	"mon_initial_members":         "is managed by the fixture; use WithMonitorCount",
	"mon_addr":                    "is managed by the fixture",
	"public_network":              "is managed by the fixture; use WithHostNetwork or WithSeparateClusterNetwork",
	"cluster_network":             "is managed by the fixture; use WithSeparateClusterNetwork",
	"public_addr":                 "is managed by the fixture; use WithHostAddress",
	"cluster_addr":                "is managed by the fixture; use WithHostAddress",
	"public_bind_addr":            "is managed by the fixture",
	"cluster_bind_addr":           "is managed by the fixture",
	"keyring":                     "is managed by the fixture's Cephx setup",
	"key":                         "is managed by the fixture's Cephx setup",
	"keyfile":                     "is managed by the fixture's Cephx setup",
	"include":                     "is not supported",
	"includedir":                  "is not supported",
	"admin_socket":                "is managed by the fixture",
	"run_dir":                     "is managed by the fixture",
	"mon_data":                    "is managed by the fixture",
	"osd_data":                    "is managed by the fixture",
	"mgr_data":                    "is managed by the fixture",
	"mds_data":                    "is managed by the fixture",
	"log_file":                    "is managed by the fixture; daemon logs go to container output",
	"log_to_file":                 "is managed by the fixture; daemon logs go to container output",
	"log_to_stderr":               "is managed by the fixture; daemon logs go to container output",
	"err_to_stderr":               "is managed by the fixture; daemon logs go to container output",
	"mon_cluster_log_to_file":     "is managed by the fixture; daemon logs go to container output",
	"mon_cluster_log_to_stderr":   "is managed by the fixture; daemon logs go to container output",
	"osd_pool_default_size":       "is managed by the fixture; use WithPoolDefaults",
	"osd_pool_default_min_size":   "is managed by the fixture; use WithPoolDefaults",
	"osd_pool_default_crush_rule": "is managed by the fixture; use WithDefaultCRUSHRoot",
	"osd_objectstore":             "is managed by the fixture",
	"bluestore_block_path":        "is managed by the fixture",
	"bluestore_block_create":      "is managed by the fixture",
	"bluestore_block_size":        "is managed by the fixture; use WithOSDBlockSize",
	"mds_join_fs":                 "is set on the MDS command line by the fixture",
	"mds_cache_memory_limit":      "is set on the MDS command line by the fixture",
	"rgw_frontends":               "is set on the RGW command line by the fixture",
	"rgw_thread_pool_size":        "is set on the RGW command line by the fixture",
	"rgw_exit_timeout_secs":       "is set on the RGW command line by the fixture",
	"rgw_realm":                   "is set on the RGW command line by the fixture",
	"rgw_zonegroup":               "is set on the RGW command line by the fixture",
	"rgw_zone":                    "is set on the RGW command line by the fixture",
	"rgw_sync_obj_etag_verify":    "is set on the RGW command line by the fixture",
	"rgw_sync_lease_period":       "is set on the RGW command line by the fixture",
}

var reservedMessengerMode = regexp.MustCompile(`^ms_(mon_)?(cluster|service|client)_mode$`)

func reservedConfigKey(key string) string {
	switch reason, ok := reservedConfigKeys[key]; {
	case ok:
		return reason
	case strings.HasPrefix(key, "auth_"):
		return "is managed by the fixture's Cephx setup"
	case reservedMessengerMode.MatchString(key):
		return "is managed by the fixture; use WithMessengerMode"
	case strings.HasPrefix(key, "ms_bind_"):
		return "is managed by the fixture; use WithMessengerMode or WithHostNetwork"
	}
	return ""
}

// mergeUserConfig applies later sections over earlier ones key by key while
// keeping first-seen section and key order for a stable rendered file.
func mergeUserConfig(base, overlay []userConfigSection) []userConfigSection {
	merged := slices.Clone(base)
	for _, section := range overlay {
		index := slices.IndexFunc(merged, func(existing userConfigSection) bool { return existing.name == section.name })
		if index < 0 {
			merged = append(merged, userConfigSection{name: section.name, entries: slices.Clone(section.entries)})
			continue
		}
		entries := slices.Clone(merged[index].entries)
		for _, entry := range section.entries {
			if existing := slices.IndexFunc(entries, func(old userConfigEntry) bool { return old.key == entry.key }); existing >= 0 {
				entries[existing] = entry
			} else {
				entries = append(entries, entry)
			}
		}
		merged[index].entries = entries
	}
	return merged
}

// renderUserConfig writes one header per section and one normalized key per
// line. mon.sh relies on that shape when it merges into its generated file.
func renderUserConfig(sections []userConfigSection) []byte {
	var buffer bytes.Buffer
	for _, section := range sections {
		fmt.Fprintf(&buffer, "[%s]\n", section.name)
		for _, entry := range section.entries {
			buffer.WriteString(entry.render() + "\n")
		}
	}
	return buffer.Bytes()
}
