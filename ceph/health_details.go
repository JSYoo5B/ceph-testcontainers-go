package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// HealthSnapshot is one MON health observation. Status reflects the worst
// unmuted check; HEALTH_OK can therefore include muted warnings or errors.
// Checks retain native, case-sensitive codes, including unknown future codes.
type HealthSnapshot struct {
	FSID, Status string
	Checks       map[string]HealthCheck
	Mutes        []HealthMute
}

// HealthCheck reports the native summary count and detail messages. Count is
// signed and need not equal the number of details, which Ceph may truncate.
type HealthCheck struct {
	Severity, Summary string
	Count             int64
	Details           []string
	Muted             bool
}

// HealthMute is a native stored mute, which can outlive its current check.
// ExpiresAt is empty for no TTL, otherwise the original Ceph timestamp:
// YYYY-MM-DDTHH:MM:SS.ffffff+HHMM (or -HHMM), or seconds.ffffff for an early
// epoch below 315360000 seconds. Expiry is not compared with the caller's clock;
// an expired or cleared nonsticky mute can remain visible before the MON tick.
type HealthMute struct {
	Code, Summary, ExpiresAt string
	Count                    int64
	Sticky                   bool
}

// HealthDetails reads the original cluster's health through one captured
// control CLI handle, checking the bootstrap FSID before and after the query.
// It works before storage or MGR readiness, performs no writes or retries and
// returns a zero snapshot on every failure, including post-query cancellation.
// Native command output is omitted from errors because it may contain secrets.
// External native/configuration writers must not race this observation: the
// three reads are not an atomic transaction. Poll with a caller deadline when
// asserting eventual health after changing a test topology.
func (c *Container) HealthDetails(ctx context.Context) (HealthSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return HealthSnapshot{}, err
	}
	if c == nil {
		return HealthSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return HealthSnapshot{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return HealthSnapshot{}, errors.New("ceph cluster is terminated")
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return HealthSnapshot{}, err
	}
	if control == nil {
		return HealthSnapshot{}, errors.New("ceph control container is unavailable")
	}
	if err := lockTopologyReadMutex(ctx, &c.configMu); err != nil {
		return HealthSnapshot{}, err
	}
	fsid, identityErr := monitorConfigClusterIdentity(c.config)
	confirmed := len(c.keyring) != 0
	c.configMu.RUnlock()
	if identityErr != nil || !confirmed {
		return HealthSnapshot{}, clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identityErr)
	}
	query := func(args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		if err != nil || ctx.Err() != nil {
			return nil, clientOperationError(ctx, "observe Ceph health: native query failed", err)
		}
		return data, nil
	}
	checkFSID := func() error {
		data, err := query("fsid")
		if err != nil {
			return err
		}
		native := strings.TrimSpace(string(data))
		parsed, err := uuid.Parse(native)
		if err != nil || parsed == uuid.Nil || parsed.String() != native || native != fsid {
			return clientOperationError(ctx, "native health FSID differs from the original cluster", err)
		}
		return ctx.Err()
	}
	if err := checkFSID(); err != nil {
		return HealthSnapshot{}, err
	}
	data, err := query("health", "detail", "--format", "json")
	if err != nil {
		return HealthSnapshot{}, err
	}
	snapshot, err := decodeHealthDetails(data, fsid)
	if err != nil || ctx.Err() != nil {
		return HealthSnapshot{}, clientOperationError(ctx, "decode native Ceph health details failed", err)
	}
	if err := checkFSID(); err != nil {
		return HealthSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return HealthSnapshot{}, err
	}
	return snapshot, nil
}

func decodeHealthDetails(data []byte, fsid string) (HealthSnapshot, error) {
	var native struct {
		Status *string `json:"status"`
		Checks *map[string]struct {
			Severity *string `json:"severity"`
			Summary  *struct {
				Message *string `json:"message"`
				Count   *int64  `json:"count"`
			} `json:"summary"`
			Detail *[]struct {
				Message *string `json:"message"`
			} `json:"detail"`
			Muted *bool `json:"muted"`
		} `json:"checks"`
		Mutes *[]struct {
			Code    *string `json:"code"`
			Summary *string `json:"summary"`
			Count   *int64  `json:"count"`
			Sticky  *bool   `json:"sticky"`
			TTL     *string `json:"ttl"`
		} `json:"mutes"`
	}
	failure := func() (HealthSnapshot, error) {
		return HealthSnapshot{}, errors.New("incomplete or invalid native health details")
	}
	if err := healthDetailsJSON(data, &native); err != nil || native.Status == nil || native.Checks == nil || native.Mutes == nil {
		return failure()
	}
	if _, valid := healthDetailsSeverity(*native.Status); !valid {
		return failure()
	}
	result := HealthSnapshot{FSID: fsid, Status: *native.Status,
		Checks: make(map[string]HealthCheck, len(*native.Checks)), Mutes: make([]HealthMute, 0, len(*native.Mutes))}
	mutedCodes := make(map[string]bool, len(*native.Mutes))
	for _, mute := range *native.Mutes {
		if mute.Code == nil || *mute.Code == "" || mute.Summary == nil || mute.Count == nil || mute.Sticky == nil || mutedCodes[*mute.Code] {
			return failure()
		}
		expires := ""
		if mute.TTL != nil {
			if !healthDetailsExpiry(*mute.TTL) {
				return failure()
			}
			expires = *mute.TTL
		}
		mutedCodes[*mute.Code] = true
		result.Mutes = append(result.Mutes, HealthMute{Code: *mute.Code, Summary: *mute.Summary,
			Count: *mute.Count, Sticky: *mute.Sticky, ExpiresAt: expires})
	}
	worst := 0
	for code, check := range *native.Checks {
		if code == "" || check.Severity == nil || check.Summary == nil || check.Summary.Message == nil || check.Summary.Count == nil || check.Detail == nil || check.Muted == nil {
			return failure()
		}
		severity, valid := healthDetailsSeverity(*check.Severity)
		if !valid || *check.Muted != mutedCodes[code] {
			return failure()
		}
		if !*check.Muted {
			worst = max(worst, severity)
		}
		details := make([]string, 0, len(*check.Detail))
		for _, detail := range *check.Detail {
			if detail.Message == nil {
				return failure()
			}
			details = append(details, *detail.Message)
		}
		result.Checks[code] = HealthCheck{Severity: *check.Severity, Summary: *check.Summary.Message,
			Count: *check.Summary.Count, Details: details, Muted: *check.Muted}
	}
	status, _ := healthDetailsSeverity(result.Status)
	if status != worst {
		return failure()
	}
	return result, nil
}

func healthDetailsSeverity(value string) (int, bool) {
	switch value {
	case "HEALTH_OK":
		return 0, true
	case "HEALTH_WARN":
		return 1, true
	case "HEALTH_ERR":
		return 2, true
	default:
		return 0, false
	}
}

var healthDetailsAbsoluteExpiry = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}[+-][0-9]{4}$`)
var healthDetailsEarlyExpiry = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})\.[0-9]{6}$`)

func healthDetailsExpiry(value string) bool {
	if healthDetailsEarlyExpiry.MatchString(value) {
		seconds, err := strconv.ParseUint(strings.SplitN(value, ".", 2)[0], 10, 32)
		return err == nil && seconds < 315360000
	}
	if !healthDetailsAbsoluteExpiry.MatchString(value) {
		return false
	}
	// Go permits a numeric offset of exactly 24 hours; native %z does not
	// emit an hour/minute outside the ordinary clock range.
	hours, _ := strconv.Atoi(value[len(value)-4 : len(value)-2])
	minutes, _ := strconv.Atoi(value[len(value)-2:])
	if hours > 23 || minutes > 59 {
		return false
	}
	_, err := time.Parse("2006-01-02T15:04:05.000000-0700", value)
	return err == nil
}

// Validate object keys before encoding/json's case-insensitive struct matching
// can adopt an alias. Map keys are case-sensitive native health codes; their
// values still receive the full nested struct validation. Unknown fields are
// allowed, but duplicate keys, known nulls and trailing documents are not.
func healthDetailsJSON(data []byte, value any) error {
	if len(data) == 0 || len(data) > 4<<20 {
		return errors.New("invalid health details JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func(reflect.Type, int) error
	walk = func(expected reflect.Type, depth int) error {
		if depth > 256 {
			return errors.New("health details JSON nesting exceeds limit")
		}
		for expected != nil && expected.Kind() == reflect.Pointer {
			expected = expected.Elem()
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token == nil && expected != nil {
			return errors.New("null native health details field")
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			fields := map[string]reflect.Type{}
			if expected != nil && expected.Kind() == reflect.Struct {
				for i := 0; i < expected.NumField(); i++ {
					field := expected.Field(i)
					name := strings.Split(field.Tag.Get("json"), ",")[0]
					if name != "" && name != "-" {
						fields[name] = field.Type
					}
				}
			}
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate native health details field")
				}
				seen[name] = true
				next := fields[name]
				if expected != nil && expected.Kind() == reflect.Map {
					next = expected.Elem()
				} else if next == nil {
					for canonical := range fields {
						if strings.EqualFold(name, canonical) {
							return errors.New("noncanonical native health details field")
						}
					}
				}
				if err := walk(next, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid native health details object")
			}
		case '[':
			var next reflect.Type
			if expected != nil && (expected.Kind() == reflect.Slice || expected.Kind() == reflect.Array) {
				next = expected.Elem()
			}
			for decoder.More() {
				if err := walk(next, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid native health details array")
			}
		default:
			return errors.New("invalid native health details delimiter")
		}
		return nil
	}
	if err := walk(reflect.TypeOf(value), 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing native health details JSON")
	}
	return json.Unmarshal(data, value)
}
