package ceph

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
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

// healthDetails implements Observer.HealthDetails.
func (c *Container) healthDetails(ctx context.Context) (HealthSnapshot, error) {
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
		return HealthSnapshot{}, errClusterTerminated
	}
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return HealthSnapshot{}, err
	}
	if control == nil {
		return HealthSnapshot{}, errors.New("ceph control container is unavailable")
	}
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return HealthSnapshot{}, err
	}
	fsid, identityErr := identity.fsid, identity.err
	if identityErr != nil || !identity.confirmed {
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

// healthDetailsJSON decodes the native health, PG and full-ratio reports. Known
// fields may not be null; map keys are case-sensitive native health codes whose
// values still receive the full nested validation.
func healthDetailsJSON(data []byte, value any) error {
	return strictjson.Decode(data, value, strictjson.Options{MaxBytes: 4 << 20, RejectNull: true})
}
