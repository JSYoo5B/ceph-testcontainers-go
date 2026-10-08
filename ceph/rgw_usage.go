package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// RGWUserUsage is a native storage-accounting snapshot, not dated request or
// bandwidth usage. Scope is "user" for an ordinary user and "account" for an
// account identity. OwnerID identifies that effective aggregation owner; account
// stats cover all of the account's users, roles and buckets, not only UserID.
// Native accounting updates asynchronously; this snapshot does not force sync,
// flush quota caches, prove quota enforcement or verify object content.
type RGWUserUsage struct {
	UserID, Tenant, Scope, OwnerID string
	// SizeBytes is native size; SizeActualBytes is native size_actual (rounded
	// accounting bytes), not physical allocation or replica/billing bytes.
	SizeBytes, SizeActualBytes, NumObjects uint64
	// Preserve native timestamp strings, including native zero timestamps.
	LastStatsSync, LastStatsUpdate string
}

// UserUsage reads current storage counters for an owned user's effective native
// owner. For an account root it returns the account aggregate with Scope account
// and OwnerID equal to the originally owned account ID. It rechecks the user key,
// type, account lifetime and scope before returning; no user, account, bucket or
// statistics are created, changed, synchronized, reset or adopted.
func (g *RGWContainer) UserUsage(ctx context.Context, user *RGWUser) (RGWUserUsage, error) {
	if g == nil || g.owner == nil {
		return RGWUserUsage{}, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	if err := g.owner.lockTopology(ctx); err != nil {
		return RGWUserUsage{}, err
	}
	defer g.owner.mu.Unlock()
	if err := g.validateUsageGateway(); err != nil {
		return RGWUserUsage{}, err
	}
	before, err := g.ownedUser(ctx, user)
	if err != nil {
		return RGWUserUsage{}, rgwUserUsageReadError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return RGWUserUsage{}, err
	}
	// ownedUser already checked the current runtime scope. Use its captured
	// native IDs for this read without repeating the same service-map query.
	var data []byte
	if user.state.nativeScope != nil {
		data, err = g.placementCommand(ctx, *user.state.nativeScope, "user", "stats", "--uid", user.id)
	} else {
		data, err = g.adminCommand(ctx, "user", "stats", "--uid", user.id)
	}
	if err != nil {
		return RGWUserUsage{}, err
	}
	if err := ctx.Err(); err != nil {
		return RGWUserUsage{}, err
	}
	usage, err := decodeRGWUserUsage(data)
	if err != nil {
		return RGWUserUsage{}, err
	}
	after, err := g.ownedUser(ctx, user)
	if err != nil {
		return RGWUserUsage{}, rgwUserUsageReadError(ctx, err)
	}
	if before.info.ID != after.info.ID || before.info.Tenant != after.info.Tenant || before.info.Type != after.info.Type || before.info.AccountID != after.info.AccountID {
		return RGWUserUsage{}, errors.New("RGW user storage aggregation identity changed")
	}
	if err := g.validateUsageGateway(); err != nil {
		return RGWUserUsage{}, err
	}
	if err := ctx.Err(); err != nil {
		return RGWUserUsage{}, err
	}
	usage.UserID, usage.Tenant = after.info.ID, after.info.Tenant
	usage.Scope, usage.OwnerID = "user", after.info.ID
	if after.info.AccountID != "" {
		usage.Scope, usage.OwnerID = "account", after.info.AccountID
	}
	return usage, nil
}

// Service-map/control reads in the existing identity guard may contain native
// output in an error. Only canonical context causes may leave this check.
func rgwUserUsageReadError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("RGW user storage identity read failed; native output is redacted")
}

// Only the originally registered container handle may run these typed reads.
// A public embedded handle replacement with the same CID is not adopted.
func (g *RGWContainer) validateUsageGateway() error {
	service := g.owner.services[rgwServiceName(g.config)]
	if diagnosticContainerHandle(g.Container) == nil || diagnosticContainerHandle(service) == nil {
		return errors.New("RGW gateway is no longer owned by this cluster")
	}
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	value := reflect.ValueOf(service)
	if !value.IsValid() || !value.Comparable() || service != g.Container {
		return errors.New("RGW gateway original container handle changed")
	}
	return nil
}

func decodeRGWUserUsage(data []byte) (RGWUserUsage, error) {
	bad := errors.New("decode RGW user storage usage")
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := rgwUsageJSONValue(decoder)
	if err != nil {
		return RGWUserUsage{}, bad
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return RGWUserUsage{}, bad
	}
	root, ok := value.(map[string]any)
	if !ok {
		return RGWUserUsage{}, bad
	}
	stats, ok := root["stats"].(map[string]any)
	if !ok {
		return RGWUserUsage{}, bad
	}
	var usage RGWUserUsage
	for name, dest := range map[string]*uint64{"size": &usage.SizeBytes, "size_actual": &usage.SizeActualBytes, "num_objects": &usage.NumObjects} {
		number, ok := stats[name].(json.Number)
		if !ok {
			return RGWUserUsage{}, bad
		}
		*dest, err = strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return RGWUserUsage{}, bad
		}
	}
	for name, dest := range map[string]*string{"last_stats_sync": &usage.LastStatsSync, "last_stats_update": &usage.LastStatsUpdate} {
		text, ok := root[name].(string)
		if !ok || !rgwUsageNativeTime(text) {
			return RGWUserUsage{}, bad
		}
		*dest = text
	}
	return usage, nil
}

// v20.2.4 encode_json(utime_t) uses gmtime: raw seconds below ten years,
// otherwise UTC with six fractional digits. Preserve zero and reject arbitrary
// native strings instead of publishing malformed data as a timestamp.
func rgwUsageNativeTime(text string) bool {
	const absoluteFrom = 60 * 60 * 24 * 365 * 10
	if seconds, fraction, ok := strings.Cut(text, "."); ok && len(fraction) == 6 {
		for _, digit := range fraction {
			if digit < '0' || digit > '9' {
				return false
			}
		}
		value, err := strconv.ParseUint(seconds, 10, 32)
		return err == nil && value < absoluteFrom && strconv.FormatUint(value, 10) == seconds
	}
	const layout = "2006-01-02T15:04:05.000000Z"
	value, err := time.Parse(layout, text)
	return err == nil && value.Format(layout) == text && value.Unix() >= absoluteFrom && value.Unix() <= 1<<32-1
}

// Detect duplicate members in every object, including unknown future fields.
// Native data and decoder errors are never copied into a returned error.
func rgwUsageJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			name, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := name.(string)
			if !ok {
				return nil, errors.New("invalid JSON object key")
			}
			if _, duplicate := object[key]; duplicate {
				return nil, errors.New("duplicate JSON object key")
			}
			value, err := rgwUsageJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
			return nil, errors.New("invalid JSON object ending")
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := rgwUsageJSONValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
			return nil, errors.New("invalid JSON array ending")
		}
		return array, nil
	default:
		return nil, errors.New("invalid JSON delimiter")
	}
}
