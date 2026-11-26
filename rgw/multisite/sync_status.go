package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// MetadataSyncStatus includes only native state/identity/shard counts. It
// omits metadata keys, markers and credentials from native diagnostic output.
type MetadataSyncStatus struct {
	State, PeriodID           string
	RealmEpoch                uint64
	Shards, IncrementalShards int
	CaughtUp                  bool
}

// DataSyncStatus summarizes the native comparison with one source's log.
// CaughtUp requires a running sync, complete incremental shards, no recovering
// shards and the native remote-log comparison. Disabled pipes do not count as
// caught up merely because an object's absence is expected.
type DataSyncStatus struct {
	SourceZone, SourceZoneID              string
	Syncing, CaughtUp                     bool
	Shards, IncrementalShards, FullShards int
	BehindShards, RecoveringShards        int
}

// SyncStatus is the metadata and data sync state of one attached zone, as
// Fixture.SyncStatus reads it.
type SyncStatus struct {
	periodEpoch                                  uint64
	RealmID, PeriodID, Zone, ZoneID, ZonegroupID string
	RealmEpoch                                   uint64
	MetadataMaster                               bool
	Metadata                                     MetadataSyncStatus
	DataSources                                  []DataSyncStatus
}

// SyncStatus observes native metadata/data readiness in an attached owned zone.
// Native human sync status is required for remote-log comparison even when the
// CLI is passed --format json. The structured metadata state separately guards
// period/realm epoch and every shard. This is a point-in-time observation;
// applications should quiesce writes and still verify their own S3 payloads.
//
//purpose:observation
func (f *Fixture) SyncStatus(ctx context.Context, zoneName string) (SyncStatus, error) {
	if f == nil {
		return SyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return SyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	return f.syncStatus(ctx, zoneName, false)
}

// WaitSyncReady waits up to four minutes, bounded by the caller's context, for
// metadata and the explicitly requested source-zone data streams to catch up.
// With no sources, the wait compares metadata with the current master
// and leaves DataSources empty. Setup images without Python or curl use
// combined status, which can block on an unavailable data peer.
// Use SyncStatus to inspect all data streams.
// When the wait ends unready it
// returns the latest complete observation, and its error reports that
// observation and the latest attempt's error. Native sync leases survive abrupt
// gateway restarts until expiration, so this method does not change lease TTLs,
// unlock objects or restart daemons to shorten normal recovery.
//
// Wait for metadata in every zone after creating a bucket and before writing
// objects into it. Otherwise RGW can lose those objects in another zone (seen
// with Tentacle 20.2.4): that zone's data sync stores the new bucket's metadata
// itself, a second data sync shard may sync the bucket before its index exists,
// and RGW takes the failed index updates for deleted objects; the zone then
// reports caught up while the objects stay missing until they change again.
// After an abrupt gateway stop the old process's sync leases must expire (30
// seconds for fixture gateways), and sync polls every 20 seconds by default:
// use deadlines of several minutes.
//
//purpose:observation
func (f *Fixture) WaitSyncReady(ctx context.Context, zoneName string, sourceZones ...string) (SyncStatus, error) {
	if f == nil {
		return SyncStatus{}, errors.New("RGW multisite fixture is required")
	}
	sourceZones = slices.Clone(sourceZones)
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return SyncStatus{}, err
	}
	if _, err := f.syncStatusZones(zoneName, sourceZones); err != nil {
		f.topologyMu.Unlock()
		return SyncStatus{}, err
	}
	pins := make(map[string]syncWaitZonePin, len(sourceZones)+1)
	for _, zone := range f.zoneStates() {
		if zone.Name == zoneName || slices.Contains(sourceZones, zone.Name) {
			pins[zone.Name] = syncWaitZonePin{id: zone.ID, client: zone.client}
		}
	}
	f.topologyMu.Unlock()
	// last is the latest complete observation; an attempt that failed, or
	// that the deadline cut short, does not replace it, and its error is
	// reported with it.
	var last SyncStatus
	var lastErr error
	expired := func(err error) (SyncStatus, error) {
		return last, fmt.Errorf("wait RGW zone %s sync readiness: %w; metadata_ready=%t; data=%+v; last_error=%v", zoneName, err, last.Metadata.CaughtUp, last.DataSources, lastErr)
	}
	for {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := multicluster.LockObservation(attemptCtx, &f.topologyMu); err != nil {
			attemptCancel()
			return expired(err)
		}
		if err := f.checkSyncWaitPins(pins); err != nil {
			f.topologyMu.Unlock()
			attemptCancel()
			return last, err
		}
		status, err := f.syncStatus(attemptCtx, zoneName, len(sourceZones) == 0)
		f.topologyMu.Unlock()
		attemptCancel()
		if lastErr = err; err == nil {
			last = status
		}
		if err := ctx.Err(); err != nil {
			return expired(err)
		}
		if lastErr == nil && syncStatusReady(last, sourceZones) {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return expired(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

type syncWaitZonePin struct {
	id     string
	client testcontainers.Container
}

func (f *Fixture) checkSyncWaitPins(pins map[string]syncWaitZonePin) error {
	if f.closed {
		return multisiteInvalid(errors.New("RGW multisite is terminated"))
	}
	for name, pin := range pins {
		attached := false
		for _, zone := range f.zoneStates() {
			if zone.Name == name && zone.ID == pin.id && zone.client != nil && zone.client == pin.client {
				attached = true
				break
			}
		}
		if !attached {
			return errors.New("RGW sync readiness zone is no longer attached and owned")
		}
	}
	return nil
}

func (f *Fixture) syncStatusZones(name string, sources []string) (*rgwZoneState, error) {
	if f.closed {
		return nil, multisiteInvalid(errors.New("RGW multisite is terminated"))
	}
	var target *rgwZoneState
	states := f.zoneStates()
	for _, zone := range states {
		if zone.Name == name && zone.client != nil {
			target = zone
		}
	}
	if target == nil {
		return nil, errors.New("RGW sync status requires an attached owned zone")
	}
	seen := map[string]bool{}
	for _, source := range sources {
		if source == name || seen[source] || !slices.ContainsFunc(states, func(z *rgwZoneState) bool { return z.Name == source && z.Zonegroup == target.Zonegroup }) {
			return nil, errors.New("RGW readiness data sources must be unique other owned zones in the same zonegroup")
		}
		seen[source] = true
	}
	return target, nil
}

func (f *Fixture) syncStatus(ctx context.Context, zoneName string, metadataOnly bool) (SyncStatus, error) {
	zone, err := f.syncStatusZones(zoneName, nil)
	if err != nil {
		return SyncStatus{}, err
	}
	master, current, err := f.syncPolicyMaster(ctx)
	if err != nil {
		return SyncStatus{}, err
	}
	id, epoch, err := syncCommittedPeriodIdentity(current)
	if err != nil {
		return SyncStatus{}, err
	}
	result := SyncStatus{RealmID: f.RealmID, PeriodID: id, RealmEpoch: epoch, Zone: zone.Name, ZoneID: zone.ID, ZonegroupID: f.topology.groupIDs[zone.Zonegroup], MetadataMaster: master.ID == zone.ID}
	result.periodEpoch, err = syncCommittedPeriodEpoch(current)
	if err != nil {
		return result, err
	}
	data, err := f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "period", "get")
	if err != nil {
		return result, err
	}
	local, err := syncJSON(data)
	if err != nil {
		return result, err
	}
	localEpoch, epochErr := syncCommittedPeriodEpoch(local)
	if local["id"] != id || fmt.Sprint(local["realm_epoch"]) != strconv.FormatUint(epoch, 10) || epochErr != nil || localEpoch != result.periodEpoch || !syncPeriodPolicyEqual(local, current) {
		return result, errors.New("RGW observed zone has not imported the current committed period")
	}
	var markers map[int]*string
	if !result.MetadataMaster {
		data, err = f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "metadata", "sync", "status")
		if err != nil {
			return result, err
		}
		result.Metadata, markers, err = decodeSyncMetadata(data)
		if err != nil {
			return result, err
		}
	}
	metadataCurrent := result.Metadata.State == "sync" && result.Metadata.PeriodID == id &&
		result.Metadata.RealmEpoch == epoch && result.Metadata.Shards > 0 && result.Metadata.IncrementalShards == result.Metadata.Shards
	if metadataOnly {
		switch {
		case result.MetadataMaster:
			result.Metadata = MetadataSyncStatus{State: "master", PeriodID: id, RealmEpoch: epoch, CaughtUp: true}
		case metadataCurrent:
			result.Metadata.CaughtUp, err = f.readMetadataLog(ctx, zone, master, id, markers)
		}
		if !errors.Is(err, errMetadataTransportUnavailable) {
			return result, err
		}
	}
	data, err = f.syncPolicyAdmin(ctx, zone, zone.Zonegroup, "sync", "status")
	if err != nil {
		return result, err
	}
	text, err := parseSyncStatusText(data, result, f.zoneStates())
	if err != nil {
		return result, err
	}
	if result.MetadataMaster {
		result.Metadata = MetadataSyncStatus{State: "master", PeriodID: id, RealmEpoch: epoch, CaughtUp: text.metadataMaster}
	} else {
		result.Metadata.CaughtUp = metadataCurrent && text.metadataCaughtUp
	}
	if !metadataOnly {
		result.DataSources = text.data
	}
	return result, nil
}

func syncCommittedPeriodIdentity(current map[string]any) (string, uint64, error) {
	id, _ := current["id"].(string)
	epoch, err := strconv.ParseUint(fmt.Sprint(current["realm_epoch"]), 10, 64)
	if err != nil || id == "" || strings.HasSuffix(id, ":staging") || epoch == 0 {
		return "", 0, errors.New("RGW committed period identity is malformed")
	}
	return id, epoch, nil
}

// syncCommittedPeriodEpoch identifies a version within one period and realm
// epoch. A same-master commit increments this value without changing either
// identity returned by syncCommittedPeriodIdentity.
func syncCommittedPeriodEpoch(current map[string]any) (uint64, error) {
	epoch, err := strconv.ParseUint(fmt.Sprint(current["epoch"]), 10, 64)
	if err != nil || epoch == 0 {
		return 0, errors.New("RGW committed period epoch is malformed")
	}
	return epoch, nil
}

func syncStatusReady(status SyncStatus, sources []string) bool {
	if !status.Metadata.CaughtUp {
		return false
	}
	for _, source := range sources {
		if !slices.ContainsFunc(status.DataSources, func(s DataSyncStatus) bool { return s.SourceZone == source && s.CaughtUp }) {
			return false
		}
	}
	return true
}

func decodeSyncMetadata(data []byte) (MetadataSyncStatus, map[int]*string, error) {
	var native struct {
		SyncStatus struct {
			Info struct {
				Status     string `json:"status"`
				NumShards  int    `json:"num_shards"`
				Period     string `json:"period"`
				RealmEpoch uint64 `json:"realm_epoch"`
			} `json:"info"`
			Markers []struct {
				Key *int `json:"key"`
				Val struct {
					State  int     `json:"state"`
					Marker *string `json:"marker"`
				} `json:"val"`
			} `json:"markers"`
		} `json:"sync_status"`
	}
	if json.Unmarshal(data, &native) != nil || native.SyncStatus.Info.Status == "" || native.SyncStatus.Info.NumShards < 0 {
		return MetadataSyncStatus{}, nil, errors.New("decode RGW native metadata sync status")
	}
	info := native.SyncStatus.Info
	result := MetadataSyncStatus{State: info.Status, PeriodID: info.Period, RealmEpoch: info.RealmEpoch, Shards: info.NumShards}
	markers := make(map[int]*string, len(native.SyncStatus.Markers))
	for _, marker := range native.SyncStatus.Markers {
		if marker.Key == nil || *marker.Key < 0 || *marker.Key >= result.Shards {
			return result, nil, errors.New("RGW metadata sync has an invalid shard identity")
		}
		if _, seen := markers[*marker.Key]; seen {
			return result, nil, errors.New("RGW metadata sync has a duplicate shard identity")
		}
		markers[*marker.Key] = marker.Val.Marker
		if marker.Val.State == 1 {
			result.IncrementalShards++
		}
	}
	return result, markers, nil
}

type rgwSyncText struct {
	metadataMaster, metadataCaughtUp bool
	data                             []DataSyncStatus
}

var syncShardCount = regexp.MustCompile(`^(full|incremental) sync: ([0-9]+)/([0-9]+) shards$`)
var syncBehindCount = regexp.MustCompile(`^data is behind on ([0-9]+) shards$`)
var syncRecoveryCount = regexp.MustCompile(`^([0-9]+) shards are recovering$`)

// parseSyncStatusText reads the native text of `radosgw-admin sync status`.
// The realm, zonegroup and zone headers must each appear once with the
// expected IDs; data sources must be other zones of the observed zone's
// zonegroup, each listed once. A failure, error or lag line in a section makes
// that section, or that source, not caught up.
func parseSyncStatusText(data []byte, expected SyncStatus, zones []*rgwZoneState) (rgwSyncText, error) {
	p := syncTextParser{expected: expected, zones: zones, group: syncExpectedGroup(expected, zones),
		identities: map[string]string{"realm": expected.RealmID, "zonegroup": expected.ZonegroupID, "zone": expected.ZoneID},
		seen:       map[string]bool{}, dataFailed: map[string]bool{}, current: -1}
	for _, raw := range strings.Split(string(data), "\n") {
		if err := p.line(strings.TrimSpace(raw)); err != nil {
			return p.result, err
		}
	}
	return p.finish()
}

// syncTextParser holds the state of one parseSyncStatusText run: the section
// being read and, in the data section, the index of the current source.
type syncTextParser struct {
	expected       SyncStatus
	zones          []*rgwZoneState
	group          string
	identities     map[string]string
	seen           map[string]bool
	section        string
	current        int
	metadataFailed bool
	dataFailed     map[string]bool
	result         rgwSyncText
}

func (p *syncTextParser) line(line string) error {
	if line == "" || strings.HasPrefix(line, "zonegroup features ") {
		return nil
	}
	fields := strings.Fields(line)
	if want, ok := p.identities[fields[0]]; ok {
		if len(fields) < 2 || fields[1] != want || p.seen[fields[0]] {
			return errors.New("RGW sync status native realm/zonegroup/zone identity differs")
		}
		p.seen[fields[0]] = true
		return nil
	}
	if rest, ok := strings.CutPrefix(line, "metadata sync "); ok {
		p.section, line = "metadata", strings.TrimSpace(rest)
	} else if rest, ok := strings.CutPrefix(line, "data sync "); ok {
		p.section, line = "data", strings.TrimSpace(rest)
	}
	switch p.section {
	case "metadata":
		p.metadataLine(line)
	case "data":
		return p.dataLine(line)
	}
	return nil
}

func (p *syncTextParser) metadataLine(line string) {
	if line == "no sync (zone is master)" {
		p.result.metadataMaster = true
	}
	if line == "metadata is caught up with master" {
		p.result.metadataCaughtUp = true
	}
	if strings.Contains(line, "failed") || strings.Contains(line, "different period") || strings.Contains(line, "ERROR") || strings.Contains(line, "behind on") {
		p.metadataFailed = true
	}
}

func (p *syncTextParser) dataLine(line string) error {
	if rest, ok := strings.CutPrefix(line, "source: "); ok {
		return p.source(strings.Fields(rest)[0])
	}
	if p.current < 0 {
		return nil
	}
	current := &p.result.data[p.current]
	if line == "syncing" {
		current.Syncing = true
	}
	if line == "data is caught up with source" {
		current.CaughtUp = true
	}
	if strings.Contains(line, "failed") || strings.Contains(line, "ERROR") || strings.Contains(line, "not syncing") {
		p.dataFailed[current.SourceZoneID] = true
	}
	if match := syncShardCount.FindStringSubmatch(line); match != nil {
		count, _ := strconv.Atoi(match[2])
		total, _ := strconv.Atoi(match[3])
		if current.Shards != 0 && current.Shards != total {
			p.dataFailed[current.SourceZoneID] = true
		}
		current.Shards = total
		if match[1] == "full" {
			current.FullShards = count
		} else {
			current.IncrementalShards = count
		}
	}
	if match := syncBehindCount.FindStringSubmatch(line); match != nil {
		current.BehindShards, _ = strconv.Atoi(match[1])
	}
	if match := syncRecoveryCount.FindStringSubmatch(line); match != nil {
		current.RecoveringShards, _ = strconv.Atoi(match[1])
	}
	return nil
}

// source starts the lines of one data source, which must be another owned
// zone of the observed zone's zonegroup, listed once.
func (p *syncTextParser) source(id string) error {
	var source *rgwZoneState
	for _, z := range p.zones {
		if z.ID == id && z.ID != p.expected.ZoneID && z.Zonegroup == p.group {
			source = z
		}
	}
	if source == nil || slices.ContainsFunc(p.result.data, func(s DataSyncStatus) bool { return s.SourceZoneID == id }) {
		return errors.New("RGW sync status has unexpected or duplicate source zone")
	}
	p.result.data = append(p.result.data, DataSyncStatus{SourceZone: source.Name, SourceZoneID: id})
	p.current = len(p.result.data) - 1
	return nil
}

func (p *syncTextParser) finish() (rgwSyncText, error) {
	if len(p.seen) != len(p.identities) {
		return p.result, errors.New("RGW native sync status is missing scoped identity headers")
	}
	p.result.metadataCaughtUp = p.result.metadataCaughtUp && !p.metadataFailed
	p.result.metadataMaster = p.result.metadataMaster && !p.metadataFailed
	for i := range p.result.data {
		s := &p.result.data[i]
		s.CaughtUp = s.CaughtUp && s.Syncing && s.Shards > 0 && s.IncrementalShards == s.Shards && s.FullShards == 0 && s.BehindShards == 0 && s.RecoveringShards == 0 && !p.dataFailed[s.SourceZoneID]
	}
	return p.result, nil
}

func syncExpectedGroup(status SyncStatus, zones []*rgwZoneState) string {
	for _, z := range zones {
		if z.ID == status.ZoneID {
			return z.Zonegroup
		}
	}
	return ""
}
