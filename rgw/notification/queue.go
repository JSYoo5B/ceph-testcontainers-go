package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// Sample bounds.
const (
	// MaxSampleEntries is the largest Sample.MaxEntries.
	MaxSampleEntries = 100
	// DefaultSampleBytes is the reply prefix kept for a sample whose MaxBytes
	// is zero.
	DefaultSampleBytes = 64 << 10
	// MaxSampleBytes is the largest Sample.MaxBytes.
	MaxSampleBytes = 1 << 20
)

// ErrNotPersistent reports a topic that exists but has no persistent queue:
// its notifications are pushed synchronously and nothing is pending inside
// RGW. It is a ceph.ErrInvalidRequest, never an empty queue.
var ErrNotPersistent = errors.New("RGW notification topic is not persistent")

// Topic selects one topic by its native tenant and name. Empty Tenant selects
// the legacy namespace. Name is 1 to 256 letters, digits, hyphens and
// underscores. Topics owned by an account are not selected by a tenant; read
// them with rgw.Gateway.Admin.
type Topic struct {
	Tenant, Name string
}

// Sample asks Queue for a bounded sample of the pending entries: the first
// MaxEntries entries in queue order. MaxEntries zero takes no sample;
// otherwise it is 1 to MaxSampleEntries. radosgw-admin topic dump writes the
// whole queue whatever its --max-entries, which only sizes its pages, so Queue
// keeps the first MaxBytes bytes of the reply and discards the rest as it
// arrives; zero selects DefaultSampleBytes, and MaxBytes is at most
// MaxSampleBytes. The command still runs to the end of the queue, within the
// call's context. A sample whose entries do not fit in MaxBytes is refused,
// never shortened.
type Sample struct {
	MaxEntries, MaxBytes int
}

// Metadata is the native identity and delivery policy of a persistent topic.
// The push endpoint, its arguments, the opaque data and the topic policy are
// left out: they can carry credentials and no queue assertion needs them.
//
// Native topic metadata has no generation: a topic deleted and recreated with
// the same tenant, name, owner and queue is indistinguishable from the
// original. Queue compares this identity before and after its reads and
// refuses a change it sees, nothing more.
type Metadata struct {
	Tenant, Name string
	// Owner is the native owner, such as tenant$user; empty when Ceph does
	// not report it.
	Owner string
	ARN   string
	// Queue is the native persistent queue name; empty when Ceph does not
	// report it.
	Queue string
	// TimeToLive, MaxRetries and RetrySleepDuration are the native texts,
	// such as None for the global default. A backlog can shrink because
	// entries expire or exhaust their retries, not only by delivery.
	TimeToLive, MaxRetries, RetrySleepDuration string
}

// Stats are the native statistics of the persistent queue: reserved slots
// of notifications not committed yet, committed entries and their native size
// in bytes.
type Stats struct {
	Reservations, Entries, SizeBytes uint64
}

// PendingEntry is the identity of one pending notification: the event's
// native ID, name and time, the notification configuration ID, the bucket,
// the object key, version and sequencer, and the queue entry's creation time,
// all as native text. The push endpoint and its arguments, user and request
// identities, source address, object metadata and tags are left out.
type PendingEntry struct {
	EventID, EventName, EventTime, ConfigurationID string
	Bucket, Key, VersionID, Sequencer              string
	CreationTime                                   string
}

// QueueSnapshot is one bracketed observation. Sampled reports that a sample
// was requested and read; Pending then holds up to Sample.MaxEntries entries
// in native order, and is empty, not nil, for an empty queue.
type QueueSnapshot struct {
	Topic   Metadata
	Stats   Stats
	Pending []PendingEntry
	Sampled bool
}

var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

// Queue reads the persistent queue of the selected topic through gateway g:
// radosgw-admin topic get, topic stats, optionally topic dump bounded by
// sample, and topic get again, in the gateway's scope. It refuses the result
// when the topic's tenant, name, owner, ARN or queue name differs between the
// two reads or from the selection. The statistics and the sample are separate
// native reads, not one atomic view; the queue is the one of the gateway's
// zone.
//
// The snapshot is point-in-time RGW evidence. An empty queue does not prove
// that the SUT received or processed a notification, and a shrinking queue
// can reflect expiry or exhausted retries; it is not an exactly-once or
// end-to-end delivery verdict. Correlate it with what the consumer received.
//
// The outcomes stay distinct: an invalid selection or sample is
// ceph.ErrInvalidRequest before any command; a missing topic matches
// fs.ErrNotExist and a refused read fs.ErrPermission (ceph.ErrNativeFailure
// either way); a topic without a persistent queue is ErrNotPersistent; a
// cluster before Ceph 19 is ceph.ErrUnsupported; an unexpected native reply
// is an unclassified error. Errors never contain native output. Queue never
// creates, changes, purges or removes a topic, its policy or its queue, and
// the result is plain data, not a handle: observing a SUT-created topic does
// not adopt its lifetime. The call is bounded by ctx and the cluster's startup
// timeout; no radosgw-admin outlives it.
//
//purpose:observation,evidence
func Queue(ctx context.Context, g *rgw.Gateway, topic Topic, sample Sample) (QueueSnapshot, error) {
	var core *gwbase.Core
	if g != nil {
		core = gwbase.Of(g)
	}
	return queue(ctx, core, topic, sample)
}

func normalize(topic Topic, sample Sample) (Sample, error) {
	if !topicNamePattern.MatchString(topic.Name) {
		return sample, servicehook.Invalid(errors.New("RGW topic name must be 1 to 256 letters, digits, hyphens or underscores"))
	}
	if err := gwbase.ValidateTenant(topic.Tenant); err != nil {
		return sample, err
	}
	if sample.MaxEntries < 0 || sample.MaxEntries > MaxSampleEntries {
		return sample, servicehook.Invalid(fmt.Errorf("RGW topic sample entries must be 0 to %d", MaxSampleEntries))
	}
	if sample.MaxBytes < 0 || sample.MaxBytes > MaxSampleBytes {
		return sample, servicehook.Invalid(fmt.Errorf("RGW topic sample bytes must be 0 to %d", MaxSampleBytes))
	}
	if sample.MaxBytes == 0 {
		sample.MaxBytes = DefaultSampleBytes
	}
	return sample, nil
}

func queue(ctx context.Context, g *gwbase.Core, topic Topic, sample Sample) (QueueSnapshot, error) {
	sample, err := normalize(topic, sample)
	if err != nil {
		return QueueSnapshot{}, err
	}
	cluster := g.Cluster()
	if cluster == nil {
		return QueueSnapshot{}, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return QueueSnapshot{}, err
	}
	defer servicehook.Unlock(cluster)
	target, err := g.AdmitAdmin()
	if err != nil {
		return QueueSnapshot{}, err
	}
	if servicehook.CephBefore(cluster, 19) {
		return QueueSnapshot{}, unsupported(cluster)
	}
	selector := []string{"--topic", topic.Name}
	if topic.Tenant != "" {
		selector = append(selector, "--tenant", topic.Tenant)
	}
	read := func(step string, args ...string) ([]byte, error) {
		data, err := g.AdminCommandIn(ctx, target, append(args, selector...)...)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return nil, fmt.Errorf("RGW topic %s: %w", step, err)
		}
		return data, nil
	}
	data, err := read("get", "topic", "get")
	if err != nil {
		return QueueSnapshot{}, err
	}
	before, err := decodeTopic(data, topic)
	if err != nil {
		return QueueSnapshot{}, err
	}
	var snapshot QueueSnapshot
	if data, err = read("stats", "topic", "stats"); err != nil {
		return QueueSnapshot{}, err
	}
	if snapshot.Stats, err = decodeStats(data); err != nil {
		return QueueSnapshot{}, err
	}
	if sample.MaxEntries > 0 {
		// topic dump pages through the whole queue, --max-entries only sizing
		// each page, so the sample is the first entries of a reply kept up to
		// MaxBytes.
		args := append([]string{"topic", "dump", "--max-entries", strconv.Itoa(sample.MaxEntries)}, selector...)
		data, truncated, err := g.AdminPrefixIn(ctx, target, sample.MaxBytes, args...)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return QueueSnapshot{}, fmt.Errorf("RGW topic dump: %w", err)
		}
		if snapshot.Pending, err = decodePending(data, truncated, sample.MaxEntries); err != nil {
			return QueueSnapshot{}, err
		}
		snapshot.Sampled = true
	}
	if data, err = read("get", "topic", "get"); err != nil {
		return QueueSnapshot{}, err
	}
	after, err := decodeTopic(data, topic)
	if err != nil {
		return QueueSnapshot{}, err
	}
	if before.Owner != after.Owner || before.ARN != after.ARN || before.Queue != after.Queue {
		return QueueSnapshot{}, errors.New("RGW topic identity changed during the queue observation")
	}
	snapshot.Topic = after
	return snapshot, nil
}

func unsupported(cluster *ceph.Container) error {
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		return err
	}
	return &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: "cluster", Version: facts.CephVersion,
		Capability: "radosgw-admin topic stats and topic dump",
		Err:        errors.New("RGW persistent topic queue observation needs Ceph 19 or later")}
}

var errMalformedTopic = errors.New("decode RGW topic metadata; native output is redacted")

// decodeTopic decodes radosgw-admin topic get, either the topic object or an
// object whose only member topic holds it, and checks it against the
// selection. A topic without a persistent queue is ErrNotPersistent.
func decodeTopic(data []byte, selected Topic) (Metadata, error) {
	var root map[string]any
	if strictjson.Decode(data, &root, strictjson.Options{UseNumber: true}) != nil || root == nil {
		return Metadata{}, errMalformedTopic
	}
	if inner, ok := root["topic"].(map[string]any); ok && len(root) == 1 {
		root = inner
	}
	name, nameOK := root["name"].(string)
	arn, arnOK := root["arn"].(string)
	dest, destOK := root["dest"].(map[string]any)
	if !nameOK || !arnOK || !destOK {
		return Metadata{}, errMalformedTopic
	}
	persistent, ok := dest["persistent"].(bool)
	if !ok {
		return Metadata{}, errMalformedTopic
	}
	meta := Metadata{Tenant: selected.Tenant, Name: name, ARN: arn}
	if value, present := root["owner"]; present {
		if meta.Owner, ok = value.(string); !ok {
			return Metadata{}, errMalformedTopic
		}
	}
	for key, field := range map[string]*string{"persistent_queue": &meta.Queue, "time_to_live": &meta.TimeToLive,
		"max_retries": &meta.MaxRetries, "retry_sleep_duration": &meta.RetrySleepDuration} {
		value, present := dest[key]
		if !present {
			continue
		}
		switch value := value.(type) {
		case string:
			*field = value
		case json.Number:
			if key == "persistent_queue" {
				return Metadata{}, errMalformedTopic
			}
			*field = value.String()
		default:
			return Metadata{}, errMalformedTopic
		}
	}
	// arn:aws:sns:<zonegroup>:<tenant>:<name>. The name selects the topic
	// only within the tenant, so both must be the selected ones.
	parts := strings.Split(arn, ":")
	if name != selected.Name || len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "sns" || parts[4] != selected.Tenant || parts[5] != selected.Name {
		return Metadata{}, servicehook.Invalid(errors.New("RGW topic metadata does not match the selected tenant and name"))
	}
	if !persistent {
		return Metadata{}, servicehook.Invalid(ErrNotPersistent)
	}
	return meta, nil
}

var errMalformedStats = errors.New("decode RGW topic queue statistics; native output is redacted")

// decodeStats decodes radosgw-admin topic stats: {"Topic Stats":
// {"Reservations": n, "Size": n, "Entries": n}}.
func decodeStats(data []byte) (Stats, error) {
	var root map[string]any
	if strictjson.Decode(data, &root, strictjson.Options{UseNumber: true}) != nil || root == nil {
		return Stats{}, errMalformedStats
	}
	native, ok := root["Topic Stats"].(map[string]any)
	if !ok {
		return Stats{}, errMalformedStats
	}
	var stats Stats
	for key, field := range map[string]*uint64{"Reservations": &stats.Reservations, "Entries": &stats.Entries, "Size": &stats.SizeBytes} {
		number, ok := native[key].(json.Number)
		if !ok {
			return Stats{}, errMalformedStats
		}
		value, err := strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return Stats{}, errMalformedStats
		}
		*field = value
	}
	return stats, nil
}

var errMalformedPending = errors.New("decode RGW topic pending entries; native output is redacted")

var errSampleBytes = errors.New("RGW topic dump reply reached the sample's byte limit before its entries ended; native output is discarded")

// decodePending decodes the first limit entries of radosgw-admin topic dump,
// an array of queue entries, each either the entry object or an object whose
// only member entry holds it. data is the reply's prefix, truncated when the
// reply was longer: entries after the first limit are not read, and a prefix
// that ends before limit entries or the array's end is refused rather than
// returned as a shorter sample. Only the identity fields of PendingEntry are
// kept.
func decodePending(data []byte, truncated bool, limit int) ([]PendingEntry, error) {
	elements, err := decodeArrayPrefix(data, limit)
	if err != nil {
		if truncated {
			return nil, errSampleBytes
		}
		return nil, errMalformedPending
	}
	pending := make([]PendingEntry, 0, len(elements))
	for _, raw := range elements {
		var entry map[string]any
		if strictjson.Decode(raw, &entry, strictjson.Options{UseNumber: true}) != nil || entry == nil {
			return nil, errMalformedPending
		}
		if inner, ok := entry["entry"].(map[string]any); ok && len(entry) == 1 {
			entry = inner
		}
		event, _ := entry["event"].(map[string]any)
		s3, _ := event["s3"].(map[string]any)
		bucket, _ := s3["bucket"].(map[string]any)
		object, _ := s3["object"].(map[string]any)
		if event == nil || s3 == nil || bucket == nil || object == nil {
			return nil, errMalformedPending
		}
		var item PendingEntry
		for _, field := range []struct {
			from     map[string]any
			key      string
			to       *string
			required bool
		}{
			{event, "eventName", &item.EventName, true}, {bucket, "name", &item.Bucket, true}, {object, "key", &item.Key, true},
			{event, "eventId", &item.EventID, false}, {event, "eventTime", &item.EventTime, false},
			{s3, "configurationId", &item.ConfigurationID, false}, {object, "versionId", &item.VersionID, false},
			{object, "sequencer", &item.Sequencer, false}, {entry, "creationTime", &item.CreationTime, false},
		} {
			value, present := field.from[field.key]
			if !present && !field.required {
				continue
			}
			text, ok := value.(string)
			if !ok {
				return nil, errMalformedPending
			}
			*field.to = text
		}
		pending = append(pending, item)
	}
	return pending, nil
}

// decodeArrayPrefix returns the first limit elements of the JSON array that
// data starts with, or all of them when the array ends first. What follows
// the limit-th element is not read.
func decodeArrayPrefix(data []byte, limit int) ([]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if open, err := dec.Token(); err != nil || open != json.Delim('[') {
		return nil, errMalformedPending
	}
	var elements []json.RawMessage
	for len(elements) < limit {
		if !dec.More() {
			if end, err := dec.Token(); err != nil || end != json.Delim(']') {
				return nil, errMalformedPending
			}
			if _, err := dec.Token(); err != io.EOF {
				return nil, errMalformedPending
			}
			return elements, nil
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, errMalformedPending
		}
		elements = append(elements, raw)
	}
	return elements, nil
}
