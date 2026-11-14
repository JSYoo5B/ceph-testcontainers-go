//go:build all || integration

package rgw_post_usage_bytes_received_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// The usage log's names of the operations the case reads.
const (
	categoryCreateBucket = "create_bucket"
	categoryPut          = "put_obj"
	categoryPost         = "post_obj"
)

// usageCounters is what the usage log holds for one category of operations
// on one bucket: the bytes the gateway sent and received, and the operations
// it counted and counted as successful.
type usageCounters struct {
	Sent, Received, Ops, Successful uint64
}

// bucketUsage is the usage log of one user's bucket, per category.
type bucketUsage map[string]usageCounters

// parseUsage reads a "usage show" report with its log entries, for the one
// user the report was asked for, and returns the counters of bucket per
// category. The usage log keeps one record per bucket and hour, so the
// records of bucket are added up.
//
// The report must carry its entry list, which is empty before the first
// flush but not absent or null. Every user of the list must be the user asked
// for, and every record must name its bucket and carry its category list.
// A category must have a name and all four counters: a counter that is
// missing or null is not a zero. A category named twice in one record is an
// error.
func parseUsage(report []byte, user, bucket string) (bucketUsage, error) {
	type category struct {
		Category   *string `json:"category"`
		Sent       *uint64 `json:"bytes_sent"`
		Received   *uint64 `json:"bytes_received"`
		Ops        *uint64 `json:"ops"`
		Successful *uint64 `json:"successful_ops"`
	}
	var raw struct {
		Entries *[]struct {
			User    *string `json:"user"`
			Buckets *[]struct {
				Bucket     *string     `json:"bucket"`
				Categories *[]category `json:"categories"`
			} `json:"buckets"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(report, &raw); err != nil {
		return nil, fmt.Errorf("usage log: %w", err)
	}
	if user == "" || bucket == "" {
		return nil, fmt.Errorf("usage log: no user or no bucket to read")
	}
	if raw.Entries == nil {
		return nil, fmt.Errorf("usage log: the entry list is missing")
	}
	usage := bucketUsage{}
	for _, entry := range *raw.Entries {
		if entry.User == nil || *entry.User != user {
			return nil, fmt.Errorf("usage log: an entry of another user than %s", user)
		}
		if entry.Buckets == nil {
			return nil, fmt.Errorf("usage log: the entry of %s has no bucket list", user)
		}
		for _, record := range *entry.Buckets {
			if record.Bucket == nil || record.Categories == nil {
				return nil, fmt.Errorf("usage log: a record of %s has no bucket name or no category list", user)
			}
			if *record.Bucket != bucket {
				continue
			}
			seen := map[string]bool{}
			for _, c := range *record.Categories {
				if c.Category == nil || *c.Category == "" {
					return nil, fmt.Errorf("usage log: a category of %s has no name", bucket)
				}
				if c.Sent == nil || c.Received == nil || c.Ops == nil || c.Successful == nil {
					return nil, fmt.Errorf("usage log: the category %s of %s lacks a counter", *c.Category, bucket)
				}
				if seen[*c.Category] {
					return nil, fmt.Errorf("usage log: the category %s is listed twice in one record of %s", *c.Category, bucket)
				}
				seen[*c.Category] = true
				sum := usage[*c.Category]
				sum.Sent += *c.Sent
				sum.Received += *c.Received
				sum.Ops += *c.Ops
				sum.Successful += *c.Successful
				usage[*c.Category] = sum
			}
		}
	}
	return usage, nil
}

// usageGrew requires that no counter of before is smaller or gone in after.
// The usage log only adds, so a total that went down means the log was
// trimmed or read from somewhere else between the two samples, and they say
// nothing about the uploads.
func usageGrew(before, after bucketUsage) error {
	categories := make([]string, 0, len(before))
	for name := range before {
		categories = append(categories, name)
	}
	slices.Sort(categories)
	for _, name := range categories {
		was, now := before[name], after[name]
		if _, ok := after[name]; !ok {
			return fmt.Errorf("usage not observed: the category %s is gone from the usage log", name)
		}
		if now.Sent < was.Sent || now.Received < was.Received || now.Ops < was.Ops || now.Successful < was.Successful {
			return fmt.Errorf("usage not observed: the category %s went down from %+v to %+v", name, was, now)
		}
	}
	return nil
}

// requireLogging requires the usage log of a bucket before the uploads: the
// creation of the bucket is in it, so usage logging is enabled and its flush
// reaches the log, and no upload is.
func requireLogging(usage bucketUsage) error {
	created, ok := usage[categoryCreateBucket]
	if !ok || created.Ops != 1 || created.Successful != 1 {
		return fmt.Errorf("condition not established: the usage log does not hold the one successful creation of the bucket: %v", usage)
	}
	for _, name := range []string{categoryPut, categoryPost} {
		if _, ok := usage[name]; ok {
			return fmt.Errorf("condition not established: the usage log holds %s before any upload: %v", name, usage)
		}
	}
	return nil
}

// uploaded returns the counters of the one upload of a bucket: category
// must be in the usage log, and the other upload category must not be.
func uploaded(usage bucketUsage, category string) (*usageCounters, error) {
	other := categoryPost
	if category == categoryPost {
		other = categoryPut
	}
	if _, ok := usage[other]; ok {
		return nil, fmt.Errorf("usage not observed: the bucket of the %s upload holds a %s: %v", category, other, usage)
	}
	counters, ok := usage[category]
	if !ok {
		return nil, nil
	}
	return &counters, nil
}

// judgeUploads decides what the usage log shows for the bytes received by a
// POST upload, given what it shows for a PUT upload of the same payload.
// payload is the size of the payload, and form the size of the body of the
// POST request, which carries the payload among its form fields.
//
// Both uploads must be in the log as one operation that succeeded. The PUT
// is the control of the observation: the log must hold exactly the payload
// as its received bytes, which shows that the log records what an upload
// receives. Then the symptom is returned when the POST's received bytes are
// zero, and neither a symptom nor an error when they are at least the
// payload and at most the request body. Every other count is an error: the
// gateway counted part of the payload, or more than it was sent.
func judgeUploads(put, post *usageCounters, payload, form uint64) ([]string, error) {
	if payload == 0 || form < payload {
		return nil, fmt.Errorf("outcome not observed: a payload of %d bytes in a request body of %d", payload, form)
	}
	for _, upload := range []struct {
		name     string
		counters *usageCounters
	}{{"PUT", put}, {"POST", post}} {
		if upload.counters == nil {
			return nil, fmt.Errorf("usage not observed: the usage log does not hold the %s upload", upload.name)
		}
		if upload.counters.Ops != 1 || upload.counters.Successful != 1 {
			return nil, fmt.Errorf("condition not established: the usage log holds %d operations, %d successful, for the %s upload, want one successful operation", upload.counters.Ops, upload.counters.Successful, upload.name)
		}
	}
	if put.Received != payload {
		return nil, fmt.Errorf("control not established: the usage log holds %d received bytes for the PUT upload of %d bytes", put.Received, payload)
	}
	switch {
	case post.Received == 0:
		return []string{fmt.Sprintf("the usage log holds 0 received bytes for the POST upload of %d bytes in a request body of %d; the PUT upload of the same payload holds %d", payload, form, put.Received)}, nil
	case post.Received >= payload && post.Received <= form:
		return nil, nil
	default:
		return nil, fmt.Errorf("another state: the usage log holds %d received bytes for the POST upload, neither zero nor between the payload of %d and the request body of %d", post.Received, payload, form)
	}
}

// formField is one field of the form of a POST upload.
type formField struct{ Name, Value string }

// postPolicy returns the policy document of a POST upload of exactly size
// bytes to key in bucket, which the fields other than the policy, the
// signature and the file must satisfy. expires is the policy's expiration
// in ISO 8601.
func postPolicy(expires, bucket, key string, size int, fields []formField) ([]byte, error) {
	conditions := []any{map[string]string{"bucket": bucket}, []any{"eq", "$key", key}, []any{"content-length-range", size, size}}
	for _, field := range fields {
		conditions = append(conditions, map[string]string{strings.ToLower(field.Name): field.Value})
	}
	return json.Marshal(map[string]any{"expiration": expires, "conditions": conditions})
}
