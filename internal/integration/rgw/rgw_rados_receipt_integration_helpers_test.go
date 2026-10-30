//go:build all || integration

package rgw_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// rgwBucketStats is the part of "radosgw-admin bucket stats" that names a
// bucket's RADOS objects: data objects start with the marker, and the index
// objects are ".dir.<id>.<shard>".
type rgwBucketStats struct {
	Bucket    string `json:"bucket"`
	ID        string `json:"id"`
	Marker    string `json:"marker"`
	NumShards int    `json:"num_shards"`
}

func parseRGWBucketStats(data []byte) (rgwBucketStats, error) {
	var stats rgwBucketStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return rgwBucketStats{}, err
	}
	if stats.Bucket == "" || stats.ID == "" || stats.Marker == "" {
		return rgwBucketStats{}, errors.New("bucket stats lack the bucket, id or marker")
	}
	return stats, nil
}

// rgwObjectParts finds an object's head, named "<marker>_<key>", in a data
// pool listing, and the bucket's tail stripes, named "<marker>__shadow_." and
// a random per-upload prefix. A tail name does not carry the key, so the
// tails belong to the object only while it is the bucket's one object with
// tails.
func rgwObjectParts(listing []string, marker, key string) (head string, tails []string) {
	for _, oid := range listing {
		switch {
		case oid == marker+"_"+key:
			head = oid
		case strings.HasPrefix(oid, marker+"__shadow_"):
			tails = append(tails, oid)
		}
	}
	sort.Strings(tails)
	return head, tails
}

// rgwIndexShardOf returns the one index shard object of a bucket whose omap
// holds key. shards maps each index object of the pool to its omap keys;
// objects of other buckets are ignored. No shard, or more than one, is an
// error: the key's location is then not known.
func rgwIndexShardOf(shards map[string][]string, bucketID, key string) (string, error) {
	var found []string
	for oid, keys := range shards {
		if strings.HasPrefix(oid, ".dir."+bucketID+".") && slices.Contains(keys, key) {
			found = append(found, oid)
		}
	}
	sort.Strings(found)
	switch len(found) {
	case 0:
		return "", fmt.Errorf("no index shard of bucket %s holds %q", bucketID, key)
	case 1:
		return found[0], nil
	}
	return "", fmt.Errorf("index shards %v of bucket %s all hold %q", found, bucketID, key)
}

// parseRADOSStatSize reads the size from "rados stat": "<pool>/<oid> mtime
// <time>, size <n>".
func parseRADOSStatSize(out string) (int64, error) {
	index := strings.LastIndex(out, ", size ")
	if index < 0 {
		return 0, fmt.Errorf("rados stat has no size: %q", out)
	}
	return strconv.ParseInt(strings.TrimSpace(out[index+len(", size "):]), 10, 64)
}

// rgwGCEntry is one deferred deletion in "radosgw-admin gc list".
type rgwGCEntry struct {
	Tag  string `json:"tag"`
	Objs []struct {
		Pool string `json:"pool"`
		OID  string `json:"oid"`
	} `json:"objs"`
}

// rgwGCOIDs lists every RADOS object that the garbage collector still has to
// remove.
func rgwGCOIDs(data []byte) ([]string, error) {
	var entries []rgwGCEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	oids := []string{}
	for _, entry := range entries {
		for _, obj := range entry.Objs {
			oids = append(oids, obj.OID)
		}
	}
	sort.Strings(oids)
	return oids, nil
}
