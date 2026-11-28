package multisite

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

var errMetadataTransportUnavailable = errors.New("RGW metadata transport needs supervised curl")

// readMetadataLog compares with the master through the observing zone's
// network. The RGW shard-info endpoint preserves read failures, whereas
// radosgw-admin mdlog status can emit an empty marker after a failed read.
func (f *Fixture) readMetadataLog(ctx context.Context, zone, master *rgwZoneState, period string, local map[int]*string) (bool, error) {
	endpoint, err := url.Parse(master.PeerEndpoint)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" {
		return false, errors.New("RGW metadata master peer endpoint is unavailable")
	}
	endpoint.Path = "/admin/log"
	query := url.Values{"type": {"metadata"}, "format": {"json"}}
	endpoint.RawQuery = query.Encode()
	addRequest := f.metadataLogRequests(master.Zonegroup, time.Now())
	args := addRequest([]string{"curl", "--disable", "--fail-early"}, endpoint)
	query.Set("period", period)
	query.Set("info", "true")
	for i := range len(local) {
		if local[i] == nil {
			return false, errors.New("RGW metadata sync shard is missing its marker")
		}
		query.Set("id", strconv.Itoa(i))
		endpoint.RawQuery = query.Encode()
		args = addRequest(append(args, "--next"), endpoint)
	}
	// A caller-supplied setup image can still use combined status if it lacks
	// this CLI capability. Never substitute combined status for a failed read.
	const curlCheck = `command -v curl >/dev/null 2>&1 || exit 127; exec "$@"`
	result, err := zone.cluster.RunNativeCommand(ctx, zone.client, ceph.NativeCommand{
		Argv:    append([]string{"sh", "-c", curlCheck, "sh"}, args...),
		Timeout: 25 * time.Second, MaxStdout: 1 << 20, MaxStderr: 4096, Label: "RGW metadata master comparison",
	})
	if errors.Is(err, ceph.ErrImageCapability) {
		return false, errMetadataTransportUnavailable
	}
	if err != nil {
		return false, err
	}
	if !result.Complete() {
		return false, errors.New("RGW metadata master comparison did not complete")
	}
	if result.ExitCode == 127 {
		return false, errMetadataTransportUnavailable
	}
	if result.ExitCode != 0 {
		return false, fmt.Errorf("RGW metadata master comparison exited %d", result.ExitCode)
	}
	return compareMetadataLog(result.Stdout, local)
}

// metadataLogRequests signs GET-only RGW admin requests. Older curl builds
// omit the port and payload hash header from their SigV4 signed headers.
func (f *Fixture) metadataLogRequests(region string, now time.Time) func([]string, *url.URL) []string {
	const algorithm = "AWS4-HMAC-SHA256"
	const signed = "host;x-amz-content-sha256;x-amz-date"
	stamp := now.UTC().Format("20060102T150405Z")
	day := stamp[:8]
	scope := day + "/" + region + "/s3/aws4_request"
	hash := fmt.Sprintf("%x", sha256.Sum256(nil))
	key := metadataLogHMAC([]byte("AWS4"+f.realm.systemSecret()), day)
	key = metadataLogHMAC(key, region)
	key = metadataLogHMAC(key, "s3")
	key = metadataLogHMAC(key, "aws4_request")
	return func(args []string, endpoint *url.URL) []string {
		canonical := "GET\n" + endpoint.EscapedPath() + "\n" + endpoint.RawQuery +
			"\nhost:" + endpoint.Host + "\nx-amz-content-sha256:" + hash + "\nx-amz-date:" + stamp +
			"\n\n" + signed + "\n" + hash
		toSign := fmt.Sprintf("%s\n%s\n%s\n%x", algorithm, stamp, scope, sha256.Sum256([]byte(canonical)))
		auth := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%x", algorithm,
			f.realm.systemAccess(), scope, signed, metadataLogHMAC(key, toSign))
		return append(args, "--silent", "--fail", "--noproxy", "*", "--max-filesize", "4096",
			"--header", "x-amz-date: "+stamp, "--header", "x-amz-content-sha256: "+hash,
			"--header", "Authorization: "+auth, endpoint.String())
	}
}

func metadataLogHMAC(key []byte, text string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = io.WriteString(hash, text)
	return hash.Sum(nil)
}

func compareMetadataLog(data []byte, local map[int]*string) (bool, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var index struct {
		Shards int `json:"num_objects"`
	}
	if err := decoder.Decode(&index); err != nil || index.Shards <= 0 || index.Shards != len(local) {
		return false, errors.New("RGW metadata master returned an invalid shard inventory")
	}
	caughtUp := true
	for i := range len(local) {
		marker := local[i]
		var shard struct {
			Marker *string `json:"marker"`
		}
		if err := decoder.Decode(&shard); err != nil || shard.Marker == nil || marker == nil {
			return false, errors.New("RGW metadata master comparison is missing a shard marker")
		}
		// RGW compares the opaque metadata log markers lexicographically.
		caughtUp = caughtUp && *marker >= *shard.Marker
	}
	if !errors.Is(decoder.Decode(new(any)), io.EOF) {
		return false, errors.New("RGW metadata master returned unexpected shard data")
	}
	return caughtUp, nil
}
