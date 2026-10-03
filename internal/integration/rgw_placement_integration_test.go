//go:build integration && features

package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRGWPlacementStorageClasses(t *testing.T) {
	testRGWPlacementStorageClasses(t)
}

func TestHostNetworkRGWPlacementStorageClasses(t *testing.T) {
	testRGWPlacementStorageClasses(t, ceph.WithHostNetwork())
}

func TestRGWPlacementRealmStorageClasses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, options := integrationImages(t)
	options = append(options, ceph.WithOSDCount(1))
	clusters := make([]*ceph.Container, 2)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, image, options...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cluster
		for _, name := range []string{"tc-realm-index", "tc-realm-extra", "tc-realm-standard", "tc-realm-ia"} {
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, PGNum: 1, Application: "rgw"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	fixture, err := multicluster.RunRGWMultisite(ctx, rgwImage, multicluster.RGWMultisiteConfig{Source: clusters[0], Destination: clusters[1], ControlImage: image})
	if fixture != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := fixture.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer diagnosticCancel()
		for _, site := range []struct {
			name    string
			gateway *ceph.RGWContainer
		}{{"source", fixture.Source}, {"destination", fixture.Destination}} {
			for _, command := range [][]string{{"sync", "status"}, {"metadata", "sync", "status"}, {"data", "sync", "status", "--source-zone", fixture.SourceZoneID}, {"sync", "error", "list"}} {
				data, err := site.gateway.Admin(diagnosticCtx, command...)
				if err != nil {
					t.Logf("%s native %s diagnostic: %v", site.name, strings.Join(command[:min(3, len(command))], " "), err)
					continue
				}
				t.Logf("%s native %s diagnostic: %s", site.name, strings.Join(command[:min(3, len(command))], " "), rgwPlacementSafeDiagnostic(data, site.gateway))
			}
			logs, err := site.gateway.Logs(diagnosticCtx)
			if err == nil {
				data, readErr := io.ReadAll(io.LimitReader(logs, 2<<20))
				_ = logs.Close()
				if readErr == nil {
					t.Logf("%s recent RGW diagnostics: %s", site.name, rgwPlacementSafeLogDiagnostic(data, site.gateway))
				}
			}
		}
	})
	config := ceph.RGWPlacementConfig{Name: "tc-realm-tiered", IndexPool: "tc-realm-index", DataExtraPool: "tc-realm-extra", StorageClasses: []ceph.RGWStorageClassConfig{{Name: "STANDARD", DataPool: "tc-realm-standard"}, {Name: "STANDARD_IA", DataPool: "tc-realm-ia"}}}
	sourcePlacement, err := fixture.Source.CreatePlacement(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	destinationPlacement, err := fixture.Destination.CreatePlacement(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if sourcePlacement.RealmID == "" || sourcePlacement.RealmID != destinationPlacement.RealmID || sourcePlacement.ZonegroupID != destinationPlacement.ZonegroupID || sourcePlacement.ZoneID == destinationPlacement.ZoneID {
		t.Fatal("multisite placement scopes are not exact native identities")
	}
	if err := fixture.Destination.ApplyPlacement(ctx, destinationPlacement); err == nil {
		t.Fatal("secondary zone published a realm placement")
	}
	// Native period update can stage a change without publishing it. Restore
	// the zonegroup object afterward so only the staging period differs; Apply
	// must inspect that pending policy before its own period update overwrites it.
	groupData, err := fixture.Source.Admin(ctx, "zonegroup", "get")
	var nativeGroup struct {
		APIName string `json:"api_name"`
	}
	if err != nil || json.Unmarshal(groupData, &nativeGroup) != nil || nativeGroup.APIName == "" {
		t.Fatal("cannot inspect native realm zonegroup API name")
	}
	if _, err := fixture.Source.Admin(ctx, "zonegroup", "modify", "--api-name", nativeGroup.APIName+"-pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Source.Admin(ctx, "period", "update"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Source.Admin(ctx, "zonegroup", "modify", "--api-name", nativeGroup.APIName); err != nil {
		t.Fatal(err)
	}
	stagingArgs := []string{"period", "get", "--period", sourcePlacement.RealmID + ":staging", "--epoch", "1"}
	stagedBefore, err := fixture.Source.Admin(ctx, stagingArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Source.ApplyPlacement(ctx, sourcePlacement); err == nil || !strings.Contains(err.Error(), "existing staging period") {
		t.Fatalf("unrelated staged policy was not refused before overwrite: %v", err)
	}
	stagedAfter, err := fixture.Source.Admin(ctx, stagingArgs...)
	var before, after any
	if err != nil || json.Unmarshal(stagedBefore, &before) != nil || json.Unmarshal(stagedAfter, &after) != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("refused native staging period was changed")
	}
	// The test explicitly restages the restored zonegroup before permitting
	// publication; only the matching placement target now differs from current.
	if _, err := fixture.Source.Admin(ctx, "period", "update"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Source.ApplyPlacement(ctx, sourcePlacement); err != nil {
		t.Fatal(err)
	}
	if err := fixture.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Destination.ReloadPlacement(ctx, destinationPlacement); err != nil {
		t.Fatal(err)
	}
	for _, option := range []string{"rgw_sync_lease_period", "rgw_meta_sync_poll_interval", "rgw_data_sync_poll_interval"} {
		value, err := clusters[1].Ceph(ctx, "config", "get", "client.admin", option)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("native destination central config %s=%s (seconds)", option, strings.TrimSpace(string(value)))
	}
	endpoints := make([]string, 2)
	for i, gateway := range []*ceph.RGWContainer{fixture.Source, fixture.Destination} {
		endpoints[i], err = gateway.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	clients := []s3HTTPClient{{endpoint: endpoints[0], accessKey: fixture.Source.AccessKey, secretKey: fixture.Source.SecretKey, region: fixture.Source.Region, http: &http.Client{Timeout: 30 * time.Second}}, {endpoint: endpoints[1], accessKey: fixture.Source.AccessKey, secretKey: fixture.Source.SecretKey, region: fixture.Destination.Region, http: &http.Client{Timeout: 30 * time.Second}}}
	const bucket = "/tc-realm-placement-bucket"
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + sourcePlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	rgwPlacementWaitCreateBucket(t, ctx, clients[0], bucket, body)
	payload := bytes.Repeat([]byte("realm-selected-storage-class\n"), 4096)
	rgwPlacementRequest(t, ctx, clients[0], http.MethodPut, bucket+"/replicated", payload, "STANDARD_IA", http.StatusOK)
	rgwPlacementWaitReplica(t, ctx, clients[1], bucket+"/replicated", payload)
	var replicaListing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(clients[1].request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &replicaListing); err != nil || len(replicaListing.Contents) != 1 || replicaListing.Contents[0].Key != "replicated" || replicaListing.Contents[0].StorageClass != "STANDARD_IA" {
		t.Fatalf("replica did not retain STANDARD_IA: %+v error=%v", replicaListing, err)
	}
	rgwPlacementPoolPayload(t, ctx, clusters[1], "tc-realm-ia", "replicated STANDARD_IA", payload)
	for _, gateway := range []*ceph.RGWContainer{fixture.Source, fixture.Destination} {
		state, err := gateway.PlacementStatus(ctx, map[*ceph.RGWContainer]*ceph.RGWPlacement{fixture.Source: sourcePlacement, fixture.Destination: destinationPlacement}[gateway])
		if err != nil || state.DefaultPlacement != "default-placement" {
			t.Fatal("realm publication changed native default or lost zone-local mapping")
		}
	}
	clients[0].request(t, ctx, http.MethodDelete, bucket+"/replicated", nil, http.StatusNoContent)
	clients[0].request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	t.Log("matching zone-local placement mappings published through metadata-master guarded period commit; secondary publication refused, named bucket STANDARD_IA object replicated across native zones")
}

func rgwPlacementWaitReplica(t *testing.T, parent context.Context, client s3HTTPClient, path string, payload []byte) {
	t.Helper()
	// A shutdown/reload can abort native lease coroutines without unlocking.
	// v20.2.4's unchanged 120s lease, 30s backoff and 20s metadata/data polls
	// therefore need more time than the ordinary one-minute S3 policy fixture.
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	started := time.Now()
	var last string
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		client.sign(request, nil, time.Now().UTC())
		response, err := client.http.Do(request)
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && bytes.Equal(body, payload) {
				t.Logf("native realm STANDARD_IA replica matched all %d bytes after %s", len(payload), time.Since(started).Round(time.Millisecond))
				return
			}
			var failure struct{ Code, Message string }
			_ = xml.Unmarshal(body, &failure)
			last = fmt.Sprintf("HTTP=%d code=%s message=%s bytes=%d expected=%d read=%v", response.StatusCode, failure.Code, failure.Message, len(body), len(payload), readErr)
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("native realm replica did not converge after %s: %s error=%v", time.Since(started).Round(time.Millisecond), last, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func rgwPlacementSafeDiagnostic(data []byte, gateway *ceph.RGWContainer) string {
	text := string(data)
	for _, secret := range []string{gateway.AccessKey, gateway.SecretKey} {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[redacted]")
		}
	}
	// Native sync-status marker entries use "key" for shard IDs. Redact
	// credential fields structurally instead of dropping an entire JSON line.
	var native any
	if json.Unmarshal([]byte(text), &native) == nil {
		var redact func(any) any
		redact = func(value any) any {
			switch typed := value.(type) {
			case map[string]any:
				for name, child := range typed {
					lower := strings.ReplaceAll(strings.ToLower(name), "-", "_")
					if lower == "keys" || strings.Contains(lower, "secret") || strings.Contains(lower, "access_key") || strings.Contains(lower, "system_key") || strings.Contains(lower, "private_key") || strings.Contains(lower, "authorization") || strings.Contains(lower, "credential") || strings.Contains(lower, "password") || strings.Contains(lower, "token") || strings.Contains(lower, "signature") {
						typed[name] = "[redacted]"
					} else {
						typed[name] = redact(child)
					}
				}
			case []any:
				for i := range typed {
					typed[i] = redact(typed[i])
				}
			}
			return value
		}
		encoded, _ := json.Marshal(redact(native))
		if len(encoded) > 24000 {
			encoded = append(encoded[:24000], []byte(" [truncated]")...)
		}
		return string(encoded)
	}
	lines := strings.Split(text, "\n")
	safe := make([]string, 0, len(lines))
	for _, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "access_key") || strings.Contains(lower, "access-key") || strings.Contains(lower, "authorization") || strings.Contains(lower, "credential") || strings.Contains(lower, "signature") || strings.Contains(lower, "token") || strings.Contains(lower, "key=") || strings.Contains(lower, "key:") || strings.Contains(lower, `"key"`) {
			safe = append(safe, "[credential-bearing diagnostic omitted]")
			continue
		}
		safe = append(safe, line)
	}
	if len(safe) > 35 {
		safe = safe[len(safe)-35:]
	}
	text = strings.Join(safe, "\n")
	if len(text) > 6000 {
		text = text[len(text)-6000:]
	}
	return text
}

func rgwPlacementSafeLogDiagnostic(data []byte, gateway *ceph.RGWContainer) string {
	var selected []string
	for _, line := range strings.Split(string(data), "\n") {
		lower := strings.ToLower(line)
		for _, marker := range []string{"meta sync:", "data sync:", "sync error", "lease", "couldn't lock", "backoff", "failed", "error", "warning", "rgw realm reloader:", "placement target", "placement rule", "placement pool"} {
			if strings.Contains(lower, marker) {
				selected = append(selected, line)
				break
			}
		}
	}
	if len(selected) == 0 {
		return "no native sync/lease/reload errors in captured logs"
	}
	return rgwPlacementSafeDiagnostic([]byte(strings.Join(selected, "\n")), gateway)
}

func rgwPlacementWaitCreateBucket(t *testing.T, parent context.Context, client s3HTTPClient, path string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	var status int
	var body []byte
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, client.endpoint+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		client.sign(req, payload, time.Now().UTC())
		response, err := client.http.Do(req)
		if err == nil {
			body, err = io.ReadAll(response.Body)
			response.Body.Close()
			status = response.StatusCode
			if err != nil {
				t.Fatal(err)
			}
			if status == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("realm gateway did not reload placement: HTTP=%d body=%s err=%v", status, body, ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func testRGWPlacementStorageClasses(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	options = append(options, ceph.WithOSDCount(3))
	cluster, control := newServiceCluster(t, options...)
	for _, config := range []ceph.PoolConfig{
		{Name: "tc-rgw-index", Application: "rgw", PGNum: 1},
		{Name: "tc-rgw-extra", Application: "rgw", PGNum: 1},
		{Name: "tc-rgw-standard", Application: "rgw", PGNum: 1},
		{Name: "tc-rgw-ec", Application: "rgw", PGNum: 1, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}},
	} {
		if _, err := cluster.CreatePool(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := cluster.StartRGW(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := s3HTTPClient{endpoint: endpoint, accessKey: gateway.AccessKey, secretKey: gateway.SecretKey, region: gateway.Region, http: &http.Client{Timeout: 30 * time.Second}}
	const defaultBucket, tieredBucket = "/tc-rgw-original-default", "/tc-rgw-tiered-placement"
	defaultPayload := []byte("existing default-placement bucket survives a new target")
	client.request(t, ctx, http.MethodPut, defaultBucket, nil, http.StatusOK)
	client.request(t, ctx, http.MethodPut, defaultBucket+"/original", defaultPayload, http.StatusOK)
	inline := false
	config := ceph.RGWPlacementConfig{Name: "tc-tiered", IndexPool: "tc-rgw-index", DataExtraPool: "tc-rgw-extra", InlineData: &inline,
		StorageClasses: []ceph.RGWStorageClassConfig{{Name: "STANDARD", DataPool: "tc-rgw-standard"}, {Name: "STANDARD_IA", DataPool: "tc-rgw-ec"}}}
	placement, err := gateway.CreatePlacement(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	state, err := gateway.PlacementStatus(ctx, placement)
	if err != nil || state.DefaultPlacement != "default-placement" || placement.ZoneID == "" || placement.ZonegroupID == "" || state.LocationConstraint == "" {
		t.Fatalf("native placement policy: %+v %v", state, err)
	}
	if _, err := gateway.CreatePlacement(ctx, config); err == nil {
		t.Fatal("existing placement could be overwritten")
	}
	if err := gateway.ApplyPlacement(ctx, placement); err != nil {
		t.Fatal(err)
	}
	// Docker bridge restarts may publish a new port. Native host mode reuses
	// its allocated listener; both return the endpoint of the actual process.
	client.endpoint, err = gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := rgwFixtureWaitStatus(t, ctx, client, defaultBucket+"/original", http.StatusOK); !bytes.Equal(actual, defaultPayload) {
		t.Fatal("placement activation changed an existing default bucket")
	}
	createBody := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + placement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	client.request(t, ctx, http.MethodPut, tieredBucket, createBody, http.StatusOK)
	var bucketStats struct {
		PlacementRule string `json:"placement_rule"`
	}
	data, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(tieredBucket, "/"))
	if err != nil || json.Unmarshal(data, &bucketStats) != nil || bucketStats.PlacementRule != placement.Name {
		t.Fatalf("bucket did not retain named placement target: %s %v", data, err)
	}
	payload := bytes.Repeat([]byte("tiered-data-actual-pool\n"), 4096)
	for _, class := range []string{"STANDARD", "STANDARD_IA"} {
		path := tieredBucket + "/" + strings.ToLower(class)
		rgwPlacementRequest(t, ctx, client, http.MethodPut, path, payload, class, http.StatusOK)
		if actual := client.request(t, ctx, http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatalf("storage class %s changed object bytes", class)
		}
	}
	var objects struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.request(t, ctx, http.MethodGet, tieredBucket+"?list-type=2", nil, http.StatusOK), &objects); err != nil {
		t.Fatal(err)
	}
	if len(objects.Contents) != 2 {
		t.Fatalf("unexpected class object listing: %+v", objects.Contents)
	}
	for _, object := range objects.Contents {
		if object.StorageClass != strings.ToUpper(object.Key) {
			t.Fatalf("S3 did not report requested storage class: %+v", object)
		}
	}
	for _, mapping := range []struct{ pool, key string }{{"tc-rgw-standard", "standard"}, {"tc-rgw-ec", "standard_ia"}} {
		rgwPlacementPoolPayload(t, ctx, control, mapping.pool, strings.ToUpper(mapping.key), payload)
	}
	// A new bucket created without a location still follows the untouched
	// zonegroup default, while the preexisting default object remains readable.
	const laterDefault = "/tc-rgw-later-default"
	client.request(t, ctx, http.MethodPut, laterDefault, nil, http.StatusOK)
	data, err = gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(laterDefault, "/"))
	if err != nil || json.Unmarshal(data, &bucketStats) != nil || bucketStats.PlacementRule != "default-placement" {
		t.Fatal("named placement silently changed the default for new buckets")
	}
	if actual := client.request(t, ctx, http.MethodGet, defaultBucket+"/original", nil, http.StatusOK); !bytes.Equal(actual, defaultPayload) {
		t.Fatal("new placement operations changed existing default data")
	}
	for _, key := range []string{"standard", "standard_ia"} {
		client.request(t, ctx, http.MethodDelete, tieredBucket+"/"+key, nil, http.StatusNoContent)
	}
	client.request(t, ctx, http.MethodDelete, tieredBucket, nil, http.StatusNoContent)
	client.request(t, ctx, http.MethodDelete, defaultBucket+"/original", nil, http.StatusNoContent)
	client.request(t, ctx, http.MethodDelete, defaultBucket, nil, http.StatusNoContent)
	client.request(t, ctx, http.MethodDelete, laterDefault, nil, http.StatusNoContent)
	t.Log("named bucket placement + replicated STANDARD + EC STANDARD_IA: native mappings, S3 class listing, bytes and actual RADOS pools verified; original and later default buckets retained their original policy")
}

func rgwPlacementPoolPayload(t *testing.T, ctx context.Context, control testcontainers.Container, pool, class string, payload []byte) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	expectedHash := s3Hash(payload)
	code, reader, err := control.Exec(probeCtx, []string{"python3", "-c", rgwPlacementPoolPayloadScript, pool, strconv.Itoa(len(payload)), expectedHash}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	var proof struct {
		Pool, Object, SHA256 string
		Size                 int
	}
	if err != nil || code != 0 || json.Unmarshal(data, &proof) != nil || proof.Pool != pool || proof.Object == "" || proof.Size != len(payload) || proof.SHA256 != expectedHash {
		t.Fatalf("native %s data pool %s did not contain the exact S3 payload: %s error=%v", class, pool, data, err)
	}
	t.Logf("native %s data pool %s contains %d payload bytes SHA256=%s in object %s", class, pool, proof.Size, proof.SHA256, proof.Object)
}

// RGW shadow object names are opaque. Read native objects with their actual
// locator/namespace and prove their full content, using a fixture smaller than
// one RGW stripe. An empty head or a merely nonempty pool is insufficient proof.
const rgwPlacementPoolPayloadScript = `import hashlib, json, os, rados, sys, threading
pool, size, expected_hash = sys.argv[1:]
size = int(size)
assert 0 < size < 4 * 1024 * 1024, 'fixture must fit in one native RGW stripe'
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
match = None
observed = []
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin',
                 conf={'rados_mon_op_timeout': '15', 'rados_osd_op_timeout': '15'}) as cluster:
    with cluster.open_ioctx(pool) as io:
        for index, obj in enumerate(io.list_objects()):
            assert index < 32, 'unexpected object count in fresh placement data pool'
            native_size, _ = obj.stat()
            observed.append({'object': obj.key, 'size': native_size})
            if native_size != size:
                continue
            data = obj.read(size + 1)
            actual_hash = hashlib.sha256(data).hexdigest()
            if len(data) == size and actual_hash == expected_hash:
                match = {'pool': pool, 'object': obj.key, 'size': len(data), 'sha256': actual_hash}
                break
assert match is not None, 'exact S3 payload absent from actual pool: ' + json.dumps(observed)
deadline.cancel()
print(json.dumps(match, sort_keys=True))
`

func rgwPlacementRequest(t *testing.T, ctx context.Context, client s3HTTPClient, method, path string, payload []byte, storageClass string, status int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Storage-Class", storageClass)
	now := time.Now().UTC()
	client.sign(req, payload, now)
	// All supplied x-amz headers, including the storage-class selector, belong
	// in the canonical request even though the general fixture signer has no
	// optional policy headers.
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	hash := s3Hash(payload)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
	headers := "host:" + req.URL.Host + "\nx-amz-content-sha256:" + hash + "\nx-amz-date:" + stamp + "\nx-amz-storage-class:" + storageClass + "\n"
	canonical := strings.Join([]string{method, req.URL.EscapedPath(), strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20"), headers, signedHeaders, hash}, "\n")
	scope := day + "/" + client.region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + s3Hash([]byte(canonical))
	key := s3HMAC([]byte("AWS4"+client.secretKey), day)
	key = s3HMAC(key, client.region)
	key = s3HMAC(key, "s3")
	key = s3HMAC(key, "aws4_request")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", client.accessKey, scope, signedHeaders, hex.EncodeToString(s3HMAC(key, toSign))))
	response, err := client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != status {
		t.Fatalf("S3 storageclass %s returned %d: %s %v", storageClass, response.StatusCode, data, err)
	}
	return data
}
