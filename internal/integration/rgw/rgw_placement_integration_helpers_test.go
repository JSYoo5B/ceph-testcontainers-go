//go:build all || (integration && features)

package rgw_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// interruptedPlacementRestart drops the completed Docker Stop reply, refuses
// the first Start, and loses one read after the next Start actually succeeds.
type interruptedPlacementRestart struct {
	testcontainers.Container
	cancel                       context.CancelFunc
	stops, starts, startAttempts int
	lostRead                     bool
}

func (g *interruptedPlacementRestart) Stop(ctx context.Context, grace *time.Duration) error {
	if err := g.Container.Stop(ctx, grace); err != nil {
		return err
	}
	g.stops++
	g.cancel()
	return errors.New("injected loss of the completed Docker stop reply")
}

func (g *interruptedPlacementRestart) Start(ctx context.Context) error {
	g.startAttempts++
	if g.startAttempts == 1 {
		return errors.New("injected engine failure before Start")
	}
	if err := g.Container.Start(ctx); err != nil {
		return err
	}
	g.starts++
	return nil
}

func (g *interruptedPlacementRestart) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if g.starts > 0 && !g.lostRead && len(args) == 1 && args[0] == "hostname" {
		g.lostRead = true
		return 0, nil, errors.New("injected loss of the restarted hostname read")
	}
	return g.Container.Exec(ctx, args, opts...)
}

// losePlacementReplies drops the first successful staging update and commit replies.
type losePlacementReplies struct {
	testcontainers.Container
	updates, commits int
}

func (g *losePlacementReplies) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, out, err := g.Container.Exec(ctx, args, opts...)
	if err == nil && code == 0 && len(args) >= 2 && args[len(args)-2] == "period" {
		switch args[len(args)-1] {
		case "update":
			g.updates++
			if g.updates > 1 {
				return code, out, nil
			}
		case "commit":
			g.commits++
		default:
			return code, out, nil
		}
		if _, err := io.Copy(io.Discard, out); err != nil {
			return 0, nil, err
		}
		return 0, nil, errors.New("injected loss of the completed native period command reply")
	}
	return code, out, err
}

// rgwPlacementBindSource captures the source gateway's container and process
// start before the replicated write, within one inspection bound.
func rgwPlacementBindSource(t *testing.T, parent context.Context, gateway *rgw.Gateway) rgwReplicaSource {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, rgwPlacementReplicaBounds.Inspect)
	defer cancel()
	source, err := rgwReplicaBindSource(ctx, gateway)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func rgwPlacementWaitReplica(t *testing.T, parent context.Context, client harness.S3HTTPClient, source rgwReplicaSource, path string, payload []byte) {
	t.Helper()
	// A shutdown/reload can abort native lease coroutines without unlocking.
	// v20.2.4's unchanged 120s lease, 30s backoff and 20s metadata/data polls
	// therefore need more time than the ordinary one-minute S3 policy fixture.
	seen, err := rgwReplicaConverged(parent, client, source, path, payload, rgwPlacementReplicaBounds)
	if err != nil {
		t.Fatalf("native realm replica did not converge: %v", err)
	}
	t.Logf("native realm STANDARD_IA replica matched all %d bytes after %s", len(payload), seen.Elapsed.Round(time.Millisecond))
}

// rgwPlacementReplicaBounds keep the four-minute convergence budget; one
// probe of the small replicated object is bounded well inside it, and its
// source inspection takes at most 10 seconds of that probe.
var rgwPlacementReplicaBounds = rgwReplicaBounds{Wait: 4 * time.Minute, Read: 30 * time.Second, Inspect: 10 * time.Second, Interval: time.Second}

func rgwPlacementSafeDiagnostic(data []byte, gateway *rgw.Gateway) string {
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

func rgwPlacementSafeLogDiagnostic(data []byte, gateway *rgw.Gateway) string {
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
	text := "no native sync/lease/reload errors in captured logs"
	if len(selected) != 0 {
		text = rgwPlacementSafeDiagnostic([]byte(strings.Join(selected, "\n")), gateway)
	}
	// A crash prints its signal and backtrace before the dump of recent
	// events, which fills the window above; keep them apart.
	if crash := rgwCrashTrace(data); crash != "" {
		text += "\ncrash trace:\n" + rgwPlacementSafeDiagnostic([]byte(crash), gateway)
	}
	return text
}

// rgwCrashTrace returns the first crash report in a daemon log, from its
// signal or assertion to the end of its backtrace, or "".
func rgwCrashTrace(data []byte) string {
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "caught signal") || strings.Contains(lower, "ceph_assert") || strings.Contains(lower, "terminate called") {
			end := i + 1
			for end < len(lines) && end < i+35 && !strings.Contains(lines[end], "begin dump of recent events") {
				end++
			}
			return strings.Join(lines[i:end], "\n")
		}
	}
	return ""
}

func rgwPlacementWaitCreateBucket(t *testing.T, parent context.Context, client harness.S3HTTPClient, path string, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	var status int
	var body []byte
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, client.Endpoint+path, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		client.Sign(req, payload, time.Now().UTC())
		response, err := client.HTTP.Do(req)
		if err == nil {
			body, err = io.ReadAll(response.Body)
			_ = response.Body.Close()
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

// rgwPlacementRecoverRestart checks both sides of the restart on a real
// gateway. The peer changes and restores a live field the placement pinned.
func rgwPlacementRecoverRestart(t *testing.T, ctx context.Context, target *placement.Placement, gateway, peer *rgw.Gateway) {
	t.Helper()
	applyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	interrupted := &interruptedPlacementRestart{Container: gateway.Container, cancel: cancel}
	gateway.Container = interrupted
	defer func() { gateway.Container = interrupted.Container }()
	err := target.Apply(applyCtx, gateway)
	if err == nil || !errors.Is(applyCtx.Err(), context.Canceled) || interrupted.startAttempts != 1 || interrupted.starts != 0 {
		t.Fatalf("first restart: err=%v, caller=%v, attempts=%d starts=%d", err, applyCtx.Err(), interrupted.startAttempts, interrupted.starts)
	}

	apiName, _, ok := strings.Cut(target.LocationConstraint, ":")
	if !ok {
		t.Fatal("placement has no zonegroup API name")
	}
	if _, err := peer.Admin(ctx, "zonegroup", "modify", "--api-name", apiName+"-outside"); err != nil {
		t.Fatal(err)
	}
	refused := target.Apply(ctx, gateway)
	// Restore the native zonegroup before checking the outcome so the same
	// handle can finish the interrupted restart.
	if _, err := peer.Admin(ctx, "zonegroup", "modify", "--api-name", apiName); err != nil {
		t.Fatal(err)
	}
	state, err := gateway.State(ctx)
	if !errors.Is(refused, ceph.ErrInvalidRequest) || err != nil || state == nil || state.Running || interrupted.startAttempts != 1 {
		t.Fatalf("changed policy reached Start: refusal=%v, inspect=%v, state=%v attempts=%d", refused, err, state, interrupted.startAttempts)
	}
	if err := target.Apply(ctx, gateway); err == nil || !interrupted.lostRead || interrupted.starts != 1 {
		t.Fatalf("post-start read: err=%v, read lost=%t starts=%d", err, interrupted.lostRead, interrupted.starts)
	}
	if err := target.Apply(ctx, gateway); err != nil || interrupted.stops != 1 || interrupted.starts != 1 {
		t.Fatalf("post-start retry: err=%v stops=%d starts=%d", err, interrupted.stops, interrupted.starts)
	}
}

func testRGWPlacementStorageClasses(t *testing.T, options ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	options = append(options, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	cluster, control := harness.NewServiceCluster(t, options...)
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
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	gateway, err := rgw.Start(ctx, cluster, rgw.Config{})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 30 * time.Second}}
	const defaultBucket, tieredBucket = "/tc-rgw-original-default", "/tc-rgw-tiered-placement"
	defaultPayload := []byte("existing default-placement bucket survives a new target")
	client.Request(t, ctx, http.MethodPut, defaultBucket, nil, http.StatusOK)
	client.Request(t, ctx, http.MethodPut, defaultBucket+"/original", defaultPayload, http.StatusOK)
	inline := false
	config := placement.Config{Name: "tc-tiered", IndexPool: "tc-rgw-index", DataExtraPool: "tc-rgw-extra", InlineData: &inline,
		StorageClasses: []placement.StorageClass{{Name: "STANDARD", DataPool: "tc-rgw-standard"}, {Name: "STANDARD_IA", DataPool: "tc-rgw-ec"}}}
	target, err := placement.Create(ctx, gateway, config)
	if err != nil {
		t.Fatal(err)
	}
	state, err := target.Status(ctx)
	if err != nil || state.DefaultPlacement != "default-placement" || target.ZoneID == "" || target.ZonegroupID == "" || state.LocationConstraint == "" {
		t.Fatalf("native placement policy: %+v %v", state, err)
	}
	if _, err := placement.Create(ctx, gateway, config); err == nil {
		t.Fatal("existing placement could be overwritten")
	}
	// The stopped gateway cannot run radosgw-admin. Leave a peer serving the
	// same scope so recovery can verify the startup defaults before Start.
	peer, err := rgw.Start(ctx, cluster, rgw.Config{Name: "placement-control", SkipUserCreation: true})
	if err != nil {
		t.Fatal(err)
	}
	rgwPlacementRecoverRestart(t, ctx, target, gateway, peer)
	// Docker bridge restarts may publish a new port. Native host mode reuses
	// its allocated listener; both return the endpoint of the actual process.
	client.Endpoint, err = gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := rgwFixtureWaitStatus(t, ctx, client, defaultBucket+"/original", http.StatusOK); !bytes.Equal(actual, defaultPayload) {
		t.Fatal("placement activation changed an existing default bucket")
	}
	createBody := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + target.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	client.Request(t, ctx, http.MethodPut, tieredBucket, createBody, http.StatusOK)
	var bucketStats struct {
		PlacementRule string `json:"placement_rule"`
	}
	data, err := gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(tieredBucket, "/"))
	if err != nil || json.Unmarshal(data, &bucketStats) != nil || bucketStats.PlacementRule != target.Name {
		t.Fatalf("bucket did not retain named placement target: %s %v", data, err)
	}
	payload := bytes.Repeat([]byte("tiered-data-actual-pool\n"), 4096)
	for _, class := range []string{"STANDARD", "STANDARD_IA"} {
		path := tieredBucket + "/" + strings.ToLower(class)
		rgwPlacementRequest(t, ctx, client, http.MethodPut, path, payload, class, http.StatusOK)
		if actual := client.Request(t, ctx, http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(actual, payload) {
			t.Fatalf("storage class %s changed object bytes", class)
		}
	}
	var objects struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.Request(t, ctx, http.MethodGet, tieredBucket+"?list-type=2", nil, http.StatusOK), &objects); err != nil {
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
		harness.RGWPlacementPoolPayload(t, ctx, control, mapping.pool, strings.ToUpper(mapping.key), payload)
	}
	// A new bucket created without a location still follows the untouched
	// zonegroup default, while the preexisting default object remains readable.
	const laterDefault = "/tc-rgw-later-default"
	client.Request(t, ctx, http.MethodPut, laterDefault, nil, http.StatusOK)
	data, err = gateway.Admin(ctx, "bucket", "stats", "--bucket", strings.TrimPrefix(laterDefault, "/"))
	if err != nil || json.Unmarshal(data, &bucketStats) != nil || bucketStats.PlacementRule != "default-placement" {
		t.Fatal("named placement silently changed the default for new buckets")
	}
	if actual := client.Request(t, ctx, http.MethodGet, defaultBucket+"/original", nil, http.StatusOK); !bytes.Equal(actual, defaultPayload) {
		t.Fatal("new placement operations changed existing default data")
	}
	for _, key := range []string{"standard", "standard_ia"} {
		client.Request(t, ctx, http.MethodDelete, tieredBucket+"/"+key, nil, http.StatusNoContent)
	}
	client.Request(t, ctx, http.MethodDelete, tieredBucket, nil, http.StatusNoContent)
	client.Request(t, ctx, http.MethodDelete, defaultBucket+"/original", nil, http.StatusNoContent)
	client.Request(t, ctx, http.MethodDelete, defaultBucket, nil, http.StatusNoContent)
	client.Request(t, ctx, http.MethodDelete, laterDefault, nil, http.StatusNoContent)
	t.Log("named bucket placement + replicated STANDARD + EC STANDARD_IA: native mappings, S3 class listing, bytes and actual RADOS pools verified; original and later default buckets retained their original policy")
}

func rgwPlacementRequest(t *testing.T, ctx context.Context, client harness.S3HTTPClient, method, path string, payload []byte, storageClass string, status int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, client.Endpoint+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Storage-Class", storageClass)
	now := time.Now().UTC()
	client.Sign(req, payload, now)
	// All supplied x-amz headers, including the storage-class selector, belong
	// in the canonical request even though the general fixture signer has no
	// optional policy headers.
	stamp, day := now.Format("20060102T150405Z"), now.Format("20060102")
	hash := harness.S3Hash(payload)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"
	headers := "host:" + req.URL.Host + "\nx-amz-content-sha256:" + hash + "\nx-amz-date:" + stamp + "\nx-amz-storage-class:" + storageClass + "\n"
	canonical := strings.Join([]string{method, req.URL.EscapedPath(), strings.ReplaceAll(req.URL.Query().Encode(), "+", "%20"), headers, signedHeaders, hash}, "\n")
	scope := day + "/" + client.Region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + harness.S3Hash([]byte(canonical))
	key := harness.S3HMAC([]byte("AWS4"+client.SecretKey), day)
	key = harness.S3HMAC(key, client.Region)
	key = harness.S3HMAC(key, "s3")
	key = harness.S3HMAC(key, "aws4_request")
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", client.AccessKey, scope, signedHeaders, hex.EncodeToString(harness.S3HMAC(key, toSign))))
	response, err := client.HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != status {
		t.Fatalf("S3 storageclass %s returned %d: %s %v", storageClass, response.StatusCode, data, err)
	}
	return data
}
