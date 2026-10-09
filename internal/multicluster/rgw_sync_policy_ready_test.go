package multicluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This wrapper gives the destination a genuinely independent metadata view;
// the existing fake's global caught-up status must not stand in for import.
type rgwPolicyImportCLI struct {
	*rgwSyncCLI
	localPolicy                   map[string]any
	localPeriod                   map[string]any
	localBuckets                  map[string]string
	missingBuckets                map[string]bool
	policyLagReads, localReads    int
	localMetadataID, localGroupID string
	malformedStats                bool
	onLocalPolicyRead             func()
}

func policyImportJSON(value any) io.Reader {
	data, _ := json.Marshal(value)
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.Write(data)
	return bytes.NewReader(stream.Bytes())
}

func (c *rgwPolicyImportCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if syncTestFlag(args, "--zone-id") != "dest-id" {
		return c.rgwSyncCLI.Exec(ctx, args, opts...)
	}
	if syncTestCommand(args, "period", "get") && c.localPeriod != nil {
		c.calls = append(c.calls, slices.Clone(args))
		return 0, policyImportJSON(c.localPeriod), nil
	}
	if syncTestCommand(args, "bucket", "stats") {
		name := syncTestFlag(args, "--bucket")
		if c.missingBuckets[name] {
			c.calls = append(c.calls, slices.Clone(args))
			return 2, policyImportJSON(map[string]any{"secret": "missing native metadata secret-marker"}), nil
		}
		if c.malformedStats {
			c.calls = append(c.calls, slices.Clone(args))
			return 0, policyImportJSON(map[string]any{"bucket": "foreign", "zonegroup": "foreign"}), nil
		}
		if id, exists := c.localBuckets[name]; exists {
			c.calls = append(c.calls, slices.Clone(args))
			tenant, bucket := "", name
			if prefix, suffix, found := strings.Cut(name, "/"); found {
				tenant, bucket = prefix, suffix
			}
			group := "group-id"
			if c.localGroupID != "" {
				group = c.localGroupID
			}
			return 0, policyImportJSON(map[string]any{"bucket": bucket, "tenant": tenant, "id": id, "zonegroup": group}), nil
		}
	}
	if syncTestCommand(args, "metadata", "get") {
		c.calls = append(c.calls, slices.Clone(args))
		c.localReads++
		key := args[len(args)-1]
		name, id, _ := strings.Cut(strings.TrimPrefix(key, "bucket.instance:"), ":")
		tenant := ""
		if prefix, suffix, found := strings.Cut(name, "/"); found {
			tenant, name = prefix, suffix
		}
		if c.localMetadataID != "" {
			id = c.localMetadataID
		}
		policy := c.localPolicy
		if c.policyLagReads > 0 {
			c.policyLagReads--
			policy = map[string]any{"groups": []any{}}
		}
		if c.onLocalPolicyRead != nil {
			c.onLocalPolicyRead()
		}
		return 0, policyImportJSON(map[string]any{"key": key, "data": map[string]any{"bucket_info": map[string]any{"bucket": map[string]any{"name": name, "tenant": tenant, "bucket_id": id}, "zonegroup": "group-id", "sync_policy": policy}}}), nil
	}
	return c.rgwSyncCLI.Exec(ctx, args, opts...)
}

func newRGWPolicyImportFixture(t *testing.T) (*RGWMultisite, *rgwPolicyImportCLI, *RGWSyncGroup) {
	t.Helper()
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	observer := &rgwPolicyImportCLI{rgwSyncCLI: cli, localPolicy: syncJSONClone(cli.bucketPolicy)}
	f.sourceClient, f.destinationClient = observer, observer
	return f, observer, g
}

func requirePolicyImportReadOnly(t *testing.T, calls [][]string) {
	t.Helper()
	for _, args := range calls {
		if !syncTestCommand(args, "period", "get") && !syncTestCommand(args, "zonegroup", "get") && !syncTestCommand(args, "bucket", "stats") && !syncTestCommand(args, "metadata", "get") {
			t.Fatalf("policy import observer issued a non-observation command: %v", args)
		}
	}
}

func TestRGWBucketSyncPolicyReadyRequiresIndependentLocalSnapshot(t *testing.T) {
	f, cli, g := newRGWPolicyImportFixture(t)
	cli.policyLagReads = 1
	before := len(cli.calls)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	state, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
	if err != nil || !state.Imported || !state.PeriodImported || !state.PolicyImported || !state.BucketsImported || cli.localReads < 2 || state.PeriodID != "period-original" || state.GroupID != g.ID() || state.ZoneID != "dest-id" || state.Bucket.ID != "bucket-original" || len(state.Buckets) != 1 {
		t.Fatalf("local metadata was not independently observed: %+v %v", state, err)
	}
	requirePolicyImportReadOnly(t, cli.calls[before:])
	state.Buckets[0].ID = "caller-mutated"
	state, err = f.WaitBucketSyncPolicyReady(t.Context(), g, "destination")
	if err != nil || !state.Imported || state.Buckets[0].ID != "bucket-original" || g.bucketID != "bucket-original" {
		t.Fatal("returned identity slice could redirect the owned policy observer")
	}
}

func TestRGWBucketSyncPolicyReadyKeepsTenantAndRequiredSideInstances(t *testing.T) {
	f, native, _, _ := newRGWSyncTestFixture()
	native.bucketTenant = "tenant-a"
	native.additionalBuckets = map[string]string{"tenant-b/output": "output-original"}
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{Bucket: "bucket", Tenant: "tenant-a"}, RGWSyncGroupConfig{ID: "tenant-import", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "translated", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &RGWSyncBucketSelector{Name: "bucket", Tenant: "tenant-a"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output", Tenant: "tenant-b"}}); err != nil {
		t.Fatal(err)
	}
	cli := &rgwPolicyImportCLI{rgwSyncCLI: native, localPolicy: syncJSONClone(native.bucketPolicy)}
	f.sourceClient, f.destinationClient = cli, cli
	before := len(cli.calls)
	status, err := f.WaitBucketSyncPolicyReady(t.Context(), g, "destination")
	want := []RGWSyncBucketIdentity{{Name: "bucket", Tenant: "tenant-a", ID: "bucket-original"}, {Name: "output", Tenant: "tenant-b", ID: "output-original"}}
	if err != nil || !status.Imported || !slices.Equal(status.Buckets, want) || status.Bucket != want[0] {
		t.Fatalf("tenant-qualified source/destination imports lost their captured identities: %+v %v", status, err)
	}
	for _, args := range cli.calls[before:] {
		if syncTestFlag(args, "--tenant") != "" {
			t.Fatal("observer used a global user tenant option instead of a qualified bucket namespace")
		}
	}
	requirePolicyImportReadOnly(t, cli.calls[before:])
	cli.localBuckets = map[string]string{"tenant-b/output": "replacement"}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	status, err = f.WaitBucketSyncPolicyReady(ctx, g, "destination")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || status.Imported || status.BucketsImported {
		t.Fatalf("recreated required destination instance was adopted or treated as ordinary import lag: %+v %v", status, err)
	}
}

func TestRGWBucketSyncPolicyReadyWaitsForPolicyPeriodAndReferencedBucketImports(t *testing.T) {
	for _, scenario := range []string{"policy", "period", "destination-bucket"} {
		t.Run(scenario, func(t *testing.T) {
			f, cli, g := newRGWPolicyImportFixture(t)
			cli.additionalBuckets = map[string]string{"output": "output-original"}
			if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "translated", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &RGWSyncBucketSelector{Name: "bucket"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output"}}); err != nil {
				t.Fatal(err)
			}
			cli.localPolicy = syncJSONClone(cli.bucketPolicy)
			switch scenario {
			case "policy":
				cli.localPolicy = map[string]any{"groups": []any{}}
			case "period":
				cli.localPeriod = syncJSONClone(cli.current)
				cli.localPeriod["epoch"] = json.Number("0")
			case "destination-bucket":
				cli.missingBuckets = map[string]bool{"output": true}
			}
			before := len(cli.calls)
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			state, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
			if !errors.Is(err, context.DeadlineExceeded) || state.Imported || len(state.Buckets) != 2 || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("partial local imports became ready or leaked native text: %+v %v", state, err)
			}
			if scenario == "policy" && (!state.PeriodImported || state.PolicyImported || !state.BucketsImported) {
				t.Fatal("policy lag was confused with period/bucket readiness")
			}
			requirePolicyImportReadOnly(t, cli.calls[before:])
		})
	}
}

func TestRGWBucketSyncPolicyReadyRefusesRecreatedForeignAndMalformedMetadata(t *testing.T) {
	for _, scenario := range []string{"recreated-bucket", "missing-id", "foreign-group", "foreign-period", "staging-period", "metadata-id", "malformed-stats"} {
		t.Run(scenario, func(t *testing.T) {
			f, cli, g := newRGWPolicyImportFixture(t)
			switch scenario {
			case "recreated-bucket":
				cli.localBuckets = map[string]string{"bucket": "replacement"}
			case "missing-id":
				cli.localBuckets = map[string]string{"bucket": ""}
			case "foreign-group":
				cli.localBuckets = map[string]string{"bucket": "bucket-original"}
				cli.localGroupID = "foreign"
			case "foreign-period":
				cli.localPeriod = syncJSONClone(cli.current)
				cli.localPeriod["realm_id"] = "foreign"
			case "staging-period":
				cli.localPeriod = syncJSONClone(cli.current)
				cli.localPeriod["id"] = "realm:staging"
			case "metadata-id":
				cli.localMetadataID = "replacement"
			case "malformed-stats":
				cli.malformedStats = true
			}
			before := len(cli.calls)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			state, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
			if err == nil || errors.Is(err, context.DeadlineExceeded) || state.Imported || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("invalid metadata was adopted or treated as ordinary lag: %+v %v", state, err)
			}
			requirePolicyImportReadOnly(t, cli.calls[before:])
		})
	}
}

func TestRGWBucketSyncPolicyReadyRejectsInvalidAdmissionBeforeCommands(t *testing.T) {
	for _, scenario := range []string{"unowned", "removed", "pending", "global", "wrong-zone", "wrong-direction", "unattached", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f, cli, g := newRGWPolicyImportFixture(t)
			zone := "destination"
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			switch scenario {
			case "unowned":
				g = &RGWSyncGroup{}
			case "removed":
				g.state.removed = true
			case "pending":
				g.state.pending = &rgwSyncPending{}
			case "global":
				g.scope.Bucket = ""
			case "wrong-zone":
				zone = "foreign"
			case "wrong-direction":
				zone = "source"
			case "unattached":
				f.destinationClient = nil
			case "canceled":
				cancel()
			}
			before := len(cli.calls)
			if _, err := f.WaitBucketSyncPolicyReady(ctx, g, zone); err == nil || len(cli.calls) != before {
				t.Fatal("invalid observer admission invoked native commands")
			}
		})
	}
}

func TestRGWBucketSyncPolicyReadyBoundsMutexQueueAndCancellationAfterRead(t *testing.T) {
	f, cli, g := newRGWPolicyImportFixture(t)
	f.topologyMu.Lock()
	before := len(cli.calls)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	started := time.Now()
	_, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
	cancel()
	f.topologyMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || len(cli.calls) != before {
		t.Fatal("policy observer excluded mutex queue time from the caller deadline")
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	cli.onLocalPolicyRead = cancel
	state, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
	if !errors.Is(err, context.Canceled) || state.Imported {
		t.Fatal("ready local metadata completed after caller cancellation")
	}
}

func TestRGWBucketSyncPolicyReadyRevalidatesMasterWhileLocalPolicyLags(t *testing.T) {
	for _, change := range []string{"policy", "period", "instance"} {
		t.Run(change, func(t *testing.T) {
			f, cli, g := newRGWPolicyImportFixture(t)
			cli.policyLagReads = 1
			cli.onLocalPolicyRead = func() {
				cli.onLocalPolicyRead = nil
				switch change {
				case "policy":
					syncNativeGroup(cli.bucketPolicy, g.ID())["status"] = "forbidden"
				case "period":
					cli.current["epoch"] = json.Number("2")
				case "instance":
					cli.bucketID = "recreated"
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			state, err := f.WaitBucketSyncPolicyReady(ctx, g, "destination")
			if err == nil || errors.Is(err, context.DeadlineExceeded) || state.Imported {
				t.Fatalf("changed master state became a new import target: %+v %v", state, err)
			}
		})
	}
}
