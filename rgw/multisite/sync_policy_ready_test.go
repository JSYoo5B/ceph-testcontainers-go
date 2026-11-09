package multisite

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/moby/moby/api/pkg/stdcopy"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This wrapper gives the destination a genuinely independent metadata view;
// the existing fake's global caught-up status must not stand in for import.
type rgwPolicyImportCLI struct {
	*rgwSyncCLI
	localPolicy  map[string]any
	localPeriod  map[string]any
	localBuckets map[string]string
	// missingBuckets answers bucket stats as radosgw-admin does for a bucket
	// the destination has not imported: exit 2, no output and the
	// ERR_NO_SUCH_BUCKET line on stderr. failedBuckets fails the same read
	// with output, and stderrFailedBuckets with exit 2, no output and another
	// diagnostic; both are failed reads rather than an absent bucket.
	missingBuckets, failedBuckets, stderrFailedBuckets map[string]bool
	policyLagReads, localReads                         int
	localMetadataID, localGroupID                      string
	malformedStats                                     bool
	// rawStats, when set, is served verbatim as the destination bucket stats,
	// so a test can send repeated or case-aliased keys a map cannot hold.
	rawStats          string
	onLocalPolicyRead func()
	// onExec, when set, sees each command before the wrapper serves it.
	onExec func(context.Context, []string)
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

func policyImportRaw(data string) io.Reader {
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stdout)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.WriteString(data)
	return bytes.NewReader(stream.Bytes())
}

func policyImportStderr(data string) io.Reader {
	var stream bytes.Buffer
	header := [8]byte{byte(stdcopy.Stderr)}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	stream.Write(header[:])
	stream.WriteString(data)
	return bytes.NewReader(stream.Bytes())
}

func (c *rgwPolicyImportCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if c.onExec != nil {
		c.onExec(ctx, args)
	}
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
			return 2, policyImportStderr("failure: (2002) Unknown error 2002: \n"), nil
		}
		if c.stderrFailedBuckets[name] {
			c.calls = append(c.calls, slices.Clone(args))
			return 2, policyImportStderr("monclient: unable to find a keyring on /etc/ceph/secret-marker.keyring: (2) No such file or directory\n"), nil
		}
		if c.failedBuckets[name] {
			c.calls = append(c.calls, slices.Clone(args))
			return 2, policyImportJSON(map[string]any{"secret": "missing native metadata secret-marker"}), nil
		}
		if c.rawStats != "" {
			c.calls = append(c.calls, slices.Clone(args))
			return 0, policyImportRaw(c.rawStats), nil
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

func newRGWPolicyImportFixture(t *testing.T) (*Fixture, *rgwPolicyImportCLI, *SyncGroup) {
	t.Helper()
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	observer := &rgwPolicyImportCLI{rgwSyncCLI: cli, localPolicy: syncJSONClone(cli.bucketPolicy)}
	f.realm.sourceClient, f.realm.destinationClient = observer, observer
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
	_, cli, g := newRGWPolicyImportFixture(t)
	cli.policyLagReads = 1
	before := len(cli.calls)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	state, err := g.WaitBucketPolicyReady(ctx, "destination")
	if err != nil || !state.Imported || !state.PeriodImported || !state.PolicyImported || !state.BucketsImported || cli.localReads < 2 || state.PeriodID != "period-original" || state.GroupID != g.ID() || state.ZoneID != "dest-id" || state.Bucket.ID != "bucket-original" || len(state.Buckets) != 1 {
		t.Fatalf("local metadata was not independently observed: %+v %v", state, err)
	}
	requirePolicyImportReadOnly(t, cli.calls[before:])
	state.Buckets[0].ID = "caller-mutated"
	state, err = g.WaitBucketPolicyReady(t.Context(), "destination")
	if err != nil || !state.Imported || state.Buckets[0].ID != "bucket-original" || g.bucketID != "bucket-original" {
		t.Fatal("returned identity slice could redirect the owned policy observer")
	}
}

func TestRGWBucketSyncPolicyReadyKeepsTenantAndRequiredSideInstances(t *testing.T) {
	f, native, _, _ := newRGWSyncTestFixture()
	native.bucketTenant = "tenant-a"
	native.additionalBuckets = map[string]string{"tenant-b/output": "output-original"}
	g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{Bucket: "bucket", Tenant: "tenant-a"}, SyncGroupConfig{ID: "tenant-import", Status: SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreatePipe(t.Context(), SyncPipeConfig{ID: "translated", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "bucket", Tenant: "tenant-a"}, DestinationBucket: &SyncBucketSelector{Name: "output", Tenant: "tenant-b"}}); err != nil {
		t.Fatal(err)
	}
	cli := &rgwPolicyImportCLI{rgwSyncCLI: native, localPolicy: syncJSONClone(native.bucketPolicy)}
	f.realm.sourceClient, f.realm.destinationClient = cli, cli
	before := len(cli.calls)
	status, err := g.WaitBucketPolicyReady(t.Context(), "destination")
	want := []SyncBucketIdentity{{Name: "bucket", Tenant: "tenant-a", ID: "bucket-original"}, {Name: "output", Tenant: "tenant-b", ID: "output-original"}}
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
	status, err = g.WaitBucketPolicyReady(ctx, "destination")
	if err == nil || errors.Is(err, context.DeadlineExceeded) || status.Imported || status.BucketsImported {
		t.Fatalf("recreated required destination instance was adopted or treated as ordinary import lag: %+v %v", status, err)
	}
}

func TestRGWBucketSyncPolicyReadyWaitsForPolicyPeriodAndReferencedBucketImports(t *testing.T) {
	for _, scenario := range []string{"policy", "period", "destination-bucket"} {
		t.Run(scenario, func(t *testing.T) {
			_, cli, g := newRGWPolicyImportFixture(t)
			cli.additionalBuckets = map[string]string{"output": "output-original"}
			if err := g.CreatePipe(t.Context(), SyncPipeConfig{ID: "translated", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "bucket"}, DestinationBucket: &SyncBucketSelector{Name: "output"}}); err != nil {
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
			// The deadline passes once the first observation of the
			// destination ended, rather than after a wall-clock interval
			// that a slow (race-instrumented) run can end mid-observation.
			ctx := clustertest.NewSignalDeadline(t.Context())
			cli.onExec = expireAfterDestinationObservation(ctx)
			state, err := g.WaitBucketPolicyReady(ctx, "destination")
			if !errors.Is(err, context.DeadlineExceeded) || state.Imported || len(state.Buckets) != 2 || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("partial local imports became ready or leaked native text: %+v %v", state, err)
			}
			if scenario == "policy" && (!state.PeriodImported || state.PolicyImported || !state.BucketsImported) {
				t.Fatal("policy lag was confused with period/bucket readiness")
			}
			// A destination bucket not imported yet is a stage that the read
			// reports, not a failed read.
			if scenario == "destination-bucket" && (!state.PeriodImported || !state.PolicyImported || state.BucketsImported) {
				t.Fatalf("a destination bucket not imported yet was not reported as the bucket stage: %+v", state)
			}
			requirePolicyImportReadOnly(t, cli.calls[before:])
		})
	}
}

func TestRGWBucketSyncPolicyReadyRefusesRecreatedForeignAndMalformedMetadata(t *testing.T) {
	for _, scenario := range []string{"recreated-bucket", "missing-id", "foreign-group", "foreign-period", "staging-period", "metadata-id", "malformed-stats", "repeated-tenant-null", "case-alias-bucket"} {
		t.Run(scenario, func(t *testing.T) {
			_, cli, g := newRGWPolicyImportFixture(t)
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
			case "repeated-tenant-null":
				// The later null wins; the earlier valid tenant must not.
				cli.rawStats = `{"bucket":"bucket","tenant":"","tenant":null,"id":"bucket-original","zonegroup":"group-id"}`
			case "case-alias-bucket":
				// Only the exact native key names the bucket.
				cli.rawStats = `{"bucket":"other","BUCKET":"bucket","tenant":"","id":"bucket-original","zonegroup":"group-id"}`
			}
			before := len(cli.calls)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			state, err := g.WaitBucketPolicyReady(ctx, "destination")
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
				g = &SyncGroup{}
			case "removed":
				// A removed handle keeps the destinations its pipes selected.
				if err := g.Remove(ctx); err != nil {
					t.Fatal(err)
				}
				zone = "source"
			case "pending":
				g.state.pending = &rgwSyncPending{}
			case "global":
				g.scope.Bucket = ""
			case "wrong-zone":
				zone = "foreign"
			case "wrong-direction":
				zone = "source"
			case "unattached":
				f.realm.destinationClient = nil
			case "canceled":
				cancel()
			}
			before := len(cli.calls)
			if _, err := g.WaitBucketPolicyReady(ctx, zone); err == nil || len(cli.calls) != before {
				t.Fatal("invalid observer admission invoked native commands")
			}
		})
	}
}

// A handle removed through the fixture observes the removal's import: the
// destination's instance of the group's bucket must stop carrying the group.
// Only that bucket is needed, so a pipe's other bucket that the destination
// lacks does not hold the wait.
func TestRGWBucketSyncPolicyReadyObservesARemoval(t *testing.T) {
	_, cli, g := newRGWPolicyImportFixture(t)
	cli.additionalBuckets = map[string]string{"output": "output-original"}
	if err := g.CreatePipe(t.Context(), SyncPipeConfig{ID: "translated", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "bucket"}, DestinationBucket: &SyncBucketSelector{Name: "output"}}); err != nil {
		t.Fatal(err)
	}
	stale := syncJSONClone(cli.bucketPolicy)
	copy := *g
	if err := g.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	cli.localPolicy, cli.missingBuckets = stale, map[string]bool{"output": true}
	before := len(cli.calls)
	ctx := clustertest.NewSignalDeadline(t.Context())
	cli.onExec = expireAfterDestinationObservation(ctx)
	state, err := g.WaitBucketPolicyReady(ctx, "destination")
	scope := SyncBucketIdentity{Name: "bucket", ID: "bucket-original"}
	if !errors.Is(err, context.DeadlineExceeded) || state.Imported || !state.PeriodImported || state.PolicyImported || !state.BucketsImported || !slices.Equal(state.Buckets, []SyncBucketIdentity{scope}) {
		t.Fatalf("a destination that still carries the removed group read as its removal: %+v %v", state, err)
	}
	cli.onExec, cli.localPolicy = nil, map[string]any{"groups": []any{}}
	state, err = copy.WaitBucketPolicyReady(t.Context(), "destination")
	if err != nil || !state.Imported || !state.PolicyImported || state.Bucket != scope || !slices.Equal(state.Buckets, []SyncBucketIdentity{scope}) {
		t.Fatalf("the imported removal was not observed through a copy of the handle: %+v %v", state, err)
	}
	requirePolicyImportReadOnly(t, cli.calls[before:])
	// The group back on the master is not the removal the handle confirmed.
	cli.bucketPolicy = stale
	ctx2, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if state, err := g.WaitBucketPolicyReady(ctx2, "destination"); err == nil || errors.Is(err, context.DeadlineExceeded) || state.Imported {
		t.Fatalf("a group recreated on the master was adopted as the removal: %+v %v", state, err)
	}
}

func TestRGWBucketSyncPolicyReadyBoundsMutexQueueAndCancellationAfterRead(t *testing.T) {
	f, cli, g := newRGWPolicyImportFixture(t)
	f.topologyMu.Lock()
	before := len(cli.calls)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	started := time.Now()
	_, err := g.WaitBucketPolicyReady(ctx, "destination")
	cancel()
	f.topologyMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || len(cli.calls) != before {
		t.Fatal("policy observer excluded mutex queue time from the caller deadline")
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	cli.onLocalPolicyRead = cancel
	state, err := g.WaitBucketPolicyReady(ctx, "destination")
	if !errors.Is(err, context.Canceled) || state.Imported {
		t.Fatal("ready local metadata completed after caller cancellation")
	}
}

func TestRGWBucketSyncPolicyReadyRevalidatesMasterWhileLocalPolicyLags(t *testing.T) {
	for _, change := range []string{"policy", "period", "instance"} {
		t.Run(change, func(t *testing.T) {
			_, cli, g := newRGWPolicyImportFixture(t)
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
			state, err := g.WaitBucketPolicyReady(ctx, "destination")
			if err == nil || errors.Is(err, context.DeadlineExceeded) || state.Imported {
				t.Fatalf("changed master state became a new import target: %+v %v", state, err)
			}
		})
	}
}

func TestRGWBucketSyncPolicyReadyRetriesAFailedMasterRead(t *testing.T) {
	_, cli, g := newRGWPolicyImportFixture(t)
	cli.policyLagReads = 1
	failed := false
	cli.onExec = func(_ context.Context, args []string) {
		if !failed && syncTestFlag(args, "--zone-id") == "dest-id" && syncTestCommand(args, "period", "get") {
			failed, cli.remainingReadFailures = true, 1
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	state, err := g.WaitBucketPolicyReady(ctx, "destination")
	if err != nil || !state.Imported || !failed || cli.remainingReadFailures != 0 {
		t.Fatalf("a failed master read ended the wait: %+v %v", state, err)
	}
}

// Only the native ERR_NO_SUCH_BUCKET answer of bucket stats is an absent
// bucket; every other failure stays a failure.
func TestRGWNoSuchBucketAnswerRecognizesOnlyThatAnswer(t *testing.T) {
	for name, test := range map[string]struct {
		code           int
		stdout, stderr string
		absent         bool
	}{
		"no such bucket":           {2, "", "failure: (2002) Unknown error 2002: \n", true},
		"no such bucket, no space": {2, "", "failure: (2002) Unknown error 2002:", true},
		"missing keyring":          {2, "", "auth: unable to find a keyring on /k: (2) No such file or directory\n", false},
		"ENOENT of another entry":  {2, "", "ERROR: can't get key: (2) No such file or directory\n", false},
		"extra diagnostic":         {2, "", "warning\nfailure: (2002) Unknown error 2002: \n", false},
		"output":                   {2, "{}", "failure: (2002) Unknown error 2002: \n", false},
		"other status":             {1, "", "failure: (2002) Unknown error 2002: \n", false},
	} {
		if got := rgwNoSuchBucketAnswer(test.code, []byte(test.stdout), []byte(test.stderr)); got != test.absent {
			t.Errorf("%s: absent %t, want %t", name, got, test.absent)
		}
	}
	if bucketStatsAbsent(nil) || bucketStatsAbsent(errors.New("RGW native command failed; output is redacted")) {
		t.Fatal("an error without the decision read as an absent bucket")
	}
}

// A failed native command returns no byte of its output in any formatting of
// the error chain, the absent-bucket decision included.
func TestRGWNativeFailureKeepsNoOutput(t *testing.T) {
	_, cli, _ := newRGWPolicyImportFixture(t)
	const marker = "secret-marker"
	// The Go-syntax bytes of the marker, as %#v prints a byte slice field.
	goBytes := strings.TrimSuffix(strings.TrimPrefix(fmt.Sprintf("%#v", []byte(marker)), "[]byte{"), "}")
	args := []string{"radosgw-admin", "--zone-id", "dest-id", "--format", "json", "bucket", "stats", "--bucket", "bucket"}
	for name, set := range map[string]func(){
		"absent": func() {
			cli.missingBuckets, cli.stderrFailedBuckets, cli.failedBuckets = map[string]bool{"bucket": true}, nil, nil
		},
		"stderr only": func() {
			cli.missingBuckets, cli.stderrFailedBuckets, cli.failedBuckets = nil, map[string]bool{"bucket": true}, nil
		},
		"output": func() {
			cli.missingBuckets, cli.stderrFailedBuckets, cli.failedBuckets = nil, nil, map[string]bool{"bucket": true}
		},
	} {
		set()
		_, err := syncNativeExecMode(t.Context(), cli, false, args...)
		if err == nil {
			t.Fatalf("%s: no failure", name)
		}
		if bucketStatsAbsent(err) != (name == "absent") {
			t.Fatalf("%s: absent decision %t", name, bucketStatsAbsent(err))
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			for chained := error(err); chained != nil; chained = errors.Unwrap(chained) {
				if text := fmt.Sprintf(verb, chained); strings.Contains(text, marker) || strings.Contains(text, fmt.Sprintf("%x", marker)) ||
					strings.Contains(text, "2002") || strings.Contains(text, goBytes) {
					t.Fatalf("%s: %s of %T shows native output: %s", name, verb, chained, text)
				}
			}
		}
	}
}
