//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_post_test_verification))))

//ci: timeout=25m job-timeout=35 image-roles=essential

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
)

// TestPostTestVerificationLogEnvelope pairs a real WRN cluster log entry of
// the MON with INF entries that quote a WRN marker. The probe is the MON's
// own health check entry for a set noout flag. The INF entries are produced
// by the `log` command, which the MON writes at INF to the cluster channel and, as
// the command line, to the audit channel. A required warning that only the
// quoting INF entries could satisfy must stay unsatisfied, while the real
// WRN probe satisfies its own. The raw MON output is read independently to
// show that both kinds of line were present, and the verification must
// report no log gap.
func TestPostTestVerificationLogEnvelope(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	cluster, err := ceph.Run(ctx, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))...)
	recorder := &finalizationRecorder{TB: t}
	var finalization *ceph.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = cluster.TerminateOnCleanup(recorder, ceph.FinalizationConfig{Archive: true, ArchiveDirectory: t.TempDir(), Scenario: "post-test-log-envelope"})
		if registerErr != nil {
			_ = cluster.Terminate(context.WithoutCancel(ctx))
			t.Fatal(registerErr)
		}
		t.Cleanup(recorder.finalize)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	nonce := make([]byte, 6)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	token := "envelopequote" + hex.EncodeToString(nonce)
	policy, err := finalization.Verify(ctx, envelopeConfig("log-envelope", token))
	if err != nil {
		t.Fatal(err)
	}

	// The INF entries: the cluster channel entry and the audit entry of the
	// command, both quoting a warning marker in their message.
	// The MON echoes every command it handles to its own output at level 0
	// ("handle_command mon_command(...)"), outside any log envelope. The
	// command therefore carries the marker as JSON escapes, which the MON
	// decodes into the message but never prints as a literal marker.
	quoteCommand := `{"prefix": "log", "logtext": ["` + token + ` quotes \u005bWRN\u005d"]}`
	t.Logf("quote command accepted: %s", execOutput(t, ctx, cluster.Container, "python3", "-c", envelopeMonCommand, quoteCommand))
	// The real WRN entry: a set noout flag fails the OSDMAP_FLAGS check.
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	t.Cleanup(func() {
		if err := noout.Restore(context.WithoutCancel(ctx)); err != nil {
			t.Error("restore noout:", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	probe, quoted := waitEnvelopeLines(t, ctx, cluster, token)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	report, err := policy.Evaluate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	logVerification(t, "log envelope", report)
	if err := judgeEnvelopeEvidence(report, token, probe, quoted); err != nil {
		t.Fatal(err)
	}
	mon := report.Sources[slices.IndexFunc(report.Sources, func(source ceph.VerificationSource) bool { return source.Source == envelopeSource })]
	t.Logf("LOG_ENVELOPE_RECEIPT source %s generation %s started %s image %s driver %s: %d log bytes read in window %s to %s; raw MON lines with the probe by priority %v, with the quoted token by priority %v (%d unenveloped command echoes, %d with a literal marker); %s matched %d, %s matched %d; %d log gaps",
		mon.Source, mon.ContainerID, mon.StartedAt, mon.ImageID, mon.LogDriver, mon.LogBytes,
		report.Window.StartedAt.Format(time.RFC3339Nano), report.Window.EndedAt.Format(time.RFC3339Nano),
		probe.Priorities, quoted.Priorities, quoted.Echo, quoted.Unrecognized,
		report.Logs.Expectations[0].Name, report.Logs.Expectations[0].Matched, report.Logs.Expectations[1].Name, report.Logs.Expectations[1].Matched, len(report.Logs.Gaps))

	// The unsatisfied required expectation fails the finalization.
	recorder.finalize()
	if len(recorder.failures()) == 0 {
		t.Fatal("the finalization recorded no failure for the unsatisfied required warning")
	}
}

// waitEnvelopeLines polls the MONs' output until it holds the real WRN probe
// and an INF line that quotes the token, and returns the priorities of both.
func waitEnvelopeLines(t *testing.T, ctx context.Context, cluster *ceph.Container, token string) (probe, quoted envelopeRaw) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var lines []string
		for _, mon := range clusteraccess.Monitors(t, cluster) {
			reader, err := mon.Logs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(io.LimitReader(reader, 64<<20))
			_ = reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			lines = append(lines, strings.Split(string(data), "\n")...)
		}
		probe = rawEnvelopeCounts(lines, "Health check failed: noout flag(s) set (OSDMAP_FLAGS)")
		quoted = rawEnvelopeCounts(lines, token)
		if probe.Priorities["WRN"] > 0 && quoted.Priorities["INF"] > 0 {
			return probe, quoted
		}
		if time.Now().After(deadline) {
			t.Fatalf("the MON output lacks the probe or the quoted token after two minutes: probe %+v, quoted %+v", probe, quoted)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// envelopeMonCommand sends one raw JSON command to the MON through librados,
// so that the command text reaches the MON byte for byte.
const envelopeMonCommand = `import rados,sys,json
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_mon_op_timeout':'30'})
client.connect()
try:
    r,out,err=client.mon_command(sys.argv[1],b'')
    print(json.dumps({'r':r,'status':err}))
    sys.exit(0 if r==0 else 1)
finally:
    client.shutdown()
`
