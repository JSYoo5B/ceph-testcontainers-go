package multicluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rbdConnectionRefreshArchive struct {
	testcontainers.Container
	name, config, keyring string
	running               bool
	copies, cliCalls      int
	copyErr               error
}

func (c *rbdConnectionRefreshArchive) GetContainerID() string { return c.name }

func (c *rbdConnectionRefreshArchive) Exec(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.cliCalls++
	return 0, nil, errors.New("old bootstrap monitors are unavailable")
}

func (c *rbdConnectionRefreshArchive) CopyFileFromContainer(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path != "/etc/ceph/ceph.conf" {
		return nil, fmt.Errorf("unexpected archive read %q", path)
	}
	return io.NopCloser(strings.NewReader(c.config)), nil
}

func (c *rbdConnectionRefreshArchive) CopyToContainer(ctx context.Context, content []byte, path string, mode int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if path != "/etc/ceph/ceph.conf" || mode != 0o644 {
		return fmt.Errorf("unexpected archive copy %q mode=%o", path, mode)
	}
	if c.copyErr != nil {
		return c.copyErr
	}
	c.config = string(content)
	c.copies++
	return nil
}

type rbdConnectionRefreshFixture struct {
	link                         *RBDMirror
	source, destination          *rbdNamespacePolicyFixture
	clients                      []*rbdConnectionRefreshArchive
	sourceCluster, targetCluster *ceph.Container
}

func newRBDConnectionRefreshFixture(t *testing.T, sourceNamespace, destinationNamespace string) *rbdConnectionRefreshFixture {
	t.Helper()
	link, _, _, _ := newRBDImageStatusFixture(t, RBDMirrorModeSnapshot, sourceNamespace, destinationNamespace)
	source, destination := newRBDNamespacePolicyFixture(), newRBDNamespacePolicyFixture()
	for _, site := range []struct {
		client    *rbdNamespacePolicyFixture
		namespace string
		name      string
		identity  rbdMirrorSitePolicyIdentity
	}{
		{source, sourceNamespace, "source", link.policyIdentities.source},
		{destination, destinationNamespace, "destination", link.policyIdentities.destination},
	} {
		site.client.site = site.name
		site.client.policies["images"] = site.identity.base
		if site.namespace != "" {
			site.client.policies["images/"+site.namespace] = site.identity.selected
		}
	}
	fixture := &rbdConnectionRefreshFixture{link: link, source: source, destination: destination,
		sourceCluster: &ceph.Container{Container: source}, targetCluster: &ceph.Container{Container: destination}}
	link.config.Source, link.config.Destination = fixture.sourceCluster, fixture.targetCluster
	for _, name := range []string{"source-cli", "destination-cli", "daemon-a", "daemon-b"} {
		fixture.clients = append(fixture.clients, &rbdConnectionRefreshArchive{
			name: name, config: "# " + name + " private marker\n[global]\nmon host = old\nfsid = retained\n[client]\nclient mount timeout = 37\n",
			keyring: name + " private key", running: name != "daemon-b",
		})
	}
	link.sourceClient, link.destinationClient = fixture.clients[0], fixture.clients[1]
	link.daemons = []*RBDMirrorDaemon{
		{Container: fixture.clients[2], DaemonName: "a", ClientName: "client.rbd-mirror.tc-a"},
		{Container: fixture.clients[3], DaemonName: "b", ClientName: "client.rbd-mirror.tc-b"},
	}
	link.Container = fixture.clients[2]
	return fixture
}

// The ceph package tests cover the callback's strict template/native/target
// FSID and archive parser. This fake replaces only the native mon_host
// value, so these tests isolate link ownership, deadline and partial retries.
func (f *rbdConnectionRefreshFixture) refresh(ctx context.Context, cluster *ceph.Container, client testcontainers.Container) error {
	archive, ok := client.(*rbdConnectionRefreshArchive)
	if !ok {
		return errors.New("refresh reached an unowned target")
	}
	expected, address := f.targetCluster, "destination-final"
	if archive == f.clients[0] {
		expected, address = f.sourceCluster, "source-final"
	}
	if cluster != expected {
		return errors.New("refresh used the wrong local cluster")
	}
	reader, err := archive.CopyFileFromContainer(ctx, "/etc/ceph/ceph.conf")
	if err != nil {
		return err
	}
	before, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return err
	}
	after := bytes.Replace(before, []byte("mon host = old\n"), []byte("mon host = "+address+"\n"), 1)
	if bytes.Equal(before, after) {
		return nil
	}
	return archive.CopyToContainer(ctx, after, "/etc/ceph/ceph.conf", 0o644)
}

func TestRBDMirrorMonitorRefreshUsesCurrentControlsAndOwnedLocalTargets(t *testing.T) {
	for _, mapping := range []struct{ source, destination string }{{"", ""}, {"ns-a", "ns-b"}, {"", "ns-b"}, {"ns-a", ""}} {
		t.Run(mapping.source+"-"+mapping.destination, func(t *testing.T) {
			fixture := newRBDConnectionRefreshFixture(t, mapping.source, mapping.destination)
			link := fixture.link
			policies := link.policyIdentities
			initialContainer := link.Container
			var order []string
			beforeConfig, beforeKeyring, beforeState := make([]string, len(fixture.clients)), make([]string, len(fixture.clients)), make([]bool, len(fixture.clients))
			for i, client := range fixture.clients {
				beforeConfig[i], beforeKeyring[i], beforeState[i] = client.config, client.keyring, client.running
			}
			if err := link.refreshRBDMirrorMonitorConfig(t.Context(), func(ctx context.Context, cluster *ceph.Container, client testcontainers.Container) error {
				order = append(order, client.(*rbdConnectionRefreshArchive).name)
				return fixture.refresh(ctx, cluster, client)
			}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(order, []string{"source-cli", "destination-cli", "daemon-a", "daemon-b"}) {
				t.Fatalf("owned refresh targets differ: %v", order)
			}
			for i, client := range fixture.clients {
				address := "destination-final"
				if i == 0 {
					address = "source-final"
				}
				if client.config != strings.Replace(beforeConfig[i], "mon host = old", "mon host = "+address, 1) || client.keyring != beforeKeyring[i] || client.running != beforeState[i] || client.cliCalls != 0 || client.copies != 1 {
					t.Fatalf("refresh changed private data/process or used stale CLI %s: %+v", client.name, client)
				}
			}
			if link.Container != initialContainer || link.policyIdentities != policies || link.daemons[0].ClientName != "client.rbd-mirror.tc-a" || link.daemons[1].ClientName != "client.rbd-mirror.tc-b" || len(fixture.source.mutations)+len(fixture.destination.mutations) != 0 {
				t.Fatal("monitor refresh changed daemon/auth/policy identity or ran native mutation")
			}
		})
	}
}

func TestRBDMirrorMonitorRefreshRetainsPartialCopiesForFreshContextRetry(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "ns-a", "ns-b")
	copyErr := errors.New("temporary archive failure")
	fixture.clients[2].copyErr = copyErr
	if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); !errors.Is(err, copyErr) {
		t.Fatalf("partial copy cause was lost: %v", err)
	}
	for i, client := range fixture.clients {
		want := 1
		if i == 2 {
			want = 0
		}
		if client.copies != want {
			t.Fatalf("partial copy rolled back/skipped healthy target %s: copies=%d", client.name, client.copies)
		}
	}
	fixture.clients[2].copyErr = nil
	if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); err != nil {
		t.Fatal(err)
	}
	for _, client := range fixture.clients {
		if client.copies != 1 || strings.Contains(client.config, "mon host = old") || client.cliCalls != 0 {
			t.Fatalf("retry rewrote a completed copy or lost a target: %+v", client)
		}
	}
}

func TestRBDMirrorMonitorRefreshPreservesPoolScopeAndNamespacePolicies(t *testing.T) {
	for _, mapping := range []struct{ source, destination string }{{"", ""}, {"ns-a", "ns-b"}} {
		t.Run(mapping.source+"-"+mapping.destination, func(t *testing.T) {
			fixture := newRBDConnectionRefreshFixture(t, mapping.source, mapping.destination)
			link := fixture.link
			link.config.Scope, link.config.Mode = RBDMirrorScopePool, RBDMirrorModeJournal
			for _, site := range []struct {
				control   *rbdNamespacePolicyFixture
				namespace string
				identity  *rbdMirrorSitePolicyIdentity
			}{
				{fixture.source, mapping.source, &link.policyIdentities.source},
				{fixture.destination, mapping.destination, &link.policyIdentities.destination},
			} {
				if site.identity.base.Mode == "image" {
					site.identity.base.Mode = "pool"
				}
				site.identity.selected.Mode = "pool"
				site.control.policies["images"] = site.identity.base
				if site.namespace != "" {
					site.control.policies["images/"+site.namespace] = site.identity.selected
				}
			}
			beforeSource, beforeDestination := link.policyIdentities.source, link.policyIdentities.destination
			if err := link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); err != nil {
				t.Fatal(err)
			}
			if link.config.Scope != RBDMirrorScopePool || link.config.Mode != RBDMirrorModeJournal ||
				!sameRBDMirrorPolicy(link.policyIdentities.source.base, beforeSource.base) || !sameRBDMirrorPolicy(link.policyIdentities.source.selected, beforeSource.selected) ||
				!sameRBDMirrorPolicy(link.policyIdentities.destination.base, beforeDestination.base) || !sameRBDMirrorPolicy(link.policyIdentities.destination.selected, beforeDestination.selected) || len(fixture.source.mutations)+len(fixture.destination.mutations) != 0 {
				t.Fatal("monitor refresh changed pool journal enrollment or namespace mapping")
			}
		})
	}
}

func TestRBDMirrorMonitorRefreshRejectsChangedPoliciesBeforeCopy(t *testing.T) {
	for _, fault := range []string{"source pool", "destination pool", "source base UUID", "destination selected UUID", "source scope", "destination mapping", "source site", "malformed policy", "missing control"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newRBDConnectionRefreshFixture(t, "ns-a", "ns-b")
			switch fault {
			case "source pool":
				fixture.source.poolID++
			case "destination pool":
				fixture.destination.poolID++
			case "source base UUID":
				policy := fixture.source.policies["images"]
				policy.MirrorUUID = "replacement-base"
				fixture.source.policies["images"] = policy
			case "destination selected UUID":
				policy := fixture.destination.policies["images/ns-b"]
				policy.MirrorUUID = "replacement-selected"
				fixture.destination.policies["images/ns-b"] = policy
			case "source scope":
				policy := fixture.source.policies["images/ns-a"]
				policy.Mode = "pool"
				fixture.source.policies["images/ns-a"] = policy
			case "destination mapping":
				policy := fixture.destination.policies["images/ns-b"]
				policy.RemoteNamespace = stringPointer("foreign")
				fixture.destination.policies["images/ns-b"] = policy
			case "source site":
				fixture.source.site = "outside-site"
			case "malformed policy":
				fixture.destination.infoOverride = "{"
			case "missing control":
				fixture.link.config.Destination = &ceph.Container{}
			}
			calls := 0
			if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), func(context.Context, *ceph.Container, testcontainers.Container) error {
				calls++
				return nil
			}); err == nil || calls != 0 {
				t.Fatalf("foreign/malformed native %s reached file copying: err=%v calls=%d", fault, err, calls)
			}
		})
	}
}

func TestRBDMirrorMonitorRefreshRejectsUnavailableOwnership(t *testing.T) {
	for _, fault := range []string{"nil", "closed", "source cluster", "destination cluster", "source client", "destination client", "pool identities", "policy identities", "nil daemon", "missing daemon container"} {
		t.Run(fault, func(t *testing.T) {
			fixture := newRBDConnectionRefreshFixture(t, "", "")
			link := fixture.link
			switch fault {
			case "nil":
				link = nil
			case "closed":
				link.closed = true
			case "source cluster":
				link.config.Source = nil
			case "destination cluster":
				link.config.Destination = nil
			case "source client":
				link.sourceClient = nil
			case "destination client":
				link.destinationClient = nil
			case "pool identities":
				link.poolIdentities = nil
			case "policy identities":
				link.policyIdentities = nil
			case "nil daemon":
				link.daemons = append(link.daemons, nil)
			case "missing daemon container":
				link.daemons[0].Container = nil
			}
			calls := 0
			if err := link.refreshRBDMirrorMonitorConfig(t.Context(), func(context.Context, *ceph.Container, testcontainers.Container) error {
				calls++
				return nil
			}); err == nil || calls != 0 {
				t.Fatalf("unconfirmed %s reached copying: err=%v calls=%d", fault, err, calls)
			}
		})
	}
}

func TestRBDMirrorMonitorRefreshNativeQueryFailureCanRetryWithoutAdoptingIdentity(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "ns-a", "ns-b")
	queryErr := errors.New("private native transport detail")
	control := &rbdImageStatusFixture{rbdNamespacePolicyFixture: fixture.source,
		execError: func([]string) error { return queryErr }}
	fixture.sourceCluster.Container = control
	err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh)
	if !errors.Is(err, queryErr) || strings.Contains(err.Error(), queryErr.Error()) {
		t.Fatalf("native query failure lost classification or exposed output: %v", err)
	}
	for _, client := range fixture.clients {
		if client.copies != 0 {
			t.Fatal("unverified native identity reached archive copying")
		}
	}
	captured := fixture.link.policyIdentities
	control.execError = nil
	if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); err != nil || fixture.link.policyIdentities != captured {
		t.Fatalf("fresh-context retry did not retain confirmed policies: %v", err)
	}
}

func TestRBDMirrorMonitorRefreshPublicMethodRequiresOriginalClusterBootstrap(t *testing.T) {
	// Native pool and policies match, but these hand-built Ceph handles have no
	// original private template/keyring. The public callback must still refuse
	// archive writes rather than treating control reads as confirmed ownership.
	fixture := newRBDConnectionRefreshFixture(t, "ns-a", "ns-b")
	if err := fixture.link.RefreshMonitorConfig(t.Context()); err == nil {
		t.Fatal("public monitor refresh accepted an uninitialized original cluster")
	}
	for _, client := range fixture.clients {
		if client.copies != 0 || client.cliCalls != 0 {
			t.Fatal("public monitor refresh bypassed the strict cluster bootstrap guard")
		}
	}
}

func TestRBDMirrorMonitorRefreshIncludesMutationLockWaitInCallerDeadline(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "", "")
	fixture.link.mu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()
	calls := 0
	err := fixture.link.refreshRBDMirrorMonitorConfig(ctx, func(context.Context, *ceph.Container, testcontainers.Container) error {
		calls++
		return nil
	})
	fixture.link.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || calls != 0 || len(fixture.source.infoCounts) != 0 {
		t.Fatalf("lock wait ignored caller deadline or ran native reads: err=%v calls=%d", err, calls)
	}
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if err := fixture.link.RefreshMonitorConfig(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("public refresh lost caller cancellation: %v", err)
	}
}

func TestRBDMirrorMonitorRefreshCancellationRetainsCompletedFileOnly(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "", "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	err := fixture.link.refreshRBDMirrorMonitorConfig(ctx, func(ctx context.Context, cluster *ceph.Container, client testcontainers.Container) error {
		calls++
		if err := fixture.refresh(ctx, cluster, client); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 || fixture.clients[0].copies != 1 {
		t.Fatalf("cancel lost cause or completed file: err=%v calls=%d", err, calls)
	}
	for _, client := range fixture.clients[1:] {
		if client.copies != 0 {
			t.Fatal("cancelled refresh continued copying another target")
		}
	}
}

func TestRBDMirrorMonitorRefreshRejectsPostCopyPolicyDriftWithoutAdoptingIt(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "ns-a", "ns-b")
	captured := fixture.link.policyIdentities
	err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), func(ctx context.Context, cluster *ceph.Container, client testcontainers.Container) error {
		if err := fixture.refresh(ctx, cluster, client); err != nil {
			return err
		}
		if client == fixture.clients[3] {
			fixture.destination.site = "outside-destination"
		}
		return nil
	})
	if err == nil || fixture.link.policyIdentities != captured {
		t.Fatalf("post-copy policy drift was adopted or reported complete: %v", err)
	}
	for _, client := range fixture.clients {
		if client.copies != 1 {
			t.Fatal("a completed file was rolled back after outside policy drift")
		}
	}
	if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); err == nil {
		t.Fatal("retry adopted the replacement policy")
	}
}

func TestRBDMirrorMonitorRefreshSupportsZeroDaemonOutage(t *testing.T) {
	fixture := newRBDConnectionRefreshFixture(t, "", "")
	fixture.link.daemons, fixture.link.Container = nil, nil
	if err := fixture.link.refreshRBDMirrorMonitorConfig(t.Context(), fixture.refresh); err != nil {
		t.Fatal(err)
	}
	for i, client := range fixture.clients {
		want := 0
		if i < 2 {
			want = 1
		}
		if client.copies != want {
			t.Fatalf("zero-daemon refresh reached removed process %s", client.name)
		}
	}
}
