//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

// cephFSAdmissionActor initializes a libcephfs client without mounting it
// and publishes receipt 1 with its client ID, which is 0 until the mount, and
// the FSID it was configured with. At the "mount" gate it mounts a root and
// publishes receipt 2: with the errno of a refused mount, after which it shuts down and publishes
// receipt 3 at once, or with its client address vector after it created the
// listed directories. After "finish" it unmounts, shuts down and publishes
// receipt 3. Arguments after the common ones: the Cephx user, its keyring
// path or "-", the mount root, a client admin socket path or "-", the
// feature bits that the client advertises (client_debug_inject_features) or
// "-", and a comma-separated list of directories to create, possibly empty.
const cephFSAdmissionActor harness.CephFSActorScript = harness.CephFSActorHelpers + `
user, keyring, root, asok, features, mkdirs = sys.argv[4:10]
def errno_of(error):
    code = getattr(error, "errno", None) or (error.args[0] if error.args and isinstance(error.args[0], int) else -1)
    return abs(code)
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf", auth_id=user)
if keyring != "-":
    fs.conf_set("keyring", keyring)
fs.conf_set("client_mount_timeout", "30")
if asok != "-":
    fs.conf_set("admin_socket", asok)
if features != "-":
    fs.conf_set("client_debug_inject_features", features)
fs.init()
publish(1, client=fs.get_instance_id(), fsid=fs.conf_get("fsid"))
gate("mount")
try:
    fs.mount(mount_root=root.encode(), filesystem_name=name.encode())
except cephfs.Error as error:
    publish(2, client=fs.get_instance_id(), errno=errno_of(error), error=str(error)[:200])
    fs.shutdown()
    publish(3, closed=True)
    sys.exit(0)
for directory in filter(None, mkdirs.split(",")):
    fs.mkdir(directory.encode(), 0o755)
publish(2, client=fs.get_instance_id(), addrs=str(fs.get_addrs()))
gate("finish")
closing = {}
try:
    fs.unmount()
except cephfs.Error as error:
    closing.update(close_errno=errno_of(error))
fs.shutdown()
publish(3, closed=True, **closing)
`

// admissionAddr is one entry of a native address vector.
type admissionAddr struct {
	Type  string `json:"type"`
	Addr  string `json:"addr"`
	Nonce uint64 `json:"nonce"`
}

func (a admissionAddr) String() string {
	return a.Type + ":" + a.Addr + "/" + strconv.FormatUint(a.Nonce, 10)
}

// admissionRank is rank 0 of the filesystem in the FSMap.
type admissionRank struct {
	FSMapEpoch, MDSMapEpoch int
	FilesystemID            int64
	Name                    string
	GID                     uint64
	State                   string
	Addrs                   []admissionAddr
	// Required is the MDSMap's required_client_features, as raw JSON.
	Required string
}

// v2 returns the msgr2 entry of the address vector.
func (r admissionRank) v2() (admissionAddr, bool) {
	for _, addr := range r.Addrs {
		if addr.Type == "v2" {
			return addr, true
		}
	}
	return admissionAddr{}, false
}

// readAdmissionRank reads rank 0 of filesystem from the native "fs dump".
func readAdmissionRank(ctx context.Context, cluster *ceph.Container, filesystem string) (admissionRank, error) {
	out, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return admissionRank{}, fmt.Errorf("fs dump: %w", err)
	}
	var dump struct {
		Epoch       int `json:"epoch"`
		Filesystems []struct {
			ID     int64 `json:"id"`
			MDSMap struct {
				Epoch    int             `json:"epoch"`
				Name     string          `json:"fs_name"`
				Required json.RawMessage `json:"required_client_features"`
				Info     map[string]struct {
					GID   uint64 `json:"gid"`
					Name  string `json:"name"`
					Rank  int    `json:"rank"`
					State string `json:"state"`
					Addrs struct {
						Addrvec []admissionAddr `json:"addrvec"`
					} `json:"addrs"`
				} `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		return admissionRank{}, fmt.Errorf("fs dump: %w: %.512s", err, out)
	}
	for _, fs := range dump.Filesystems {
		if fs.MDSMap.Name != filesystem {
			continue
		}
		for _, info := range fs.MDSMap.Info {
			if info.Rank == 0 && info.State == "up:active" {
				return admissionRank{
					FSMapEpoch: dump.Epoch, MDSMapEpoch: fs.MDSMap.Epoch, FilesystemID: fs.ID,
					Name: info.Name, GID: info.GID, State: info.State, Addrs: info.Addrs.Addrvec,
					Required: string(fs.MDSMap.Required),
				}, nil
			}
		}
		return admissionRank{}, fmt.Errorf("filesystem %s has no active rank 0: %.1024s", filesystem, out)
	}
	return admissionRank{}, fmt.Errorf("fs dump has no filesystem %s", filesystem)
}

// admissionSession is one entry of the MDS's native "session ls".
type admissionSession struct {
	ID     uint64  `json:"id"`
	State  string  `json:"state"`
	Uptime float64 `json:"uptime"`
	Entity struct {
		Addr admissionAddr `json:"addr"`
	} `json:"entity"`
	Metadata struct {
		Features struct {
			Bits string `json:"feature_bits"`
		} `json:"client_features"`
		EntityID string `json:"entity_id"`
		Root     string `json:"root"`
		Hostname string `json:"hostname"`
		PID      string `json:"pid"`
	} `json:"client_metadata"`
}

// admissionSessions lists the MDS's sessions by client ID.
func admissionSessions(ctx context.Context, cluster *ceph.Container, mds string) (map[uint64]admissionSession, error) {
	out, err := cluster.Ceph(ctx, "tell", "mds."+mds, "session", "ls", "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("session ls: %w", err)
	}
	var sessions []admissionSession
	if err := json.Unmarshal(out, &sessions); err != nil {
		return nil, fmt.Errorf("session ls: %w: %.512s", err, out)
	}
	byID := make(map[uint64]admissionSession, len(sessions))
	for _, session := range sessions {
		byID[session.ID] = session
	}
	return byID, nil
}

// clientMDSSessions is the client's own view of its MDS sessions, from the
// "mds_sessions" command of its admin socket.
type clientMDSSessions struct {
	ID          uint64 `json:"id"`
	Addr        string `json:"addr_str"`
	MDSMapEpoch int    `json:"mdsmap_epoch"`
	Sessions    []struct {
		MDS   int `json:"mds"`
		Addrs struct {
			Addrvec []admissionAddr `json:"addrvec"`
		} `json:"addrs"`
		CapTTL    string `json:"cap_ttl"`
		LastRenew string `json:"last_cap_renew_request"`
		RenewSeq  uint64 `json:"cap_renew_seq"`
		State     string `json:"state"`
	} `json:"sessions"`
}

func readClientMDSSessions(ctx context.Context, cluster *ceph.Container, client testcontainers.Container, asok string) (clientMDSSessions, error) {
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: []string{"ceph", "--admin-daemon", asok, "mds_sessions"}, Timeout: 20 * time.Second,
		MaxStdout: 64 << 10, MaxStderr: 16 << 10, Label: "client mds_sessions",
	})
	if err != nil || result.ExitCode != 0 {
		return clientMDSSessions{}, fmt.Errorf("client mds_sessions: exit=%d err=%v stderr=%.512s", result.ExitCode, err, result.Stderr)
	}
	var sessions clientMDSSessions
	if err := json.Unmarshal(result.Stdout, &sessions); err != nil {
		return clientMDSSessions{}, fmt.Errorf("client mds_sessions: %w: %.1024s", err, result.Stdout)
	}
	if len(sessions.Sessions) != 1 || sessions.Sessions[0].MDS != 0 {
		return clientMDSSessions{}, fmt.Errorf("client mds_sessions, want one rank 0 session: %.1024s", result.Stdout)
	}
	return sessions, nil
}

// cephFSAdmission is the cluster of one admission test.
type cephFSAdmission struct {
	cluster  *ceph.Container
	client   testcontainers.Container
	fs       *cephfs.Filesystem
	mds      string
	runs     int
	hostname string
}

// admissionAttempt is one admission actor and what it reported.
type admissionAttempt struct {
	run     *harness.CephFSActorRun
	label   string
	init    harness.CephFSActorReceipt
	mounted harness.CephFSActorReceipt
	session admissionSession
}

// attempt starts an admission actor and checks that its initialized client
// has no session yet; then it lets the actor mount. Whether the client has a
// GlobalID before it mounts depends on the release: Tentacle's libcephfs
// authenticates at mount, Squid's already at init. A client that had one
// keeps it when it mounts. A refused mount must leave no session of the process, and
// the actor has shut down when attempt returns. A mounted client must hold
// one open session under the GlobalID that it reports.
func (h *cephFSAdmission) attempt(t *testing.T, ctx context.Context, label, user, keyring, root, asok, features, mkdirs string) *admissionAttempt {
	t.Helper()
	h.runs++
	workdir := "/tmp/admission-" + strconv.Itoa(h.runs)
	a := &admissionAttempt{label: label}
	a.run = harness.StartCephFSActor(t, ctx, h.cluster, h.client, cephFSAdmissionActor, h.fs.FilesystemName, workdir, user, keyring, root, asok, features, mkdirs)
	a.init = a.run.Receipt(t, ctx, 1)
	if found := h.processSessions(t, ctx, a); len(found) != 0 {
		t.Fatalf("%s: PID %d has a session before it mounted: %+v", label, a.init.PID, found)
	}
	a.run.Open(t, ctx, "mount")
	a.mounted = a.run.Receipt(t, ctx, 2)
	if a.mounted.Errno != 0 {
		if closed := a.run.Receipt(t, ctx, 3); !closed.Closed {
			t.Fatalf("%s: closing receipt %+v", label, closed)
		}
		a.run.Wait(t, ctx)
		if found := h.processSessions(t, ctx, a); len(found) != 0 {
			t.Fatalf("%s: refused PID %d left a session: %+v", label, a.init.PID, found)
		}
		return a
	}
	if a.init.Client != 0 && a.mounted.Client != a.init.Client {
		t.Fatalf("%s: client.%d before the mount became client.%d", label, a.init.Client, a.mounted.Client)
	}
	found := h.processSessions(t, ctx, a)
	if len(found) != 1 || found[0].ID != a.mounted.Client || found[0].State != "open" || found[0].Entity.Addr.Addr == "" {
		t.Fatalf("%s: mounted client.%d (PID %d) has no single open session: %+v", label, a.mounted.Client, a.init.PID, found)
	}
	a.session = found[0]
	return a
}

// processSessions returns the sessions that the actor's process opened,
// matched by the hostname and PID in their client metadata, so that a client
// is found before it has a GlobalID.
func (h *cephFSAdmission) processSessions(t *testing.T, ctx context.Context, a *admissionAttempt) []admissionSession {
	t.Helper()
	sessions, err := admissionSessions(ctx, h.cluster, h.mds)
	if err != nil {
		t.Fatal(err)
	}
	var found []admissionSession
	for _, session := range sessions {
		if session.Metadata.Hostname == h.hostname && session.Metadata.PID == strconv.Itoa(a.init.PID) {
			found = append(found, session)
		}
	}
	return found
}

// finish unmounts a mounted attempt and waits until its session is gone
// without a blocklist entry for its address.
func (h *cephFSAdmission) Finish(t *testing.T, ctx context.Context, a *admissionAttempt) harness.CephFSActorReceipt {
	t.Helper()
	a.run.Open(t, ctx, "finish")
	closed := a.run.Receipt(t, ctx, 3)
	if !closed.Closed {
		t.Fatalf("%s: closing receipt %+v", a.label, closed)
	}
	a.run.Wait(t, ctx)
	h.waitSessionGone(t, ctx, a)
	return closed
}

// waitSessionGone waits until the MDS no longer lists the attempt's client.
func (h *cephFSAdmission) waitSessionGone(t *testing.T, ctx context.Context, a *admissionAttempt) {
	t.Helper()
	seen, err := harness.Observe(ctx, time.Minute, 30*time.Second, 500*time.Millisecond, func(probeCtx context.Context) (string, bool, error) {
		sessions, err := admissionSessions(probeCtx, h.cluster, h.mds)
		if err != nil {
			return "", false, err
		}
		session, ok := sessions[a.mounted.Client]
		return session.State, !ok, nil
	})
	if err != nil {
		t.Fatalf("%s: session of client.%d still %q: %v", a.label, a.mounted.Client, seen.Value, err)
	}
}

// blocklisted reports whether the attempt's session address is blocklisted.
func (h *cephFSAdmission) blocklisted(t *testing.T, ctx context.Context, a *admissionAttempt) bool {
	t.Helper()
	entries, err := harness.ObserveCluster(h.cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	address := a.session.Entity.Addr.Addr + "/" + strconv.FormatUint(a.session.Entity.Addr.Nonce, 10)
	return slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == address })
}

// clusterLogContains waits up to 30 s until one of the last cluster log lines
// at warning level contains text; the MDS sends its warnings to the MONs
// asynchronously.
func (h *cephFSAdmission) clusterLogContains(t *testing.T, ctx context.Context, text string) bool {
	t.Helper()
	_, err := harness.Observe(ctx, 30*time.Second, 15*time.Second, time.Second, func(probeCtx context.Context) (bool, bool, error) {
		out, err := h.cluster.Ceph(probeCtx, "log", "last", "200", "warn", "cluster")
		if err != nil {
			return false, false, err
		}
		found := strings.Contains(string(out), text)
		return found, found, nil
	})
	return err == nil
}

// cephFSFeature is one entry of "fs feature ls".
type cephFSFeature struct {
	Index int    `json:"index"`
	Name  string `json:"name"`
}

// testCephFSSessionAdmission follows one client from discovery to a closed
// session, then admits and refuses mounts by Cephx path and by required
// client features.
//
// Discovery: the FSMap names rank 0's daemon, GID and address vector, and the
// client's own admin socket reports a rank 0 session to that same address
// vector, the client's GlobalID and an MDSMap epoch no older than the one
// read; the MDS's "session ls" reports the same GlobalID with its address.
//
// Lifecycle: an initialized but unmounted client's process has no session; after the mount the session is open; with
// session_timeout at 30 s, the client renews about every 10 s, and each
// renewal that the MDS acknowledges moves cap_renew_seq,
// last_cap_renew_request and cap_ttl, which is the request time plus the
// timeout; after the unmount the session is gone and its address is not
// blocklisted.
//
// Authorization: a client whose MDS cap allows one path mounts that path and
// is refused with EPERM for the root and another path, without a session;
// the admin client mounts the root.
//
// Required features: a client of the image's release advertises every
// feature that "fs feature ls" lists, so none can be required that it lacks.
// The test makes a client that leaves one out with
// client_debug_inject_features. Requiring that feature evicts its mounted
// session and blocklists it, refuses its next mount with EPERM and still
// admits an ordinary client; removing the requirement admits it again.
func testCephFSSessionAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, imageOpts := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, append(imageOpts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Secure)))...)
	fs := cephfs.Filesystems(cluster)[0]
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil || len(status.Active) != 1 {
		t.Fatalf("want one active MDS: %+v, %v", status, err)
	}
	active := status.Active[0]
	h := &cephFSAdmission{cluster: cluster, client: client, fs: fs, mds: active.Name, hostname: strings.TrimSpace(harness.ExecOutput(t, ctx, client, "hostname"))}

	// Discovery.
	fsid := strings.TrimSpace(string(harness.MustCeph(t, ctx, cluster, "fsid")))
	rank, err := readAdmissionRank(ctx, cluster, fs.FilesystemName)
	if err != nil {
		t.Fatal(err)
	}
	mdsV2, ok := rank.v2()
	if rank.Name != active.Name || rank.GID != active.GID || rank.FilesystemID != status.FilesystemID || !ok {
		t.Fatalf("fs dump rank 0 %+v, MDSStatus %+v (filesystem %d)", rank, active, status.FilesystemID)
	}
	addresses, err := harness.ObserveCluster(cluster).DaemonAddresses(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mdsIP, err := netip.ParseAddrPort(mdsV2.Addr)
	if err != nil {
		t.Fatal(err)
	}
	var monV2 admissionAddr
	for _, addr := range addresses.Monitors[0].Addrs {
		if addr.Type == "v2" {
			monV2 = admissionAddr{Type: addr.Type, Addr: addr.Addr, Nonce: uint64(addr.Nonce)}
		}
	}
	monIP, err := netip.ParseAddrPort(monV2.Addr)
	if err != nil || monIP.Addr().Is4() != mdsIP.Addr().Is4() {
		t.Fatalf("MON a v2 %+v (%v) and MDS v2 %s differ in family", monV2, err, mdsV2)
	}
	if mdsV2.Nonce == 0 {
		t.Fatalf("MDS v2 address %s has no nonce", mdsV2)
	}
	daemons := map[string]bool{}
	for _, mon := range addresses.Monitors {
		for _, addr := range mon.Addrs {
			daemons[addr.Addr] = true
		}
	}
	if addresses.ActiveManager != nil {
		for _, addr := range addresses.ActiveManager.Addrs {
			daemons[addr.Addr] = true
		}
	}
	for _, osd := range addresses.OSDs {
		for _, addr := range osd.Public {
			daemons[addr.Addr] = true
		}
	}
	if daemons[mdsV2.Addr] {
		t.Fatalf("MDS v2 address %s is another daemon's", mdsV2)
	}

	// Lifecycle, with renewals every 10 s.
	change, err := fs.OverrideSessionTimeouts(ctx, cephfs.SessionTimeouts{Timeout: 30 * time.Second, Autoclose: 5 * time.Minute})
	if change != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := change.Restore(cleanupCtx); err != nil {
				t.Errorf("restore session timeouts: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	const asok = "/tmp/admission-admin.asok"
	admin := h.attempt(t, ctx, "admin", "admin", "-", "/", asok, "-", "/admitted,/other")
	if admin.mounted.Errno != 0 {
		t.Fatalf("admin mount refused: %+v", admin.mounted)
	}
	if admin.init.FSID != fsid {
		t.Fatalf("client FSID %q, cluster %q", admin.init.FSID, fsid)
	}
	if admin.session.Metadata.EntityID != "admin" || admin.session.Metadata.Root != "/" {
		t.Fatalf("admin session metadata %+v", admin.session.Metadata)
	}
	own, err := readClientMDSSessions(ctx, cluster, client, asok)
	if err != nil {
		t.Fatal(err)
	}
	if own.ID != admin.mounted.Client || own.MDSMapEpoch < rank.MDSMapEpoch || own.Sessions[0].State != "open" ||
		!slices.Equal(own.Sessions[0].Addrs.Addrvec, rank.Addrs) {
		t.Fatalf("client mds_sessions %+v, want client.%d open on %+v at MDSMap epoch >= %d", own, admin.mounted.Client, rank.Addrs, rank.MDSMapEpoch)
	}
	if !strings.Contains(admin.mounted.Addrs, admin.session.Entity.Addr.Addr) {
		t.Fatalf("client addrs %s do not hold session address %s", admin.mounted.Addrs, admin.session.Entity.Addr.Addr)
	}
	// A renewal is acknowledged when cap_ttl is the last request time plus
	// session_timeout: the client computes it only from a RENEWCAPS reply
	// that carries its current cap_renew_seq.
	first := own.Sessions[0]
	firstTTL, err := renewalTTL(first.LastRenew, first.CapTTL)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := harness.Observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (clientMDSSessions, bool, error) {
		now, err := readClientMDSSessions(probeCtx, cluster, client, asok)
		if err != nil {
			return now, false, err
		}
		session := now.Sessions[0]
		ttl, err := renewalTTL(session.LastRenew, session.CapTTL)
		if err != nil {
			return now, false, err
		}
		return now, session.RenewSeq >= first.RenewSeq+2 && ttl == 30*time.Second, nil
	})
	if err != nil {
		t.Fatalf("no two acknowledged renewals after %+v: last %+v: %v", first, renewed.Value, err)
	}
	last := renewed.Value.Sessions[0]
	if firstTTL != 30*time.Second || last.State != "open" || last.LastRenew <= first.LastRenew || last.CapTTL <= first.CapTTL ||
		!slices.Equal(last.Addrs.Addrvec, rank.Addrs) || renewed.Value.ID != own.ID {
		t.Fatalf("renewals did not advance the session: before %+v after %+v", first, last)
	}
	sessions, err := admissionSessions(ctx, cluster, h.mds)
	if err != nil {
		t.Fatal(err)
	}
	if after := sessions[admin.mounted.Client]; after.State != "open" || after.Uptime <= admin.session.Uptime || after.Entity.Addr != admin.session.Entity.Addr {
		t.Fatalf("admin session after renewals %+v, before %+v", after, admin.session)
	}
	if err := change.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if closed := h.Finish(t, ctx, admin); closed.CloseErrno != 0 {
		t.Fatalf("admin unmount: %+v", closed)
	}
	if h.blocklisted(t, ctx, admin) {
		t.Fatalf("admin %s blocklisted after unmount", admin.session.Entity.Addr)
	}

	// Authorization.
	restricted, err := cluster.CreateClient(ctx, "admission-restricted", ceph.ClientCaps{
		Mon: "allow r", MDS: "allow rw path=/admitted", OSD: "allow rw tag cephfs data=" + fs.FilesystemName,
	})
	if err != nil {
		t.Fatal(err)
	}
	cc, err := restricted.ConnectionConfig()
	keyring := cc.Keyring
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyToContainer(ctx, keyring, restricted.KeyringPath(), 0o600); err != nil {
		t.Fatal(err)
	}
	admitted := h.attempt(t, ctx, "restricted /admitted", restricted.User(), restricted.KeyringPath(), "/admitted", "-", "-", "")
	if admitted.mounted.Errno != 0 || admitted.session.Metadata.EntityID != restricted.User() || admitted.session.Metadata.Root != "/admitted" {
		t.Fatalf("restricted mount of /admitted: %+v session %+v", admitted.mounted, admitted.session)
	}
	h.Finish(t, ctx, admitted)
	// The MDS rejects the session open of a root outside the cap's path,
	// and the client's mount fails with EPERM.
	for _, root := range []string{"/", "/other"} {
		if a := h.attempt(t, ctx, "restricted "+root, restricted.User(), restricted.KeyringPath(), root, "-", "-", ""); a.mounted.Errno != 1 {
			t.Fatalf("restricted mount of %s: %+v, want EPERM", root, a.mounted)
		}
	}

	// Required client features. Every client of the release advertises
	// every feature that "fs feature ls" lists.
	out := harness.MustCeph(t, ctx, cluster, "fs", "feature", "ls", "--format", "json")
	var features []cephFSFeature
	if err := json.Unmarshal(out, &features); err != nil || len(features) == 0 {
		t.Fatalf("fs feature ls: %v: %.1024s", err, out)
	}
	const feature = "metric_collect"
	var all, lacking uint64
	var bits []string
	required := -1
	for _, f := range features {
		if f.Index < 0 || f.Index >= 64 {
			t.Fatalf("fs feature ls: index %d: %s", f.Index, out)
		}
		all |= 1 << f.Index
		if f.Name == feature {
			required = f.Index
			continue
		}
		lacking |= 1 << f.Index
		bits = append(bits, strconv.Itoa(f.Index))
	}
	if required < 0 {
		t.Fatalf("fs feature ls has no %s: %s", feature, out)
	}
	if advertised, err := featureBits(admin.session); err != nil || advertised != all {
		t.Fatalf("admin client advertises %s (%v), fs feature ls has %#x", admin.session.Metadata.Features.Bits, err, all)
	}
	if rank.Required != "{}" {
		t.Fatalf("required_client_features before the test: %s", rank.Required)
	}
	inject := strings.Join(bits, ",")
	lagging := h.attempt(t, ctx, "lagging", "admin", "-", "/", "-", inject, "")
	if advertised, err := featureBits(lagging.session); lagging.mounted.Errno != 0 || err != nil || advertised != lacking {
		t.Fatalf("client without %s: %+v session %+v (%v)", feature, lagging.mounted, lagging.session, err)
	}
	added := false
	t.Cleanup(func() {
		if !added {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if _, err := cluster.Ceph(cleanupCtx, "fs", "required_client_features", fs.FilesystemName, "rm", feature); err != nil {
			t.Errorf("remove required client feature %s: %v", feature, err)
		}
	})
	added = true
	harness.MustCeph(t, ctx, cluster, "fs", "required_client_features", fs.FilesystemName, "add", feature)
	requiring, err := readAdmissionRank(ctx, cluster, fs.FilesystemName)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf(`{"feature_%d":%q}`, required, feature); requiring.Required != want || requiring.GID != rank.GID {
		t.Fatalf("required_client_features %s on GID %d, want %s on %d", requiring.Required, requiring.GID, want, rank.GID)
	}
	// The MDS evicts the mounted session that lacks the feature, and with
	// the default mds_session_blocklist_on_evict blocklists its address.
	h.waitSessionGone(t, ctx, lagging)
	if !h.blocklisted(t, ctx, lagging) || !h.clusterLogContains(t, ctx, "missing required features") {
		t.Fatalf("client without %s left without a blocklist entry or eviction warning", feature)
	}
	lagging.run.Open(t, ctx, "finish")
	laggingClosed := lagging.run.Receipt(t, ctx, 3)
	lagging.run.Wait(t, ctx)

	ordinary := h.attempt(t, ctx, "ordinary", "admin", "-", "/", "-", "-", "")
	if ordinary.mounted.Errno != 0 {
		t.Fatalf("ordinary client refused with %s required: %+v", feature, ordinary.mounted)
	}
	h.Finish(t, ctx, ordinary)
	refused := h.attempt(t, ctx, "lacking", "admin", "-", "/", "-", inject, "")
	if refused.mounted.Errno != 1 || !h.clusterLogContains(t, ctx, "lacks required features") {
		t.Fatalf("client without %s while it is required: %+v, want EPERM and a rejection warning", feature, refused.mounted)
	}

	harness.MustCeph(t, ctx, cluster, "fs", "required_client_features", fs.FilesystemName, "rm", feature)
	added = false
	restored, err := readAdmissionRank(ctx, cluster, fs.FilesystemName)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Required != rank.Required {
		t.Fatalf("required_client_features %s after removal, %s before", restored.Required, rank.Required)
	}
	readmitted := h.attempt(t, ctx, "readmitted", "admin", "-", "/", "-", inject, "")
	if readmitted.mounted.Errno != 0 {
		t.Fatalf("client without %s refused after the requirement was removed: %+v", feature, readmitted.mounted)
	}
	h.Finish(t, ctx, readmitted)

	t.Logf("CEPHFS_SESSION_ADMISSION fsid=%s fs=%s id=%d fsmap=%d mdsmap=%d rank0=mds.%s gid=%d addrs=%s (MON a %s, monmap %d); "+
		"client.%d (PID %d, no session before mount) session open from %s to %s at client mdsmap %d, features %s; "+
		"renew seq %d->%d in %s with cap_ttl=last_renew+30s; unmount closed it without blocklist; "+
		"restricted client.%d mounted /admitted, / and /other refused EPERM without a session; "+
		"%s (bit %d) required at mdsmap %d: client.%d advertising %s evicted and blocklisted (unmount errno %d), ordinary client.%d admitted, a client without it refused EPERM, readmitted client.%d after rm",
		fsid, fs.FilesystemName, rank.FilesystemID, rank.FSMapEpoch, rank.MDSMapEpoch, rank.Name, rank.GID, rank.Addrs, monV2, addresses.MonMapEpoch,
		admin.mounted.Client, admin.init.PID, admin.session.Entity.Addr, own.Sessions[0].Addrs.Addrvec, own.MDSMapEpoch, admin.session.Metadata.Features.Bits,
		first.RenewSeq, last.RenewSeq, renewed.Elapsed.Round(time.Second),
		admitted.mounted.Client,
		feature, required, requiring.MDSMapEpoch, lagging.mounted.Client, lagging.session.Metadata.Features.Bits, laggingClosed.CloseErrno, ordinary.mounted.Client, readmitted.mounted.Client)
}

// renewalTTL returns cap_ttl minus last_cap_renew_request, as the client's
// admin socket prints them.
func renewalTTL(lastRenew, capTTL string) (time.Duration, error) {
	const layout = "2006-01-02T15:04:05.999999-0700"
	requested, err := time.Parse(layout, lastRenew)
	if err != nil {
		return 0, err
	}
	ttl, err := time.Parse(layout, capTTL)
	if err != nil {
		return 0, err
	}
	return ttl.Sub(requested), nil
}

// featureBits decodes the client feature bitmap of a session's metadata.
func featureBits(session admissionSession) (uint64, error) {
	return strconv.ParseUint(strings.TrimPrefix(session.Metadata.Features.Bits, "0x"), 16, 64)
}
