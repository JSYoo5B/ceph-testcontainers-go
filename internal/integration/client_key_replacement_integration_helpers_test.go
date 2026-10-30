//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// heldSessionScript keeps one librados connection of client argv[1] and, each
// second, sends a MON and an MGR read and reads object argv[5] in pool
// argv[3], namespace argv[4], through the same connection. It prints the time,
// the three results and the number of the connection. Its MON client logs
// each authentication and ticket renewal to stderr. A client whose MON
// session the MONs rejected keeps failing, unless argv[2] is reconnect: then
// it connects anew after each failed MON read. It also connects anew when the
// file /tmp/held.reconnect appears, which it removes.
const heldSessionScript = `import json,os,rados,sys,time
reconnect=sys.argv[2]=='reconnect'
pool,namespace,obj=sys.argv[3:6]
def connect():
 c=rados.Rados(conffile='/etc/ceph/ceph.conf',rados_id=sys.argv[1],conf={'rados_mon_op_timeout':'5','rados_osd_op_timeout':'5','debug_monc':'10/10'})
 c.connect(timeout=10)
 io=c.open_ioctx(pool)
 io.set_namespace(namespace)
 return c,io
def call(send,prefix):
 try:
  return send(json.dumps({'prefix':prefix,'format':'json'}),b'',timeout=5)[0]
 except Exception:
  return -1000
def read(io):
 try:
  return 0 if io.read(obj) else -1001
 except rados.Error as e:
  return -abs(getattr(e,'errno',None) or 1002)
 except Exception:
  return -1000
(c,io),conn=connect(),1
while True:
 if os.path.exists('/tmp/held.reconnect'):
  os.unlink('/tmp/held.reconnect')
  if c is not None:
   try:
    c.shutdown()
   except Exception:
    pass
  c=None
 if c is None:
  try:
   c,io=connect()
   conn+=1
  except Exception:
   pass
 mon=call(c.mon_command,'mon stat') if c else -2000
 mgr=call(c.mgr_command,'pg stat') if c else -2000
 osd=read(io) if c else -2000
 print('%.3f %d %d %d %d'%(time.time(),mon,mgr,osd,conn),flush=True)
 if reconnect and mon!=0 and c is not None:
  try:
   c.shutdown()
  except Exception:
   pass
  c=None
 time.sleep(1)
`

// The object that the tests' clients read, in a namespace of their caps.
const (
	heldPool      = "data"
	heldNamespace = "tenant-a"
	heldObject    = "held-object"
)

// prepareHeldObject creates the pool of heldObject and writes it with random
// bytes as client.admin, and returns the bytes.
func prepareHeldObject(t *testing.T, ctx context.Context, cluster *ceph.Container) []byte {
	t.Helper()
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: heldPool, PGNum: 8, Replicas: 1, MinSize: 1, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4096)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	control := toolkitContainer(t, cluster)
	if err := control.CopyToContainer(ctx, payload, "/tmp/held-object", 0o644); err != nil {
		t.Fatal(err)
	}
	if code, reader, err := control.Exec(ctx, []string{"rados", "-p", heldPool, "-N", heldNamespace, "put", heldObject, "/tmp/held-object"}, tcexec.Multiplexed()); err != nil || code != 0 {
		out, _ := io.ReadAll(reader)
		t.Fatalf("write %s: exit %d, %v: %s", heldObject, code, err, out)
	}
	return payload
}

// osdRead reads heldObject as client in ctr with a fresh connection, and
// returns the exit code and the bytes.
func osdRead(t *testing.T, ctx context.Context, ctr testcontainers.Container, client *ceph.ClientConfig) (int, []byte) {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"sh", "-c", `rados --id "$1" --client_mount_timeout 10 -p "$2" -N "$3" get "$4" /tmp/read-object && cat /tmp/read-object`,
		"sh", client.User(), heldPool, heldNamespace, heldObject}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(reader)
	return code, out
}

// clientKeyMonTicketTTL makes a held session renew its MON ticket, and so
// meet a replaced or removed key, within seconds.
const clientKeyMonTicketTTL = 20 * time.Second

// testClientKeyReplacement replaces the keys of clients of each key type the
// image supports while a session with the old key is held, and checks native
// authentication: the old key fails for a fresh client at once and for the
// held session when it renews, the new key works for MON and MGR, the caps and
// other identities stay as they were, and every MON of a three-MON cluster
// agrees. Calls canceled at different times leave a key that a later call
// confirms.
func testClientKeyReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	ciphers, auth := clientKeyCiphers(t, ctx, image)
	cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(opts), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)), ceph.WithAuth(auth))...)
	payload := prepareHeldObject(t, ctx, cluster)
	bystander, err := cluster.CreateClient(ctx, "bystander", ceph.ClientCaps{Mon: "allow r"})
	if err != nil {
		t.Fatal(err)
	}
	bystanderEntry := authEntry(t, ctx, cluster, "client.bystander")
	for _, cipher := range ciphers {
		t.Run(string(cipher), func(t *testing.T) {
			caps := ceph.ClientCaps{Mon: "allow r", MGR: "allow r", OSD: "allow rw pool=data namespace=tenant-a"}
			client, err := cluster.CreateClient(ctx, "renewed-"+string(cipher), caps, ceph.ClientOptions{}.WithKeyType(cipher))
			if err != nil {
				t.Fatal(err)
			}
			before := authEntry(t, ctx, cluster, client.Name())
			holder := startClientContainer(t, ctx, cluster, client)
			startHeldSession(t, ctx, holder, client, "hold")
			replacedAt := containerTime(t, ctx, holder)
			next, err := client.ReplaceKey(ctx)
			if err != nil {
				t.Fatal(err)
			}
			after := authEntry(t, ctx, cluster, client.Name())
			if after.Key == before.Key || after.Key != keyOf(t, next) || !maps.Equal(after.Caps, before.Caps) {
				t.Fatalf("auth entry after the replacement: caps %v (before %v), key changed %t, key is the new descriptor's %t",
					after.Caps, before.Caps, after.Key != before.Key, after.Key == keyOf(t, next))
			}
			if got := cephxKeyType(t, clientKeyring(t, next), client.Name()); got != cipher {
				t.Fatalf("the new key is %s", got)
			}
			if got, err := next.Capabilities(ctx); err != nil || got != caps {
				t.Fatalf("ClientCapabilities with the new descriptor: %+v, %v", got, err)
			}
			if _, err := client.Capabilities(ctx); err == nil {
				t.Fatal("the old descriptor still reads capabilities")
			}
			// The holder keeps the old keyring.
			if code, out := clientCommand(t, ctx, holder, client, "mon", "stat"); code == 0 {
				t.Fatalf("a fresh client with the old key authenticated: %s", out)
			}
			if code, _ := osdRead(t, ctx, holder, client); code == 0 {
				t.Fatal("a fresh client with the old key read from the OSD")
			}
			fresh := startClientContainer(t, ctx, cluster, next)
			for _, command := range [][]string{{"mon", "stat"}, {"pg", "stat"}} {
				if code, out := clientCommand(t, ctx, fresh, next, command...); code != 0 {
					t.Fatalf("%s with the new key: exit %d: %s", strings.Join(command, " "), code, out)
				}
			}
			if code, read := osdRead(t, ctx, fresh, next); code != 0 || !bytes.Equal(read, payload) {
				t.Fatalf("OSD read with the new key: exit %d, %d bytes, same bytes %t", code, len(read), bytes.Equal(read, payload))
			}
			rejected, failed := waitHeldRenewalRejected(t, ctx, holder, replacedAt, clientKeyMonTicketTTL+30*time.Second)
			t.Logf("%s: a fresh client with the old key failed at once for MON and OSD; the MONs rejected the held session's renewal %.1fs after the replacement (its MON reads failed after that: %t, its OSD reads: %s)",
				cipher, rejected, failed, heldOSDReads(t, ctx, holder, replacedAt))
			if len(ciphers) > 1 {
				// A replacement may change the key type; the service cipher
				// stays the same.
				other := ciphers[0]
				if other == cipher {
					other = ciphers[1]
				}
				changed, err := next.ReplaceKey(ctx, ceph.ClientOptions{}.WithKeyType(other))
				if err != nil {
					t.Fatal(err)
				}
				if got := cephxKeyType(t, clientKeyring(t, changed), client.Name()); got != other {
					t.Fatalf("the key replaced with type %s is %s", other, got)
				}
				if code, _ := clientCommand(t, ctx, fresh, next, "mon", "stat"); code == 0 {
					t.Fatalf("the %s key still authenticated after a replacement with a %s key", cipher, other)
				}
				changedClient := startClientContainer(t, ctx, cluster, changed)
				if code, out := clientCommand(t, ctx, changedClient, changed, "mon", "stat"); code != 0 {
					t.Fatalf("mon stat with the %s key: exit %d: %s", other, code, out)
				}
				if code, read := osdRead(t, ctx, changedClient, changed); code != 0 || !bytes.Equal(read, payload) {
					t.Fatalf("OSD read with the %s key: exit %d, same bytes %t", other, code, bytes.Equal(read, payload))
				}
				t.Logf("%s -> %s: the new key type authenticated for MON and OSD with service cipher %s", cipher, other, auth.ServiceCipher)
			}
			if entry := authEntry(t, ctx, cluster, "client.bystander"); entry.Key != bystanderEntry.Key || !maps.Equal(entry.Caps, bystanderEntry.Caps) {
				t.Fatal("client.bystander changed")
			}
		})
	}
	if _, err := bystander.Capabilities(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("three MONs", func(t *testing.T) {
		cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(opts), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithMonitorCount(3)))...)
		client, err := cluster.CreateClient(ctx, "renewed", ceph.ClientCaps{Mon: "allow r", MGR: "allow r"})
		if err != nil {
			t.Fatal(err)
		}
		holder := startClientContainer(t, ctx, cluster, client)
		// Calls canceled at different points leave the key that the next
		// call confirms, or a key the next call replaces.
		current := client
		for _, delay := range []time.Duration{50 * time.Millisecond, 300 * time.Millisecond, time.Second, 3 * time.Second} {
			callCtx, callCancel := context.WithTimeout(ctx, delay)
			attempted, err := current.ReplaceKey(callCtx)
			callCancel()
			resolved, resolveErr := current.ReplaceKey(ctx)
			if resolveErr != nil {
				t.Fatalf("after a call canceled after %v (%v), the next call returned %v", delay, err, resolveErr)
			}
			switch {
			case err == nil && resolved != attempted:
				t.Fatalf("a call that finished within %v returned another descriptor than the next call", delay)
			case err == nil:
				t.Logf("a call with %v finished in time", delay)
			case attempted == resolved:
				t.Logf("a call canceled after %v applied, which the next call confirmed: %v", delay, err)
			default:
				t.Logf("a call canceled after %v did not apply, and the next call replaced the key: %v", delay, err)
			}
			if authEntry(t, ctx, cluster, client.Name()).Key != keyOf(t, resolved) {
				t.Fatalf("after a call canceled after %v, the MONs hold another key than the resolved descriptor", delay)
			}
			current = resolved
		}
		fresh := startClientContainer(t, ctx, cluster, current)
		for _, mon := range monitorOverrides(t, ctx, cluster) {
			if code, out := clientCommand(t, ctx, fresh, current, "--mon_host_override", mon, "mon", "stat"); code != 0 {
				t.Fatalf("the new key through %s: exit %d: %s", mon, code, out)
			}
			if code, out := clientCommand(t, ctx, holder, client, "--mon_host_override", mon, "mon", "stat"); code == 0 {
				t.Fatalf("the old key through %s authenticated: %s", mon, out)
			}
		}
	})
}

// clientKeyCiphers returns the key types that image supports and a cluster
// configuration whose MONs allow them all, with short MON tickets.
func clientKeyCiphers(t *testing.T, ctx context.Context, image string) ([]ceph.CephxCipher, ceph.AuthConfig) {
	t.Helper()
	auth := ceph.AuthConfig{MonTicketTTL: clientKeyMonTicketTTL, ServiceTicketTTL: 10 * time.Second}
	ctr, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	_, reader, err := ctr.Exec(ctx, []string{"ceph-authtool", "--help"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	usage, _ := io.ReadAll(reader)
	if !strings.Contains(string(usage), "--key-type") {
		return []ceph.CephxCipher{ceph.CipherAES}, auth
	}
	// The service cipher stays fixed whatever key type the clients have.
	auth.KeyType, auth.ServiceCipher = ceph.CipherAES256K, ceph.CipherAES256K
	auth.AllowedCiphers = []ceph.CephxCipher{ceph.CipherAES, ceph.CipherAES256K}
	return []ceph.CephxCipher{ceph.CipherAES, ceph.CipherAES256K}, auth
}

type nativeAuthEntry struct {
	Key  string            `json:"key"`
	Caps map[string]string `json:"caps"`
}

func authEntry(t *testing.T, ctx context.Context, cluster *ceph.Container, entity string) nativeAuthEntry {
	t.Helper()
	out, err := cluster.Ceph(ctx, "auth", "get", entity, "--format", "json")
	var entries []nativeAuthEntry
	if err != nil || json.Unmarshal(out, &entries) != nil || len(entries) != 1 {
		t.Fatalf("auth get %s: %v", entity, err)
	}
	return entries[0]
}

func keyOf(t *testing.T, client *ceph.ClientConfig) string {
	t.Helper()
	for _, line := range strings.Split(string(clientKeyring(t, client)), "\n") {
		if name, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok && strings.TrimSpace(name) == "key" {
			return strings.TrimSpace(value)
		}
	}
	t.Fatalf("no key for %s", client.Name())
	return ""
}

func startClientContainer(t *testing.T, ctx context.Context, cluster *ceph.Container, client *ceph.ClientConfig) testcontainers.Container {
	t.Helper()
	ctr, err := testcontainers.Run(ctx, nativeClientImage(t, staticFacts(cluster).Images.Client), client.WithIdentity(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	return ctr
}

// clientCommand runs the ceph CLI as client in ctr with a fresh connection.
func clientCommand(t *testing.T, ctx context.Context, ctr testcontainers.Container, client *ceph.ClientConfig, args ...string) (int, string) {
	t.Helper()
	argv := append([]string{"ceph", "--id", client.User(), "--connect-timeout", "10"}, args...)
	code, reader, err := ctr.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(reader)
	if code != 0 && code != 13 && !strings.Contains(string(out), "ermission denied") {
		t.Fatalf("%s failed for another reason than authentication: exit %d: %s", strings.Join(argv, " "), code, out)
	}
	return code, string(out)
}

// startHeldSession starts heldSessionScript in ctr with mode as its second
// argument, reading heldObject, and waits for its first successful reads.
func startHeldSession(t *testing.T, ctx context.Context, ctr testcontainers.Container, client *ceph.ClientConfig, mode string) {
	t.Helper()
	if err := ctr.CopyToContainer(ctx, []byte(heldSessionScript), "/tmp/held.py", 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, err := ctr.Exec(ctx, []string{"sh", "-c", `python3 /tmp/held.py "$@" > /tmp/held.log 2> /tmp/held.err &`, "sh",
		client.User(), mode, heldPool, heldNamespace, heldObject}); err != nil || code != 0 {
		t.Fatalf("start the held session: exit %d, %v", code, err)
	}
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		for _, line := range heldSessionLog(t, ctx, ctr) {
			if line.mon == 0 && line.mgr == 0 && line.osd == 0 {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("the held session never succeeded:\n%s", containerFile(t, ctx, ctr, "/tmp/held.log"))
}

type heldSessionLine struct {
	at                  float64
	mon, mgr, osd, conn int
}

func heldSessionLog(t *testing.T, ctx context.Context, ctr testcontainers.Container) []heldSessionLine {
	t.Helper()
	var lines []heldSessionLine
	for _, line := range strings.Split(containerFile(t, ctx, ctr, "/tmp/held.log"), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 5 {
			continue
		}
		at, atErr := strconv.ParseFloat(fields[0], 64)
		mon, monErr := strconv.Atoi(fields[1])
		mgr, mgrErr := strconv.Atoi(fields[2])
		osd, osdErr := strconv.Atoi(fields[3])
		conn, connErr := strconv.Atoi(fields[4])
		if atErr == nil && monErr == nil && mgrErr == nil && osdErr == nil && connErr == nil {
			lines = append(lines, heldSessionLine{at, mon, mgr, osd, conn})
		}
	}
	return lines
}

func containerTime(t *testing.T, ctx context.Context, ctr testcontainers.Container) float64 {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"python3", "-c", "import time; print(time.time())"}, tcexec.Multiplexed())
	out, _ := io.ReadAll(reader)
	at, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || code != 0 || parseErr != nil {
		t.Fatalf("read the container clock: %q %v", out, err)
	}
	return at
}

var heldAuthResult = regexp.MustCompile(`^(\S+) \S+ +\d+ monclient: _finish_auth (-?\d+)$`)

// authRejected is the result of an authentication that the MONs refused
// (EACCES).
const authRejected = -13

// heldAuthResults returns the time and the result of each authentication and
// ticket renewal of the held session's MON client.
func heldAuthResults(t *testing.T, ctx context.Context, ctr testcontainers.Container) []heldSessionLine {
	t.Helper()
	var results []heldSessionLine
	for _, line := range strings.Split(containerFile(t, ctx, ctr, "/tmp/held.err"), "\n") {
		match := heldAuthResult.FindStringSubmatch(strings.TrimSpace(line))
		if match == nil {
			continue
		}
		at, err := time.Parse("2006-01-02T15:04:05.000-0700", match[1])
		result, _ := strconv.Atoi(match[2])
		if err == nil {
			results = append(results, heldSessionLine{at: float64(at.UnixMicro()) / 1e6, mon: result})
		}
	}
	return results
}

// reconnectHeldSession asks the held session to connect anew and returns the
// container time of the request.
func reconnectHeldSession(t *testing.T, ctx context.Context, ctr testcontainers.Container) float64 {
	t.Helper()
	requested := containerTime(t, ctx, ctr)
	if code, _, err := ctr.Exec(ctx, []string{"touch", "/tmp/held.reconnect"}); err != nil || code != 0 {
		t.Fatalf("request a reconnect of the held session: exit %d, %v", code, err)
	}
	return requested
}

// waitHeldSessionReconnected waits until the held session, after since,
// authenticated on a new connection and read from the MON, the MGR and the
// OSD through it, and returns how many seconds after since it authenticated.
func waitHeldSessionReconnected(t *testing.T, ctx context.Context, ctr testcontainers.Container, since float64, limit time.Duration) float64 {
	t.Helper()
	before := 0
	for _, line := range heldSessionLog(t, ctx, ctr) {
		if line.at <= since {
			before = line.conn
		}
	}
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		for _, result := range heldAuthResults(t, ctx, ctr) {
			if result.at <= since || result.mon != 0 {
				continue
			}
			for _, line := range heldSessionLog(t, ctx, ctr) {
				if line.at > result.at && line.conn > before && line.mon == 0 && line.mgr == 0 && line.osd == 0 {
					return result.at - since
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("the held session did not authenticate on a new connection within %v of the request at %.3f:\n%s\n%s", limit, since,
		containerFile(t, ctx, ctr, "/tmp/held.log"), heldAuthTrace(t, ctx, ctr))
	return 0
}

// heldAuthTrace returns the held session's authentication results, one per
// line, for a failure message.
func heldAuthTrace(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	var trace []string
	for _, result := range heldAuthResults(t, ctx, ctr) {
		trace = append(trace, fmt.Sprintf("%.3f _finish_auth %d", result.at, result.mon))
	}
	return strings.Join(trace, "\n")
}

// heldOSDReads summarizes the held session's OSD reads after since.
func heldOSDReads(t *testing.T, ctx context.Context, ctr testcontainers.Container, since float64) string {
	t.Helper()
	ok, failed := 0, 0
	for _, line := range heldSessionLog(t, ctx, ctr) {
		switch {
		case line.at <= since:
		case line.osd == 0:
			ok++
		default:
			failed++
		}
	}
	return fmt.Sprintf("%d succeeded, %d failed", ok, failed)
}

// waitHeldRenewalRejected waits until the MONs reject an authentication or
// ticket renewal of the held session with EACCES after since, and returns how
// many seconds after since that was and whether the session's MON reads
// failed since then. Squid keeps the MON session open and answering after a
// rejected renewal, while Tentacle 20.2.4 drops it. Either way the session's
// MGR reads keep working.
func waitHeldRenewalRejected(t *testing.T, ctx context.Context, ctr testcontainers.Container, since float64, limit time.Duration) (float64, bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		for _, result := range heldAuthResults(t, ctx, ctr) {
			if result.at > since && result.mon == authRejected {
				// The session's reads show within a few seconds whether it
				// lost its MON session, and that it kept its MGR session.
				failed, mgr := false, false
				for wait := time.Now().Add(15 * time.Second); time.Now().Before(wait) && !(failed && mgr); time.Sleep(time.Second) {
					for _, line := range heldSessionLog(t, ctx, ctr) {
						failed = failed || (line.at > result.at && line.mon != 0)
						mgr = mgr || (line.at > result.at && line.mgr == 0)
					}
				}
				if !mgr {
					t.Fatalf("the held session lost its MGR session after the rejection:\n%s", containerFile(t, ctx, ctr, "/tmp/held.log"))
				}
				return result.at - since, failed
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("the MONs did not reject the held session for %v after the change:\n%s", limit, containerFile(t, ctx, ctr, "/tmp/held.err"))
	return 0, false
}

// monitorOverrides returns, per MON, the address vector that
// mon_host_override takes.
func monitorOverrides(t *testing.T, ctx context.Context, cluster *ceph.Container) []string {
	t.Helper()
	out, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
	var dump struct {
		Mons []struct {
			PublicAddrs struct {
				Addrvec []struct{ Type, Addr string } `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"mons"`
	}
	if err != nil || json.Unmarshal(out, &dump) != nil || len(dump.Mons) == 0 {
		t.Fatalf("mon dump: %v", err)
	}
	var overrides []string
	for _, mon := range dump.Mons {
		var addrs []string
		for _, addr := range mon.PublicAddrs.Addrvec {
			addrs = append(addrs, fmt.Sprintf("%s:%s", addr.Type, addr.Addr))
		}
		overrides = append(overrides, "["+strings.Join(addrs, ",")+"]")
	}
	return overrides
}
