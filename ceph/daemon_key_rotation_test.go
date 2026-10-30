package ceph

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestRotateKeyRefusesBeforeRunningCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit   func(*Container)
		daemon string
		want   string
	}{
		// A MON, an MDS or a malformed name has no handle with RotateKey, so
		// the call does not compile and no runtime refusal remains.
		"unknown OSD":    {nil, "osd.7", "not owned"},
		"purged OSD":     {nil, "osd.1", "not owned"},
		"terminated MGR": {nil, "mgr.b", "not owned"},
		"closed cluster": {func(c *Container) { c.closed = true }, "osd.0", "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			err := rotateKeyOf(t, c, test.daemon)
			if err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, control.Calls)
			}
		})
	}
}

// rotationWorld models what a key rotation can leave behind for osd.0: the
// MONs' key and pending key, the key the running daemon authenticates with,
// the files of the daemon container, the control container's files, and the
// commands that may still apply. Faults change one occurrence of an
// operation, counted per operation.
type rotationWorld struct {
	current, pending, live string
	// daemonFiles holds the daemon container's files, keyring files and
	// staged copies alike.
	daemonFiles, controlFiles map[string]string
	generated                 int
	counts                    map[string]int
	faults                    map[string]rotationFault
	// route is the MON that a CLI without mon_host_override reaches; the
	// leader is "a". election and monmapEpoch describe the quorum.
	route              string
	election, monEpoch int
	// barrierAlsoReaches makes a barrier CLI lose its MON and continue
	// through this one, and electionAfterBarrier starts an election after the
	// next barrier.
	barrierAlsoReaches   string
	electionAfterBarrier bool
	// nextRoute, when set, is the route of the calls after the first one.
	nextRoute string
	// daemonID and started identify the daemon container and its process.
	daemonID, started string
	// identities holds the auth entities other than osd.0.
	identities map[string]bool
	// late holds, in order, the commands that reached a MON or the daemon
	// and have not applied yet.
	late []lateCommand
	// stopped holds the supervisor markers of stopped CLIs, and cancelFails
	// counts the cancellations that fail before one succeeds.
	stopped     map[string]bool
	cancelFails int
	// killsSupervisor kills the supervisor of a CLI that hangs, which then
	// runs on until a cancellation stops it.
	killsSupervisor bool
	// sessions holds, per MON, the nonces of the MON clients whose
	// connections the MON still holds; nonceOf maps a supervisor marker to
	// the nonce of its MON client, and nonces counts the clients.
	sessions map[string]map[string]bool
	nonceOf  map[string]string
	nonces   int
	// listed holds the MON and nonce pairs of the sessions that a MON has
	// listed.
	listed map[string]bool
	// daemonPaused makes Docker refuse to start execs in the daemon
	// container, and renewAfterHandoff makes the daemon renew its tickets
	// right after a handoff.
	daemonPaused, renewAfterHandoff bool
	// execs holds the Docker execs of the daemon container, and dockerDown
	// makes every Docker exec request fail.
	execs      map[string]fakeExec
	dockerDown bool
	// releaseBefore names the operations right before which every late
	// command that can still apply does, the worst time for most of them.
	releaseBefore map[string]bool
	// onControlCopy runs before the control container stores a file, and
	// afterExecStart after each request to start an exec. onHang, when set,
	// runs when a command hangs, after which the Exec of the supervisor ends
	// early; otherwise the supervisor stops a hanging command at its deadline.
	onControlCopy, afterExecStart, onHang func()
	// onDaemonCopy runs before the daemon container stores a file.
	onDaemonCopy func()
	// cleanupTimeout, when set, replaces keyRotationCleanupTimeout during the
	// test.
	cleanupTimeout time.Duration
	// clients holds the auth entries of client identities.
	clients map[string]fakeClient
	// lateCopies holds control container copies that Docker completes after
	// lateCopyDelay more commands in the container, by default after the next
	// one.
	lateCopies    map[string]lateCopy
	lateCopyDelay int
}

type lateCopy struct {
	content string
	after   int
}

// fakeClient is the auth entry of a client identity.
type fakeClient struct {
	key  string
	caps map[string]string
}

// lateCommand is a command whose CLI hung, or a copy that Docker completes
// late. A MON command went through the MON via from the MON client nonce; a
// later write through the same MON, a barrier among them, happens after it
// once it arrived. One that is still in flight arrives before the MON sees its
// connection close, whenever that is. A MON command that was not prepared
// applies only while the MON holds its client's connection, and a rotate-key
// that the daemon has not received only while its CLI runs. A rotate-key went
// to the daemon process that started at via.
type lateCommand struct {
	op, key, via, session, nonce string
	prepared, unsent, inFlight   bool
	// entity and input are the client and the keyring of a client command,
	// and caps the capabilities of auth caps.
	entity, input string
	caps          []string
}

// fakeExec is a Docker exec in the daemon container. A start whose request
// was lost may still reach Docker; a running exec has not applied yet.
type fakeExec struct {
	argv                              []string
	container                         string
	started, done, lostStart, dropped bool
}

type rotationFault struct {
	occurrence int
	effect     faultEffect
}

type faultEffect int

const (
	// failsBefore reports an error without any effect; a lost Docker exec
	// start request may still reach Docker.
	failsBefore faultEffect = iota
	// failsAfter applies the effect and then reports an error, as a lost
	// reply does.
	failsAfter
	// hangsPrepared hangs the CLI after the MON prepared the change or the
	// daemon received the key, so that the effect applies later; a copy that
	// fails with it completes later.
	hangsPrepared
	// hangsQueued hangs the CLI before the command was prepared or received.
	hangsQueued
	// hangsUnsent hangs the CLI before it sent the command.
	hangsUnsent
	// hangsInFlight hangs the CLI after it sent the command, which the
	// network delays together with the end of its connection.
	hangsInFlight
	// runsOn leaves a Docker exec running after its start succeeded.
	runsOn
	// answersWithoutApplying answers a MON command with success without
	// applying it, as a MON does when the entity has an uncommitted change.
	answersWithoutApplying
	// duplicatedInFlight applies a MON command and answers it, but the
	// client had also resent it through another MON after losing its
	// session, and that copy is still in flight.
	duplicatedInFlight
)

var (
	rotationMonitorNames = []string{"a", "b", "c"}
	rotationMonitorIPs   = map[string]string{"a": "10.0.0.1", "b": "10.0.0.2", "c": "10.0.0.3"}
	rotationKeyringPaths = []string{"/etc/ceph/osd.keyring", "/var/lib/ceph/osd/ceph-0/keyring"}
)

func newRotationWorld() *rotationWorld {
	w := &rotationWorld{current: "key-0", live: "key-0", route: "a", election: 10, monEpoch: 3, daemonID: "osd-0-ctr", started: "start-1",
		counts: map[string]int{}, faults: map[string]rotationFault{}, daemonFiles: map[string]string{}, controlFiles: map[string]string{},
		identities: map[string]bool{}, stopped: map[string]bool{}, execs: map[string]fakeExec{}, releaseBefore: map[string]bool{},
		sessions: map[string]map[string]bool{}, nonceOf: map[string]string{}, listed: map[string]bool{}, clients: map[string]fakeClient{}}
	for _, path := range rotationKeyringPaths {
		w.daemonFiles[path] = string(daemonKeyring("osd.0", "key-0"))
	}
	return w
}

// accepted reports whether the MONs authenticate key.
func (w *rotationWorld) accepted(key string) bool {
	return key == w.current || (w.pending != "" && key == w.pending)
}

func (w *rotationWorld) apply(cmd lateCommand) {
	switch cmd.op {
	case "create":
		if w.pending == "" {
			w.generated++
			w.pending = fmt.Sprintf("key-%d", w.generated)
		}
	case "clear":
		w.pending = ""
	case "commit":
		if w.pending != "" {
			w.current, w.pending = w.pending, ""
		}
	case "barrier":
		w.identities[cmd.key] = true
	case "tell":
		if cmd.via == w.started {
			w.live = cmd.key
		}
	case "copy":
		w.daemonFiles[cmd.via] = cmd.key
	case "import":
		// auth import replaces the key and the caps of every entity of its
		// keyring at once.
		entity, client := parseFakeKeyring(cmd.input)
		w.clients[entity] = client
	case "remove":
		delete(w.clients, cmd.entity)
	case "add":
		// auth add with a keyring creates its entry, and leaves an existing
		// one as it is: unchanged when it matches, refused when it does not.
		entity, client := parseFakeKeyring(cmd.input)
		if _, ok := w.clients[entity]; !ok && entity == cmd.entity {
			w.clients[entity] = client
		}
	case "caps":
		// auth caps replaces every capability of an existing entity.
		if client, ok := w.clients[cmd.entity]; ok {
			client.caps = map[string]string{}
			for i := 0; i+1 < len(cmd.caps); i += 2 {
				client.caps[cmd.caps[i]] = cmd.caps[i+1]
			}
			w.clients[cmd.entity] = client
		}
	}
}

var fakeKeyringLine = regexp.MustCompile(`^\s*(?:\[(\S+)\]|key = (\S+)|caps (\S+) = "(.*)")\s*$`)

func parseFakeKeyring(keyring string) (string, fakeClient) {
	entity, client := "", fakeClient{caps: map[string]string{}}
	for _, line := range strings.Split(keyring, "\n") {
		match := fakeKeyringLine.FindStringSubmatch(line)
		switch {
		case match == nil:
		case match[1] != "":
			entity = match[1]
		case match[2] != "":
			client.key = match[2]
		default:
			client.caps[match[3]] = match[4]
		}
	}
	return entity, client
}

func fakeClientKeyring(entity string, client fakeClient) string {
	text := "[" + entity + "]\n\tkey = " + client.key + "\n"
	for _, service := range slices.Sorted(maps.Keys(client.caps)) {
		text += fmt.Sprintf("\tcaps %s = %q\n", service, client.caps[service])
	}
	return text
}

// fakeCephxKey returns a key of cipher, encoded as Ceph encodes its type.
func fakeCephxKey(cipher CephxCipher, n int) string {
	kind := map[CephxCipher]byte{CipherAES: 1, CipherAES256K: 2}[cipher]
	return base64.StdEncoding.EncodeToString(append([]byte{kind, 0}, fmt.Sprintf("fake-secret-%04d", n)...))
}

// release lets the late commands that match go ahead in order: those that can
// still apply do, and the others are dropped. Commands in flight go ahead only
// when they arrive.
func (w *rotationWorld) release(match func(lateCommand) bool, arrive bool) {
	var waiting []lateCommand
	for _, cmd := range w.late {
		switch {
		case cmd.unsent && w.stopped[cmd.session]:
			// Its CLI is gone, so the command never arrives.
		case !match(cmd) || (cmd.inFlight && !arrive):
			waiting = append(waiting, cmd)
		case cmd.prepared:
			w.apply(cmd)
		case cmd.nonce != "":
			if w.sessions[cmd.via][cmd.nonce] {
				w.apply(cmd)
			}
		case !w.stopped[cmd.session]:
			w.apply(cmd)
		}
		if cmd.inFlight && arrive && match(cmd) && w.stopped[cmd.session] {
			// The end of its connection arrives right after it.
			delete(w.sessions[cmd.via], cmd.nonce)
		}
	}
	w.late = waiting
}

// stop stops the CLI of a supervisor marker. Its MON client's connections
// close at once unless a command of it is still in flight.
func (w *rotationWorld) stop(marker string) {
	w.stopped[marker] = true
	nonce := w.nonceOf[marker]
	if nonce == "" {
		return
	}
	for mon, sessions := range w.sessions {
		if !slices.ContainsFunc(w.late, func(cmd lateCommand) bool { return cmd.inFlight && cmd.nonce == nonce && cmd.via == mon }) {
			delete(sessions, nonce)
		}
	}
}

// authenticate authenticates the daemon with key; the MONs commit a pending
// key that the daemon uses.
func (w *rotationWorld) authenticate(key string) {
	if w.pending != "" && key == w.pending {
		w.current, w.pending = w.pending, ""
	}
}

// releaseAll lets every late command, lost exec start and running exec go
// ahead, and the daemon renew its tickets.
func (w *rotationWorld) releaseAll() {
	w.release(func(lateCommand) bool { return true }, true)
	for _, id := range slices.Sorted(maps.Keys(w.execs)) {
		exec := w.execs[id]
		if exec.lostStart && !exec.started && !exec.dropped {
			exec.started = true
		}
		w.execs[id] = exec
		w.finishExec(id)
	}
	w.authenticate(w.live)
}

// finishExec applies a started exec that has not applied yet.
func (w *rotationWorld) finishExec(id string) int {
	exec := w.execs[id]
	if !exec.started || exec.done || exec.dropped {
		return 0
	}
	exec.done = true
	w.execs[id] = exec
	if len(exec.argv) != 4 || exec.argv[0] != "mv" || exec.argv[1] != "-f" {
		return 127
	}
	content, ok := w.daemonFiles[exec.argv[2]]
	if !ok {
		return 1
	}
	w.daemonFiles[exec.argv[3]] = content
	delete(w.daemonFiles, exec.argv[2])
	return 0
}

// restart restarts the daemon container: the new process reads the keyring
// in its data directory, as an initialized OSD does, and neither a rotate-key
// on its way to the old process nor an exec of the old container can still
// apply.
func (w *rotationWorld) restart() {
	w.started += "-restarted"
	w.live = keyringKey.FindStringSubmatch(w.daemonFiles["/var/lib/ceph/osd/ceph-0/keyring"])[1]
	w.authenticate(w.live)
	for id, exec := range w.execs {
		if !exec.done {
			exec.dropped = true
			w.execs[id] = exec
		}
	}
}

func (w *rotationWorld) clone() *rotationWorld {
	future := *w
	future.late = slices.Clone(w.late)
	future.identities = maps.Clone(w.identities)
	future.daemonFiles = maps.Clone(w.daemonFiles)
	future.clients = maps.Clone(w.clients)
	future.execs = maps.Clone(w.execs)
	future.sessions = map[string]map[string]bool{}
	for mon, sessions := range w.sessions {
		future.sessions[mon] = maps.Clone(sessions)
	}
	return &future
}

var keyringKey = regexp.MustCompile(`key = (\S+)`)

// requireUsable checks the property a rotation must never break: the running
// daemon and a restarted daemon can both authenticate, now and after every
// late command that can still apply did.
func (w *rotationWorld) requireUsable(t *testing.T) {
	t.Helper()
	w.checkUsable(t, "")
	future := w.clone()
	future.releaseAll()
	future.checkUsable(t, " once the late commands applied")
}

func (w *rotationWorld) checkUsable(t *testing.T, when string) {
	t.Helper()
	if !w.accepted(w.live) {
		t.Fatalf("the running daemon's key %s is not accepted%s (current %s, pending %q)", w.live, when, w.current, w.pending)
	}
	for _, path := range rotationKeyringPaths {
		content := w.daemonFiles[path]
		match := keyringKey.FindStringSubmatch(content)
		if match == nil || !w.accepted(match[1]) {
			t.Fatalf("a restart would read %q from %s, which is not accepted%s (current %s, pending %q)", content, path, when, w.current, w.pending)
		}
	}
}

// requireRotated checks that the daemon, its files and the MONs hold only key
// and that nothing of the rotation is left.
func (w *rotationWorld) requireRotated(t *testing.T, key string) {
	t.Helper()
	w.requireUsable(t)
	if w.current != key || w.pending != "" || w.live != key || len(w.controlFiles) != 0 || len(w.identities) != 0 || len(w.daemonFiles) != len(rotationKeyringPaths) {
		t.Fatalf("rotation to %s incomplete: current %s, pending %q, live %s, control files %v, identities %v, daemon files %v", key, w.current, w.pending, w.live, w.controlFiles, w.identities, w.daemonFiles)
	}
	for _, path := range rotationKeyringPaths {
		if content := w.daemonFiles[path]; content != string(daemonKeyring("osd.0", key)) {
			t.Fatalf("%s holds %q", path, content)
		}
	}
}

// fault reports the effect of this occurrence of op, if it has one.
func (w *rotationWorld) fault(op string) (faultEffect, bool) {
	w.counts[op]++
	fault, ok := w.faults[op]
	if !ok || fault.occurrence != w.counts[op] {
		return 0, false
	}
	return fault.effect, true
}

type rotationReply struct {
	stdout, stderr string
	code           int
	hang           bool
}

func (w *rotationWorld) execute(cmd lateCommand, output func() string) rotationReply {
	effect, faulted := w.fault(cmd.op)
	if faulted && effect == failsBefore {
		return rotationReply{code: 1, stderr: "injected " + cmd.op + " failure"}
	}
	if cmd.op != "tell" {
		// A MON handles a write after every earlier command that reached it.
		w.release(func(late lateCommand) bool {
			return late.op != "tell" && late.op != "copy" && late.via == cmd.via && !late.unsent
		}, false)
	}
	if faulted && effect == answersWithoutApplying {
		return rotationReply{stdout: "[]"}
	}
	if faulted && effect == duplicatedInFlight {
		copy := cmd
		copy.via, copy.inFlight = "c", true
		if cmd.via == "c" {
			copy.via = "b"
		}
		if w.sessions[copy.via] == nil {
			w.sessions[copy.via] = map[string]bool{}
		}
		w.sessions[copy.via][cmd.nonce] = true
		w.late = append(w.late, copy)
		faulted = false
	}
	if faulted && effect != failsAfter {
		cmd.prepared, cmd.unsent, cmd.inFlight = effect == hangsPrepared, effect == hangsUnsent, effect == hangsInFlight
		w.late = append(w.late, cmd)
		return rotationReply{hang: true}
	}
	w.apply(cmd)
	if faulted {
		return rotationReply{code: 1, stderr: "injected " + cmd.op + " failure after its effect"}
	}
	if output == nil {
		return rotationReply{}
	}
	return rotationReply{stdout: output()}
}

func (w *rotationWorld) quorumStatus() string {
	var mons []string
	for _, name := range rotationMonitorNames {
		mons = append(mons, fmt.Sprintf(`{"name":%q,"public_addrs":{"addrvec":[{"type":"v2","addr":"%s:3300","nonce":0}]}}`, name, rotationMonitorIPs[name]))
	}
	return fmt.Sprintf(`{"election_epoch":%d,"quorum_names":["a","b","c"],"quorum_leader_name":"a","monmap":{"epoch":%d,"mons":[%s]}}`, w.election, w.monEpoch, strings.Join(mons, ","))
}

// barrier runs auth add through the MON that mon_host_override names, and
// logs the MONs the CLI picked as a MON client does.
func (w *rotationWorld) barrier(override, entity, session string) rotationReply {
	target := ""
	for name, ip := range rotationMonitorIPs {
		if override == "[v2:"+ip+":3300]" {
			target = name
		}
	}
	if target == "" {
		return rotationReply{code: 1, stderr: "unknown override " + override}
	}
	reached := []string{target}
	if w.barrierAlsoReaches != "" && w.barrierAlsoReaches != target {
		reached = append(reached, w.barrierAlsoReaches)
	}
	var log []string
	for _, name := range reached {
		log = append(log, fmt.Sprintf("monclient(hunting): picked mon.%s con 0x1 addr [v2:%s:3300/0]", name, rotationMonitorIPs[name]))
	}
	reply := w.execute(lateCommand{op: "barrier", key: entity, via: reached[len(reached)-1], session: session}, nil)
	reply.stderr = strings.Join(log, "\n") + "\n" + reply.stderr
	if w.electionAfterBarrier && reply.code == 0 && !reply.hang {
		// A command that a peon forwarded to the old leader reaches the new
		// one through the old leader, whose barrier has already passed.
		w.election++
		w.electionAfterBarrier = false
		for i, cmd := range w.late {
			if cmd.op != "tell" && cmd.op != "copy" {
				w.late[i].via = target
			}
		}
	}
	return reply
}

// run applies one command of the CLI with the supervisor marker session.
func (w *rotationWorld) entry() string {
	if w.pending == "" {
		return fmt.Sprintf(`[{"entity":"osd.0","key":%q}]`, w.current)
	}
	return fmt.Sprintf(`[{"entity":"osd.0","key":%q,"pending_key":%q}]`, w.current, w.pending)
}

// mutation runs monitorMutationScript: its client connects to the MON that
// route names, stores its nonce in path and sends command.
func (w *rotationWorld) mutation(path, command, session, input string) rotationReply {
	var cmd struct {
		Prefix, Entity, Format string
		Caps                   []string
	}
	op := ""
	if json.Unmarshal([]byte(command), &cmd) == nil {
		switch {
		case cmd.Entity == "osd.0" && cmd.Format == "json" && input == "":
			op = map[string]string{"auth get-or-create-pending": "create", "auth clear-pending": "clear", "auth commit-pending": "commit"}[cmd.Prefix]
		case cmd.Prefix == "auth import" && cmd.Entity == "" && input != "":
			op = "import"
		case cmd.Prefix == "auth rm" && strings.HasPrefix(cmd.Entity, "client.") && input == "":
			op = "remove"
		case cmd.Prefix == "auth caps" && strings.HasPrefix(cmd.Entity, "client.") && input == "" && len(cmd.Caps) > 0:
			op = "caps"
		case cmd.Prefix == "auth add" && strings.HasPrefix(cmd.Entity, "client.") && input != "":
			op = "add"
		}
	}
	if op == "" {
		return rotationReply{code: 1, stderr: "unexpected MON command " + command}
	}
	if effect, faulted := w.fault("connect"); faulted && effect == failsBefore {
		return rotationReply{code: 1, stderr: "injected connect failure"}
	}
	w.nonces++
	nonce := strconv.Itoa(w.nonces)
	w.controlFiles[path], w.nonceOf[session] = nonce, nonce
	if w.sessions[w.route] == nil {
		w.sessions[w.route] = map[string]bool{}
	}
	w.sessions[w.route][nonce] = true
	if w.releaseBefore[op] {
		w.releaseAll()
	}
	var output func() string
	if op == "create" {
		output = w.entry
	}
	content := ""
	if op == "import" || op == "add" {
		var ok bool
		if content, ok = w.controlFiles[input]; !ok {
			// The script fails to read its input and sends nothing.
			w.stop(session)
			return rotationReply{code: 1, stderr: "no input"}
		}
		if _, client := parseFakeKeyring(content); op == "import" && len(client.caps) == 0 {
			// AuthMonitor::import_keyring refuses an entry without caps.
			w.stop(session)
			return rotationReply{code: 1, stderr: "auth import: no caps supplied"}
		}
	}
	reply := w.execute(lateCommand{op: op, via: w.route, session: session, nonce: nonce, entity: cmd.Entity, input: content, caps: cmd.Caps}, output)
	if !reply.hang {
		// The client exits, and the MONs see its connections close.
		w.stop(session)
	}
	return reply
}

// monitorSessions answers tell mon.<mon> sessions. A command still in flight
// to mon from a stopped client arrives, and its connection ends, only by the
// time mon is asked again after it listed the session.
func (w *rotationWorld) monitorSessions(mon string) rotationReply {
	w.release(func(cmd lateCommand) bool {
		return cmd.inFlight && cmd.via == mon && w.stopped[cmd.session] && w.listed[mon+"/"+cmd.nonce]
	}, true)
	var sessions []string
	for _, nonce := range slices.Sorted(maps.Keys(w.sessions[mon])) {
		w.listed[mon+"/"+nonce] = true
		sessions = append(sessions, fmt.Sprintf(`{"entity_name":"client.admin","addrs":{"type":"any","addr":"10.0.0.9:0","nonce":%s}}`, nonce))
	}
	return rotationReply{stdout: "[" + strings.Join(sessions, ",") + "]"}
}

func (w *rotationWorld) run(args []string, session string) rotationReply {
	if len(args) == 5 && args[0] == "python3" && args[1] == "-c" && args[2] == monitorMutationScript {
		return w.mutation(args[3], args[4], session, "")
	}
	if len(args) == 6 && args[0] == "python3" && args[1] == "-c" && args[2] == monitorMutationScript {
		return w.mutation(args[3], args[4], session, args[5])
	}
	if len(args) == 5 && strings.Join(args[:4], " ") == `sh -c test ! -e "$1" || cat "$1" sh` {
		return rotationReply{stdout: w.controlFiles[args[4]]}
	}
	words := strings.Join(args, " ")
	if rest, ok := strings.CutPrefix(words, "ceph --mon_host_override "); ok {
		override, command, _ := strings.Cut(rest, " ")
		fields := strings.Fields(command)
		if strings.Join(fields[:len(fields)-1], " ") != "--connect-timeout 5 --debug_monc 10/10 --log_to_stderr true --err_to_stderr true auth add" {
			return rotationReply{code: 1, stderr: "unexpected barrier " + command}
		}
		if w.releaseBefore["barrier"] {
			w.releaseAll()
		}
		return w.barrier(override, fields[len(fields)-1], session)
	}
	command, ok := strings.CutPrefix(words, "ceph --connect-timeout 5 ")
	if !ok {
		command = words
	}
	fields := strings.Fields(command)
	switch {
	case command == "quorum_status --format json":
		return rotationReply{stdout: w.quorumStatus()}
	case command == "auth get osd.0 --format json":
		return rotationReply{stdout: w.entry()}
	case len(fields) >= 3 && fields[0] == "auth" && (fields[1] == "get" || fields[1] == "del") && strings.HasPrefix(fields[2], "client."):
		client, ok := w.clients[fields[2]]
		switch {
		case !ok:
			return rotationReply{code: 2, stderr: "Error ENOENT: failed to find " + fields[2] + " in keyring"}
		case fields[1] == "del" && len(fields) == 3:
			delete(w.clients, fields[2])
			return rotationReply{}
		case len(fields) == 3:
			return rotationReply{stdout: fakeClientKeyring(fields[2], client)}
		case strings.Join(fields[3:], " ") == "--format json":
			entry, _ := json.Marshal([]map[string]any{{"entity": fields[2], "key": client.key, "caps": client.caps}})
			return rotationReply{stdout: string(entry)}
		}
	case len(fields) >= 2 && fields[0] == "ceph-authtool" && fields[1] == "--gen-print-key":
		// Without --key-type, ceph-authtool prints a key of the release
		// default type.
		cipher := CipherAES
		if len(fields) == 4 && fields[2] == "--key-type" {
			cipher = CephxCipher(fields[3])
		} else if len(fields) != 2 {
			break
		}
		w.generated++
		return rotationReply{stdout: fakeCephxKey(cipher, w.generated) + "\n"}
	case len(fields) == 5 && fields[0] == "tell" && strings.HasPrefix(fields[1], "mon.") && strings.Join(fields[2:], " ") == "sessions --format json":
		return w.monitorSessions(strings.TrimPrefix(fields[1], "mon."))
	case len(fields) == 3 && fields[0] == "auth" && fields[1] == "rm" && strings.HasPrefix(fields[2], "client.tc-key-rotation-"):
		delete(w.identities, fields[2])
		return rotationReply{}
	case len(fields) == 5 && strings.Join(fields[:4], " ") == "tell osd.0 rotate-key -i":
		if w.releaseBefore["tell"] {
			w.releaseAll()
		}
		key, ok := w.controlFiles[fields[4]]
		if !ok {
			return rotationReply{code: 1, stderr: "no key file"}
		}
		reply := w.execute(lateCommand{op: "tell", key: key, via: w.started, session: session}, nil)
		if reply.code == 0 && !reply.hang && w.renewAfterHandoff {
			w.authenticate(w.live)
		}
		return reply
	case len(fields) >= 3 && fields[0] == "rm" && fields[1] == "-f":
		for _, path := range fields[2:] {
			delete(w.controlFiles, path)
		}
		return rotationReply{}
	}
	return rotationReply{code: 1, stderr: "unexpected command " + words}
}

type rotationControl struct {
	testcontainers.Container
	w *rotationWorld
}

// Exec runs the rotation's commands under the supervisor, or directly. The
// supervisor's cancellation, and its deadline, stop the command's CLI.
func (ctr *rotationControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	w := ctr.w
	// A copy that Docker completes late lands right after a command.
	arriving := map[string]string{}
	for path, late := range w.lateCopies {
		if late.after--; late.after <= 0 {
			arriving[path] = late.content
			delete(w.lateCopies, path)
		} else {
			w.lateCopies[path] = late
		}
	}
	defer func() {
		for path, content := range arriving {
			w.controlFiles[path] = content
		}
	}()
	switch {
	case len(args) > 3 && args[0] == "python3" && args[2] == supervised.CancelScript:
		if w.cancelFails > 0 {
			w.cancelFails--
			return 3, bytes.NewReader(nil), nil
		}
		w.stop(args[3])
		return 0, bytes.NewReader(nil), nil
	case len(args) > 6 && args[0] == "python3" && args[2] == supervised.Script:
		marker := supervisedMarker(opts)
		reply := w.run(args[6:], marker)
		switch {
		case reply.hang && w.onHang != nil:
			// The caller gives up, or the exec stream breaks.
			w.onHang()
			if err := ctx.Err(); err != nil {
				return 0, nil, err
			}
			return 0, nil, errors.New("the exec stream broke")
		case reply.hang && w.killsSupervisor:
			return 128 + 9, bytes.NewReader(nil), nil
		case reply.hang:
			w.stop(marker)
			return 0, bytes.NewReader(clustertest.QuorumEnvelope(nil, nil, -9, true, false)), nil
		}
		return 0, bytes.NewReader(clustertest.QuorumEnvelope([]byte(reply.stdout), []byte(reply.stderr), reply.code, false, false)), nil
	}
	reply := w.run(args, "")
	if reply.hang {
		// Nothing stops a CLI that runs without the supervisor.
		return 0, nil, errors.New("the CLI did not answer")
	}
	// Docker multiplexes the output of an exec without a terminal.
	var stream bytes.Buffer
	for kind, output := range [][]byte{1: []byte(reply.stdout), 2: []byte(reply.stderr)} {
		if len(output) > 0 {
			header := make([]byte, 8)
			header[0] = byte(kind)
			binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
			stream.Write(header)
			stream.Write(output)
		}
	}
	return reply.code, &stream, nil
}

func supervisedMarker(opts []tcexec.ProcessOption) string {
	processOptions := tcexec.NewProcessOptions(nil)
	for _, opt := range opts {
		opt.Apply(processOptions)
	}
	for _, value := range processOptions.ExecConfig.Env {
		if marker, ok := strings.CutPrefix(value, supervised.MarkerVariable+"="); ok {
			return marker
		}
	}
	return ""
}

func (ctr *rotationControl) State(context.Context) (*container.State, error) {
	return &container.State{Running: true, StartedAt: "control-start"}, nil
}

func (ctr *rotationControl) CopyToContainer(ctx context.Context, content []byte, path string, _ int64) error {
	if ctr.w.onControlCopy != nil {
		ctr.w.onControlCopy()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	effect, faulted := ctr.w.fault("control-copy")
	switch {
	case !faulted || effect == failsAfter:
		ctr.w.controlFiles[path] = string(content)
	case effect == hangsPrepared:
		if ctr.w.lateCopies == nil {
			ctr.w.lateCopies = map[string]lateCopy{}
		}
		ctr.w.lateCopies[path] = lateCopy{content: string(content), after: max(ctr.w.lateCopyDelay, 1)}
	}
	if faulted {
		return errors.New("injected control copy failure")
	}
	return nil
}

type rotationDaemon struct {
	testcontainers.Container
	w *rotationWorld
}

func (ctr *rotationDaemon) GetContainerID() string { return ctr.w.daemonID }

func (ctr *rotationDaemon) State(context.Context) (*container.State, error) {
	return &container.State{Running: true, StartedAt: ctr.w.started}, nil
}

// CopyToContainer stores a file; a copy that fails with hangsPrepared
// completes later, as Docker may after a request failed.
func (ctr *rotationDaemon) CopyToContainer(ctx context.Context, content []byte, path string, _ int64) error {
	if ctr.w.onDaemonCopy != nil {
		ctr.w.onDaemonCopy()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	effect, faulted := ctr.w.fault("daemon-copy")
	switch {
	case !faulted || effect == failsAfter:
		ctr.w.daemonFiles[path] = string(content)
	case effect == hangsPrepared:
		ctr.w.late = append(ctr.w.late, lateCommand{op: "copy", key: string(content), via: path, prepared: true})
	}
	if faulted {
		return errors.New("injected daemon copy failure")
	}
	return nil
}

func (ctr *rotationDaemon) CopyFileFromContainer(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader([]byte(ctr.w.daemonFiles[path]))), nil
}

// rotationDocker is the Docker exec API of the daemon container.
type rotationDocker struct{ w *rotationWorld }

func (d rotationDocker) Close() error { return nil }

func (d rotationDocker) ExecCreate(ctx context.Context, id string, options mobycl.ExecCreateOptions) (mobycl.ExecCreateResult, error) {
	if err := ctx.Err(); err != nil {
		return mobycl.ExecCreateResult{}, err
	}
	if d.w.dockerDown {
		return mobycl.ExecCreateResult{}, errors.New("Docker is unavailable")
	}
	execID := fmt.Sprintf("exec-%d", len(d.w.execs)+1)
	d.w.execs[execID] = fakeExec{argv: slices.Clone(options.Cmd), container: id}
	return mobycl.ExecCreateResult{ID: execID}, nil
}

func (d rotationDocker) ExecStart(ctx context.Context, execID string, _ mobycl.ExecStartOptions) (mobycl.ExecStartResult, error) {
	w := d.w
	if w.afterExecStart != nil {
		defer w.afterExecStart()
	}
	if err := ctx.Err(); err != nil {
		return mobycl.ExecStartResult{}, err
	}
	if w.dockerDown {
		return mobycl.ExecStartResult{}, errors.New("Docker is unavailable")
	}
	exec, ok := w.execs[execID]
	switch {
	case !ok || exec.dropped:
		return mobycl.ExecStartResult{}, fmt.Errorf("exec %s: %w", execID, errdefs.ErrNotFound)
	case exec.started:
		return mobycl.ExecStartResult{}, fmt.Errorf("exec %s has already run: %w", execID, errdefs.ErrConflict)
	case w.daemonPaused:
		return mobycl.ExecStartResult{}, fmt.Errorf("container is paused: %w", errdefs.ErrConflict)
	}
	effect, faulted := w.fault("exec-start")
	if faulted && effect == failsBefore {
		exec.lostStart = true
		w.execs[execID] = exec
		return mobycl.ExecStartResult{}, errors.New("injected exec start failure")
	}
	exec.started = true
	w.execs[execID] = exec
	if !faulted || effect != runsOn {
		w.finishExec(execID)
	}
	if faulted && effect == failsAfter {
		return mobycl.ExecStartResult{}, errors.New("injected exec start failure after its effect")
	}
	return mobycl.ExecStartResult{}, nil
}

// ExecInspect reports an exec; one that runs on ends by the time Docker is
// asked again while it is available.
func (d rotationDocker) ExecInspect(ctx context.Context, execID string, _ mobycl.ExecInspectOptions) (mobycl.ExecInspectResult, error) {
	w := d.w
	if err := ctx.Err(); err != nil {
		return mobycl.ExecInspectResult{}, err
	}
	if w.dockerDown {
		return mobycl.ExecInspectResult{}, errors.New("Docker is unavailable")
	}
	exec, ok := w.execs[execID]
	if !ok || exec.dropped {
		return mobycl.ExecInspectResult{}, fmt.Errorf("exec %s: %w", execID, errdefs.ErrNotFound)
	}
	if !exec.started {
		return mobycl.ExecInspectResult{ID: execID}, nil
	}
	code := w.finishExec(execID)
	return mobycl.ExecInspectResult{ID: execID, ExitCode: code, PID: 100}, nil
}

func rotationCluster(w *rotationWorld) *Container {
	return &Container{Container: &rotationControl{w: w}, settings: options{startupTimeout: time.Minute},
		osdHandles:  osdRegistry{owned: map[int]*OSDContainer{0: {ID: 0, Container: &rotationDaemon{w: w}}}},
		daemonExecs: daemonExecutor{docker: func(context.Context) (keyRotationExecClient, error) { return rotationDocker{w}, nil }}}
}

func TestRotateDaemonKeyCompletes(t *testing.T) {
	for name, renew := range map[string]bool{"commit applies": false, "daemon commits by renewing first": true} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.renewAfterHandoff = renew
			if err := osdHandle(t, t.Context(), rotationCluster(w), 0).RotateKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRotated(t, "key-1")
		})
	}
}

// A failure before the handoff keeps both keys and whatever the files hold,
// whenever the daemon restarts; the next call finishes the rotation.
func TestRotateDaemonKeyKeepsBothKeysBeforeTheHandoff(t *testing.T) {
	for name, setup := range map[string]func(*rotationWorld, context.CancelFunc){
		"input copy failed": func(w *rotationWorld, _ context.CancelFunc) {
			w.faults["control-copy"] = rotationFault{occurrence: 1}
		},
		"second keyring write failed": func(w *rotationWorld, _ context.CancelFunc) {
			w.faults["daemon-copy"] = rotationFault{occurrence: 2}
		},
		"canceled": func(w *rotationWorld, cancel context.CancelFunc) {
			w.onControlCopy = cancel
		},

		"restart between the keyring writes, then a failure": func(w *rotationWorld, _ context.CancelFunc) {
			w.faults["control-copy"] = rotationFault{occurrence: 1}
			w.onDaemonCopy = func() {
				if w.counts["daemon-copy"] == 1 {
					w.restart()
				}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			setup(w, cancel)
			c := rotationCluster(w)
			if err := osdHandle(t, ctx, c, 0).RotateKey(ctx); err == nil || !strings.Contains(err.Error(), "accepting both keys") {
				t.Fatalf("the failed rotation returned %v", err)
			}
			w.onControlCopy, w.onDaemonCopy = nil, nil
			w.requireUsable(t)
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRotated(t, "key-1")
		})
	}
}

// A failure after a restart made the MONs commit the new key from the
// daemon's files has finished the rotation.
func TestRotateDaemonKeyFinishesWhenARestartCommittedTheKey(t *testing.T) {
	w := newRotationWorld()
	w.faults["control-copy"] = rotationFault{occurrence: 1}
	w.onControlCopy = w.restart
	c := rotationCluster(w)
	if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
		t.Fatalf("a rotation that a restart finished returned %v", err)
	}
	w.requireRotated(t, "key-1")
}

func TestRotateDaemonKeyResumesAfterUnconfirmedHandoffOrCommit(t *testing.T) {
	for name, fault := range map[string]struct {
		op     string
		effect faultEffect
	}{
		"handoff never reached the daemon":  {"tell", failsBefore},
		"handoff reply lost":                {"tell", failsAfter},
		"handoff still on its way":          {"tell", hangsPrepared},
		"handoff stopped before it arrived": {"tell", hangsQueued},
		"commit failed":                     {"commit", failsBefore},
		"commit stopped before it applied":  {"commit", hangsQueued},
		"commit stopped before it was sent": {"commit", hangsUnsent},
		"commit answered without applying":  {"commit", answersWithoutApplying},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults[fault.op] = rotationFault{occurrence: 1, effect: fault.effect}
			c := rotationCluster(w)
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "RotateKey again") {
				t.Fatalf("failed rotation returned %v", err)
			}
			w.requireUsable(t)
			if w.pending != "key-1" || w.current != "key-0" {
				t.Fatalf("an uncertain rotation changed the accepted keys: current %s, pending %q", w.current, w.pending)
			}
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRotated(t, "key-1")
		})
	}
}

// A resumed call never clears the pending key, which the daemon may hold.
func TestRotateDaemonKeyResumedFailureKeepsPendingKey(t *testing.T) {
	w := newRotationWorld()
	w.faults["tell"] = rotationFault{occurrence: 1, effect: failsAfter}
	c := rotationCluster(w)
	_ = osdHandle(t, t.Context(), c, 0).RotateKey(t.Context())
	w.faults["control-copy"] = rotationFault{occurrence: 2}
	if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil {
		t.Fatal("the failed resumed rotation succeeded")
	}
	w.requireUsable(t)
	if w.pending != "key-1" {
		t.Fatalf("a resumed rotation cleared the pending key: current %s, pending %q", w.current, w.pending)
	}
}

// A commit that fails but applied, at once or once the MONs are settled, is
// a completed rotation.
func TestRotateDaemonKeyReadsTheOutcomeOfAFailedCommit(t *testing.T) {
	for name, effect := range map[string]faultEffect{"reply lost": failsAfter, "applies later": hangsPrepared} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults["commit"] = rotationFault{occurrence: 1, effect: effect}
			if err := osdHandle(t, t.Context(), rotationCluster(w), 0).RotateKey(t.Context()); err != nil {
				t.Fatalf("an applied commit was reported as %v", err)
			}
			w.requireRotated(t, "key-1")
		})
	}
}

// A commit or clear that a call left behind must not apply to the pending key
// of a later rotation. In the first call the CLI may reach another MON than
// the leader, as after a lost MON session; the third call creates key-3 or
// later, every late command arrives right before its handoff, and the handoff
// then fails without reaching the daemon.
func TestRotateDaemonKeyLeavesNoMonitorCommandBehind(t *testing.T) {
	for name, test := range map[string]struct {
		op     string
		effect faultEffect
		// route is the MON the first call's CLI reaches.
		route string
		// settle breaks the first call's settling, and canceled cancels the
		// first call while its command hangs. cancelFails counts the
		// cancellations that fail.
		settle      func(*rotationWorld)
		canceled    bool
		cancelFails int
	}{
		"prepared commit, settled at once":                {"commit", hangsPrepared, "a", nil, false, 0},
		"prepared commit through a peon, settled at once": {"commit", hangsPrepared, "b", nil, false, 0},
		"prepared commit, barrier failed":                 {"commit", hangsPrepared, "b", failBarrier(2), false, 0},
		"prepared commit, barrier strayed to the leader":  {"commit", hangsPrepared, "c", strayBarrier("a"), false, 0},
		"prepared commit, election while settling":        {"commit", hangsPrepared, "b", electionWhileSettling, false, 0},
		"queued commit of a canceled call":                {"commit", hangsQueued, "b", nil, true, 0},
		"prepared commit of a canceled call":              {"commit", hangsPrepared, "b", nil, true, 0},
		"unsent commit whose CLI could not be stopped":    {"commit", hangsUnsent, "b", nil, true, 1},
		"queued commit whose CLI could not be stopped":    {"commit", hangsQueued, "c", nil, true, 2},
		"commit in flight":                                {"commit", hangsInFlight, "b", nil, false, 0},
		"commit in flight of a canceled call":             {"commit", hangsInFlight, "a", nil, true, 0},
		"unsent create whose CLI could not be stopped":    {"create", hangsUnsent, "b", nil, true, 1},
		"create in flight":                                {"create", hangsInFlight, "b", nil, false, 0},
		"answered commit with a copy in flight":           {"commit", duplicatedInFlight, "b", nil, false, 0},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults[test.op] = rotationFault{occurrence: 1, effect: test.effect}
			if test.settle != nil {
				test.settle(w)
			}
			c := rotationCluster(w)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				w.onHang = cancel
			}
			w.cancelFails = test.cancelFails
			w.route = test.route
			_ = osdHandle(t, ctx, c, 0).RotateKey(ctx)
			w.onHang, w.route, w.barrierAlsoReaches, w.electionAfterBarrier = nil, "a", "", false
			if w.nextRoute != "" {
				w.route = w.nextRoute
			}
			w.requireUsable(t)
			// The daemon renews its tickets, and the MONs commit a pending key
			// that it holds, so the next call creates a new key. Every late
			// command arrives right before that key's handoff, which then fails
			// without reaching the daemon.
			w.authenticate(w.live)
			w.releaseBefore["tell"] = true
			w.faults["tell"] = rotationFault{occurrence: w.counts["tell"] + 1}
			var err error
			for attempt := 0; attempt < 4; attempt++ {
				// A fence that cannot be lifted yet refuses without a change.
				if err = osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "cannot be") {
					break
				}
				w.requireUsable(t)
			}
			if err == nil || !strings.Contains(err.Error(), "hand the new key") {
				t.Fatalf("the failed handoff returned %v", err)
			}
			w.requireUsable(t)
			if w.pending == "" || w.live != w.current || len(w.late) != 0 {
				t.Fatalf("a late command changed the keys: current %s, pending %q, live %s, late %v", w.current, w.pending, w.live, w.late)
			}
		})
	}
}

// A signal that kills the supervisor of a MON command skips its cleanup, so
// the CLI may still send the command. The call fences it like a CLI that it
// could not stop, and the next call stops it before it goes on.
func TestRotateDaemonKeyFencesAKilledSupervisor(t *testing.T) {
	for name, effect := range map[string]faultEffect{"queued commit": hangsQueued, "unsent commit": hangsUnsent, "prepared commit": hangsPrepared} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults["commit"] = rotationFault{occurrence: 1, effect: effect}
			w.killsSupervisor = true
			c := rotationCluster(w)
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil {
				t.Fatal("a call whose supervisor was killed succeeded")
			}
			w.killsSupervisor = false
			w.requireUsable(t)
			w.authenticate(w.live)
			w.releaseBefore["tell"] = true
			w.faults["tell"] = rotationFault{occurrence: w.counts["tell"] + 1}
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "hand the new key") {
				t.Fatalf("the failed handoff returned %v", err)
			}
			w.requireUsable(t)
			if w.pending == "" || w.live != w.current || len(w.late) != 0 {
				t.Fatalf("a late command changed the keys: current %s, pending %q, live %s, late %v", w.current, w.pending, w.live, w.late)
			}
		})
	}
}

// failBarrier makes the barrier through every MON fail from its occurrence on
// within the first call.
func failBarrier(occurrence int) func(*rotationWorld) {
	return func(w *rotationWorld) { w.faults["barrier"] = rotationFault{occurrence: occurrence} }
}

// strayBarrier makes every barrier CLI of the first call lose its MON and go
// on through mon.
func strayBarrier(mon string) func(*rotationWorld) {
	return func(w *rotationWorld) { w.barrierAlsoReaches = mon }
}

// electionWhileSettling starts an election after the first barrier, and the
// next call's CLI reaches another MON than the one through which a forwarded
// command went on.
func electionWhileSettling(w *rotationWorld) { w.electionAfterBarrier, w.nextRoute = true, "b" }

// After a handoff that may still reach the daemon, no other key replaces the
// one it carries until the daemon restarts, also after the rotation finished.
func TestRotateDaemonKeyWaitsForARestartAfterAnUncertainHandoff(t *testing.T) {
	for name, effect := range map[string]faultEffect{
		"handoff on its way":  hangsPrepared,
		"handoff reply lost":  failsAfter,
		"handoff not arrived": failsBefore,
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			w.faults["tell"] = rotationFault{occurrence: 1, effect: effect}
			c := rotationCluster(w)
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil {
				t.Fatal("the uncertain handoff succeeded")
			}
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRotated(t, "key-1")
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "until its container restarts") || w.generated != 1 {
				t.Fatalf("a new key after an uncertain handoff: %v, %d keys generated", err, w.generated)
			}
			w.requireRotated(t, "key-1")
			w.restart()
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRotated(t, "key-2")
		})
	}
}

// The handoff fence stays while it cannot be lifted safely.
func TestRotateDaemonKeyKeepsFencesItCannotLift(t *testing.T) {
	uncertainHandoff := func(w *rotationWorld) *Container {
		w.faults["tell"] = rotationFault{occurrence: 1, effect: hangsPrepared}
		c := rotationCluster(w)
		_ = osdHandle(t, t.Context(), c, 0).RotateKey(t.Context())
		return c
	}
	t.Run("unknown start time", func(t *testing.T) {
		w := newRotationWorld()
		w.started = ""
		c := uncertainHandoff(w)
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		// The start time becomes known without a restart: the process that
		// runs now may still get the handoff.
		w.started = "start-1"
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || w.generated != 1 {
			t.Fatalf("a daemon whose start time was unknown got a new key: %v", err)
		}
		w.requireUsable(t)
		w.restart()
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		w.requireRotated(t, "key-2")
	})
	t.Run("restart during the call", func(t *testing.T) {
		// The handoff may reach the process that started during the call.
		w := newRotationWorld()
		w.onControlCopy = w.restart
		c := uncertainHandoff(w)
		w.onControlCopy = nil
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || w.generated != 1 {
			t.Fatalf("a new key while the handoff may reach the restarted daemon: %v", err)
		}
		w.requireUsable(t)
	})
	t.Run("replaced pending key", func(t *testing.T) {
		w := newRotationWorld()
		c := uncertainHandoff(w)
		w.pending = "key-other"
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || w.pending != "key-other" || w.current != "key-0" {
			t.Fatalf("a rotation resumed with another pending key: %v, current %s, pending %q", err, w.current, w.pending)
		}
	})
	t.Run("recreated container", func(t *testing.T) {
		w := newRotationWorld()
		c := uncertainHandoff(w)
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		w.daemonID = "osd-0-ctr-2"
		w.restart()
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		w.requireRotated(t, "key-2")
	})
	t.Run("handoff CLI not stopped", func(t *testing.T) {
		// The CLI may deliver the handoff to whichever process runs until it
		// is stopped, a restarted one included.
		w := newRotationWorld()
		w.faults["tell"] = rotationFault{occurrence: 1, effect: hangsQueued}
		c := rotationCluster(w)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		w.onHang, w.cancelFails = cancel, 2
		_ = osdHandle(t, ctx, c, 0).RotateKey(ctx)
		w.onHang = nil
		w.restart()
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "cannot be stopped") {
			t.Fatalf("a call went on while the handoff CLI may run: %v", err)
		}
		w.requireUsable(t)
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil || w.generated != 1 {
			t.Fatalf("a new key right after the handoff CLI stopped: %v", err)
		}
		w.requireUsable(t)
		w.restart()
		if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		w.requireRotated(t, "key-2")
	})
}

// A keyring write whose Docker exec may still run keeps the pending key, and
// the next call waits for the exec before it continues.
func TestRotateDaemonKeyWaitsForKeyringWrites(t *testing.T) {
	for name, setup := range map[string]func(*rotationWorld){
		"start request lost": func(w *rotationWorld) {
			w.faults["exec-start"] = rotationFault{occurrence: 1}
			w.afterExecStart = func() {
				if w.counts["exec-start"] >= 1 {
					w.dockerDown = true
				}
			}
		},
		"end not confirmed": func(w *rotationWorld) {
			w.faults["exec-start"] = rotationFault{occurrence: 2, effect: runsOn}
			w.afterExecStart = func() {
				if w.counts["exec-start"] >= 2 {
					w.dockerDown = true
				}
			}
		},
		"start lost while paused": func(w *rotationWorld) {
			// The second write's start request is lost, and the container is
			// paused before it is retried, until the call gives up.
			w.cleanupTimeout = 200 * time.Millisecond
			w.faults["exec-start"] = rotationFault{occurrence: 2}
			starts := 0
			w.afterExecStart = func() {
				starts++
				w.daemonPaused = w.daemonPaused || starts == 2
			}
		},
		"late staged copy": func(w *rotationWorld) {
			w.faults["daemon-copy"] = rotationFault{occurrence: 2, effect: hangsPrepared}
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newRotationWorld()
			setup(w)
			if w.cleanupTimeout != 0 {
				previous := keyRotationCleanupTimeout
				keyRotationCleanupTimeout = w.cleanupTimeout
				t.Cleanup(func() { keyRotationCleanupTimeout = previous })
			}
			c := rotationCluster(w)
			if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err == nil {
				t.Fatal("the failed write succeeded")
			}
			w.requireUsable(t)
			w.afterExecStart, w.dockerDown, w.daemonPaused = nil, false, false
			// The second call finishes with the same key, and a third one
			// moves on to another key, which no earlier write may replace.
			for range 2 {
				if err := osdHandle(t, t.Context(), c, 0).RotateKey(t.Context()); err != nil {
					t.Fatal(err)
				}
				w.requireUsable(t)
			}
			w.releaseAll()
			w.requireUsable(t)
			if w.current != w.live || w.pending != "" || w.generated != 2 {
				t.Fatalf("rotation incomplete: current %s, pending %q, live %s", w.current, w.pending, w.live)
			}
		})
	}
}
