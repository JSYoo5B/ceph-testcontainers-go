//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// binaryCall is one command a python-rados client sent with its input
// buffer: the channel it went over (osd, mgr-tell and mon-tell are MCommand
// to one named daemon, mgr is MMgrCommand on the client's session with the
// active MGR, mon is MMonCommand on its MON session), the signed result, and
// the length and SHA-256 of the input and output buffers.
type binaryCall struct {
	Label     string          `json:"label"`
	Channel   string          `json:"channel"`
	Target    string          `json:"target"`
	Prefix    string          `json:"prefix"`
	Ret       int             `json:"ret"`
	Status    string          `json:"status"`
	InBytes   int             `json:"in_bytes"`
	InSHA256  string          `json:"in_sha256"`
	Bytes     int             `json:"bytes"`
	SHA256    string          `json:"sha256"`
	JSON      json.RawMessage `json:"json,omitempty"`
	ReplyFrom string          `json:"reply_from,omitempty"`
}

// commandWireLine is one command message or reply as the receiving side's
// messenger logs it at debug_ms 1 when it dispatches the message: the
// sender's entity, the message, and the lengths of its front, middle and
// data segments. The data segment carries a command's input buffer and a
// reply's output buffer.
type commandWireLine struct {
	From   string `json:"from"`
	Addr   string `json:"addr"`
	To     string `json:"to,omitempty"`
	Kind   string `json:"kind"`
	Tid    int    `json:"tid,omitempty"`
	Ret    int    `json:"ret"`
	Prefix string `json:"prefix,omitempty"`
	Front  int    `json:"front"`
	Middle int    `json:"middle"`
	Data   int    `json:"data"`
	Mode   string `json:"mode"`
}

var (
	dispatchLine   = regexp.MustCompile(`(?s)(?:-- (\S+) )?<== (\S+) (\S+) \d+ ==== (.*) ==== (\d+)\+(\d+)\+(\d+) \((\w+) `)
	commandMessage = regexp.MustCompile(`^command\(tid (\d+): `)
	commandReply   = regexp.MustCompile(`^command_reply\(tid (\d+): (-?\d+) `)
	mgrReply       = regexp.MustCompile(`^mgr_command_reply\(tid (\d+): (-?\d+) `)
	monAckResult   = regexp.MustCompile(`\]=(-?\d+) `)
	monPrefix      = regexp.MustCompile(`prefix(?:": "|=)([^",}\]]+)`)
)

// parseCommandWireLines picks the MCommand, MCommandReply,
// MMgrCommandReply, MMonCommand and MMonCommandAck dispatch lines out of a
// debug_ms 1 log.
func parseCommandWireLines(lines []string) []commandWireLine {
	var result []commandWireLine
	for _, line := range lines {
		m := dispatchLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		front, _ := strconv.Atoi(m[5])
		middle, _ := strconv.Atoi(m[6])
		data, _ := strconv.Atoi(m[7])
		wire := commandWireLine{To: plainAddr(m[1]), From: m[2], Addr: plainAddr(m[3]), Front: front, Middle: middle, Data: data, Mode: m[8]}
		message := m[4]
		switch {
		case strings.HasPrefix(message, "command("):
			c := commandMessage.FindStringSubmatch(message)
			if c == nil {
				continue
			}
			wire.Kind = "command"
			wire.Tid, _ = strconv.Atoi(c[1])
		case strings.HasPrefix(message, "command_reply("):
			c := commandReply.FindStringSubmatch(message)
			if c == nil {
				continue
			}
			wire.Kind = "command_reply"
			wire.Tid, _ = strconv.Atoi(c[1])
			wire.Ret, _ = strconv.Atoi(c[2])
		case strings.HasPrefix(message, "mgr_command_reply("):
			c := mgrReply.FindStringSubmatch(message)
			if c == nil {
				continue
			}
			wire.Kind = "mgr_command_reply"
			wire.Tid, _ = strconv.Atoi(c[1])
			wire.Ret, _ = strconv.Atoi(c[2])
		case strings.HasPrefix(message, "mon_command_ack("):
			r := monAckResult.FindStringSubmatch(message)
			if r == nil {
				continue
			}
			wire.Kind = "mon_command_ack"
			wire.Ret, _ = strconv.Atoi(r[1])
		case strings.HasPrefix(message, "mon_command("):
			wire.Kind = "mon_command"
		default:
			continue
		}
		if p := monPrefix.FindStringSubmatch(message); p != nil {
			wire.Prefix = p[1]
		}
		result = append(result, wire)
	}
	return result
}

// plainAddr drops the type prefix of an address, which a log prints for
// some address types and not for others.
func plainAddr(addr string) string {
	for _, prefix := range []string{"v1:", "v2:", "any:"} {
		if rest, ok := strings.CutPrefix(addr, prefix); ok {
			return rest
		}
	}
	return addr
}

// clientAddr is the address of the client whose log lines are given. A
// daemon logs a message on a client's MON session as from "client.?", so
// the address with its nonce, not the entity, names the client.
func clientAddr(t *testing.T, lines []string) string {
	t.Helper()
	for _, line := range parseCommandWireLines(lines) {
		if line.To != "" {
			return line.To
		}
	}
	t.Fatal("the client log names no local address")
	return ""
}

// entityType is the type part of an entity name such as osd.0 or mgr.4123.
func entityType(entity string) string {
	kind, _, _ := strings.Cut(entity, ".")
	return kind
}

// replyKinds maps a call's channel to the daemon type that answers it and
// the reply message.
var replyKinds = map[string][2]string{
	"osd":      {"osd", "command_reply"},
	"mgr":      {"mgr", "mgr_command_reply"},
	"mgr-tell": {"mgr", "command_reply"},
	"mon":      {"mon", "mon_command_ack"},
	"mon-tell": {"mon", "command_reply"},
}

// bindWireReplies pairs each call with the reply the client's messenger
// received for it: per channel the replies arrive in the order the client
// sent the calls, each from one daemon, with the call's result and a data
// segment exactly as long as the output buffer librados returned. It sets
// each call's ReplyFrom.
func bindWireReplies(calls []binaryCall, lines []commandWireLine) error {
	for channel, kind := range replyKinds {
		var replies []commandWireLine
		for _, line := range lines {
			if entityType(line.From) == kind[0] && line.Kind == kind[1] {
				replies = append(replies, line)
			}
		}
		next := 0
		for i := range calls {
			call := &calls[i]
			if call.Channel != channel {
				continue
			}
			if next >= len(replies) {
				return fmt.Errorf("%s: no %s from a %s on the wire", call.Label, kind[1], kind[0])
			}
			reply := replies[next]
			next++
			if reply.Ret != call.Ret || reply.Data != call.Bytes || reply.Middle != 0 {
				return fmt.Errorf("%s: librados returned %d with %d bytes, the wire reply %+v", call.Label, call.Ret, call.Bytes, reply)
			}
			if channel == "mon" && reply.Prefix != call.Prefix {
				return fmt.Errorf("%s: the reply is for %q, not %q", call.Label, reply.Prefix, call.Prefix)
			}
			call.ReplyFrom = reply.From
		}
		if next != len(replies) {
			return fmt.Errorf("%d %s replies from %s daemons, %d calls", len(replies), kind[1], kind[0], next)
		}
	}
	return nil
}

// bindWireRequests checks that a daemon's messenger received each OSD and
// MON session call whose input buffer is not empty from the client at the
// address from, with a data segment as long as the input; each line answers for one call. A MON call is matched
// by its prefix, an OSD call by its tid when tids has one for its label.
func bindWireRequests(from string, calls []binaryCall, lines []commandWireLine, tids map[string]int) error {
	used := make([]bool, len(lines))
	for _, call := range calls {
		if call.InBytes == 0 || call.Channel != "mon" && call.Channel != "osd" {
			continue
		}
		found := false
		for i, line := range lines {
			if used[i] || line.Addr != from || line.Data != call.InBytes || line.Middle != 0 {
				continue
			}
			switch call.Channel {
			case "mon":
				found = line.Kind == "mon_command" && line.Prefix == call.Prefix
			case "osd":
				tid, ok := tids[call.Label]
				found = line.Kind == "command" && (!ok || line.Tid == tid)
			}
			if found {
				used[i] = true
				break
			}
		}
		if !found {
			var seen []commandWireLine
			for _, line := range lines {
				if line.Addr == from {
					seen = append(seen, line)
				}
			}
			return fmt.Errorf("%s: no %s request from %s with %d data bytes; its requests: %+v", call.Label, call.Channel, from, call.InBytes, seen)
		}
	}
	return nil
}

// cancelledTid reads the client log of the cancellation probe: the tid that
// the client's timeout cancelled, the reply that the OSD sent for it after
// the cancellation, and the Objecter dropping that reply as unknown, in
// this order.
func cancelledTid(lines []string) (int, commandWireLine, error) {
	cancelLine := regexp.MustCompile(`command_op_cancel tid (\d+)`)
	tid, at := -1, -1
	for i, line := range lines {
		if m := cancelLine.FindStringSubmatch(line); m != nil {
			if tid >= 0 {
				return 0, commandWireLine{}, fmt.Errorf("more than one cancellation: %q", line)
			}
			tid, _ = strconv.Atoi(m[1])
			at = i
		}
	}
	if tid < 0 {
		return 0, commandWireLine{}, fmt.Errorf("no command_op_cancel line")
	}
	reply, replyAt := commandWireLine{}, -1
	for i := at + 1; i < len(lines); i++ {
		parsed := parseCommandWireLines(lines[i : i+1])
		if len(parsed) == 1 && parsed[0].Kind == "command_reply" && parsed[0].Tid == tid {
			reply, replyAt = parsed[0], i
			break
		}
	}
	if replyAt < 0 {
		return tid, reply, fmt.Errorf("no late reply for cancelled tid %d", tid)
	}
	dropped := fmt.Sprintf("handle_command_reply tid %d not found", tid)
	if !slices.ContainsFunc(lines[replyAt+1:], func(line string) bool { return strings.Contains(line, dropped) }) {
		return tid, reply, fmt.Errorf("the late reply for tid %d was not dropped as unknown", tid)
	}
	return tid, reply, nil
}

// targetedJSON runs a ceph command with JSON output and decodes it.
func targetedJSON[T any](t *testing.T, ctx context.Context, cluster *ceph.Container, args ...string) T {
	t.Helper()
	var value T
	out, err := cluster.Ceph(ctx, append(args, "--format", "json")...)
	if err == nil {
		err = json.Unmarshal(out, &value)
	}
	if err != nil {
		t.Fatalf("ceph %v: %v: %.500s", args, err, out)
	}
	return value
}

// targetedDebugMS raises debug_ms to 1 on the named daemons at runtime, so
// that their messengers log what they receive, and returns a function that
// sets each back to its value before and checks it.
func targetedDebugMS(t *testing.T, ctx context.Context, cluster *ceph.Container, daemons ...string) func() {
	t.Helper()
	before := map[string]string{}
	for _, daemon := range daemons {
		value := targetedJSON[map[string]string](t, ctx, cluster, "tell", daemon, "config", "get", "debug_ms")["debug_ms"]
		if value == "" {
			t.Fatalf("%s reports no debug_ms", daemon)
		}
		before[daemon] = value
		mustCeph(t, ctx, cluster, "tell", daemon, "config", "set", "debug_ms", "1")
	}
	return func() {
		t.Helper()
		for _, daemon := range daemons {
			mustCeph(t, ctx, cluster, "tell", daemon, "config", "set", "debug_ms", before[daemon])
			if got := targetedJSON[map[string]string](t, ctx, cluster, "tell", daemon, "config", "get", "debug_ms")["debug_ms"]; got != before[daemon] {
				t.Fatalf("%s debug_ms is %q after the restore, was %q", daemon, got, before[daemon])
			}
		}
	}
}

// daemonWireLines parses the command dispatch lines of the given daemon
// containers' logs.
func daemonWireLines(t *testing.T, ctx context.Context, containers ...testcontainers.Container) []commandWireLine {
	t.Helper()
	var lines []commandWireLine
	for _, ctr := range containers {
		logs, err := ctr.Logs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(logs, 256<<20))
		_ = logs.Close()
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, parseCommandWireLines(strings.Split(string(data), "\n"))...)
	}
	return lines
}

// awaitDaemonRequests waits until the daemons' logs show every request that
// bind accepts: a daemon writes its log asynchronously, so the line may
// follow the reply.
func awaitDaemonRequests(t *testing.T, ctx context.Context, bind func([]commandWireLine) error, containers ...testcontainers.Container) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		err := bind(daemonWireLines(t, ctx, containers...))
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon side: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

// binaryProbeResult is what targetedBinaryProbe prints.
type binaryProbeResult struct {
	GID   uint64            `json:"gid"`
	Calls []binaryCall      `json:"calls"`
	Texts map[string]string `json:"texts"`
	Log   []string          `json:"log"`
}

func (r binaryProbeResult) call(t *testing.T, label string) binaryCall {
	t.Helper()
	for _, call := range r.Calls {
		if call.Label == label {
			return call
		}
	}
	t.Fatalf("no call %s", label)
	return binaryCall{}
}

// targetedBinaryProbe sends commands with binary input and output buffers
// through python-rados as client.admin, with debug_ms 1 written to
// <work>/binary.log, and prints a binaryProbeResult. Arguments: the work
// directory, the OSD id, the active MGR's name and a MON's name.
//
//   - osd: "status" to the OSD with 1 MiB holding every byte value, and
//     "config show", whose reply is the OSD's whole configuration.
//   - mgr-tell: "crash ls" to the named MGR; a MGR answers only its admin
//     socket commands to a targeted command.
//   - mgr: on the client's MGR session, the crash module's "crash post"
//     takes a crash report as input and "crash info" returns it as
//     json.dumps(indent=4, sort_keys=True), so a report in that form comes
//     back byte for byte, with the newline that the MGR appends to a
//     module's output. The report is dated 30 days back: older than the
//     RECENT_CRASH window, younger than the retention. "crash rm" removes
//     it. A report that is not UTF-8 is refused, as the MGR hands module
//     commands their input as a str.
//   - mon-tell: "config get mon_config_key_max_entry_size" and "osd
//     getcrushmap" to the named MON; a MON answers only its admin socket
//     commands to a targeted command.
//   - mon: "config-key set" of exactly the MON's entry limit of bytes, every
//     byte value included, "config-key get" of it, one byte more refused,
//     "config-key rm"; then "osd getcrushmap" and "osd getmap" decoded by
//     crushtool and osdmaptool, a crushtool-compiled map with one more rule
//     set with "osd setcrushmap", a copy with its magic broken refused, the
//     original bytes set back and sent again.
//
// If the probe fails after it changed the CRUSH map, it sets the original
// back. It shuts its client down.
const targetedBinaryProbe = `import datetime,hashlib,json,os,re,subprocess,sys,rados
work,osd,mgr,mon=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
log=work+'/binary.log'
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'60','rados_mon_op_timeout':'60','debug_ms':'1','log_to_file':'true','log_file':log,'log_to_stderr':'false','err_to_stderr':'false'})
client.connect(timeout=30)
sha=lambda b:hashlib.sha256(b).hexdigest()
calls=[]
texts={}
def call(label,channel,cmd,inbuf=b'',target=None,decode=True):
    command=json.dumps(cmd)
    if channel=='osd':
        ret,out,status=client.osd_command(target,command,inbuf)
    elif channel=='mgr':
        ret,out,status=client.mgr_command(command,inbuf)
    elif channel=='mgr-tell':
        ret,out,status=client.mgr_command(command,inbuf,target=target)
    elif channel=='mon-tell':
        ret,out,status=client.mon_command(command,inbuf,target=target)
    else:
        ret,out,status=client.mon_command(command,inbuf)
    rec={'label':label,'channel':channel,'target':'' if target is None else str(target),'prefix':cmd['prefix'],'ret':ret,'status':status[:300],'in_bytes':len(inbuf),'in_sha256':sha(inbuf),'bytes':len(out),'sha256':sha(out)}
    if decode and cmd.get('format')=='json' and ret==0:
        rec['json']=json.loads(out)
    calls.append(rec)
    return ret,out,status
def run(*argv):
    r=subprocess.run(argv,capture_output=True,timeout=120)
    if r.returncode:
        raise RuntimeError('%s: %s'%(argv,r.stderr.decode(errors='replace')))
    return r.stdout.decode()
def save(name,data):
    with open(work+'/'+name,'wb') as f:
        f.write(data)
    return work+'/'+name
def every(n):
    return (bytes(range(256))+os.urandom(n))[:n]
gid=client.get_instance_id()
original=None
try:
    call('osd-status','osd',{'prefix':'status','format':'json'},every(1<<20),osd)
    call('osd-config-show','osd',{'prefix':'config show','format':'json'},b'',osd,decode=False)

    call('mgr-tell-crash-ls','mgr-tell',{'prefix':'crash ls','format':'json'},b'',mgr)
    call('crash-ls','mgr',{'prefix':'crash ls','format':'json'})
    ts=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    crash='%s_tc-binary-io-%d'%(ts,gid)
    report=json.dumps({'crash_id':crash,'timestamp':ts,'entity_name':'client.%d'%gid,'tc_payload':os.urandom(49152).hex()},indent=4,sort_keys=True).encode()
    call('crash-post','mgr',{'prefix':'crash post'},report)
    _,info,_=call('crash-info','mgr',{'prefix':'crash info','id':crash})
    texts['crash_echo']='exact' if info==report else 'report+LF' if info==report+b'\n' else 'differs'
    call('crash-post-not-utf8','mgr',{'prefix':'crash post'},b'\xff\xfe'+report)
    call('crash-rm','mgr',{'prefix':'crash rm','id':crash})
    call('crash-info-removed','mgr',{'prefix':'crash info','id':crash})

    ret,out,_=call('mon-tell-limit','mon-tell',{'prefix':'config get','var':'mon_config_key_max_entry_size','format':'json'},b'',mon)
    call('mon-tell-getcrushmap','mon-tell',{'prefix':'osd getcrushmap'},b'',mon)
    limit=int(json.loads(out)['mon_config_key_max_entry_size'])
    key='tc/binary-io/%d'%gid
    blob=every(limit)
    call('config-key-set','mon',{'prefix':'config-key set','key':key},blob)
    call('config-key-get','mon',{'prefix':'config-key get','key':key})
    call('config-key-set-over','mon',{'prefix':'config-key set','key':key+'/over'},blob+b'\0')
    call('config-key-rm','mon',{'prefix':'config-key rm','key':key})
    call('config-key-get-removed','mon',{'prefix':'config-key get','key':key})

    _,c0,v0=call('getcrushmap','mon',{'prefix':'osd getcrushmap'})
    _,m0,_=call('getmap','mon',{'prefix':'osd getmap'})
    original=c0
    texts['crush']=run('crushtool','-d',save('crush0',c0))
    texts['osdmap']=run('osdmaptool','--print',save('osdmap0',m0))
    run('osdmaptool',work+'/osdmap0','--export-crush',work+'/exported')
    texts['exported_crush']=run('crushtool','-d',work+'/exported')
    ids=[int(i) for i in re.findall(r'^rule \S+ \{\n\s*id (\d+)$',texts['crush'],re.M)]
    texts['rule']='rule tc-binary-io {\n\tid %d\n\ttype replicated\n\tstep take default\n\tstep chooseleaf firstn 0 type osd\n\tstep emit\n}\n'%(max(ids)+1)
    save('crush1.txt',(texts['crush']+texts['rule']).encode())
    run('crushtool','-c',work+'/crush1.txt','-o',work+'/crush1')
    with open(work+'/crush1','rb') as f:
        c1=f.read()
    texts['compiled_crush']=run('crushtool','-d',work+'/crush1')
    v0=int(v0)
    call('setcrushmap','mon',{'prefix':'osd setcrushmap','prior_version':v0},c1)
    call('setcrushmap-bad-magic','mon',{'prefix':'osd setcrushmap','prior_version':v0+1},bytes([c1[0]^0xff])+c1[1:])
    _,c1back,_=call('getcrushmap-set','mon',{'prefix':'osd getcrushmap'})
    texts['crush_set']=run('crushtool','-d',save('crush1back',c1back))
    call('getmap-set','mon',{'prefix':'osd getmap'})
    call('rules-set','mon',{'prefix':'osd crush rule ls','format':'json'})
    call('setcrushmap-restore','mon',{'prefix':'osd setcrushmap','prior_version':v0+1},c0)
    call('setcrushmap-resend','mon',{'prefix':'osd setcrushmap','prior_version':v0+1},c0)
    call('getcrushmap-restored','mon',{'prefix':'osd getcrushmap'})
    call('getmap-restored','mon',{'prefix':'osd getmap'})
    call('rules-restored','mon',{'prefix':'osd crush rule ls','format':'json'})
    original=None
finally:
    if original is not None:
        ret,current,_=client.mon_command(json.dumps({'prefix':'osd getcrushmap'}),b'')
        if ret==0 and current!=original:
            client.mon_command(json.dumps({'prefix':'osd setcrushmap'}),original)
    client.shutdown()
entries=[]
with open(log,errors='replace') as f:
    for l in f:
        if re.match(r'\d{4}-\d\d-\d\dT',l) or not entries:
            entries.append(l.rstrip('\n'))
        else:
            entries[-1]+='\n'+l.rstrip('\n')
lines=[l for l in entries if ' <== ' in l and 'command' in l]
print(json.dumps({'gid':gid,'calls':calls,'texts':texts,'log':lines}))
`

// targetedCancelProbe sends "status" with a binary input buffer to one OSD
// three times through one python-rados client, whose op timeout is the
// third argument, and takes its turns from the test through FIFOs in the
// work directory: it opens the session with the first command, writes
// "ready" and waits for "go" while the test pauses the OSD; the second
// command, now unanswered, ends at the timeout; it writes "cancelled" and
// waits for "resumed" before the third command, which the OSD answers after
// the reply to the second. It logs debug_ms 1 and debug_objecter 10 to
// <work>/cancel.log, prints a binaryProbeResult with the log's command
// lines, and shuts its client down. An alarm ends it if the test does not
// take its turn.
const targetedCancelProbe = `import hashlib,json,os,re,signal,sys,rados
work,osd,timeout=sys.argv[1],int(sys.argv[2]),sys.argv[3]
def expire(*_):
    raise TimeoutError('the test did not take its turn')
signal.signal(signal.SIGALRM,expire)
signal.alarm(300)
log=work+'/cancel.log'
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'30','debug_ms':'1','debug_objecter':'10','log_to_file':'true','log_file':log,'log_to_stderr':'false','err_to_stderr':'false'})
client.connect(timeout=30)
sha=lambda b:hashlib.sha256(b).hexdigest()
calls=[]
def call(label,n):
    inbuf=(bytes(range(256))+os.urandom(n))[:n]
    ret,out,status=client.osd_command(osd,json.dumps({'prefix':'status','format':'json'}),inbuf)
    calls.append({'label':label,'channel':'osd','target':str(osd),'prefix':'status','ret':ret,'status':status[:300],'in_bytes':n,'in_sha256':sha(inbuf),'bytes':len(out),'sha256':sha(out)})
def turn(name,mode,text=''):
    with open(work+'/'+name,mode) as f:
        return f.write(text) if mode=='w' else f.read()
try:
    call('before',4096)
    turn('ready','w','ready\n')
    turn('go','r')
    call('cancelled',262144)
    turn('cancelled','w','cancelled\n')
    turn('resumed','r')
    call('after',4096)
    gid=client.get_instance_id()
finally:
    signal.alarm(0)
    client.shutdown()
entries=[]
with open(log,errors='replace') as f:
    for l in f:
        if re.match(r'\d{4}-\d\d-\d\dT',l) or not entries:
            entries.append(l.rstrip('\n'))
        else:
            entries[-1]+='\n'+l.rstrip('\n')
lines=[l for l in entries if ' <== ' in l and 'command' in l or 'command_op_cancel' in l or 'handle_command_reply' in l]
print(json.dumps({'gid':gid,'calls':calls,'texts':{},'log':lines}))
`

// runBinaryProbe runs a probe and decodes its binaryProbeResult.
func runBinaryProbe(ctx context.Context, ctr testcontainers.Container, args ...string) (binaryProbeResult, error) {
	execCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, append([]string{"python3", "-c"}, args...), tcexec.Multiplexed())
	if err != nil {
		return binaryProbeResult{}, err
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return binaryProbeResult{}, err
	}
	if code != 0 {
		return binaryProbeResult{}, fmt.Errorf("probe exited %d: %.4000s", code, out)
	}
	// The result is the last line; a warning on standard error may precede it.
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var result binaryProbeResult
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		return result, fmt.Errorf("probe output: %w: %.4000s", err, out)
	}
	return result, nil
}

// targetedBinaryIO runs targetedBinaryProbe against osd.0, the active MGR
// and the rank 0 MON, checks every reply against the client's and the
// daemons' messenger logs and the native tools, and returns the receipt.
// The caller raised debug_ms on osd.0 and every MON.
func targetedBinaryIO(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid string, mons map[int]string, mgrGID uint64, mgrName string) map[string]any {
	t.Helper()
	control := cluster.ControlContainer()
	const work = "/tmp/tc-binary-io"
	execOutput(t, ctx, control, "sh", "-c", `rm -rf "$1" && mkdir -p "$1"`, "sh", work)
	result, err := runBinaryProbe(ctx, control, targetedBinaryProbe, work, "0", mgrName, mons[0])
	if err != nil {
		t.Fatal(err)
	}
	execOutput(t, ctx, control, "rm", "-rf", work)
	if err := bindWireReplies(result.Calls, parseCommandWireLines(result.Log)); err != nil {
		t.Fatalf("client side: %v\n%s", err, strings.Join(result.Log, "\n"))
	}
	call := func(label string, ret int) binaryCall {
		t.Helper()
		c := result.call(t, label)
		if c.Ret != ret {
			t.Fatalf("%s: %+v, want %d", label, c, ret)
		}
		return c
	}
	version := func(c binaryCall) int {
		t.Helper()
		v, err := strconv.Atoi(c.Status)
		if err != nil {
			t.Fatalf("%s: status %q is not a CRUSH version", c.Label, c.Status)
		}
		return v
	}
	epoch := func(c binaryCall) int {
		t.Helper()
		var e int
		if _, err := fmt.Sscanf(c.Status, "got osdmap epoch %d", &e); err != nil {
			t.Fatalf("%s: status %q names no epoch", c.Label, c.Status)
		}
		return e
	}

	// MCommand to osd.0: 1 MiB of input that the OSD receives and ignores.
	status := call("osd-status", 0)
	identity, err := parseOSDStatusIdentity(status.JSON)
	if err != nil || identity.WhoAmI != 0 || identity.ClusterFSID != fsid || status.ReplyFrom != "osd.0" || status.InBytes != 1<<20 {
		t.Fatalf("osd-status %+v: %+v %v", status, identity, err)
	}
	if show := call("osd-config-show", 0); show.ReplyFrom != "osd.0" || show.Bytes < 1<<16 {
		t.Fatalf("osd-config-show: %+v", show)
	}

	// The MGR: a targeted command reaches only its admin socket; the crash
	// module on the MGR session takes a report and returns its bytes.
	mgr := fmt.Sprintf("mgr.%d", mgrGID)
	if tell := call("mgr-tell-crash-ls", -22); tell.ReplyFrom != mgr {
		t.Fatalf("mgr-tell-crash-ls answered by %s, the active MGR is %s", tell.ReplyFrom, mgr)
	}
	call("crash-ls", 0)
	post := call("crash-post", 0)
	info := call("crash-info", 0)
	// The MGR ends a module's output with a newline.
	if result.Texts["crash_echo"] != "report+LF" || info.Bytes != post.InBytes+1 || post.InBytes < 98304 {
		t.Fatalf("crash info returned %d bytes (%s) for a %d byte report", info.Bytes, result.Texts["crash_echo"], post.InBytes)
	}
	call("crash-post-not-utf8", -22)
	call("crash-rm", 0)
	// The crash module's refusal of a missing report reaches the client as
	// 22, not -22, on Squid and Tentacle; only a nonzero result is checked.
	if removed := result.call(t, "crash-info-removed"); removed.Ret == 0 || !strings.Contains(removed.Status, "not found") {
		t.Fatalf("crash-info-removed: %+v", removed)
	}
	for _, c := range result.Calls {
		if c.Channel == "mgr" && c.ReplyFrom != mgr {
			t.Fatalf("%s answered by %s, the active MGR is %s", c.Label, c.ReplyFrom, mgr)
		}
	}

	// MON: a targeted command reaches only its admin socket; the MON session
	// keeps arbitrary bytes up to the entry limit.
	limit := call("mon-tell-limit", 0)
	if refused := call("mon-tell-getcrushmap", -22); refused.ReplyFrom != "mon.0" || limit.ReplyFrom != "mon.0" {
		t.Fatalf("mon.%s answered as %s and %s", mons[0], limit.ReplyFrom, refused.ReplyFrom)
	}
	set := call("config-key-set", 0)
	got := call("config-key-get", 0)
	if got.SHA256 != set.InSHA256 || got.Bytes != set.InBytes {
		t.Fatalf("config-key get returned %d bytes %s for %d bytes %s", got.Bytes, got.SHA256, set.InBytes, set.InSHA256)
	}
	if over := call("config-key-set-over", -27); over.InBytes != set.InBytes+1 {
		t.Fatalf("config-key-set-over sent %d bytes", over.InBytes)
	}
	call("config-key-rm", 0)
	call("config-key-get-removed", -2)
	session := ""
	for _, c := range result.Calls {
		if c.Channel == "mon" {
			if session == "" {
				session = c.ReplyFrom
			}
			if c.ReplyFrom != session {
				t.Fatalf("%s answered by %s, the session's MON is %s", c.Label, c.ReplyFrom, session)
			}
		}
	}
	rank, err := strconv.Atoi(strings.TrimPrefix(session, "mon."))
	if err != nil || mons[rank] == "" {
		t.Fatalf("the MON session is %q, the MonMap has %v", session, mons)
	}

	// CRUSH and OSD maps: binary output decoded by the native tools, binary
	// input set, refused and set back.
	crush0 := call("getcrushmap", 0)
	map0 := call("getmap", 0)
	v0, e0 := version(crush0), epoch(map0)
	texts := result.Texts
	if !strings.Contains(texts["crush"], "device 0 osd.0") || !strings.Contains(texts["crush"], "device 1 osd.1") {
		t.Fatalf("crushtool -d of getcrushmap:\n%s", texts["crush"])
	}
	if !strings.Contains(texts["osdmap"], fmt.Sprintf("epoch %d\n", e0)) || !strings.Contains(texts["osdmap"], "fsid "+fsid+"\n") ||
		!strings.Contains(texts["osdmap"], "osd.0 up") || !strings.Contains(texts["osdmap"], "osd.1 up") {
		t.Fatalf("osdmaptool --print of epoch %d:\n%s", e0, texts["osdmap"])
	}
	if texts["exported_crush"] != texts["crush"] {
		t.Fatalf("the CRUSH map in osd getmap differs from osd getcrushmap:\n%s\n---\n%s", texts["exported_crush"], texts["crush"])
	}
	setMap := call("setcrushmap", 0)
	if version(setMap) != v0+1 {
		t.Fatalf("setcrushmap: %+v, want version %d", setMap, v0+1)
	}
	if bad := call("setcrushmap-bad-magic", -22); !strings.Contains(bad.Status, "Failed to parse crushmap") || bad.InBytes != setMap.InBytes {
		t.Fatalf("setcrushmap-bad-magic: %+v", bad)
	}
	if v := version(call("getcrushmap-set", 0)); v != v0+1 || texts["crush_set"] != texts["compiled_crush"] {
		t.Fatalf("getcrushmap after the set: version %d, want %d; decompiled:\n%s\n---\n%s", v, v0+1, texts["crush_set"], texts["compiled_crush"])
	}
	e1 := epoch(call("getmap-set", 0))
	rules := func(label string) []string {
		t.Helper()
		var names []string
		if err := json.Unmarshal(call(label, 0).JSON, &names); err != nil {
			t.Fatal(label, err)
		}
		return names
	}
	if names := rules("rules-set"); !slices.Contains(names, "tc-binary-io") || e1 <= e0 {
		t.Fatalf("after setcrushmap: rules %v, epoch %d after %d", names, e1, e0)
	}
	if v := version(call("setcrushmap-restore", 0)); v != v0+2 {
		t.Fatalf("setcrushmap-restore: version %d, want %d", v, v0+2)
	}
	if v := version(call("setcrushmap-resend", 0)); v != v0+2 {
		t.Fatalf("setcrushmap-resend: version %d, want %d", v, v0+2)
	}
	restored := call("getcrushmap-restored", 0)
	if restored.SHA256 != crush0.SHA256 || restored.Bytes != crush0.Bytes || version(restored) != v0+2 {
		t.Fatalf("getcrushmap after the restore: %+v, the original %+v", restored, crush0)
	}
	e2 := epoch(call("getmap-restored", 0))
	if names := rules("rules-restored"); slices.Contains(names, "tc-binary-io") || e2 <= e1 {
		t.Fatalf("after the restore: rules %v, epoch %d after %d", names, e2, e1)
	}

	// The daemons' messengers received each input buffer whole.
	daemons := []testcontainers.Container{cluster.OSDs()[0]}
	for _, mon := range cluster.Monitors() {
		daemons = append(daemons, mon)
	}
	awaitDaemonRequests(t, ctx, func(lines []commandWireLine) error {
		return bindWireRequests(clientAddr(t, result.Log), result.Calls, lines, nil)
	}, daemons...)

	return map[string]any{
		"client": fmt.Sprintf("client.%d", result.GID), "calls": result.Calls,
		"osd": identity, "mgr": map[string]any{"gid": mgrGID, "name": mgrName},
		"mon_session":    map[string]any{"rank": rank, "name": mons[rank]},
		"crush_versions": []int{v0, v0 + 1, v0 + 2}, "osdmap_epochs": []int{e0, e1, e2},
		"config_key_limit": set.InBytes, "added_rule": texts["rule"],
	}
}

// targetedCancellation runs targetedCancelProbe against osd.1 and pauses
// the OSD, under nodown, for its second command. It returns the receipt:
// the three calls, the cancelled tid and the late reply that the client
// dropped. The caller raised debug_ms on osd.1.
func targetedCancellation(t *testing.T, ctx context.Context, cluster *ceph.Container) map[string]any {
	t.Helper()
	control := cluster.ControlContainer()
	osd := cluster.OSDs()[1]
	const work = "/tmp/tc-binary-cancel"
	execOutput(t, ctx, control, "sh", "-c", `rm -rf "$1" && mkdir -p "$1" && cd "$1" && mkfifo ready go cancelled resumed`, "sh", work)
	type outcome struct {
		result binaryProbeResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runBinaryProbe(ctx, control, targetedCancelProbe, work, "1", "5")
		done <- outcome{result, err}
	}()
	turn := func(script string) { execOutput(t, ctx, control, "sh", "-c", script, "sh", work) }
	turn(`cat "$1/ready"`)
	nodown, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
	if err != nil {
		t.Fatal(err)
	}
	pause, err := fault.Pause(ctx, cluster, osd)
	if pause != nil {
		defer func() { _ = pause.Resume(ctx) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	turn(`echo go > "$1/go"`)
	turn(`cat "$1/cancelled"`)
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := nodown.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	turn(`echo resumed > "$1/resumed"`)
	out := <-done
	if out.err != nil {
		t.Fatal(out.err)
	}
	result := out.result
	execOutput(t, ctx, control, "rm", "-rf", work)
	before, cancelled, after := result.call(t, "before"), result.call(t, "cancelled"), result.call(t, "after")
	if before.Ret != 0 || cancelled.Ret != -110 || after.Ret != 0 {
		t.Fatalf("calls %+v %+v %+v; want 0, -110, 0", before, cancelled, after)
	}
	tid, late, err := cancelledTid(result.Log)
	if err != nil || late.From != "osd.1" || late.Ret != 0 || late.Data == 0 {
		t.Fatalf("cancellation: tid %d, late reply %+v: %v\n%s", tid, late, err, strings.Join(result.Log, "\n"))
	}
	answered := []binaryCall{before, after}
	var replies []commandWireLine
	for _, line := range parseCommandWireLines(result.Log) {
		if line.Tid != tid {
			replies = append(replies, line)
		}
	}
	if err := bindWireReplies(answered, replies); err != nil || answered[0].ReplyFrom != "osd.1" {
		t.Fatalf("client side: %v %+v\n%s", err, answered, strings.Join(result.Log, "\n"))
	}
	awaitDaemonRequests(t, ctx, func(lines []commandWireLine) error {
		return bindWireRequests(clientAddr(t, result.Log), result.Calls, lines, map[string]int{"cancelled": tid})
	}, osd)
	return map[string]any{
		"client": fmt.Sprintf("client.%d", result.GID), "calls": []binaryCall{answered[0], cancelled, answered[1]},
		"cancelled_tid": tid, "late_reply": late,
	}
}
