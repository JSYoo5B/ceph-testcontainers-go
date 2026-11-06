//go:build all || integration

package integration_test

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The encrypted OSD backing store of issue 478. A test-owned keeper holds a
// backing file, a loop device, a LUKS2 dm-crypt mapping on the loop and, on
// top of it, the linear device-mapper node that the OSD is given through the
// block-device path. The linear node keeps its device number while the crypt
// mapping under it is closed and opened again, so the OSD container's device
// and its recorded number stay valid across a lock and an unlock. The test
// holds the key; the keeper receives it only on standard input. The judges
// below read what the keeper, the OSD container and the client report.

// encKeeperScript runs in the privileged keeper, which sees the engine host's
// /dev. It prints one JSON object per action and exits 0 unless its own
// arguments are wrong, so a refused kernel or cryptsetup step is a receipt.
// It never prints key material: a crypt table's key field is replaced before
// the table is reported, and cryptsetup's own messages name no key.
//
//	admit -                         kernel release, registered targets, cryptsetup version
//	create FILE BYTES               backing file and loop device
//	format LOOP                     LUKS2 header on the loop, key on stdin
//	open LOOP CRYPT                 open the crypt mapping, key (possibly none) on stdin
//	header LOOP                     LUKS UUID and digest of the header area
//	linear NODE CRYPT UUID          the OSD's linear node over the crypt mapping
//	retarget NODE error|CRYPT       load an error table, or a linear table over CRYPT
//	stack NODE CRYPT LOOP FILE      what each layer maps and who holds it
//	close CRYPT                     close the crypt mapping once nothing holds it
//	scan PATH MARKER                count a hex marker's occurrences in a device or file
//	remove NODE CRYPT FILE          release whatever of the chain still exists
const encKeeperScript = `import hashlib, json, os, subprocess, sys, time

def run(*argv, data=None):
    r = subprocess.run(argv, input=data, capture_output=True, timeout=120)
    return r.returncode, r.stdout.decode(errors="replace").strip(), r.stderr.decode(errors="replace").strip()[-400:]

def number(path):
    st = os.stat(path)
    return "%d:%d" % (os.major(st.st_rdev), os.minor(st.st_rdev))

def exists(name):
    return run("dmsetup", "info", name)[0] == 0

def opened(name):
    rc, out, _ = run("dmsetup", "info", "-c", "--noheadings", "-o", "open", name)
    return int(out) if rc == 0 and out.strip().isdigit() else None

def holders(num):
    try:
        return sorted(os.listdir("/sys/dev/block/%s/holders" % num))
    except OSError:
        return None

def redact(table):
    # crypt: start length crypt cipher key iv_offset device offset [options]
    f = table.split()
    if len(f) >= 5 and f[2] == "crypt":
        f[4] = "<redacted>"
    return " ".join(f)

def header(loop):
    rc, uuid, _ = run("cryptsetup", "luksUUID", loop)
    rc2, dump, _ = run("cryptsetup", "luksDump", loop)
    offset = None
    for line in dump.splitlines():
        line = line.strip()
        if line.startswith("offset:") and "[bytes]" in line and offset is None:
            offset = int(line.split()[1])
    out = {"luks_uuid": uuid if rc == 0 else None, "data_offset": offset}
    if offset:
        h = hashlib.sha256()
        with open(loop, "rb") as f:
            h.update(f.read(offset))
        out["header_sha256"] = h.hexdigest()
    return out

def key():
    return sys.stdin.buffer.read()

action, arg = sys.argv[1], sys.argv[2]
out = {"action": action}
if action == "admit":
    rc, listed, err = run("dmsetup", "targets")
    out["targets"] = [line.split()[0] for line in listed.splitlines() if line.strip()] if rc == 0 else None
    out["kernel"] = os.uname().release
    out["cryptsetup"] = run("cryptsetup", "--version")[1]
    if rc != 0:
        out["error"] = "dmsetup targets: " + err
elif action == "create":
    path, size = arg, int(sys.argv[3])
    with open(path, "xb") as f:
        f.truncate(size)
    out["file"] = path
    rc, loop, err = run("losetup", "--find", "--show", path)
    if rc != 0:
        out["error"] = "losetup: " + err
    else:
        out["loop"], out["loop_number"] = loop, number(loop)
elif action == "format":
    rc, _, err = run("cryptsetup", "luksFormat", "--batch-mode", "--type", "luks2", "--cipher", "aes-xts-plain64",
                     "--key-size", "512", "--hash", "sha256", "--pbkdf", "pbkdf2", "--pbkdf-force-iterations", "1000",
                     "--key-file", "-", arg, data=key())
    out["rc"] = rc
    if rc != 0:
        out["error"] = "luksFormat: " + err
    out.update(header(arg))
elif action == "open":
    name = sys.argv[3]
    out["existed"] = exists(name)
    rc, _, err = run("cryptsetup", "open", "--type", "luks2", "--key-file", "-", arg, name, data=key())
    out["rc"], out["message"] = rc, err
    out["mapped"] = exists(name)
    if out["mapped"]:
        node = "/dev/mapper/" + name
        out["number"] = number(node)
        out["bytes"] = int(run("blockdev", "--getsize64", node)[1] or 0)
    out.update(header(arg))
elif action == "header":
    out.update(header(arg))
elif action == "linear":
    crypt, uuid = sys.argv[3], sys.argv[4]
    lower = number("/dev/mapper/" + crypt)
    sectors = int(run("blockdev", "--getsz", "/dev/mapper/" + crypt)[1])
    rc, _, err = run("dmsetup", "create", "--verifyudev", arg, "--uuid", uuid, "--table", "0 %d linear %s 0" % (sectors, lower))
    out["rc"] = rc
    if rc != 0:
        out["error"] = "dmsetup create: " + err
    elif exists(arg):
        node = "/dev/mapper/" + arg
        out["path"], out["number"] = node, number(node)
        out["bytes"] = int(run("blockdev", "--getsize64", node)[1])
elif action == "retarget":
    target = sys.argv[3]
    rc, size, err = run("blockdev", "--getsz", "/dev/mapper/" + arg)
    sectors = int(size)
    table = "0 %d error" % sectors if target == "error" else "0 %d linear %s 0" % (sectors, number("/dev/mapper/" + target))
    steps = []
    for argv in (("dmsetup", "suspend", "--noudevsync", arg), ("dmsetup", "load", "--noudevsync", arg, "--table", table),
                 ("dmsetup", "resume", "--noudevsync", arg)):
        rc, _, err = run(*argv)
        steps.append({"step": argv[1], "rc": rc})
        if rc != 0:
            out["error"] = "dmsetup %s: %s" % (argv[1], err)
            break
    out["steps"], out["table"] = steps, run("dmsetup", "table", arg)[1]
elif action == "stack":
    crypt, loop, path = sys.argv[3], sys.argv[4], sys.argv[5]
    out["node_table"] = run("dmsetup", "table", arg)[1]
    out["node_open"] = opened(arg)
    if exists(crypt):
        num = number("/dev/mapper/" + crypt)
        out["crypt_number"], out["crypt_table"] = num, redact(run("dmsetup", "table", crypt)[1])
        out["crypt_open"], out["crypt_holders"] = opened(crypt), holders(num)
    out["loop_number"] = number(loop)
    rc, back, _ = run("losetup", "--list", "--noheadings", "--output", "BACK-FILE", loop)
    out["loop_backing"] = back.strip() if rc == 0 else None
    out["loop_holders"] = holders(out["loop_number"])
    out["file"] = path
elif action == "close":
    if not exists(arg):
        out["closed"] = True
    else:
        num = number("/dev/mapper/" + arg)
        out["open_before"], out["holders_before"] = opened(arg), holders(num)
        if out["open_before"] != 0 or out["holders_before"] != []:
            out["error"] = "the crypt mapping is still held"
        else:
            for attempt in range(10):
                rc, _, err = run("cryptsetup", "close", arg)
                if rc == 0 or not exists(arg):
                    break
                time.sleep(0.5)
            out["attempts"], out["closed"] = attempt + 1, not exists(arg)
            if not out["closed"]:
                out["error"] = "cryptsetup close: " + err
elif action == "scan":
    marker, count, tail = bytes.fromhex(sys.argv[3]), 0, b""
    with open(arg, "rb") as f:
        while True:
            chunk = f.read(8 << 20)
            if not chunk:
                break
            data = tail + chunk
            count += data.count(marker)
            # Keep less than one marker, so a match is never counted twice.
            tail = data[-(len(marker) - 1):]
    out["path"], out["count"] = arg, count
elif action == "remove":
    crypt, path = sys.argv[3], sys.argv[4]
    if exists(arg):
        if opened(arg) != 0:
            out["error"] = "the OSD's node is still open"
        else:
            rc, _, err = run("dmsetup", "remove", "--verifyudev", arg)
            if rc != 0:
                out["error"] = "dmsetup remove: " + err
    out["node_gone"] = not exists(arg)
    if out["node_gone"] and exists(crypt):
        num = number("/dev/mapper/" + crypt)
        if opened(crypt) != 0 or holders(num) != []:
            out["error"] = "the crypt mapping is still held"
        else:
            for attempt in range(10):
                if run("cryptsetup", "close", crypt)[0] == 0 or not exists(crypt):
                    break
                time.sleep(0.5)
    out["crypt_gone"] = not exists(crypt)
    # The file is unlinked only after its loops are detached, so a missing
    # file has no loop left.
    out["loop_released"] = not os.path.exists(path)
    if out["node_gone"] and out["crypt_gone"] and os.path.exists(path):
        rc, listed, _ = run("losetup", "--list", "--noheadings", "--output", "NAME", "-j", path)
        for loop in listed.split():
            rc, _, err = run("losetup", "-d", loop)
            if rc != 0:
                out["error"] = "losetup -d: " + err
        rc, listed, _ = run("losetup", "-j", path)
        out["loop_released"] = rc == 0 and listed == ""
        if out["loop_released"]:
            os.unlink(path)
    out["file_removed"] = not os.path.exists(path)
else:
    raise SystemExit("unknown action " + action)
print(json.dumps(out))`

// encWrites writes COUNT objects named PREFIX-N whose bytes are the hex
// MARKER repeated to SIZE bytes, and prints the mutation ledger. Arguments:
// pool, prefix, count, size, marker.
const encWrites = `import hashlib, json, rados, sys
pool, prefix, count, size, marker = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), bytes.fromhex(sys.argv[5])
data = (marker * (size // len(marker) + 1))[:size]
ledger = []
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(pool) as io:
        for n in range(count):
            key = "%s-%d" % (prefix, n)
            entry = {"key": key, "sha256": hashlib.sha256(data).hexdigest()}
            try:
                io.write_full(key, data)
                entry["outcome"] = "acked"
            except rados.Error as e:
                entry["outcome"] = "failed %s" % getattr(e, "errno", "")
            ledger.append(entry)
print(json.dumps(ledger))`

// encReadback reads each object and prints its digest, or why it was not
// read. Arguments: pool, keys.
const encReadback = `import hashlib, json, rados, sys
out = {}
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"client_mount_timeout": "30", "rados_mon_op_timeout": "30",
                                                       "rados_osd_op_timeout": "30"}) as c:
    with c.open_ioctx(sys.argv[1]) as io:
        for key in sys.argv[2:]:
            try:
                size = io.stat(key)[0]
                out[key] = hashlib.sha256(io.read(key, size, 0)).hexdigest()
            except rados.ObjectNotFound:
                out[key] = "absent"
            except rados.Error as e:
                out[key] = "error %s" % getattr(e, "errno", "")
print(json.dumps(out))`

// encReply is one action's receipt from encKeeperScript.
type encReply struct {
	Action       string   `json:"action"`
	Error        string   `json:"error,omitempty"`
	Targets      []string `json:"targets,omitempty"`
	Kernel       string   `json:"kernel,omitempty"`
	Cryptsetup   string   `json:"cryptsetup,omitempty"`
	File         string   `json:"file,omitempty"`
	Loop         string   `json:"loop,omitempty"`
	LoopNumber   string   `json:"loop_number,omitempty"`
	RC           *int     `json:"rc,omitempty"`
	Message      string   `json:"message,omitempty"`
	Existed      bool     `json:"existed,omitempty"`
	Mapped       bool     `json:"mapped,omitempty"`
	Path         string   `json:"path,omitempty"`
	Number       string   `json:"number,omitempty"`
	Bytes        int64    `json:"bytes,omitempty"`
	LUKSUUID     *string  `json:"luks_uuid,omitempty"`
	DataOffset   *int64   `json:"data_offset,omitempty"`
	HeaderSHA256 string   `json:"header_sha256,omitempty"`
	Table        string   `json:"table,omitempty"`
	NodeTable    string   `json:"node_table,omitempty"`
	NodeOpen     *int     `json:"node_open,omitempty"`
	CryptNumber  string   `json:"crypt_number,omitempty"`
	CryptTable   string   `json:"crypt_table,omitempty"`
	CryptOpen    *int     `json:"crypt_open,omitempty"`
	CryptHolders []string `json:"crypt_holders,omitempty"`
	LoopBacking  *string  `json:"loop_backing,omitempty"`
	LoopHolders  []string `json:"loop_holders,omitempty"`
	Closed       bool     `json:"closed,omitempty"`
	Attempts     int      `json:"attempts,omitempty"`
	Count        *int     `json:"count,omitempty"`
	NodeGone     bool     `json:"node_gone,omitempty"`
	CryptGone    bool     `json:"crypt_gone,omitempty"`
	LoopReleased bool     `json:"loop_released,omitempty"`
	FileRemoved  bool     `json:"file_removed,omitempty"`
}

func parseEncReply(raw []byte, action string) (encReply, error) {
	var r encReply
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, fmt.Errorf("keeper %s reply: %w", action, err)
	}
	if r.Action != action {
		return r, fmt.Errorf("keeper reply is for %q, want %q", r.Action, action)
	}
	return r, nil
}

// encHeader is the LUKS identity of the backing loop at one moment.
type encHeader struct {
	UUID, SHA256 string
	Offset       int64
}

func encHeaderOf(r encReply) encHeader {
	h := encHeader{SHA256: r.HeaderSHA256}
	if r.LUKSUUID != nil {
		h.UUID = *r.LUKSUUID
	}
	if r.DataOffset != nil {
		h.Offset = *r.DataOffset
	}
	return h
}

func (h encHeader) known() bool { return h.UUID != "" && h.SHA256 != "" && h.Offset > 0 }

// Unlock refusal verdicts.
const (
	encRefused       = "refused"        // cryptsetup failed, no mapping exists, the header is unchanged
	encOpened        = "opened"         // a mapping exists after the attempt
	encAccepted      = "accepted"       // cryptsetup reported success
	encHeaderChanged = "header-changed" // the LUKS header area differs from the recorded one
	encUnobserved    = "unobserved"     // the header could not be read back
)

// judgeEncRefusal decides whether an unlock attempt with a missing or wrong
// key was refused without changing the store: cryptsetup must fail, no
// mapping of that name may exist afterwards (nor before, or the absence
// would mean nothing), and the header area, keyslots included, must read
// back byte for byte. A changed header is a reformat or a keyslot rewrite.
func judgeEncRefusal(r encReply, recorded encHeader) (string, string) {
	after := encHeaderOf(r)
	switch {
	case r.Existed || r.Mapped:
		return encOpened, fmt.Sprintf("existed=%t mapped=%t", r.Existed, r.Mapped)
	case r.RC == nil:
		return encUnobserved, "no exit status"
	case *r.RC == 0:
		return encAccepted, "cryptsetup open exited 0"
	case !recorded.known() || !after.known():
		return encUnobserved, fmt.Sprintf("header recorded=%+v after=%+v", recorded, after)
	case after != recorded:
		return encHeaderChanged, fmt.Sprintf("header %+v, recorded %+v", after, recorded)
	}
	return encRefused, fmt.Sprintf("exit %d: %s", *r.RC, r.Message)
}

// Stack verdicts.
const (
	encStacked    = "stacked"     // node -> crypt -> loop -> owned file, each held only by the layer above
	encNotStacked = "not-stacked" // anything else; the detail names the first mismatch
)

// judgeEncStack decides whether the OSD's node maps the owned crypt mapping,
// and the crypt mapping the owned loop and its file. A held layer must be
// held exactly by the one above it (holders name dm-<minor> nodes).
func judgeEncStack(r encReply, nodeNumber string) (string, string) {
	node := strings.Fields(r.NodeTable)
	crypt := strings.Fields(r.CryptTable)
	switch {
	case len(node) != 5 || node[2] != "linear" || node[3] != r.CryptNumber || node[4] != "0":
		return encNotStacked, fmt.Sprintf("node table %q does not map crypt %s", r.NodeTable, r.CryptNumber)
	case len(crypt) < 8 || crypt[2] != "crypt" || crypt[3] != "aes-xts-plain64" || crypt[4] != "<redacted>" || crypt[6] != r.LoopNumber:
		return encNotStacked, fmt.Sprintf("crypt table %q does not map loop %s", r.CryptTable, r.LoopNumber)
	case r.LoopBacking == nil || *r.LoopBacking != r.File:
		return encNotStacked, fmt.Sprintf("loop backs %v, want %s", r.LoopBacking, r.File)
	case r.CryptOpen == nil || *r.CryptOpen != 1 || len(r.CryptHolders) != 1 || "dm-"+minorOf(nodeNumber) != r.CryptHolders[0]:
		return encNotStacked, fmt.Sprintf("crypt open=%v holders=%v, want only dm-%s", r.CryptOpen, r.CryptHolders, minorOf(nodeNumber))
	case len(r.LoopHolders) != 1 || "dm-"+minorOf(r.CryptNumber) != r.LoopHolders[0]:
		return encNotStacked, fmt.Sprintf("loop holders %v, want only dm-%s", r.LoopHolders, minorOf(r.CryptNumber))
	}
	return encStacked, fmt.Sprintf("%s -> %s -> %s -> %s", nodeNumber, r.CryptNumber, r.LoopNumber, r.File)
}

func minorOf(number string) string {
	_, minor, _ := strings.Cut(number, ":")
	return minor
}

// Ciphertext verdicts.
const (
	encCiphertext      = "ciphertext"    // the marker is in the decrypted view and absent from the backing file
	encPlaintextBehind = "plaintext"     // the marker reached the backing file
	encMarkerUnseen    = "marker-unseen" // the decrypted view has no marker, so its absence below proves nothing
	encScanUnavailable = "scan-unavailable"
)

func judgeEncCiphertext(decrypted, backing encReply) string {
	switch {
	case decrypted.Count == nil || backing.Count == nil:
		return encScanUnavailable
	case *backing.Count > 0:
		return encPlaintextBehind
	case *decrypted.Count == 0:
		return encMarkerUnseen
	}
	return encCiphertext
}

// encOSDIdentity is what names the OSD and its store at one moment.
type encOSDIdentity struct {
	ID         int    `json:"id"`
	UUID       string `json:"uuid"`       // the OSDMap's uuid
	StoreFSID  string `json:"store_fsid"` // the store's fsid in the OSD data directory
	Number     string `json:"number"`     // the device number the daemon holds read-write
	Attachment string `json:"attachment"`
}

// judgeEncIdentity requires the reopened OSD to be the original one on the
// original store and device number.
func judgeEncIdentity(before, after encOSDIdentity) (bool, string) {
	switch {
	case before.UUID == "" || before.StoreFSID != before.UUID || before.Attachment != blockAttached:
		return false, fmt.Sprintf("original identity not established: %+v", before)
	case after.Attachment != blockAttached:
		return false, fmt.Sprintf("the reopened daemon is %s", after.Attachment)
	case after != before:
		return false, fmt.Sprintf("identity %+v, originally %+v", after, before)
	}
	return true, fmt.Sprintf("osd.%d %s on %s", after.ID, after.UUID, after.Number)
}

// Locked start verdicts.
const (
	encStartRefused   = "start-refused"   // the daemon exited nonzero and never came up
	encStartedLocked  = "started-locked"  // the daemon came up although its store was locked
	encStartUndecided = "start-undecided" // still running at the deadline, or no exit status
)

// encLockedStart is what a start of the OSD with its crypt mapping closed
// left behind.
type encLockedStart struct {
	Running  bool     `json:"running"`
	ExitCode int      `json:"exit_code"`
	CameUp   bool     `json:"came_up"`
	Log      []string `json:"log"`
}

func judgeEncLockedStart(s encLockedStart) string {
	switch {
	case s.CameUp:
		return encStartedLocked
	case s.Running || s.ExitCode == 0:
		return encStartUndecided
	}
	return encStartRefused
}

// encLedgerEntry is one write of encWrites.
type encLedgerEntry struct {
	Key     string `json:"key"`
	SHA256  string `json:"sha256"`
	Outcome string `json:"outcome"`
}

// judgeEncReadback requires every acknowledged write to read back with its
// digest, and at least one write to have been acknowledged.
func judgeEncReadback(ledger []encLedgerEntry, read map[string]string) (bool, string) {
	acked := 0
	for _, e := range ledger {
		if e.Outcome != mutAcked {
			continue
		}
		acked++
		if read[e.Key] != e.SHA256 {
			return false, fmt.Sprintf("%s read back as %q", e.Key, read[e.Key])
		}
	}
	if acked == 0 {
		return false, "no acknowledged write"
	}
	return true, fmt.Sprintf("%d acknowledged objects intact", acked)
}
