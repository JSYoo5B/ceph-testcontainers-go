//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/moby/moby/api/pkg/stdcopy"
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
// The host's device-mapper names are shared with everything else on the
// engine host, so a name alone never identifies the test's mapping. Every
// action that changes or removes a mapping takes the DM UUID and device
// number the test recorded when it created the mapping, and acts on that UUID
// only while it still has that number; a loop is detached only while it still
// backs the recorded inode. "-" stands for a layer the test does not hold.
//
//	admit -                         kernel release, registered targets, cryptsetup version
//	create FILE BYTES               backing file and loop device, with their identity
//	format LOOP                     LUKS2 header on the loop, key on stdin
//	open LOOP CRYPT                 open the crypt mapping, key (possibly none) on stdin
//	header LOOP                     LUKS UUID and digest of the header area
//	linear NODE UUID CUUID CNUM     the OSD's linear node, with DM UUID UUID, over the crypt mapping
//	retarget NUUID NNUM error       load an error table into the node
//	retarget NUUID NNUM CUUID CNUM  load a linear table over the crypt mapping
//	stack NODE CRYPT LOOP FILE      what each layer maps and who holds it
//	close CUUID CNUM                close the crypt mapping once nothing holds it
//	scan PATH MARKER                count a hex marker's occurrences in a device or file
//	foreign NAME UUID               a one-sector error mapping (a collision control)
//	observe NODE NUUID CRYPT CUUID LOOP
//	                                the live identity of each layer and of the mappings named NODE and CRYPT
//	remove NUUID NNUM CUUID CNUM LOOP LNUM LDEV LINO FILE
//	                                release the recorded layers that still have their identity
const encKeeperScript = `import errno, fcntl, hashlib, json, os, struct, subprocess, sys, time

def run(*argv, data=None):
    r = subprocess.run(argv, input=data, capture_output=True, timeout=120)
    return r.returncode, r.stdout.decode(errors="replace").strip(), r.stderr.decode(errors="replace").strip()[-400:]

def number(path):
    st = os.stat(path)
    return "%d:%d" % (os.major(st.st_rdev), os.minor(st.st_rdev))

def mapping(*select):
    # The mapping that select (a name, or "-u", UUID) names: its name, DM
    # UUID, number and open count, or exists False; an unreadable answer is
    # an error, never an absence.
    rc, out, err = run("dmsetup", "info", "-c", "--noheadings", "--separator", "|", "-o", "name,uuid,major,minor,open", *select)
    if rc != 0:
        return {"exists": False} if "does not exist" in err else {"exists": False, "error": "dmsetup info: " + err}
    f = out.split("|")
    if len(f) != 5:
        return {"exists": False, "error": "dmsetup info: %r" % out}
    return {"exists": True, "name": f[0], "uuid": f[1], "number": "%s:%s" % (f[2], f[3]), "open": int(f[4])}

def exists(name):
    return mapping(name)["exists"]

def opened(name):
    m = mapping(name)
    return m["open"] if m["exists"] else None

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

def owned(uuid, num):
    # The mapping with DM UUID uuid while it still has number num; otherwise
    # why not. A UUID is unique on the host, the number tells a mapping
    # created again under the same UUID.
    m = mapping("-u", uuid)
    if m.get("error"):
        return None, m["error"]
    if not m["exists"]:
        return None, "gone"
    if m["number"] != num:
        return None, "%s now has number %s, not %s" % (uuid, m["number"], num)
    return m, ""

def loopinfo(loop):
    # LOOP_GET_STATUS64: the backing file's device and inode, read at once
    # from the kernel (sysfs and losetup lag a detach).
    try:
        num = number(loop)
        fd = os.open(loop, os.O_RDONLY)
    except OSError as e:
        return {"error": "%s: %s" % (loop, e)}
    try:
        buf = fcntl.ioctl(fd, 0x4C05, bytes(232))
    except OSError as e:
        if e.errno == errno.ENXIO:
            return {"attached": False, "number": num}
        return {"error": "%s: %s" % (loop, e)}
    finally:
        os.close(fd)
    dev, ino = struct.unpack_from("=QQ", buf)
    return {"attached": True, "number": num, "back_dev": "%d:%d" % (os.major(dev), os.minor(dev)), "inode": ino,
            "holders": holders(num)}

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

def seen(name, uuid):
    # The recorded mapping (by UUID) or, when none is recorded, the mapping
    # that has the name; name_uuid is whoever has the name now.
    by_name = mapping(name)
    out = mapping("-u", uuid) if uuid != "-" else dict(by_name)
    out["name_uuid"] = by_name.get("uuid", "") if by_name["exists"] else ""
    if by_name.get("error"):
        out.setdefault("error", by_name["error"])
    if out["exists"]:
        out["table"] = redact(run("dmsetup", "table", out["name"])[1])
    return out

def release_dm(uuid, num):
    if uuid == "-":
        return {"outcome": "skipped"}
    m, why = owned(uuid, num)
    if m is None:
        return {"outcome": "gone"} if why == "gone" else {"outcome": "refused", "error": why}
    if m["open"] != 0 or holders(num) != []:
        return {"outcome": "held", "error": "open %d, holders %s" % (m["open"], holders(num))}
    err = ""
    for attempt in range(10):
        rc, _, err = run("dmsetup", "remove", "--verifyudev", "-u", uuid)
        if rc == 0 or not mapping("-u", uuid)["exists"]:
            return {"outcome": "removed"}
        time.sleep(0.5)
    return {"outcome": "failed", "error": "dmsetup remove: " + err}

def release_loop(loop, num, dev, ino):
    if loop == "-":
        return {"outcome": "skipped"}
    li = loopinfo(loop)
    if li.get("error"):
        return {"outcome": "refused", "error": li["error"]}
    if not li["attached"]:
        return {"outcome": "gone"}
    if (li["number"], li["back_dev"], li["inode"]) != (num, dev, int(ino)):
        return {"outcome": "refused", "error": "%s is %s backing %s inode %d, not %s backing %s inode %s" % (
            loop, li["number"], li["back_dev"], li["inode"], num, dev, ino)}
    if li["holders"] != []:
        # A held loop would only be marked for a lazy detach.
        return {"outcome": "held", "error": "holders %s" % li["holders"]}
    rc, _, err = run("losetup", "-d", loop)
    for attempt in range(20):
        li = loopinfo(loop)
        if not li.get("error") and (not li["attached"] or (li["back_dev"], li["inode"]) != (dev, int(ino))):
            return {"outcome": "removed"}
        time.sleep(0.25)
    return {"outcome": "failed", "error": "losetup -d: rc %d %s" % (rc, err)}

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
    st = os.stat(path)
    out["file"] = path
    rc, loop, err = run("losetup", "--find", "--show", path)
    if rc != 0:
        out["error"] = "losetup: " + err
    else:
        out["loop"] = loop
        li = loopinfo(loop)
        if li.get("error") or not li["attached"]:
            out["error"] = "loop identity: %s" % li
        elif (li["back_dev"], li["inode"]) != ("%d:%d" % (os.major(st.st_dev), os.minor(st.st_dev)), st.st_ino):
            out["error"] = "%s backs %s inode %d, not %s" % (loop, li["back_dev"], li["inode"], path)
        else:
            out["loop_number"], out["back_dev"], out["inode"] = li["number"], li["back_dev"], li["inode"]
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
    m = mapping(name)
    out["mapped"] = m["exists"]
    if m["exists"]:
        out["uuid"], out["number"] = m["uuid"], m["number"]
        out["table"] = redact(run("dmsetup", "table", name)[1])
        out["bytes"] = int(run("blockdev", "--getsize64", "/dev/mapper/" + name)[1] or 0)
    out.update(header(arg))
elif action == "header":
    out.update(header(arg))
elif action == "linear":
    uuid, cuuid, cnum = sys.argv[3], sys.argv[4], sys.argv[5]
    crypt, why = owned(cuuid, cnum)
    if crypt is None:
        out["error"] = "crypt mapping: " + why
    else:
        sectors = int(run("blockdev", "--getsz", "/dev/mapper/" + crypt["name"])[1])
        rc, _, err = run("dmsetup", "create", "--verifyudev", arg, "--uuid", uuid, "--table", "0 %d linear %s 0" % (sectors, cnum))
        out["rc"] = rc
        if rc != 0:
            out["error"] = "dmsetup create: " + err
        m = mapping("-u", uuid)
        if m["exists"]:
            node = "/dev/mapper/" + m["name"]
            out["uuid"], out["path"], out["number"] = m["uuid"], node, m["number"]
            out["bytes"] = int(run("blockdev", "--getsize64", node)[1])
elif action == "retarget":
    node, why = owned(arg, sys.argv[3])
    target = sys.argv[4]
    lower = None
    if node is not None and target != "error":
        lower, why = owned(target, sys.argv[5])
    if node is None or (target != "error" and lower is None):
        out["error"] = "not the recorded mapping: " + why
    else:
        sectors = int(run("dmsetup", "table", "-u", arg)[1].split()[1])
        table = "0 %d error" % sectors if target == "error" else "0 %d linear %s 0" % (sectors, lower["number"])
        steps = []
        for argv in (("dmsetup", "suspend", "--noudevsync", node["name"]), ("dmsetup", "load", "--noudevsync", node["name"], "--table", table),
                     ("dmsetup", "resume", "--noudevsync", node["name"])):
            rc, _, err = run(*argv)
            steps.append({"step": argv[1], "rc": rc})
            if rc != 0:
                out["error"] = "dmsetup %s: %s" % (argv[1], err)
                break
        out["steps"], out["table"] = steps, run("dmsetup", "table", "-u", arg)[1]
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
    m, why = owned(arg, sys.argv[3])
    if m is None:
        out["closed"] = why == "gone"
        if not out["closed"]:
            out["error"] = "not the recorded mapping: " + why
    else:
        out["open_before"], out["holders_before"] = m["open"], holders(m["number"])
        r = release_dm(arg, sys.argv[3])
        out["closed"] = r["outcome"] in ("removed", "gone")
        if not out["closed"]:
            out["error"] = "%s: %s" % (r["outcome"], r.get("error", ""))
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
elif action == "foreign":
    rc, _, err = run("dmsetup", "create", "--verifyudev", arg, "--uuid", sys.argv[3], "--table", "0 1 error")
    out["rc"] = rc
    if rc != 0:
        out["error"] = "dmsetup create: " + err
    m = mapping("-u", sys.argv[3])
    if m["exists"]:
        out["uuid"], out["number"] = m["uuid"], m["number"]
elif action == "observe":
    node_uuid, crypt, crypt_uuid, loop = sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
    out["seen_node"], out["seen_crypt"] = seen(arg, node_uuid), seen(crypt, crypt_uuid)
    out["seen_loop"] = loopinfo(loop) if loop != "-" else {"attached": False}
elif action == "remove":
    node_uuid, node_num, crypt_uuid, crypt_num, loop, loop_num, loop_dev, loop_ino, path = sys.argv[2:11]
    # Top down: a layer is removed only once nothing holds it, so a kept
    # layer keeps everything under it.
    out["release_node"] = release_dm(node_uuid, node_num)
    out["release_crypt"] = release_dm(crypt_uuid, crypt_num)
    out["release_loop"] = release_loop(loop, loop_num, loop_dev, loop_ino)
    if path == "-":
        out["release_file"] = {"outcome": "skipped"}
    elif not os.path.exists(path):
        out["release_file"] = {"outcome": "gone"}
    else:
        rc, listed, err = run("losetup", "-j", path)
        if rc != 0 or listed != "":
            out["release_file"] = {"outcome": "held", "error": "loops %r %s" % (listed, err)}
        else:
            os.unlink(path)
            out["release_file"] = {"outcome": "removed"}
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
	BackDev      string   `json:"back_dev,omitempty"`
	Inode        uint64   `json:"inode,omitempty"`
	RC           *int     `json:"rc,omitempty"`
	Message      string   `json:"message,omitempty"`
	Existed      bool     `json:"existed,omitempty"`
	Mapped       bool     `json:"mapped,omitempty"`
	UUID         string   `json:"uuid,omitempty"`
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
	Count        *int     `json:"count,omitempty"`
	// observe
	SeenNode  *encDMSeen   `json:"seen_node,omitempty"`
	SeenCrypt *encDMSeen   `json:"seen_crypt,omitempty"`
	SeenLoop  *encLoopSeen `json:"seen_loop,omitempty"`
	// remove
	ReleaseNode  *encOutcome `json:"release_node,omitempty"`
	ReleaseCrypt *encOutcome `json:"release_crypt,omitempty"`
	ReleaseLoop  *encOutcome `json:"release_loop,omitempty"`
	ReleaseFile  *encOutcome `json:"release_file,omitempty"`
}

// encDMSeen is the live state of one device-mapper layer: the mapping the
// test recorded, looked up by its DM UUID, or, when none is recorded, the
// mapping that has the test's name. NameUUID is the DM UUID of whatever has
// the name now.
type encDMSeen struct {
	Exists   bool   `json:"exists"`
	Name     string `json:"name,omitempty"`
	UUID     string `json:"uuid,omitempty"`
	Number   string `json:"number,omitempty"`
	Open     *int   `json:"open,omitempty"`
	Table    string `json:"table,omitempty"`
	NameUUID string `json:"name_uuid"`
	Error    string `json:"error,omitempty"`
}

// encLoopSeen is the kernel's view of the recorded loop device.
type encLoopSeen struct {
	Attached bool     `json:"attached"`
	Number   string   `json:"number,omitempty"`
	BackDev  string   `json:"back_dev,omitempty"`
	Inode    uint64   `json:"inode,omitempty"`
	Holders  []string `json:"holders,omitempty"`
	Error    string   `json:"error,omitempty"`
}

// encOutcome is what the keeper did with one recorded layer: removed, gone
// (already absent), skipped (not recorded), held, refused (its identity
// changed) or failed.
type encOutcome struct {
	Outcome string `json:"outcome"`
	Error   string `json:"error,omitempty"`
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

// encDMRecord is a device-mapper layer the test created: the DM UUID it gave
// the node, or that cryptsetup derives from the test's own LUKS header, and
// the number the kernel gave it. The UUID is recorded before the step that
// creates the mapping and the number once the keeper confirms it, so a step
// whose receipt was lost leaves a UUID without a number. The zero value is a
// layer the test does not hold.
type encDMRecord struct{ UUID, Number string }

// encLoopRecord is the loop the test attached: its node, its number and the
// device and inode of the backing file it was attached to.
type encLoopRecord struct {
	Loop, Number, BackDev string
	Inode                 uint64
}

// encChainRecord is what the test holds of its chain. The release acts only
// on what is recorded here, and only while the live layer still has the
// recorded identity.
type encChainRecord struct {
	File        string
	Loop        encLoopRecord
	Crypt, Node encDMRecord
}

func (r encChainRecord) held() bool { return r != encChainRecord{} }

// encCryptDMUUID is the DM UUID that cryptsetup gives a LUKS2 mapping: the
// LUKS UUID without dashes and the mapping name. The LUKS UUID is that of the
// test's own header, so no mapping the test did not open carries it.
func encCryptDMUUID(luksUUID, name string) string {
	return "CRYPT-LUKS2-" + strings.ReplaceAll(luksUUID, "-", "") + "-" + name
}

// encLower is the device that a one-target table maps: the lower device of a
// linear target at offset 0 or of a crypt target, "error" for an error
// target, and "" for anything else.
func encLower(table string) string {
	if strings.Contains(strings.TrimSpace(table), "\n") {
		return ""
	}
	f := strings.Fields(table)
	switch {
	case len(f) == 5 && f[2] == "linear" && f[4] == "0":
		return f[3]
	case len(f) >= 8 && f[2] == "crypt":
		return f[6]
	case len(f) == 3 && f[2] == "error":
		return "error"
	}
	return ""
}

// encLoopAcquired is the loop of a create receipt, once the keeper read its
// identity from the kernel.
func encLoopAcquired(r encReply) (encLoopRecord, bool) {
	if r.Error != "" || r.Loop == "" || r.LoopNumber == "" || r.BackDev == "" || r.Inode == 0 {
		return encLoopRecord{}, false
	}
	return encLoopRecord{Loop: r.Loop, Number: r.LoopNumber, BackDev: r.BackDev, Inode: r.Inode}, true
}

// encCryptAcquired is the crypt mapping of an open receipt, when that open
// created it: cryptsetup succeeded, no mapping of the name existed before,
// and the live mapping of the name has the UUID of the test's header and maps
// the test's loop. A mapping that already had the name is someone else's.
func encCryptAcquired(r encReply, luksUUID, name string, loop encLoopRecord) (encDMRecord, string) {
	switch {
	case r.RC == nil || *r.RC != 0 || !r.Mapped:
		return encDMRecord{}, fmt.Sprintf("not opened: %s", r.Message)
	case r.Existed:
		return encDMRecord{}, "a mapping of that name existed before the open"
	case luksUUID == "" || r.UUID != encCryptDMUUID(luksUUID, name):
		return encDMRecord{}, fmt.Sprintf("DM UUID %q is not that of the test's header", r.UUID)
	case loop.Number == "" || encLower(r.Table) != loop.Number:
		return encDMRecord{}, fmt.Sprintf("table %q does not map the test's loop %s", r.Table, loop.Number)
	}
	return encDMRecord{UUID: r.UUID, Number: r.Number}, ""
}

// encNodeAcquired is the linear node of a linear receipt, when the keeper
// created it with the UUID the test gave it.
func encNodeAcquired(r encReply, uuid string) (encDMRecord, string) {
	switch {
	case r.RC == nil || *r.RC != 0 || r.Error != "":
		return encDMRecord{}, "not created: " + r.Error
	case r.UUID != uuid || r.Number == "":
		return encDMRecord{}, fmt.Sprintf("the node has DM UUID %q, number %q", r.UUID, r.Number)
	}
	return encDMRecord{UUID: uuid, Number: r.Number}, ""
}

// Ownership verdicts of one layer at release.
const (
	encLayerOwned      = "owned"      // the recorded layer with its recorded identity: it is released
	encLayerNone       = "none"       // never acquired, and nothing has the test's name
	encLayerForeign    = "foreign"    // never acquired; the name belongs to a mapping the test did not create
	encLayerGone       = "gone"       // acquired and no longer present
	encLayerReplaced   = "replaced"   // the recorded UUID or loop now has another identity
	encLayerRetargeted = "retargeted" // the recorded mapping maps a device the test did not put under it
	encLayerUnknown    = "unknown"    // the keeper could not read it
)

// encLayer is the ownership verdict of one layer and, when it is owned, the
// number the keeper must still find before it removes it.
type encLayer struct {
	Verdict, Detail, Number string
}

// judgeEncDM decides whether a device-mapper layer is the test's to remove:
// a recorded one must still have its DM UUID and number (a UUID without a
// number is a creation whose receipt was lost; the test-generated UUID alone
// identifies it), and must map one of lowers. A layer the test never
// recorded is never removed, whatever has its name.
func judgeEncDM(rec encDMRecord, seen *encDMSeen, lowers ...string) encLayer {
	switch {
	case seen == nil:
		return encLayer{Verdict: encLayerUnknown, Detail: "not observed"}
	case seen.Error != "":
		return encLayer{Verdict: encLayerUnknown, Detail: seen.Error}
	case rec.UUID == "" && seen.Exists:
		return encLayer{Verdict: encLayerForeign, Detail: fmt.Sprintf("%s is %s (DM UUID %q), which the test did not create", seen.Name, seen.Number, seen.UUID)}
	case rec.UUID == "":
		return encLayer{Verdict: encLayerNone}
	case !seen.Exists && seen.NameUUID != "":
		return encLayer{Verdict: encLayerGone, Detail: fmt.Sprintf("%s is gone; its name now has DM UUID %q", rec.UUID, seen.NameUUID)}
	case !seen.Exists:
		return encLayer{Verdict: encLayerGone, Detail: rec.UUID + " is gone"}
	case seen.UUID != rec.UUID || (rec.Number != "" && seen.Number != rec.Number):
		return encLayer{Verdict: encLayerReplaced, Detail: fmt.Sprintf("%s is %s, recorded %s %s", seen.UUID, seen.Number, rec.UUID, rec.Number)}
	case !slices.Contains(lowers, encLower(seen.Table)):
		return encLayer{Verdict: encLayerRetargeted, Detail: fmt.Sprintf("%s maps %q, not one of %v", rec.UUID, seen.Table, lowers)}
	}
	detail := rec.UUID + " " + seen.Number
	if rec.Number == "" {
		detail += ", identified by its UUID after a lost receipt"
	}
	return encLayer{Verdict: encLayerOwned, Detail: detail, Number: seen.Number}
}

// judgeEncLoop decides whether the recorded loop is the test's to detach: it
// must still be attached to the recorded backing file.
func judgeEncLoop(rec encLoopRecord, seen *encLoopSeen) encLayer {
	switch {
	case rec.Loop == "":
		return encLayer{Verdict: encLayerNone}
	case seen == nil:
		return encLayer{Verdict: encLayerUnknown, Detail: "not observed"}
	case seen.Error != "":
		return encLayer{Verdict: encLayerUnknown, Detail: seen.Error}
	case !seen.Attached:
		return encLayer{Verdict: encLayerGone, Detail: rec.Loop + " is detached"}
	case seen.Number != rec.Number || seen.BackDev != rec.BackDev || seen.Inode != rec.Inode:
		return encLayer{Verdict: encLayerReplaced, Detail: fmt.Sprintf("%s is %s backing %s inode %d, recorded %s backing %s inode %d",
			rec.Loop, seen.Number, seen.BackDev, seen.Inode, rec.Number, rec.BackDev, rec.Inode)}
	}
	return encLayer{Verdict: encLayerOwned, Detail: rec.Loop + " " + rec.Number, Number: rec.Number}
}

// encOwnership is the verdict of each layer of the chain.
type encOwnership struct {
	Node  encLayer `json:"node"`
	Crypt encLayer `json:"crypt"`
	Loop  encLayer `json:"loop"`
}

// judgeEncOwnership judges each layer of an observe receipt against the
// record. The crypt mapping must map the recorded loop, and the node either
// the error table of a locked store or the crypt mapping when that is owned.
func judgeEncOwnership(rec encChainRecord, r encReply) encOwnership {
	var own encOwnership
	own.Loop = judgeEncLoop(rec.Loop, r.SeenLoop)
	own.Crypt = judgeEncDM(rec.Crypt, r.SeenCrypt, rec.Loop.Number)
	lowers := []string{"error"}
	if own.Crypt.Verdict == encLayerOwned {
		lowers = append(lowers, own.Crypt.Number)
	}
	own.Node = judgeEncDM(rec.Node, r.SeenNode, lowers...)
	return own
}

// encRemoveArgs are the arguments of the keeper's remove action: each owned
// layer with the identity the keeper must still find, and "-" for every other
// layer, which the keeper does not touch.
func encRemoveArgs(rec encChainRecord, own encOwnership) []string {
	args := make([]string, 0, 9)
	for _, l := range []struct {
		layer encLayer
		uuid  string
	}{{own.Node, rec.Node.UUID}, {own.Crypt, rec.Crypt.UUID}} {
		if l.layer.Verdict == encLayerOwned {
			args = append(args, l.uuid, l.layer.Number)
		} else {
			args = append(args, "-", "-")
		}
	}
	if own.Loop.Verdict == encLayerOwned {
		args = append(args, rec.Loop.Loop, rec.Loop.Number, rec.Loop.BackDev, strconv.FormatUint(rec.Loop.Inode, 10))
	} else {
		args = append(args, "-", "-", "-", "-")
	}
	if rec.File == "" {
		return append(args, "-")
	}
	return append(args, rec.File)
}

// encReleased is the record after a remove receipt: a layer that was removed
// or found gone is no longer held; every other layer stays recorded, so the
// chain and its keeper are kept and reported.
func encReleased(rec encChainRecord, own encOwnership, r encReply) encChainRecord {
	done := func(verdict string, o *encOutcome) bool {
		return verdict == encLayerGone || (verdict == encLayerOwned && o != nil && (o.Outcome == "removed" || o.Outcome == "gone"))
	}
	if done(own.Node.Verdict, r.ReleaseNode) {
		rec.Node = encDMRecord{}
	}
	if done(own.Crypt.Verdict, r.ReleaseCrypt) {
		rec.Crypt = encDMRecord{}
	}
	if done(own.Loop.Verdict, r.ReleaseLoop) {
		rec.Loop = encLoopRecord{}
	}
	if r.ReleaseFile != nil && (r.ReleaseFile.Outcome == "removed" || r.ReleaseFile.Outcome == "gone") {
		rec.File = ""
	}
	return rec
}

// encStream is an attached exec stream: the multiplexed output to read and
// the standard input to write and close.
type encStream interface {
	io.ReadWriter
	CloseWrite() error
	Close() error
}

// encCopy writes input to s, closes its write side and demultiplexes its
// output until EOF. Neither the write nor the read observes ctx, so when ctx
// ends first encCopy closes s, which ends a blocked one, and returns the
// context's error.
func encCopy(ctx context.Context, s encStream, input []byte) ([]byte, []byte, error) {
	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()
	var stdout, stderr bytes.Buffer
	err := func() error {
		if len(input) > 0 {
			if _, err := s.Write(input); err != nil {
				return err
			}
		}
		if err := s.CloseWrite(); err != nil {
			return err
		}
		_, err := stdcopy.StdCopy(&stdout, &stderr, s)
		return err
	}()
	if ctx.Err() != nil {
		return stdout.Bytes(), stderr.Bytes(), fmt.Errorf("exec stream closed: %w", context.Cause(ctx))
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

// encUnsettled is a keeper exec that was started and whose exit the test did
// not see: its stream or its status was lost, so the command may still be
// running in the keeper.
type encUnsettled struct {
	ExecID string
	Err    error
}

func (u *encUnsettled) Error() string {
	return fmt.Sprintf("keeper exec %s may still be running: %v", u.ExecID, u.Err)
}

func (u *encUnsettled) Unwrap() error { return u.Err }

// encExecState is an unsettled keeper exec as inspected before a release.
type encExecState struct {
	ID      string
	Running bool
	Err     string
}

// judgeEncSettled allows a release only once every unsettled keeper exec is
// known to have exited. A command still running (a scan holding the crypt
// mapping open, a retarget between its suspend and its resume) would race
// the release.
func judgeEncSettled(states []encExecState) (bool, string) {
	for _, s := range states {
		switch {
		case s.Err != "":
			return false, fmt.Sprintf("keeper exec %s: %s", s.ID, s.Err)
		case s.Running:
			return false, fmt.Sprintf("keeper exec %s is still running", s.ID)
		}
	}
	return true, fmt.Sprintf("%d unsettled execs exited", len(states))
}
