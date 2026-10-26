//go:build all || integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
)

// scratchFillerScript is the bounded filler writer and its independent
// observers for the capacity-exhaustion scratch filesystem (issue 63). It runs
// as `python3 -I -c <script> <mode> <args>` so the same text can later run
// inside the container that mounts the scratch volume. Modes:
//
//	write DIR NAME CHUNK BUDGET DEADLINE PAUSE
//	    Create DIR/NAME exclusively and append CHUNK-byte writes until BUDGET
//	    bytes are attempted, DEADLINE seconds pass, SIGTERM/SIGINT arrives or an
//	    operation fails. It always closes its descriptor, confirms the closure
//	    (the number must answer EBADF) and prints exactly one JSON receipt.
//	release DIR NAME INO DEV SEED
//	    Unlink DIR/NAME only when it is still the identified file (same inode
//	    and device, a regular file), then report capacity before and after and
//	    the SHA-256 of DIR/SEED. It never holds the filler open.
//	hold DIR NAME
//	    A control: keep a 4 KiB file open, unlink its path and report what the
//	    descriptor still shows.
//
// A writer killed before it prints produces no receipt; that is a lost writer,
// never an allocation refusal. SIGXFSZ is ignored so a file-size limit surfaces
// as EFBIG instead of killing the writer.
const scratchFillerScript = `import errno, fcntl, hashlib, json, os, signal, stat, sys, time

def vfs(path):
    try:
        v = os.statvfs(path)
    except OSError as e:
        return None
    return {"bsize": v.f_frsize, "blocks": v.f_blocks, "bfree": v.f_bfree, "bavail": v.f_bavail,
            "files": v.f_files, "ffree": v.f_ffree, "favail": v.f_favail}

def mountinfo(dev):
    key = "%d:%d" % (os.major(dev), os.minor(dev))
    try:
        for line in open("/proc/self/mountinfo"):
            left, _, right = line.partition(" - ")
            f = left.split()
            if len(f) > 4 and f[2] == key:
                return {"id": key, "mountpoint": f[4], "fstype": right.split()[0]}
    except OSError:
        pass
    return {"id": key, "mountpoint": "", "fstype": ""}

def fstat(fd):
    s = os.fstat(fd)
    return {"ino": s.st_ino, "dev": s.st_dev, "nlink": s.st_nlink, "size": s.st_size,
            "allocated": s.st_blocks * 512}

def err(e):
    return e.errno or 0, errno.errorcode.get(e.errno or 0, "")

def write(d, name, chunk, budget, deadline, pause):
    stop = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *a: stop.append(1))
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    r = {"version": 1, "dir": d, "name": name, "chunk": chunk, "budget": budget,
         "ops": [], "outcome": "", "exception": "", "opened": False, "closed": False,
         "close_errno": 0, "vfs_before": vfs(d), "vfs_refusal": None, "vfs_after_close": None,
         "file": None, "mount": None}
    try:
        r["mount"] = mountinfo(os.stat(d).st_dev)
    except OSError:
        pass
    fd = -1
    attempted = 0
    start = time.monotonic()
    def op(kind, requested, written, e):
        n, s = err(e) if e else (0, "")
        r["ops"].append({"op": kind, "requested": requested, "written": written, "errno": n, "errname": s})
        return n
    try:
        try:
            fd = os.open(os.path.join(d, name), os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            r["opened"] = True
            op("create", 0, 0, None)
        except OSError as e:
            op("create", 0, 0, e)
            r["vfs_refusal"] = vfs(d)
            r["outcome"] = "errno"
        if fd >= 0:
            buf = b"\xa5" * chunk
            while not r["outcome"]:
                if stop:
                    r["outcome"] = "cancelled"
                elif attempted >= budget:
                    r["outcome"] = "budget"
                elif time.monotonic() - start >= deadline:
                    r["outcome"] = "deadline"
                else:
                    want = min(chunk, budget - attempted)
                    try:
                        got = os.write(fd, buf[:want])
                        op("write", want, got, None)
                        attempted += want
                        if pause:
                            time.sleep(pause)
                    except OSError as e:
                        op("write", want, 0, e)
                        r["vfs_refusal"] = vfs(d)
                        r["outcome"] = "errno"
            try:
                os.fsync(fd)
                op("fsync", 0, 0, None)
            except OSError as e:
                op("fsync", 0, 0, e)
                if r["outcome"] in ("budget", "deadline", "cancelled"):
                    r["outcome"] = "errno"
            r["file"] = fstat(fd)
            if r["vfs_refusal"] is None:
                r["vfs_refusal"] = vfs(d)
    except BaseException as e:
        r["exception"] = type(e).__name__
        r["outcome"] = "exception"
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError as e:
                r["close_errno"] = err(e)[0]
            try:
                fcntl.fcntl(fd, fcntl.F_GETFD)
            except OSError as e:
                r["closed"] = e.errno == errno.EBADF
        else:
            r["closed"] = True
        r["vfs_after_close"] = vfs(d)
        print(json.dumps(r), flush=True)

def release(d, name, ino, dev, seed):
    r = {"version": 1, "identified": False, "unlinked": False, "path_absent": False, "errno": 0,
         "vfs_before": vfs(d), "vfs_after": None, "seed_sha256": "", "seed_len": -1}
    path = os.path.join(d, name)
    try:
        s = os.lstat(path)
        if stat.S_ISREG(s.st_mode) and s.st_ino == ino and s.st_dev == dev:
            r["identified"] = True
            os.unlink(path)
            r["unlinked"] = True
    except FileNotFoundError:
        pass
    except OSError as e:
        r["errno"] = err(e)[0]
    r["path_absent"] = not os.path.lexists(path)
    r["vfs_after"] = vfs(d)
    try:
        data = open(os.path.join(d, seed), "rb").read()
        r["seed_sha256"], r["seed_len"] = hashlib.sha256(data).hexdigest(), len(data)
    except OSError:
        pass
    print(json.dumps(r), flush=True)

def hold(d, name):
    path = os.path.join(d, name)
    data = bytes(range(256)) * 16
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    r = {"version": 1}
    try:
        os.write(fd, data)
        os.fsync(fd)
        r["before_unlink"] = fstat(fd)
        os.unlink(path)
        r["path_absent"] = not os.path.lexists(path)
        r["held"] = fstat(fd)
        os.lseek(fd, 0, 0)
        r["readable"] = os.read(fd, len(data) + 1) == data
    finally:
        os.close(fd)
    print(json.dumps(r), flush=True)

m, a = sys.argv[1], sys.argv[2:]
if m == "write":
    write(a[0], a[1], int(a[2]), int(a[3]), float(a[4]), float(a[5]))
elif m == "release":
    release(a[0], a[1], int(a[2]), int(a[3]), a[4])
elif m == "hold":
    hold(a[0], a[1])
`

type scratchVFS struct {
	BSize  *int64 `json:"bsize"`
	Blocks *int64 `json:"blocks"`
	BFree  *int64 `json:"bfree"`
	BAvail *int64 `json:"bavail"`
	Files  *int64 `json:"files"`
	FFree  *int64 `json:"ffree"`
	FAvail *int64 `json:"favail"`
}

type scratchOp struct {
	Op        *string `json:"op"`
	Requested *int64  `json:"requested"`
	Written   *int64  `json:"written"`
	Errno     *int    `json:"errno"`
	ErrName   *string `json:"errname"`
}

type scratchFile struct {
	Ino       *uint64 `json:"ino"`
	Dev       *uint64 `json:"dev"`
	NLink     *int64  `json:"nlink"`
	Size      *int64  `json:"size"`
	Allocated *int64  `json:"allocated"`
}

type scratchMount struct {
	ID         *string `json:"id"`
	Mountpoint *string `json:"mountpoint"`
	FSType     *string `json:"fstype"`
}

// scratchFillReceipt is the writer's single JSON line. Pointer fields make a
// missing or null value distinguishable from a legitimate zero; the parser
// refuses either instead of counting it as evidence.
type scratchFillReceipt struct {
	Version       *int          `json:"version"`
	Dir           *string       `json:"dir"`
	Name          *string       `json:"name"`
	Chunk         *int64        `json:"chunk"`
	Budget        *int64        `json:"budget"`
	Ops           []scratchOp   `json:"ops"`
	Outcome       *string       `json:"outcome"`
	Exception     *string       `json:"exception"`
	Opened        *bool         `json:"opened"`
	Closed        *bool         `json:"closed"`
	CloseErrno    *int          `json:"close_errno"`
	VFSBefore     *scratchVFS   `json:"vfs_before"`
	VFSRefusal    *scratchVFS   `json:"vfs_refusal"`
	VFSAfterClose *scratchVFS   `json:"vfs_after_close"`
	File          *scratchFile  `json:"file"`
	Mount         *scratchMount `json:"mount"`
}

// decodeStrict accepts exactly one JSON line with no unknown fields and nothing
// after the value (requireEOF, not Decoder.More, which misses a dangling
// closing delimiter).
func decodeStrict(data []byte, into any) error {
	data = bytes.TrimSpace(data)
	if bytes.Contains(data, []byte("\n")) {
		return errors.New("expected exactly one JSON line")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return err
	}
	return requireEOF(dec)
}

func (v *scratchVFS) complete() bool {
	return v != nil && v.BSize != nil && v.Blocks != nil && v.BFree != nil && v.BAvail != nil &&
		v.Files != nil && v.FFree != nil && v.FAvail != nil && *v.BSize > 0
}

// parseScratchFillReceipt rejects a receipt with a missing, null or wrong-typed
// required field. Fields that depend on the outcome (the file identity after a
// create refusal, the refusal-time capacity) are checked by the classifier.
func parseScratchFillReceipt(out []byte) (*scratchFillReceipt, error) {
	var r scratchFillReceipt
	if err := decodeStrict(out, &r); err != nil {
		return nil, fmt.Errorf("scratch writer receipt: %w", err)
	}
	switch {
	case r.Version == nil || *r.Version != 1:
		return nil, errors.New("scratch writer receipt: version missing or unsupported")
	case r.Dir == nil || r.Name == nil || r.Chunk == nil || r.Budget == nil || *r.Chunk <= 0 || *r.Budget <= 0:
		return nil, errors.New("scratch writer receipt: request fields missing")
	case r.Outcome == nil || r.Exception == nil || r.Opened == nil || r.Closed == nil || r.CloseErrno == nil:
		return nil, errors.New("scratch writer receipt: outcome or closure fields missing")
	case len(r.Ops) == 0:
		return nil, errors.New("scratch writer receipt: no operation records")
	case !r.VFSBefore.complete() || !r.VFSAfterClose.complete():
		return nil, errors.New("scratch writer receipt: capacity observation missing or incomplete")
	}
	for i, o := range r.Ops {
		if o.Op == nil || o.Requested == nil || o.Written == nil || o.Errno == nil || o.ErrName == nil {
			return nil, fmt.Errorf("scratch writer receipt: operation %d incomplete", i)
		}
	}
	return &r, nil
}

const (
	scratchUnreached      = "unreached"            // budget or deadline reached without a refusal
	scratchCancelled      = "cancelled"            // stopped by signal before any refusal
	scratchByteCeiling    = "byte-ceiling"         // ENOSPC on a write with no space for the request and inodes left
	scratchInodeLimit     = "inode-limit"          // ENOSPC on create with no free inodes
	scratchUnclassifiedNS = "unclassified-nospace" // ENOSPC without a matching limit observation
	scratchOtherErrno     = "other-errno"          // another errno, never counted as capacity evidence
)

// classifyScratchFill turns a parsed receipt into one verdict. An open
// descriptor, an exception or an inconsistent record is an error: the fill
// cannot support a release or a capacity claim.
func classifyScratchFill(r *scratchFillReceipt) (string, error) {
	if !*r.Closed {
		return "", errors.New("scratch writer descriptor closure is not confirmed")
	}
	if *r.Outcome == "exception" || *r.Exception != "" {
		return "", fmt.Errorf("scratch writer raised %q", *r.Exception)
	}
	var written int64
	var failing *scratchOp
	for i := range r.Ops {
		o := &r.Ops[i]
		if *o.Errno != 0 {
			if failing != nil {
				return "", errors.New("scratch writer recorded more than one failing operation")
			}
			failing = o
		}
		if *o.Op == "write" && *o.Errno == 0 {
			written += *o.Written
		}
	}
	if *r.Opened {
		f := r.File
		if f == nil || f.Ino == nil || f.Dev == nil || f.NLink == nil || f.Size == nil || f.Allocated == nil {
			return "", errors.New("scratch writer receipt: file identity missing")
		}
		if *f.Size != written {
			return "", fmt.Errorf("scratch writer file size %d differs from %d recorded bytes", *f.Size, written)
		}
	}
	switch *r.Outcome {
	case "budget", "deadline":
		if failing != nil {
			return "", errors.New("scratch writer reports no error but recorded one")
		}
		if *r.Outcome == "budget" && written != *r.Budget {
			return "", fmt.Errorf("scratch writer wrote %d of a %d-byte budget", written, *r.Budget)
		}
		return scratchUnreached, nil
	case "cancelled":
		if failing != nil {
			return "", errors.New("scratch writer reports cancellation but recorded an error")
		}
		return scratchCancelled, nil
	case "errno":
		if failing == nil {
			return "", errors.New("scratch writer reports an error but recorded none")
		}
	default:
		return "", fmt.Errorf("scratch writer outcome %q is not recognized", *r.Outcome)
	}
	if *failing.Errno != int(syscall.ENOSPC) {
		return scratchOtherErrno, nil
	}
	// ENOSPC alone does not identify the byte ceiling: it also covers VM
	// commit accounting and inode admission. Require the matching observation.
	if !r.VFSRefusal.complete() || r.Mount == nil || r.Mount.FSType == nil {
		return "", errors.New("scratch writer receipt: refusal-time capacity or mount identity missing")
	}
	v := r.VFSRefusal
	switch *failing.Op {
	case "write":
		if *r.Mount.FSType == "tmpfs" && *v.BAvail**v.BSize < *failing.Requested && *v.FAvail > 0 {
			return scratchByteCeiling, nil
		}
	case "create":
		if *r.Mount.FSType == "tmpfs" && *v.FAvail == 0 {
			return scratchInodeLimit, nil
		}
	}
	return scratchUnclassifiedNS, nil
}

type scratchRelease struct {
	Version    *int        `json:"version"`
	Identified *bool       `json:"identified"`
	Unlinked   *bool       `json:"unlinked"`
	PathAbsent *bool       `json:"path_absent"`
	Errno      *int        `json:"errno"`
	VFSBefore  *scratchVFS `json:"vfs_before"`
	VFSAfter   *scratchVFS `json:"vfs_after"`
	SeedSHA256 *string     `json:"seed_sha256"`
	SeedLen    *int64      `json:"seed_len"`
}

func parseScratchRelease(out []byte) (*scratchRelease, error) {
	var r scratchRelease
	if err := decodeStrict(out, &r); err != nil {
		return nil, fmt.Errorf("scratch release receipt: %w", err)
	}
	if r.Version == nil || *r.Version != 1 || r.Identified == nil || r.Unlinked == nil || r.PathAbsent == nil ||
		r.Errno == nil || r.SeedSHA256 == nil || r.SeedLen == nil || !r.VFSBefore.complete() || !r.VFSAfter.complete() {
		return nil, errors.New("scratch release receipt: required field missing, null or incomplete")
	}
	return &r, nil
}

// checkScratchRelease decides whether the filler's capacity was independently
// released: the writer's closure was confirmed, the unlink removed the
// identified file, free space grew by at least the allocated bytes, and the
// seed still has its expected digest. A missing path alone is not release.
func checkScratchRelease(fill *scratchFillReceipt, rel *scratchRelease, seedSHA256 string) error {
	if !*fill.Closed || !*fill.Opened || fill.File == nil || fill.File.Allocated == nil {
		return errors.New("filler descriptor closure or identity is not confirmed")
	}
	if !*rel.Identified || !*rel.Unlinked || *rel.Errno != 0 {
		return errors.New("the identified filler was not unlinked")
	}
	if !*rel.PathAbsent {
		return errors.New("filler path is still present")
	}
	freed := (*rel.VFSAfter.BAvail - *rel.VFSBefore.BAvail) * *rel.VFSAfter.BSize
	if freed < *fill.File.Allocated {
		return fmt.Errorf("unlink released %d bytes, filler allocated %d", freed, *fill.File.Allocated)
	}
	if *rel.SeedSHA256 != seedSHA256 {
		return errors.New("seed digest differs after release")
	}
	return nil
}
