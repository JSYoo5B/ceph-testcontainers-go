package monstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/google/uuid"
)

// This file is the inspector side of the MON store observation: the request
// that describes the short-lived read-only container of design decision D6
// (still the recommendation, not an owner decision), the listing the container
// prints, the parser of that listing and the driver that runs one container
// through a small engine interface. It implements the monStoreInspector port of
// inspect.go.
//
// What is here is checked Docker-free: the request is built and validated as
// data, the listing script is executed by the tests against a temporary
// directory, the parser is strict, and the driver is tested with a fake engine.
// What is not here is the engine itself. No Docker or moby call is made, no
// image was run, and nothing calls the inspector in production. How the
// request fields translate to the container create call, and what the engine
// does on an unreadable or truncated log stream, is the next slice and is
// settled only by a native run.

const (
	// monStoreInspectMountTarget is where the volume is mounted, read-only,
	// inside the inspector.
	monStoreInspectMountTarget = "/store"
	// monStoreInspectOutputLimit bounds the standard output the driver accepts.
	monStoreInspectOutputLimit = 16 << 20
	// monStoreInspectMaxEntries bounds the entries the script lists.
	monStoreInspectMaxEntries = 100000
	// monStoreInspectTimeout bounds start, wait and log reads together.
	monStoreInspectTimeout = 2 * time.Minute
	// monStoreInspectCleanupTimeout bounds the removal, which runs under a
	// context that does not end with the caller's.
	monStoreInspectCleanupTimeout = 30 * time.Second
	// monStoreInspectListingFormat is the first line of a listing.
	monStoreInspectListingFormat = "tc-mon-store-listing-v1"

	monStoreInspectorLabel = "org.testcontainers.ceph.mon-store-inspector"
)

// monStoreInspectScript lists the data directory given as its only argument.
// It follows no symlink: every entry is classified from lstat, regular files
// are opened with O_NOFOLLOW and must be the file that lstat saw, and a
// directory on another device (a mount point) is an "other" entry that is not
// entered. It writes one JSON object per line:
//
//	{"format": "tc-mon-store-listing-v1"}
//	{"path","kind","mode","inode","size","mtime_ns","digest"} per entry, sorted by path
//	{"end": true, "count": N, "kv_backend": "<whole file, at most 4096 bytes>"|null}
//
// and exits 0 only after the last line. Any error, a path that is not UTF-8, or
// more entries than the bound writes a reason to standard error and exits
// non-zero, so a partial listing is never mistaken for a complete one. The
// script writes nothing to the directory (it is mounted read-only anyway).
const monStoreInspectScript = `
import hashlib, json, os, stat, sys

MAX_ENTRIES = %d
MAX_MARKER = 4096

def fail(message, code=3):
    sys.stderr.write(message + "\n")
    sys.exit(code)

def emit(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=True) + "\n")

def kind_of(st, root_dev):
    if stat.S_ISLNK(st.st_mode):
        return "symlink"
    if stat.S_ISDIR(st.st_mode):
        return "dir" if st.st_dev == root_dev else "other"
    if stat.S_ISREG(st.st_mode):
        return "file"
    return "other"

def digest_of(path, st):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        fst = os.fstat(fd)
        if not stat.S_ISREG(fst.st_mode) or (fst.st_ino, fst.st_dev) != (st.st_ino, st.st_dev):
            fail("a file changed while it was listed")
        h = hashlib.sha256()
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                break
            h.update(chunk)
        return h.hexdigest()
    finally:
        os.close(fd)

def read_marker(path):
    # A complete bounded read: loop to EOF and refuse a marker beyond the bound
    # instead of keeping a prefix (the pinned MonitorDBStore reader allows 4096).
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        data = b""
        while True:
            chunk = os.read(fd, MAX_MARKER + 1 - len(data))
            if not chunk:
                break
            data += chunk
            if len(data) > MAX_MARKER:
                fail("the kv_backend marker is beyond the bound")
        return data.decode("utf-8")
    finally:
        os.close(fd)

def main():
    root = sys.argv[1]
    root_st = os.lstat(root)
    if not stat.S_ISDIR(root_st.st_mode):
        fail("the data directory is not a directory")
    entries = []
    pending = [""]
    while pending:
        rel_dir = pending.pop()
        with os.scandir(os.path.join(root, rel_dir)) as it:
            names = [e.name for e in it]
        for name in names:
            rel = name if rel_dir == "" else rel_dir + "/" + name
            try:
                rel.encode("utf-8")
            except UnicodeEncodeError:
                fail("an entry name is not valid UTF-8")
            if len(entries) >= MAX_ENTRIES:
                fail("the store has more entries than the bound")
            full = os.path.join(root, rel)
            st = os.lstat(full)
            kind = kind_of(st, root_st.st_dev)
            digest = digest_of(full, st) if kind == "file" else ""
            entries.append({"path": rel, "kind": kind, "mode": stat.S_IMODE(st.st_mode), "inode": st.st_ino,
                            "size": st.st_size, "mtime_ns": st.st_mtime_ns, "digest": digest})
            if kind == "dir":
                pending.append(rel)
    entries.sort(key=lambda e: e["path"].encode("utf-8"))
    marker = None
    for e in entries:
        if e["path"] == "kv_backend" and e["kind"] == "file":
            marker = read_marker(os.path.join(root, "kv_backend"))
    emit({"format": "tc-mon-store-listing-v1"})
    for e in entries:
        emit(e)
    emit({"end": True, "count": len(entries), "kv_backend": marker})

try:
    main()
except SystemExit:
    raise
except Exception as exc:
    fail("listing failed: " + type(exc).__name__, 4)
`

// monStoreInspectScriptSource is the script with its entry bound filled in.
func monStoreInspectScriptSource() string {
	return fmt.Sprintf(monStoreInspectScript, monStoreInspectMaxEntries)
}

// monStoreInspectRequest describes the one container of an inspection as data.
// It is what an engine turns into a create call: nothing in it is a moby type,
// so the description can be validated and compared without Docker.
type monStoreInspectRequest struct {
	image string
	// user is the numeric uid:gid that owns the store. With every capability
	// dropped root cannot read a 0600 file of another owner, so the inspector
	// runs as the owner.
	user       string
	entrypoint []string
	cmd        []string
	// labels carry the cluster's owned label, so Terminate's labelled-container
	// scan finds an inspector whose creation outcome is unknown, and a
	// per-inspection token.
	labels map[string]string
	// volume is mounted read-only at target.
	volume, target string
	readOnlyMount  bool
	// networkMode is "none": the inspector has no network.
	networkMode         string
	capDrop             []string
	noNewPrivileges     bool
	readOnlyRootfs      bool
	privileged          bool
	autoRemove          bool
	keepVolumesOnRemove bool
}

var monStoreOwnerUser = regexp.MustCompile(`^[0-9]{1,10}:[0-9]{1,10}$`)

// newMonStoreInspectRequest builds the request for one inspection of volume.
// owner is the cluster's owner ID (the value of clusterutil.OwnerLabel).
func newMonStoreInspectRequest(image, user, owner, volume, token string) (monStoreInspectRequest, error) {
	switch {
	case image == "":
		return monStoreInspectRequest{}, errors.New("the inspector needs an image")
	case !monStoreOwnerUser.MatchString(user):
		return monStoreInspectRequest{}, errors.New("the inspector needs the numeric uid:gid of the store owner")
	case owner == "":
		return monStoreInspectRequest{}, errors.New("the inspector needs the cluster owner label value")
	case volume == "":
		return monStoreInspectRequest{}, errors.New("the inspector needs a volume name")
	case token == "":
		return monStoreInspectRequest{}, errors.New("the inspector needs an inspection token")
	}
	return monStoreInspectRequest{
		image:           image,
		user:            user,
		entrypoint:      []string{"python3"},
		cmd:             []string{"-I", "-c", monStoreInspectScriptSource(), monStoreInspectMountTarget},
		labels:          map[string]string{clusterutil.OwnerLabel: owner, monStoreInspectorLabel: token},
		volume:          volume,
		target:          monStoreInspectMountTarget,
		readOnlyMount:   true,
		networkMode:     "none",
		capDrop:         []string{"ALL"},
		noNewPrivileges: true,
		readOnlyRootfs:  true,
		// The container is removed by the driver on every outcome, never by the
		// engine on its own (AutoRemove would race the log read), and its
		// removal never takes a volume with it.
		keepVolumesOnRemove: true,
	}, nil
}

// validate re-checks the properties that make the inspector read-only and
// isolated. An engine calls it on the request it is about to translate.
func (r monStoreInspectRequest) validate() error {
	var problems []string
	add := func(bad bool, text string) {
		if bad {
			problems = append(problems, text)
		}
	}
	add(r.image == "", "no image")
	add(!monStoreOwnerUser.MatchString(r.user), "user is not a numeric uid:gid")
	add(r.volume == "" || r.target != monStoreInspectMountTarget, "volume or mount target differs")
	add(!r.readOnlyMount, "the volume mount is not read-only")
	add(r.networkMode != "none", "the inspector has a network")
	add(!slices.Equal(r.capDrop, []string{"ALL"}), "capabilities are not all dropped")
	add(!r.noNewPrivileges, "privilege escalation is not blocked")
	add(!r.readOnlyRootfs, "the root file system is writable")
	add(r.privileged, "the inspector is privileged")
	add(r.autoRemove, "the engine removes the container on its own")
	add(!r.keepVolumesOnRemove, "removal may take volumes with it")
	add(r.labels[clusterutil.OwnerLabel] == "", "no owned label")
	add(r.labels[monStoreInspectorLabel] == "", "no inspection label")
	add(len(r.labels) != 2, "unexpected labels")
	add(!slices.Equal(r.entrypoint, []string{"python3"}) || len(r.cmd) != 4 || r.cmd[0] != "-I" || r.cmd[1] != "-c" || r.cmd[2] != monStoreInspectScriptSource() || r.cmd[3] != monStoreInspectMountTarget, "the command is not the listing script")
	if len(problems) != 0 {
		return fmt.Errorf("inspector request: %s", strings.Join(problems, "; "))
	}
	return nil
}

// monStoreInspectLogs is what an engine read from the container's output. It
// never holds more than the limit it was given; truncated reports that the
// stream held more.
type monStoreInspectLogs struct {
	stdout, stderr []byte
	truncated      bool
}

// monStoreInspectEngine is the Docker surface of one inspection. The driver
// calls each method at most once per inspection and never retries a call whose
// outcome is unknown.
type monStoreInspectEngine interface {
	// Create makes the container and returns its ID. An error means the
	// outcome is unknown: the container may exist.
	Create(ctx context.Context, req monStoreInspectRequest) (string, error)
	Start(ctx context.Context, id string) error
	// Wait returns the exit code once the container stopped.
	Wait(ctx context.Context, id string) (int64, error)
	// Logs reads at most limit bytes of each stream.
	Logs(ctx context.Context, id string, limit int) (monStoreInspectLogs, error)
	// Remove removes the container, forced if it still runs, without removing
	// any volume. A container that is already gone is not an error.
	Remove(ctx context.Context, id string) error
}

// monStoreContainerInspector implements monStoreInspector with one short-lived
// container per call. It holds no lock and no state between calls.
type monStoreContainerInspector struct {
	engine monStoreInspectEngine
	// image, user and owner fill the request; see newMonStoreInspectRequest.
	image, user, owner string
	newToken           func() string
	timeout            time.Duration
}

// Inspect runs the listing script in a read-only container on volume and parses
// what it printed. The container is removed on every outcome after its creation
// returned an ID; a removal failure is reported next to the first failure and
// withholds the listing. A failed creation, start or wait is not retried: the
// outcome is unknown, so the call returns its error and the labelled container,
// if any, is left to the cluster's teardown scan.
func (i monStoreContainerInspector) Inspect(ctx context.Context, volume string) (files monStoreFiles, err error) {
	if isNilPort(i.engine) {
		return monStoreFiles{}, errors.New("the MON store inspector has no engine")
	}
	token := uuid.NewString()
	if i.newToken != nil {
		token = i.newToken()
	}
	req, err := newMonStoreInspectRequest(i.image, i.user, i.owner, volume, token)
	if err != nil {
		return monStoreFiles{}, err
	}
	if err := req.validate(); err != nil {
		return monStoreFiles{}, err
	}
	id, err := i.engine.Create(ctx, req)
	if err != nil {
		return monStoreFiles{}, fmt.Errorf("create the inspector: outcome unknown, not retried: %w", err)
	}
	if id == "" {
		return monStoreFiles{}, errors.New("the engine created an inspector without an ID: outcome unknown, not retried")
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), monStoreInspectCleanupTimeout)
		defer cancel()
		if rmErr := i.engine.Remove(cleanup, id); rmErr != nil {
			files = monStoreFiles{}
			err = errors.Join(err, fmt.Errorf("remove the inspector: %w", rmErr))
		}
	}()
	timeout := i.timeout
	if timeout <= 0 {
		timeout = monStoreInspectTimeout
	}
	run, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := i.engine.Start(run, id); err != nil {
		return monStoreFiles{}, fmt.Errorf("start the inspector: outcome unknown, not retried: %w", err)
	}
	code, err := i.engine.Wait(run, id)
	if err != nil {
		return monStoreFiles{}, fmt.Errorf("wait for the inspector: outcome unknown, not retried: %w", err)
	}
	if code != 0 {
		return monStoreFiles{}, fmt.Errorf("the inspector exited with code %d", code)
	}
	logs, err := i.engine.Logs(run, id, monStoreInspectOutputLimit)
	if err != nil {
		return monStoreFiles{}, fmt.Errorf("read the inspector output: %w", err)
	}
	switch {
	case logs.truncated || len(logs.stdout) > monStoreInspectOutputLimit || len(logs.stderr) > monStoreInspectOutputLimit:
		return monStoreFiles{}, errors.New("the inspector output is beyond the limit")
	case len(logs.stderr) != 0:
		return monStoreFiles{}, errors.New("the inspector wrote to standard error")
	}
	return parseMonStoreListing(logs.stdout)
}

var monStoreKinds = []monStoreKind{monStoreKindFile, monStoreKindDir, monStoreKindSymlink, monStoreKindOther}

// parseMonStoreListing parses a listing strictly: the format line, entries in
// strictly ascending path order, and the trailer whose count equals the number
// of entries, each as one line holding one JSON object with exactly the
// expected fields. A listing without its trailer, a repeated or unordered
// path, an unknown kind, a path with a replacement character or a control
// character, or any other line is an error. Only a trailer with "kv_backend":
// null yields an unread marker; a missing key is an error. The parsed entries
// are still judged by monStoreFiles.storeFacts.
func parseMonStoreListing(stdout []byte) (monStoreFiles, error) {
	const what = "inspector listing"
	if len(stdout) == 0 || stdout[len(stdout)-1] != '\n' {
		return monStoreFiles{}, fmt.Errorf("%s is empty or has no final newline", what)
	}
	if !utf8.Valid(stdout) {
		return monStoreFiles{}, fmt.Errorf("%s is not valid UTF-8", what)
	}
	lines := bytes.Split(stdout[:len(stdout)-1], []byte("\n"))
	if len(lines) < 2 {
		return monStoreFiles{}, fmt.Errorf("%s lacks its format line or trailer", what)
	}
	header, err := monStoreObjectOf(lines[0], what+" format line")
	if err != nil {
		return monStoreFiles{}, err
	}
	if err := header.exactKeys(what+" format line", "format"); err != nil {
		return monStoreFiles{}, err
	}
	if format, err := header.str("format", what); err != nil || format != monStoreInspectListingFormat {
		return monStoreFiles{}, fmt.Errorf("%s has an unknown format", what)
	}
	var files monStoreFiles
	last := ""
	for n, line := range lines[1 : len(lines)-1] {
		item := fmt.Sprintf("%s entry %d", what, n)
		obj, err := monStoreObjectOf(line, item)
		if err != nil {
			return monStoreFiles{}, err
		}
		entry, err := parseMonStoreListingEntry(obj, item)
		if err != nil {
			return monStoreFiles{}, err
		}
		if n > 0 && entry.path <= last {
			return monStoreFiles{}, fmt.Errorf("%s is not in strictly ascending path order", item)
		}
		last = entry.path
		files.entries = append(files.entries, entry)
		if len(files.entries) > monStoreInspectMaxEntries {
			return monStoreFiles{}, fmt.Errorf("%s has more entries than the bound", what)
		}
	}
	trailer, err := monStoreObjectOf(lines[len(lines)-1], what+" trailer")
	if err != nil {
		return monStoreFiles{}, err
	}
	if err := trailer.exactKeys(what+" trailer", "end", "count", "kv_backend"); err != nil {
		return monStoreFiles{}, err
	}
	if end, err := trailer.boolean("end", what+" trailer"); err != nil || !end {
		return monStoreFiles{}, fmt.Errorf("%s trailer does not end the listing", what)
	}
	count, err := trailer.uint64("count", what+" trailer")
	if err != nil {
		return monStoreFiles{}, err
	}
	if count != uint64(len(files.entries)) {
		return monStoreFiles{}, fmt.Errorf("%s trailer counts %d entries but %d were listed", what, count, len(files.entries))
	}
	if string(bytes.TrimSpace(trailer["kv_backend"])) != "null" {
		marker, err := trailer.str("kv_backend", what+" trailer")
		if err != nil {
			return monStoreFiles{}, err
		}
		files.kvBackend = &marker
	}
	return files, nil
}

func parseMonStoreListingEntry(obj monStoreJSONObject, what string) (monStoreEntry, error) {
	var e monStoreEntry
	if err := obj.exactKeys(what, "path", "kind", "mode", "inode", "size", "mtime_ns", "digest"); err != nil {
		return e, err
	}
	var err error
	if e.path, err = obj.str("path", what); err != nil {
		return e, err
	}
	if strings.ContainsRune(e.path, utf8.RuneError) || strings.ContainsFunc(e.path, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
		return e, fmt.Errorf("%s has a path with a replacement or control character", what)
	}
	kind, err := obj.str("kind", what)
	if err != nil {
		return e, err
	}
	e.kind = monStoreKind(kind)
	if !slices.Contains(monStoreKinds, e.kind) {
		return e, fmt.Errorf("%s has an unknown kind", what)
	}
	mode, err := obj.uint64("mode", what)
	if err != nil {
		return e, err
	}
	if mode > 0o7777 {
		return e, fmt.Errorf("%s has a mode beyond the permission bits", what)
	}
	e.mode = uint32(mode)
	if e.inode, err = obj.uint64("inode", what); err != nil {
		return e, err
	}
	if e.size, err = obj.int64("size", what); err != nil {
		return e, err
	}
	if e.mtimeNs, err = obj.int64("mtime_ns", what); err != nil {
		return e, err
	}
	if e.digest, err = obj.str("digest", what); err != nil {
		return e, err
	}
	return e, nil
}
