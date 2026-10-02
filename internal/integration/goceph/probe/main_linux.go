//go:build linux && cgo

// This independent consumer fixture deliberately links go-ceph and native
// Linux libraries. The parent testcontainers module remains free of cgo.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/ceph/go-ceph/cephfs"
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
)

const (
	objectName      = "go-ceph-same-object"
	freshObjectName = "go-ceph-fresh-object"
	imageName       = "go-ceph-same-image"
	snapshotName    = "baseline"
	fileName        = "/go-ceph-same-file"
	freshFileName   = "/go-ceph-fresh-file"
	radosBytes      = 64 << 10
	rbdBytes        = 8 << 20
	cephfsBytes     = 96 << 10
)

type options struct {
	config, keyring, fsid, pool, filesystem, token, phase string
}

type storageProof struct {
	Name             string `json:"name"`
	Bytes            int    `json:"bytes"`
	RetainedSHA256   string `json:"retained_sha256,omitempty"`
	FreshSHA256      string `json:"fresh_sha256,omitempty"`
	SnapshotSHA256   string `json:"snapshot_sha256,omitempty"`
	SnapshotIsolated bool   `json:"snapshot_isolated,omitempty"`
	HeadRestored     bool   `json:"head_restored,omitempty"`
	Verified         bool   `json:"verified"`
	Deleted          bool   `json:"deleted,omitempty"`
}

type probeResult struct {
	GoCeph     string       `json:"go_ceph"`
	FSID       string       `json:"fsid"`
	CephX      bool         `json:"cephx"`
	Pool       string       `json:"pool"`
	Filesystem string       `json:"filesystem"`
	Token      string       `json:"token"`
	Phase      string       `json:"phase"`
	RADOS      storageProof `json:"rados"`
	RBD        storageProof `json:"rbd"`
	CephFS     storageProof `json:"cephfs"`
}

func main() {
	// cgo calls cannot be interrupted with context cancellation. Bound the
	// whole process so a stalled native operation cannot outlive the harness.
	deadline := time.AfterFunc(75*time.Second, func() {
		fmt.Fprintln(os.Stderr, "go-ceph probe: native operation exceeded the 75s process deadline")
		os.Exit(124)
	})
	defer deadline.Stop()
	var opts options
	flag.StringVar(&opts.config, "config", "/etc/ceph/ceph.conf", "Ceph configuration file")
	flag.StringVar(&opts.keyring, "keyring", "/etc/ceph/ceph.client.admin.keyring", "client.admin keyring file")
	flag.StringVar(&opts.fsid, "fsid", "", "expected Ceph cluster FSID")
	flag.StringVar(&opts.pool, "pool", "", "existing test pool for RADOS and RBD")
	flag.StringVar(&opts.filesystem, "filesystem", "", "existing CephFS name")
	flag.StringVar(&opts.token, "token", "", "cluster-specific deterministic payload token")
	flag.StringVar(&opts.phase, "phase", "", "seed, verify, or cleanup")
	flag.Parse()
	if flag.NArg() != 0 {
		fail(errors.New("unexpected positional arguments"))
	}
	result, err := run(opts)
	if err != nil {
		fail(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "go-ceph probe:", err)
	os.Exit(1)
}

func run(opts options) (probeResult, error) {
	for _, required := range []struct{ name, value string }{
		{"config", opts.config}, {"keyring", opts.keyring}, {"fsid", opts.fsid},
		{"pool", opts.pool}, {"filesystem", opts.filesystem}, {"token", opts.token},
	} {
		if strings.TrimSpace(required.value) == "" {
			return probeResult{}, fmt.Errorf("--%s is required", required.name)
		}
	}
	if opts.phase != "seed" && opts.phase != "verify" && opts.phase != "cleanup" {
		return probeResult{}, errors.New("--phase must be seed, verify, or cleanup")
	}
	conn, err := connect(opts)
	if err != nil {
		return probeResult{}, err
	}
	defer conn.Shutdown()
	ioctx, err := conn.OpenIOContext(opts.pool)
	if err != nil {
		return probeResult{}, fmt.Errorf("open pool: %w", err)
	}
	defer ioctx.Destroy()
	result := probeResult{GoCeph: "v0.41.0", FSID: opts.fsid, CephX: true, Pool: opts.pool,
		Filesystem: opts.filesystem, Token: opts.token, Phase: opts.phase}
	if result.RADOS, err = probeRADOS(ioctx, opts); err != nil {
		return probeResult{}, fmt.Errorf("RADOS: %w", err)
	}
	if result.RBD, err = probeRBD(ioctx, opts); err != nil {
		return probeResult{}, fmt.Errorf("RBD: %w", err)
	}
	if result.CephFS, err = probeCephFS(conn, opts); err != nil {
		return probeResult{}, fmt.Errorf("CephFS: %w", err)
	}
	return result, nil
}

func connect(opts options) (*rados.Conn, error) {
	conn, err := rados.NewConnWithUser("admin")
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			conn.Shutdown()
		}
	}()
	if err := conn.ReadConfigFile(opts.config); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	// Reject permissive authentication configurations. The native consumer
	// must authenticate with CephX, even if an anonymous server were reachable.
	for _, name := range []string{"auth_client_required", "auth_cluster_required", "auth_service_required"} {
		value, err := conn.GetConfigOption(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		if strings.TrimSpace(value) != "cephx" {
			return nil, fmt.Errorf("%s must require only cephx, got %q", name, value)
		}
	}
	for _, option := range [][2]string{
		{"keyring", opts.keyring}, {"rados_mon_op_timeout", "30"}, {"rados_osd_op_timeout", "30"},
		{"client_mount_timeout", "30"},
	} {
		if err := conn.SetConfigOption(option[0], option[1]); err != nil {
			return nil, fmt.Errorf("set %s: %w", option[0], err)
		}
	}
	if err := conn.Connect(); err != nil {
		return nil, fmt.Errorf("authenticated connect: %w", err)
	}
	fsid, err := conn.GetFSID()
	if err != nil {
		return nil, fmt.Errorf("read cluster FSID: %w", err)
	}
	if fsid != opts.fsid {
		return nil, fmt.Errorf("connected to FSID %q, expected %q", fsid, opts.fsid)
	}
	ok = true
	return conn, nil
}

func payload(token, service, stage string, size int) []byte {
	seed := sha256.Sum256([]byte("go-ceph-probe/v1/" + token + "/" + service + "/" + stage))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((int(seed[i%len(seed)])+i/len(seed))%255 + 1)
	}
	return data
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func probeRADOS(ioctx *rados.IOContext, opts options) (storageProof, error) {
	proof := storageProof{Name: objectName, Bytes: radosBytes}
	if opts.phase == "cleanup" {
		for _, name := range []string{objectName, freshObjectName} {
			if err := ioctx.Delete(name); err != nil && !errors.Is(err, rados.ErrNotFound) {
				return proof, err
			}
			if _, err := ioctx.Stat(name); !errors.Is(err, rados.ErrNotFound) {
				return proof, fmt.Errorf("deleted object %s still exists or stat failed: %v", name, err)
			}
		}
		proof.Verified, proof.Deleted = true, true
		return proof, nil
	}
	baseline := payload(opts.token, "rados", "baseline", radosBytes)
	if opts.phase == "seed" {
		if err := ioctx.WriteFull(objectName, baseline); err != nil {
			return proof, err
		}
	}
	if err := readRADOS(ioctx, objectName, baseline); err != nil {
		return proof, fmt.Errorf("retained object: %w", err)
	}
	proof.RetainedSHA256 = digest(baseline)
	if opts.phase == "verify" {
		fresh := payload(opts.token, "rados", "fresh", radosBytes)
		if err := ioctx.WriteFull(freshObjectName, fresh); err != nil {
			return proof, err
		}
		if err := readRADOS(ioctx, freshObjectName, fresh); err != nil {
			return proof, fmt.Errorf("new object: %w", err)
		}
		proof.FreshSHA256 = digest(fresh)
	}
	proof.Verified = true
	return proof, nil
}

func readRADOS(ioctx *rados.IOContext, name string, expected []byte) error {
	stat, err := ioctx.Stat(name)
	if err != nil {
		return err
	}
	if stat.Size != uint64(len(expected)) {
		return fmt.Errorf("object %s size %d, expected %d", name, stat.Size, len(expected))
	}
	actual := make([]byte, len(expected))
	n, err := ioctx.Read(name, actual, 0)
	if err != nil {
		return err
	}
	if n != len(expected) || !bytes.Equal(actual, expected) {
		return fmt.Errorf("object %s changed bytes: got %d bytes SHA256 %s, expected %s", name, n, digest(actual[:n]), digest(expected))
	}
	return nil
}

func probeRBD(ioctx *rados.IOContext, opts options) (proof storageProof, retErr error) {
	proof = storageProof{Name: imageName, Bytes: rbdBytes}
	if opts.phase == "cleanup" {
		if err := cleanupRBD(ioctx); err != nil {
			return proof, err
		}
		proof.Verified, proof.Deleted = true, true
		return proof, nil
	}
	if opts.phase == "seed" {
		if err := rbd.PoolInit(ioctx, false); err != nil {
			return proof, fmt.Errorf("initialize RBD pool: %w", err)
		}
		if _, err := rbd.Create2(ioctx, imageName, rbdBytes, rbd.FeatureLayering, 20); err != nil {
			return proof, fmt.Errorf("create image: %w", err)
		}
	}
	image, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	if err != nil {
		return proof, err
	}
	defer func() { retErr = errors.Join(retErr, image.Close()) }()
	size, err := image.GetSize()
	if err != nil || size != rbdBytes {
		return proof, fmt.Errorf("image size %d, expected %d: %v", size, rbdBytes, err)
	}
	baseline := payload(opts.token, "rbd", "baseline", rbdBytes)
	if opts.phase == "seed" {
		if err := writeRBD(image, baseline); err != nil {
			return proof, err
		}
		if _, err := image.CreateSnapshot(snapshotName); err != nil {
			return proof, err
		}
	}
	if err := readRBD(image, baseline); err != nil {
		return proof, fmt.Errorf("retained image: %w", err)
	}
	proof.RetainedSHA256 = digest(baseline)
	if opts.phase == "verify" {
		fresh := payload(opts.token, "rbd", "fresh", rbdBytes)
		if err := writeRBD(image, fresh); err != nil {
			return proof, err
		}
		if err := readRBD(image, fresh); err != nil {
			return proof, fmt.Errorf("new image bytes: %w", err)
		}
		proof.FreshSHA256 = digest(fresh)
	}
	if err := readRBDSnapshot(ioctx, baseline); err != nil {
		return proof, err
	}
	proof.SnapshotSHA256 = digest(baseline)
	proof.SnapshotIsolated = opts.phase == "verify"
	if opts.phase == "verify" {
		// Restore the baseline so each later topology phase can independently
		// verify the full image from a new process and native connection.
		if err := writeRBD(image, baseline); err != nil {
			return proof, err
		}
		if err := readRBD(image, baseline); err != nil {
			return proof, fmt.Errorf("restore baseline head: %w", err)
		}
		proof.HeadRestored = true
	}
	proof.Verified = true
	return proof, nil
}

func readRBD(image *rbd.Image, expected []byte) error {
	actual := make([]byte, len(expected))
	n, err := image.ReadAt(actual, 0)
	if err != nil {
		return err
	}
	if n != len(expected) || !bytes.Equal(actual, expected) {
		return fmt.Errorf("image bytes changed: got %d bytes SHA256 %s, expected %s", n, digest(actual[:n]), digest(expected))
	}
	return nil
}

func writeRBD(image *rbd.Image, data []byte) error {
	n, err := image.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return image.Flush()
}

func readRBDSnapshot(ioctx *rados.IOContext, expected []byte) (retErr error) {
	snapshot, err := rbd.OpenImageReadOnly(ioctx, imageName, snapshotName)
	if err != nil {
		return fmt.Errorf("open baseline snapshot: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, snapshot.Close()) }()
	return readRBD(snapshot, expected)
}

func cleanupRBD(ioctx *rados.IOContext) error {
	image, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	if errors.Is(err, rbd.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	removeErr := image.GetSnapshot(snapshotName).Remove()
	if errors.Is(removeErr, rbd.ErrNotFound) {
		removeErr = nil
	}
	if err := errors.Join(removeErr, image.Close()); err != nil {
		return err
	}
	if err := rbd.RemoveImage(ioctx, imageName); err != nil {
		return err
	}
	remaining, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	if remaining != nil {
		remaining.Close()
	}
	if !errors.Is(err, rbd.ErrNotFound) {
		return fmt.Errorf("deleted image still exists or open failed: %v", err)
	}
	return nil
}

func probeCephFS(conn *rados.Conn, opts options) (proof storageProof, retErr error) {
	proof = storageProof{Name: fileName, Bytes: cephfsBytes}
	mount, err := cephfs.CreateFromRados(conn)
	if err != nil {
		return proof, err
	}
	defer func() { retErr = errors.Join(retErr, mount.Release()) }()
	if err := mount.SelectFilesystem(opts.filesystem); err != nil {
		return proof, err
	}
	if err := mount.Mount(); err != nil {
		return proof, fmt.Errorf("userspace mount: %w", err)
	}
	defer func() { retErr = errors.Join(retErr, mount.Unmount()) }()
	if opts.phase == "cleanup" {
		for _, name := range []string{fileName, freshFileName} {
			if err := mount.Unlink(name); err != nil && !errors.Is(err, cephfs.ErrNotExist) {
				return proof, err
			}
			remaining, err := mount.Open(name, os.O_RDONLY, 0)
			if remaining != nil {
				remaining.Close()
			}
			if !errors.Is(err, cephfs.ErrNotExist) {
				return proof, fmt.Errorf("deleted file %s still exists or open failed: %v", name, err)
			}
		}
		proof.Verified, proof.Deleted = true, true
		return proof, nil
	}
	baseline := payload(opts.token, "cephfs", "baseline", cephfsBytes)
	if opts.phase == "seed" {
		if err := writeCephFS(mount, fileName, baseline, os.O_EXCL); err != nil {
			return proof, err
		}
	}
	if err := readCephFS(mount, fileName, baseline); err != nil {
		return proof, fmt.Errorf("retained file: %w", err)
	}
	proof.RetainedSHA256 = digest(baseline)
	if opts.phase == "verify" {
		fresh := payload(opts.token, "cephfs", "fresh", cephfsBytes)
		if err := writeCephFS(mount, freshFileName, fresh, os.O_TRUNC); err != nil {
			return proof, err
		}
		if err := readCephFS(mount, freshFileName, fresh); err != nil {
			return proof, fmt.Errorf("new file: %w", err)
		}
		proof.FreshSHA256 = digest(fresh)
	}
	proof.Verified = true
	return proof, nil
}

func writeCephFS(mount *cephfs.MountInfo, name string, data []byte, extraFlags int) (retErr error) {
	file, err := mount.Open(name, os.O_WRONLY|os.O_CREATE|extraFlags, 0o600)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	n, err := file.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return file.Fsync(cephfs.SyncAll)
}

func readCephFS(mount *cephfs.MountInfo, name string, expected []byte) (retErr error) {
	file, err := mount.Open(name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	actual, err := io.ReadAll(io.LimitReader(file, int64(len(expected)+1)))
	if err != nil {
		return err
	}
	if !bytes.Equal(actual, expected) {
		return fmt.Errorf("file %s changed bytes: got %d bytes SHA256 %s, expected %s", name, len(actual), digest(actual), digest(expected))
	}
	return nil
}
