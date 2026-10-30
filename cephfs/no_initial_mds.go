package cephfs

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
)

// A successful policy-only constructor confirms this authority. The first
// actual auth/start attempt consumes cold permission even if no handle returns.
// This does not turn a partial setup or failed ordinary MDS into a cold fixture.
type cephFSColdMDS struct {
	fsid      string
	identity  *cephFSNativeIdentity
	pools     map[string]int64
	dataPools []int64
	attempted bool
}

func (fs *Filesystem) captureColdMDS(ctx context.Context, originalFSID string) error {
	fsid, err := fs.readColdFSID(ctx, &cephFSColdMDS{fsid: originalFSID})
	if err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clusterutil.OperationError(ctx, "read cold filesystem pools", err)
	}
	cap := &cephFSColdMDS{fsid: fsid, identity: fs.nativeIdentity, pools: make(map[string]int64), dataPools: slices.Clone(state.dataPools)}
	if cap.identity == nil || len(state.dataPools) != 1+len(fs.config.AdditionalDataPools) {
		return errors.New("cold filesystem setup is incomplete")
	}
	for _, config := range cephFSConfiguredPools(fs.config) {
		pool, err := state.poolByName(config.Name)
		if err != nil || pool.ID < 0 {
			return errors.New("cold filesystem pool identity is unavailable")
		}
		cap.pools[pool.Name] = pool.ID
	}
	if err := fs.verifyColdMDS(ctx, cap); err != nil {
		return err
	}
	if err := fs.checkColdMDSAuth(ctx); err != nil {
		return err
	}
	if err := fs.verifyColdMDS(ctx, cap); err != nil {
		return err
	}
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		return err
	}
	defer servicehook.Unlock(fs.cluster)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !fs.registered() || fs.nativeIdentity != cap.identity || fs.Container != nil || fs.roster.count() != 0 {
		return errors.New("cold filesystem ownership changed before confirmation")
	}
	fs.coldMDS = cap
	return nil
}

// Called with the original setup gate held. Only this additive branch accepts
// nil embedded Container; successful first startup then uses ordinary scaling.
func (fs *Filesystem) scaleColdMDS(parent context.Context, active, standby int, start func(context.Context) error) error {
	if active != 1 || standby != 0 {
		return errors.New("first cold MDS scale requires one active and zero standby")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, servicehook.StartupTimeout(c))
	defer cancel()
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return err
	}
	cap := fs.coldMDS
	valid := fs.registered() && fs.FilesystemName == fs.name && fs.Container == nil && fs.roster.count() == 0 && cap != nil && !cap.attempted && cap.identity == fs.nativeIdentity
	servicehook.Unlock(c)
	if !valid {
		return servicehook.Invalid(errors.New("cold filesystem is unavailable, incomplete or already attempted"))
	}
	if err := fs.verifyColdMDS(ctx, cap); err != nil {
		return err
	}
	if err := fs.checkColdMDSAuth(ctx); err != nil {
		return err
	}
	// Reattest policy after auth admission before the starter consumes permission.
	if err := fs.verifyColdMDS(ctx, cap); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := start(ctx); err != nil {
		return err
	}
	if err := fs.WaitReady(ctx); err != nil {
		return err
	}
	// Final readiness cannot authorize replacement of an original additional pool.
	if _, err := fs.readColdFSID(ctx, cap); err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clusterutil.OperationError(ctx, "read initialized cold filesystem", err)
	}
	if !slices.Equal(state.dataPools, cap.dataPools) {
		return errors.New("original cold filesystem attachments changed")
	}
	for name, id := range cap.pools {
		pool, err := state.poolByName(name)
		if err != nil || pool.ID != id {
			return errors.New("original cold filesystem pool was replaced")
		}
	}
	if _, err := fs.readColdFSID(ctx, cap); err != nil {
		return err
	}
	return ctx.Err()
}

func (fs *Filesystem) verifyColdMDS(ctx context.Context, cap *cephFSColdMDS) error {
	if cap == nil || cap.identity == nil || cap.identity != fs.nativeIdentity {
		return errors.New("cold filesystem identity is unavailable")
	}
	if _, err := fs.readColdFSID(ctx, cap); err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clusterutil.OperationError(ctx, "read original cold filesystem", err)
	}
	if !slices.Equal(state.dataPools, cap.dataPools) {
		return errors.New("cold filesystem data pool attachments changed")
	}
	names, ids := map[string]bool{}, map[int64]bool{}
	for _, pool := range state.pools {
		if pool.Name == "" || pool.ID < 0 || names[pool.Name] || ids[pool.ID] {
			return errors.New("cold native pool listing is incomplete or duplicated")
		}
		names[pool.Name], ids[pool.ID] = true, true
	}
	for name, id := range cap.pools {
		pool, err := state.poolByName(name)
		if err != nil || pool.ID != id {
			return errors.New("original cold filesystem pool was replaced")
		}
	}
	for index, config := range append([]ceph.PoolConfig{fs.config.DataPool}, fs.config.AdditionalDataPools...) {
		if index >= len(cap.dataPools) || cap.pools[config.Name] != cap.dataPools[index] {
			return errors.New("cold filesystem data pool mapping differs from configuration")
		}
	}
	data, err := servicehook.AuthCommand(ctx, fs.cluster, "read cold native MDS map", "fs", "dump", "--format", "json")
	if err != nil {
		return err
	}
	if err := decodeColdMDSMap(data, coldMDSMapExpectation{filesystem: fs.name, reservedName: fs.roster.nextID(fs.name), native: cap.identity, dataPools: cap.dataPools}); err != nil {
		return err
	}
	_, err = fs.readColdFSID(ctx, cap)
	return err
}

func (fs *Filesystem) checkColdMDSAuth(ctx context.Context) error {
	data, err := servicehook.AuthCommand(ctx, fs.cluster, "check initial MDS identity", "auth", "ls", "--format", "json")
	if err != nil {
		return err
	}
	var auth struct {
		Entities *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if coldMDSJSON(data, &auth) != nil || auth.Entities == nil {
		return errors.New("decode initial MDS identity names")
	}
	seen := map[string]bool{}
	reserved := "mds." + fs.roster.nextID(fs.name)
	for _, entry := range *auth.Entities {
		if entry.Entity == nil || *entry.Entity == "" || strings.TrimSpace(*entry.Entity) != *entry.Entity || seen[*entry.Entity] {
			return errors.New("initial MDS identity names are incomplete or duplicated")
		}
		seen[*entry.Entity] = true
		if *entry.Entity == reserved {
			return servicehook.Invalid(errors.New("initial MDS identity already exists"))
		}
	}
	return ctx.Err()
}

type coldMDSRow struct {
	Name  *string `json:"name"`
	GID   *uint64 `json:"gid"`
	Rank  *int    `json:"rank"`
	State *string `json:"state"`
	Join  *int64  `json:"join_fscid"`
}

// New cold absence guards reject duplicate fields, aliases of known fields and
// trailing documents. Never return raw decode errors: native auth input can
// contain credentials.
func coldMDSJSON(data []byte, value any) error {
	if strictjson.Decode(data, value, strictjson.Options{MaxBytes: 4 << 20}) != nil {
		return errors.New("invalid cold native JSON")
	}
	return nil
}

// MON's canonical cluster UUID may stay stable when the owned control CLI changes.
// Pin the native identity rather than a particular exported compatibility handle.
func (fs *Filesystem) readColdFSID(ctx context.Context, cap *cephFSColdMDS) (string, error) {
	data, err := servicehook.AuthCommand(ctx, fs.cluster, "read original cold cluster identity", "fsid")
	if err != nil {
		return "", err
	}
	return coldMDSFSID(ctx, data, cap)
}
func coldMDSFSID(ctx context.Context, data []byte, cap *cephFSColdMDS) (string, error) {
	value := strings.TrimSpace(string(data))
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != value || cap != nil && cap.fsid != value {
		return "", errors.New("original cold cluster identity is invalid or changed")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return value, nil
}
func (fs *Filesystem) checkColdSelectedControl(ctx context.Context, control testcontainers.Container) error {
	data, err := servicehook.Command(ctx, control, "ceph", "--connect-timeout", "5", "fsid")
	if err != nil {
		return clusterutil.OperationError(ctx, "read selected initial MDS control identity", err)
	}
	_, err = coldMDSFSID(ctx, data, fs.coldMDS)
	return err
}
