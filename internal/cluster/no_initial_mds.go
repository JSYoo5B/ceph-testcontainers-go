package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

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

func (fs *CephFSContainer) captureColdMDS(ctx context.Context, originalFSID string) error {
	fsid, err := fs.readColdFSID(ctx, &cephFSColdMDS{fsid: originalFSID})
	if err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clientOperationError(ctx, "read cold filesystem pools", err)
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
	if err := fs.cluster.lockTopology(ctx); err != nil {
		return err
	}
	defer fs.cluster.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if fs.cluster.closed || fs.cluster.filesystems[fs.FilesystemName] != fs || fs.nativeIdentity != cap.identity || fs.Container != nil || len(fs.mdss) != 0 {
		return errors.New("cold filesystem ownership changed before confirmation")
	}
	fs.coldMDS = cap
	return nil
}

// Called with the original setup gate held. Only this additive branch accepts
// nil embedded Container; successful first startup then uses ordinary scaling.
func (fs *CephFSContainer) scaleColdMDS(parent context.Context, active, standby int, start func(context.Context) error) error {
	if active != 1 || standby != 0 {
		return errors.New("first cold MDS scale requires one active and zero standby")
	}
	c := fs.cluster
	ctx, cancel := context.WithTimeout(parent, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	cap := fs.coldMDS
	valid := !c.closed && c.filesystems[fs.FilesystemName] == fs && fs.FilesystemName == fs.config.Name && fs.Container == nil && len(fs.mdss) == 0 && cap != nil && !cap.attempted && cap.identity == fs.nativeIdentity
	c.mu.Unlock()
	if !valid {
		return errors.New("cold filesystem is unavailable, incomplete or already attempted")
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
	if err := fs.verifyColdPoolIDs(ctx, cap); err != nil {
		return err
	}
	return ctx.Err()
}

// Final readiness cannot authorize replacement of an original additional pool.
func (fs *CephFSContainer) verifyColdPoolIDs(ctx context.Context, cap *cephFSColdMDS) error {
	if _, err := fs.readColdFSID(ctx, cap); err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clientOperationError(ctx, "read initialized cold filesystem", err)
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
	_, err = fs.readColdFSID(ctx, cap)
	return err
}

func (fs *CephFSContainer) verifyColdMDS(ctx context.Context, cap *cephFSColdMDS) error {
	if cap == nil || cap.identity == nil || cap.identity != fs.nativeIdentity {
		return errors.New("cold filesystem identity is unavailable")
	}
	if _, err := fs.readColdFSID(ctx, cap); err != nil {
		return err
	}
	state, err := fs.readNativePools(ctx)
	if err != nil {
		return clientOperationError(ctx, "read original cold filesystem", err)
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
	for index, config := range append([]PoolConfig{fs.config.DataPool}, fs.config.AdditionalDataPools...) {
		if index >= len(cap.dataPools) || cap.pools[config.Name] != cap.dataPools[index] {
			return errors.New("cold filesystem data pool mapping differs from configuration")
		}
	}
	data, err := fs.cluster.clientAuthCommand(ctx, "read cold native MDS map", "fs", "dump", "--format", "json")
	if err != nil {
		return err
	}
	if err := validateColdMDSMap(data, fs.config.Name, cap, cephFSMDSID(fs.FilesystemName, fs.nextMDSIndex)); err != nil {
		return err
	}
	_, err = fs.readColdFSID(ctx, cap)
	return err
}

func (fs *CephFSContainer) checkColdMDSAuth(ctx context.Context) error {
	data, err := fs.cluster.clientAuthCommand(ctx, "check initial MDS identity", "auth", "ls", "--format", "json")
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
	reserved := "mds." + cephFSMDSID(fs.FilesystemName, fs.nextMDSIndex)
	for _, entry := range *auth.Entities {
		if entry.Entity == nil || *entry.Entity == "" || strings.TrimSpace(*entry.Entity) != *entry.Entity || seen[*entry.Entity] {
			return errors.New("initial MDS identity names are incomplete or duplicated")
		}
		seen[*entry.Entity] = true
		if *entry.Entity == reserved {
			return errors.New("initial MDS identity already exists")
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

func validateColdMDSMap(data []byte, name string, cap *cephFSColdMDS, reserved string) error {
	var native struct {
		Standbys    *[]coldMDSRow `json:"standbys"`
		Filesystems *[]struct {
			ID  *int64 `json:"id"`
			Map *struct {
				Name     *string                `json:"fs_name"`
				Max      *int                   `json:"max_mds"`
				Wanted   *int                   `json:"standby_count_wanted"`
				Metadata *int64                 `json:"metadata_pool"`
				Data     *[]int64               `json:"data_pools"`
				Info     *map[string]coldMDSRow `json:"info"`
				Up       *map[string]uint64     `json:"up"`
				In       *[]int                 `json:"in"`
				Failed   *[]int                 `json:"failed"`
				Damaged  *[]int                 `json:"damaged"`
				Stopped  *[]int                 `json:"stopped"`
				Flags    *struct {
					Joinable *bool `json:"joinable"`
					Replay   *bool `json:"allow_standby_replay"`
					Refuse   *bool `json:"refuse_standby_for_another_fs"`
				} `json:"flags_state"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if coldMDSJSON(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil {
		return errors.New("decode cold MDS map")
	}
	gids, names := map[uint64]bool{}, map[string]bool{}
	rowValid := func(row coldMDSRow) bool {
		if row.Name == nil || !poolResourceName.MatchString(*row.Name) || row.GID == nil || *row.GID == 0 || row.Rank == nil || row.State == nil || !slices.Contains([]string{"up:boot", "up:standby", "up:standby-replay", "up:creating", "up:starting", "up:replay", "up:resolve", "up:reconnect", "up:rejoin", "up:clientreplay", "up:active", "up:stopping"}, *row.State) || row.Join == nil || *row.Join < -1 || gids[*row.GID] || names[*row.Name] || *row.Name == reserved {
			return false
		}
		gids[*row.GID], names[*row.Name] = true, true
		return true
	}
	found := 0
	fsNames, fsIDs := map[string]bool{}, map[int64]bool{}
	for _, fs := range *native.Filesystems {
		if fs.ID == nil || *fs.ID < 0 || fs.Map == nil || fs.Map.Name == nil || *fs.Map.Name == "" || fsIDs[*fs.ID] || fsNames[*fs.Map.Name] || fs.Map.Info == nil {
			return errors.New("cold filesystem map entries are incomplete or duplicated")
		}
		fsIDs[*fs.ID], fsNames[*fs.Map.Name] = true, true
		if *fs.Map.Name != name {
			for _, row := range *fs.Map.Info {
				if !rowValid(row) || *row.Rank < 0 || *row.State == "up:boot" || *row.State == "up:standby" {
					return errors.New("foreign MDS map is incomplete or conflicts with initial identity")
				}
			}
			continue
		}
		found++
		m := fs.Map
		if *fs.ID != cap.identity.id || m.Metadata == nil || *m.Metadata != cap.identity.metadataPool || m.Data == nil || !slices.Equal(*m.Data, cap.dataPools) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Flags == nil || m.Flags.Joinable == nil || !*m.Flags.Joinable || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse {
			return errors.New("original cold filesystem policy or identity changed")
		}
		if m.Up == nil || m.In == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Info) != 0 || len(*m.Up) != 0 || len(*m.In) != 0 || len(*m.Failed) != 0 || len(*m.Damaged) != 0 || len(*m.Stopped) != 0 {
			return errors.New("cold filesystem already has MDS rank or worker state")
		}
	}
	if found != 1 {
		return errors.New("original cold filesystem map is absent")
	}
	for _, row := range *native.Standbys {
		if !rowValid(row) || *row.Rank != -1 || *row.State != "up:standby" {
			return errors.New("native standby map is incomplete or conflicting")
		}
		if *row.Join == -1 || *row.Join == cap.identity.id || !fsIDs[*row.Join] {
			return errors.New("foreign standby can join the cold filesystem")
		}
	}
	return nil
}

// New cold absence guards reject duplicate fields and trailing documents. Never
// return raw decode errors: native auth input can contain credentials.
func coldMDSJSON(data []byte, value any) error {
	if len(data) == 0 || len(data) > 4<<20 {
		return errors.New("invalid cold native JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate native field")
				}
				seen[name] = true
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		default:
			return errors.New("invalid native collection")
		}
		_, err = decoder.Token()
		return err
	}
	if walk() != nil {
		return errors.New("invalid cold native JSON")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("invalid cold native JSON")
	}
	if json.Unmarshal(data, value) != nil {
		return errors.New("invalid cold native JSON")
	}
	return nil
}

// MON's canonical cluster UUID may stay stable when the owned control CLI changes.
// Pin the native identity rather than a particular exported compatibility handle.
func (fs *CephFSContainer) readColdFSID(ctx context.Context, cap *cephFSColdMDS) (string, error) {
	data, err := fs.cluster.clientAuthCommand(ctx, "read original cold cluster identity", "fsid")
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
func (fs *CephFSContainer) checkColdSelectedControl(ctx context.Context, control testcontainers.Container) error {
	data, err := command(ctx, control, "ceph", "--connect-timeout", "5", "fsid")
	if err != nil {
		return clientOperationError(ctx, "read selected initial MDS control identity", err)
	}
	_, err = coldMDSFSID(ctx, data, fs.coldMDS)
	return err
}
