//go:build all || integration

package cephfs_statfs_mixed_quota_test

import (
	"fmt"
	"slices"
)

const (
	// statfsBlock is the block size libcephfs reports capacity in.
	statfsBlock = 4 << 20
	// statfsByteQuota is the byte quota of every directory that has one, and
	// statfsFileQuota the file quota.
	statfsByteQuota = 64 << 20
	statfsFileQuota = 100
	// statfsFileBytes is the size of the one file in each directory.
	statfsFileBytes = statfsBlock
)

// statfsSpec is one directory of the layout: its quotas, the directory whose
// byte quota statfs must report at it ("" for none), and the recursive bytes
// it holds once every file is written.
type statfsSpec struct {
	Path     string `json:"path"`
	MaxBytes uint64 `json:"max_bytes"`
	MaxFiles uint64 `json:"max_files"`
	RBytes   uint64 `json:"rbytes"`
	ByteRoot string `json:"-"`
	// Trigger marks the directory of the question; every other directory is
	// a control.
	Trigger bool `json:"-"`
}

// statfsLayout lists parents before their children. Each directory holds one
// file of statfsFileBytes.
var statfsLayout = []statfsSpec{
	{Path: "bytes-only", MaxBytes: statfsByteQuota, RBytes: statfsFileBytes, ByteRoot: "bytes-only"},
	{Path: "same", MaxBytes: statfsByteQuota, MaxFiles: statfsFileQuota, RBytes: statfsFileBytes, ByteRoot: "same"},
	{Path: "parent", MaxBytes: statfsByteQuota, RBytes: 3 * statfsFileBytes, ByteRoot: "parent"},
	{Path: "parent/plain", RBytes: statfsFileBytes, ByteRoot: "parent"},
	{Path: "parent/child", MaxFiles: statfsFileQuota, RBytes: statfsFileBytes, ByteRoot: "parent", Trigger: true},
	{Path: "unlimited", RBytes: statfsFileBytes},
}

// statfsDir is what one fresh libcephfs mount read at a directory: its
// inode, each quota attribute with whether it is present (ENODATA is
// absent), its recursive statistics and the statfs fields.
type statfsDir struct {
	Path     string `json:"path"`
	Inode    uint64 `json:"inode"`
	MaxBytes uint64 `json:"max_bytes"`
	BytesSet bool   `json:"max_bytes_present"`
	MaxFiles uint64 `json:"max_files"`
	FilesSet bool   `json:"max_files_present"`
	RBytes   uint64 `json:"rbytes"`
	RFiles   uint64 `json:"rfiles"`
	RSubdirs uint64 `json:"rsubdirs"`
	BSize    uint64 `json:"f_bsize"`
	Blocks   uint64 `json:"f_blocks"`
	BFree    uint64 `json:"f_bfree"`
	BAvail   uint64 `json:"f_bavail"`
	Files    uint64 `json:"f_files"`
}

// statfsPoll is one reading of every directory's recursive bytes before the
// statfs calls.
type statfsPoll struct {
	AfterMS int64             `json:"after_ms"`
	RBytes  map[string]uint64 `json:"rbytes"`
}

// statfsReport is what the observing client reports.
type statfsReport struct {
	Client    uint64       `json:"client"`
	Quota     string       `json:"client_quota"`
	QuotaDF   string       `json:"client_quota_df"`
	Polls     []statfsPoll `json:"polls"`
	Converged bool         `json:"converged"`
	Dirs      []statfsDir  `json:"dirs"`
}

// judgeStatfs compares a report with the layout. The trigger directory not
// reporting its byte quota, while every control does, is the symptom; every
// directory reporting it is its absence. Its error starts with "condition
// not established" when the report cannot answer the question, and with
// "control failed" when a control directory does not report its byte quota:
// no verdict.
func judgeStatfs(layout []statfsSpec, report statfsReport) (symptom bool, detail string, err error) {
	if report.Client == 0 || report.Quota != "true" || report.QuotaDF != "true" {
		return false, "", fmt.Errorf("condition not established: client %d runs with client_quota=%q and client_quota_df=%q", report.Client, report.Quota, report.QuotaDF)
	}
	if !report.Converged || len(report.Polls) == 0 {
		return false, "", fmt.Errorf("condition not established: the recursive bytes did not reach the layout's in %d readings: %+v", len(report.Polls), report.Polls)
	}
	if len(report.Dirs) != len(layout) {
		return false, "", fmt.Errorf("condition not established: %d directories reported, want %d", len(report.Dirs), len(layout))
	}
	seen := map[string]statfsDir{}
	inodes := map[uint64]string{}
	var unlimited *statfsDir
	for i, spec := range layout {
		dir := report.Dirs[i]
		if other, twice := inodes[dir.Inode]; dir.Path != spec.Path || dir.Inode == 0 || twice {
			return false, "", fmt.Errorf("condition not established: directory %d is %q with inode %d (also %q), want %q with an inode of its own", i, dir.Path, dir.Inode, other, spec.Path)
		}
		inodes[dir.Inode], seen[dir.Path] = dir.Path, dir
		// The client reports both quota attributes of a directory as present
		// as soon as one of them is set, the other with the value 0.
		any := spec.MaxBytes != 0 || spec.MaxFiles != 0
		if dir.MaxBytes != spec.MaxBytes || dir.BytesSet != any || dir.MaxFiles != spec.MaxFiles || dir.FilesSet != any {
			return false, "", fmt.Errorf("condition not established: %s has max_bytes %d (present %t) and max_files %d (present %t), want %d and %d, present %t", dir.Path, dir.MaxBytes, dir.BytesSet, dir.MaxFiles, dir.FilesSet, spec.MaxBytes, spec.MaxFiles, any)
		}
		if dir.RBytes != spec.RBytes || dir.BSize != statfsBlock {
			return false, "", fmt.Errorf("condition not established: %s holds %d recursive bytes and reports a block size of %d, want %d and %d", dir.Path, dir.RBytes, dir.BSize, spec.RBytes, statfsBlock)
		}
		if spec.ByteRoot == "" {
			unlimited = &report.Dirs[i]
		}
	}
	// Without a byte quota above it a directory reports the data pool, which
	// must be told apart from a quota.
	if unlimited == nil || unlimited.Blocks <= statfsByteQuota/statfsBlock {
		return false, "", fmt.Errorf("condition not established: the directory without a byte quota reports %+v, not a capacity above the quota's %d blocks", unlimited, statfsByteQuota/statfsBlock)
	}
	var controls, triggers []string
	for _, spec := range layout {
		if spec.ByteRoot == "" {
			continue
		}
		dir, root := seen[spec.Path], seen[spec.ByteRoot]
		blocks := root.MaxBytes / statfsBlock
		free := blocks - min(blocks, root.RBytes/statfsBlock)
		if dir.Blocks == blocks && dir.BFree == free && dir.BAvail == free {
			continue
		}
		what := fmt.Sprintf("%s reports %d blocks with %d free and %d available, want %d with %d free by the byte quota of %s", spec.Path, dir.Blocks, dir.BFree, dir.BAvail, blocks, free, spec.ByteRoot)
		if dir.Blocks == unlimited.Blocks {
			what += fmt.Sprintf("; %d blocks is what the directory without a byte quota reports", unlimited.Blocks)
		}
		if spec.Trigger {
			triggers = append(triggers, what)
		} else {
			controls = append(controls, what)
		}
	}
	if len(controls) > 0 {
		return false, "", fmt.Errorf("control failed: %v", slices.Concat(controls, triggers))
	}
	if len(triggers) > 0 {
		return true, fmt.Sprintf("%v", triggers), nil
	}
	return false, "every directory below a byte quota reports the capacity and the free blocks of that quota", nil
}
