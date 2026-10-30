//go:build all || integration

package cephfs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"syscall"
)

// This file holds the Docker-free part of the live byte-quota recipe: the
// deterministic bytes the actor writes and the judge of one limited write
// series.
//
// The client enforces ceph.quota.max_bytes itself, against the recursive
// byte count of the quota root that the MDS last sent it, so enforcement
// can lag behind the acknowledged writes. The judge therefore bounds the
// overshoot instead of requiring the write that crosses the limit to fail.

// byteQuotaPattern returns n bytes of the pattern that the actor writes from
// offset 0: byte i is i mod 251, so a shifted or truncated readback differs.
func byteQuotaPattern(n int64) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i % 251)
	}
	return out
}

// byteQuotaDigest is the SHA-256 of the first n pattern bytes.
func byteQuotaDigest(n int64) string {
	sum := sha256.Sum256(byteQuotaPattern(n))
	return hex.EncodeToString(sum[:])
}

// byteQuotaSeries is what the actor reports for one limited write series:
// the limit it observed before writing, the bytes acknowledged by write and
// fsync, the attempts made and the errno that stopped them (0 when none did).
type byteQuotaSeries struct {
	Limit    *uint64 `json:"limit"`
	Samples  uint64  `json:"samples"`
	Acked    *int64  `json:"acked"`
	Attempts uint64  `json:"attempts"`
	Errno    *int    `json:"errno"`
	Size     *int64  `json:"size"`
}

// judgeByteQuotaSeries accepts a series that observed limit before writing,
// acknowledged every chunk wholly below the limit, then failed with EDQUOT
// after at most overshoot bytes beyond it, and whose file size is exactly the
// acknowledged bytes. A series that never failed, failed otherwise, stopped
// before the limit or lacks a field is refused.
func judgeByteQuotaSeries(s byteQuotaSeries, limit uint64, chunk, overshoot int64) error {
	switch {
	case s.Limit == nil || s.Acked == nil || s.Errno == nil || s.Size == nil || s.Samples == 0:
		return errors.New("byte quota series is incomplete")
	case *s.Limit != limit:
		return fmt.Errorf("series observed limit %d, want %d", *s.Limit, limit)
	case *s.Errno == 0:
		return fmt.Errorf("no write failed within %d bytes beyond the limit (acknowledged %d)", overshoot, *s.Acked)
	case *s.Errno != int(syscall.EDQUOT):
		return fmt.Errorf("writes stopped with errno %d, not EDQUOT", *s.Errno)
	case *s.Acked < 0 || *s.Acked%chunk != 0 || s.Attempts != uint64(*s.Acked/chunk)+1:
		return fmt.Errorf("acknowledged %d bytes in %d attempts of %d-byte chunks", *s.Acked, s.Attempts, chunk)
	case *s.Acked+chunk <= int64(limit):
		return fmt.Errorf("EDQUOT after %d bytes, while a further chunk still fit below the limit %d", *s.Acked, limit)
	case *s.Acked > int64(limit)+overshoot:
		return fmt.Errorf("EDQUOT only after %d bytes, more than %d beyond the limit %d", *s.Acked, overshoot, limit)
	case *s.Size != *s.Acked:
		return fmt.Errorf("file size %d differs from the %d acknowledged bytes", *s.Size, *s.Acked)
	}
	return nil
}
