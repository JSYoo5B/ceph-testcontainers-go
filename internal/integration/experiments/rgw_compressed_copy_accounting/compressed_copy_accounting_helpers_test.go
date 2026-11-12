//go:build all || integration

package rgw_compressed_copy_accounting_test

import (
	"bytes"
	"fmt"
	"slices"
)

// ccCompression is the decoded user.rgw.compression attribute of an
// object, as radosgw-admin object stat prints it.
type ccCompression struct {
	Type     string `json:"compression_type"`
	OrigSize uint64 `json:"orig_size"`
	Blocks   []struct {
		Len uint64 `json:"len"`
	} `json:"blocks"`
}

// stored is the sum of the compressed block lengths, the raw bytes the
// object occupies.
func (c ccCompression) stored() uint64 {
	var n uint64
	for _, b := range c.Blocks {
		n += b.Len
	}
	return n
}

// ccIndexEntry is the bucket index entry of the case's one object: Size is
// the raw (possibly compressed) byte count, AccountedSize the logical one.
type ccIndexEntry struct {
	Size          uint64 `json:"size"`
	AccountedSize uint64 `json:"accounted_size"`
	StorageClass  string `json:"storage_class"`
}

// ccStats is the rgw.main usage of a bucket: Size is the accounted
// (logical) total, SizeUtilized the raw one, SizeActual the rounded one.
type ccStats struct {
	Size         uint64 `json:"size"`
	SizeActual   uint64 `json:"size_actual"`
	SizeUtilized uint64 `json:"size_utilized"`
	NumObjects   uint64 `json:"num_objects"`
}

// ccObservation is everything the case reads about the object of one
// bucket, through radosgw-admin and S3.
type ccObservation struct {
	Bucket      string
	Compression *ccCompression
	Index       ccIndexEntry
	Stats       ccStats
	ListedSize  int64
	Data        []byte
}

func (o ccObservation) String() string {
	compression := "none"
	if o.Compression != nil {
		compression = fmt.Sprintf("%s orig_size=%d stored=%d", o.Compression.Type, o.Compression.OrigSize, o.Compression.stored())
	}
	return fmt.Sprintf("bucket=%s compression=(%s) index=(size=%d accounted_size=%d storage_class=%q) stats=(size=%d size_actual=%d size_utilized=%d num_objects=%d) listed_size=%d get_bytes=%d",
		o.Bucket, compression, o.Index.Size, o.Index.AccountedSize, o.Index.StorageClass,
		o.Stats.Size, o.Stats.SizeActual, o.Stats.SizeUtilized, o.Stats.NumObjects, o.ListedSize, len(o.Data))
}

// judgeCopyAccounting judges the logical size a copy's destination
// accounts. compressed holds the source's compressed lengths (the index raw
// size and the sum of the compression blocks): the byte counts a copy that
// decompresses the source reads.
//
// The documented symptom: GET returns the exact bytes, and the index
// accounted_size, the listed Size and the bucket stats size all report one
// of the source's compressed lengths. All three equal to the logical length
// is its absence. Other bytes, sizes that disagree with each other, or
// another length are another fault: no verdict.
func judgeCopyAccounting(o ccObservation, data []byte, compressed []uint64) (symptom bool, detail string, err error) {
	logical := uint64(len(data))
	sizes := fmt.Sprintf("index accounted_size=%d, listed Size=%d, bucket stats size=%d", o.Index.AccountedSize, o.ListedSize, o.Stats.Size)
	accounted, listed, stats := o.Index.AccountedSize, uint64(o.ListedSize), o.Stats.Size
	switch {
	case !bytes.Equal(o.Data, data):
		return false, "", fmt.Errorf("another fault: %s: GET returned %d bytes that differ from the %d written", o.Bucket, len(o.Data), len(data))
	case accounted == logical && listed == logical && stats == logical:
		return false, fmt.Sprintf("%s: %s, the logical length", o.Bucket, sizes), nil
	case accounted == listed && listed == stats && accounted < logical && slices.Contains(compressed, accounted):
		return true, fmt.Sprintf("%s: %s, the source's compressed length, for a logical length of %d; GET returns every byte", o.Bucket, sizes, logical), nil
	default:
		return false, "", fmt.Errorf("another fault: %s: %s, want the logical length %d or one compressed length of %v", o.Bucket, sizes, logical, compressed)
	}
}
