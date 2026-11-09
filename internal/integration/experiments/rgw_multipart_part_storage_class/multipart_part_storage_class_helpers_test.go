//go:build all || integration

package rgw_multipart_part_storage_class_test

import (
	"fmt"
	"net/http"
)

// mpRead is one GET or HEAD of a part number, as RGW answered it.
type mpRead struct {
	Method      string
	Part        int
	Status      int
	Length      int64
	PartsCount  string
	Code        string
	BodyMatches bool
}

func (r mpRead) String() string {
	return fmt.Sprintf("%s partNumber=%d status=%d content-length=%d parts-count=%q code=%q body-matches=%t",
		r.Method, r.Part, r.Status, r.Length, r.PartsCount, r.Code, r.BodyMatches)
}

// partOK reports whether a read of an existing part answered its bytes: a
// 206 (200 is also valid for a part read) whose length is the part's and,
// for a GET, whose body is the part.
func partOK(read mpRead, want []byte) bool {
	if read.Status != http.StatusPartialContent && read.Status != http.StatusOK {
		return false
	}
	if read.Length != int64(len(want)) || read.PartsCount != "2" {
		return false
	}
	return read.Method != http.MethodGet || read.BodyMatches
}

// beyondRefused reports whether a GET of a part number past the last part
// was refused as out of range: 400 with the S3 code InvalidPart, as every
// measured build answers it. An authorization or server failure does not
// show that the part number was out of range.
func beyondRefused(read mpRead) bool {
	return read.Method == http.MethodGet && read.Status == http.StatusBadRequest && read.Code == "InvalidPart"
}
