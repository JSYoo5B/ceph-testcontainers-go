//go:build all || integration

package harness

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
)

// RGWOwnBuckets creates the buckets names in order with create and records
// each one whose creation succeeded. Before the first creation it registers
// a t.Cleanup that passes exactly the recorded names to teardown, so a
// failed or interrupted creation (create may also stop the test) still
// tears down every bucket already made and never one that was not. It stops
// at the first failed creation and returns its error.
func RGWOwnBuckets(t testing.TB, create func(name string) error, teardown func(created []string), names ...string) error {
	t.Helper()
	var created []string
	t.Cleanup(func() { teardown(slices.Clone(created)) })
	for _, name := range names {
		if err := create(name); err != nil {
			return fmt.Errorf("create bucket %s: %w", name, err)
		}
		created = append(created, name)
	}
	return nil
}

// RGWResultBody is the classified body of an S3 response whose status may
// be sent before the operation finishes: CompleteMultipartUpload and
// CopyObject can answer 200 and then report an S3 Error document in the
// body. Error is true for such a document, whatever its namespace, and Code
// and Message are its fields; otherwise the root is the expected result.
type RGWResultBody struct {
	Error   bool
	Code    string
	Message string
}

// RGWClassifyResultBody classifies body by its XML root element: an Error
// root, in any namespace, is an S3 error; the root named root, in any
// namespace, is the operation's result. Any other root, or a body without
// one, is an error, so a malformed answer is never taken for either.
func RGWClassifyResultBody(body []byte, root string) (RGWResultBody, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return RGWResultBody{}, errors.New("body has no XML root element")
		}
		if err != nil {
			return RGWResultBody{}, fmt.Errorf("body is not XML: %w", err)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "Error":
			var failure struct {
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}
			if err := decoder.DecodeElement(&failure, &start); err != nil {
				return RGWResultBody{}, fmt.Errorf("S3 Error document: %w", err)
			}
			return RGWResultBody{Error: true, Code: failure.Code, Message: failure.Message}, nil
		case root:
			return RGWResultBody{}, nil
		default:
			return RGWResultBody{}, fmt.Errorf("XML root %q is neither %s nor Error", start.Name.Local, root)
		}
	}
}
