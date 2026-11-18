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
// one, is an error. The entire document must be well-formed, with no second
// root or non-whitespace text outside it. Operation-specific fields remain
// the caller's responsibility.
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
			if text, ok := token.(xml.CharData); ok && len(bytes.TrimSpace(text)) != 0 {
				return RGWResultBody{}, errors.New("text before XML root")
			}
			continue
		}
		var result RGWResultBody
		switch start.Name.Local {
		case "Error":
			var failure struct {
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}
			if err := decoder.DecodeElement(&failure, &start); err != nil {
				return RGWResultBody{}, fmt.Errorf("S3 Error document: %w", err)
			}
			result = RGWResultBody{Error: true, Code: failure.Code, Message: failure.Message}
		case root:
			if err := decoder.Skip(); err != nil {
				return RGWResultBody{}, fmt.Errorf("incomplete result document: %w", err)
			}
		default:
			return RGWResultBody{}, fmt.Errorf("XML root %q is neither %s nor Error", start.Name.Local, root)
		}
		for {
			tail, err := decoder.Token()
			if errors.Is(err, io.EOF) {
				return result, nil
			}
			if err != nil {
				return RGWResultBody{}, fmt.Errorf("invalid XML suffix: %w", err)
			}
			switch value := tail.(type) {
			case xml.CharData:
				if len(bytes.TrimSpace(value)) == 0 {
					continue
				}
			case xml.Comment, xml.ProcInst:
				continue
			}
			return RGWResultBody{}, errors.New("content after XML root")
		}
	}
}
