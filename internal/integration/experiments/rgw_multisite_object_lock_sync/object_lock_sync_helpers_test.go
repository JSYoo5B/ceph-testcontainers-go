//go:build all || integration

package rgw_multisite_object_lock_sync_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// lockConfig is the meaning of a bucket's object lock configuration: whether
// it is enabled, and its default retention rule, if any. Two XML documents
// that differ in formatting only are the same lockConfig.
type lockConfig struct {
	Enabled bool
	Mode    string
	Days    int
	Years   int
}

func (c lockConfig) String() string {
	return fmt.Sprintf("enabled=%t mode=%q days=%d years=%d", c.Enabled, c.Mode, c.Days, c.Years)
}

// document is the body of PUT ?object-lock for the configuration.
func (c lockConfig) document() []byte {
	period := fmt.Sprintf("<Days>%d</Days>", c.Days)
	if c.Years != 0 {
		period = fmt.Sprintf("<Years>%d</Years>", c.Years)
	}
	return []byte(`<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><ObjectLockEnabled>Enabled</ObjectLockEnabled>` +
		`<Rule><DefaultRetention><Mode>` + c.Mode + `</Mode>` + period + `</DefaultRetention></Rule></ObjectLockConfiguration>`)
}

// parseLockConfig reads the body of a successful GET ?object-lock.
func parseLockConfig(body []byte) (lockConfig, error) {
	var doc struct {
		XMLName xml.Name `xml:"ObjectLockConfiguration"`
		Enabled string   `xml:"ObjectLockEnabled"`
		Rule    *struct {
			Retention *struct {
				Mode  string `xml:"Mode"`
				Days  int    `xml:"Days"`
				Years int    `xml:"Years"`
			} `xml:"DefaultRetention"`
		} `xml:"Rule"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return lockConfig{}, err
	}
	config := lockConfig{Enabled: doc.Enabled == "Enabled"}
	if doc.Enabled != "" && doc.Enabled != "Enabled" {
		return lockConfig{}, fmt.Errorf("ObjectLockEnabled is %q", doc.Enabled)
	}
	if doc.Rule != nil && doc.Rule.Retention != nil {
		config.Mode, config.Days, config.Years = doc.Rule.Retention.Mode, doc.Rule.Retention.Days, doc.Rule.Retention.Years
	}
	return config, nil
}

// lockAbsent is the S3 error code of GET ?object-lock on a bucket that has
// no object lock configuration.
const lockAbsent = "ObjectLockConfigurationNotFoundError"

// lockRead is one GET ?object-lock of a bucket in a zone: the answer, or the
// way the request ended without one.
type lockRead struct {
	Zone, Bucket string
	Err          string
	Status       int
	Code         string
	// Config is the parsed body of a 200; Unparsed is why it is not.
	Config   lockConfig
	Unparsed string
	// Raw is the response body as received, cut to lockRawBound bytes. The
	// parsed fields cannot tell every body apart: a rule in a layout the
	// reader does not know parses like no rule at all.
	Raw string
}

// lockRawBound bounds the response body kept as evidence. A configuration
// of the case is about 250 bytes.
const lockRawBound = 1024

// rawBody is the body as evidence: bounded, on one line.
func rawBody(body []byte) string {
	cut := ""
	if len(body) > lockRawBound {
		body, cut = body[:lockRawBound], fmt.Sprintf(" (cut, %d bytes in all)", len(body))
	}
	return fmt.Sprintf("%q%s", body, cut)
}

// parseVersioning reads the Status of a GET ?versioning body. A bucket that
// never had versioning answers a configuration without a Status, which is
// returned as "".
func parseVersioning(body []byte) (string, error) {
	var doc struct {
		XMLName xml.Name `xml:"VersioningConfiguration"`
		Status  string   `xml:"Status"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return "", err
	}
	return doc.Status, nil
}

// judgeVersioning judges the read-back of the versioning state of the
// bucket made with object lock, in the zone it was made in. Object lock
// requires versioning; a bucket that reports object lock without it is not
// the prerequisite the case compares zones for.
func judgeVersioning(status int, body []byte, err error) string {
	if err != nil || status != http.StatusOK {
		return fmt.Sprintf("condition not established: the source zone does not answer the versioning of the bucket made with object lock: status=%d error=%v", status, err)
	}
	state, err := parseVersioning(body)
	if err != nil || state != "Enabled" {
		return fmt.Sprintf("condition not established: the bucket made with object lock does not have versioning enabled in the source zone: status %q, body %s (%v)", state, rawBody(body), err)
	}
	return ""
}

func (r lockRead) String() string {
	switch {
	case r.Err != "":
		return fmt.Sprintf("zone %s bucket %s: request failed: %s", r.Zone, r.Bucket, r.Err)
	case r.Status != http.StatusOK:
		return fmt.Sprintf("zone %s bucket %s: status=%d code=%q", r.Zone, r.Bucket, r.Status, r.Code)
	case r.Unparsed != "":
		return fmt.Sprintf("zone %s bucket %s: status=200, body not a configuration: %s", r.Zone, r.Bucket, r.Unparsed)
	}
	return fmt.Sprintf("zone %s bucket %s: status=200 %s", r.Zone, r.Bucket, r.Config)
}

// has reports whether the read answered exactly the configuration.
func (r lockRead) has(want lockConfig) bool {
	return r.Err == "" && r.Status == http.StatusOK && r.Unparsed == "" && r.Config == want
}

// absent reports whether the read answered that the bucket has no object
// lock configuration.
func (r lockRead) absent() bool {
	return r.Err == "" && r.Status == http.StatusNotFound && r.Code == lockAbsent
}

// judgeSource judges the read-back of the configuration in the zone it was
// applied in. Nothing can be said about its propagation unless the source
// itself answers it.
func judgeSource(read lockRead, want lockConfig) string {
	if !read.has(want) {
		return fmt.Sprintf("condition not established: the source zone does not read back the configuration it accepted (%s): %s", want, read)
	}
	return ""
}

// judgeDestination judges the last read of the configuration in the other
// zone after the metadata checkpoint. Only an answer about the configuration
// is the defect: none at all, or another one. A transport failure, a refusal
// or an unsupported operation says nothing about propagation.
func judgeDestination(read lockRead, want lockConfig) string {
	switch {
	case read.has(want):
		return ""
	case read.absent():
		return fmt.Sprintf("observed defect: the destination zone has no object lock configuration for a bucket whose source has %s: %s", want, read)
	case read.Err == "" && read.Status == http.StatusOK && read.Unparsed == "":
		return fmt.Sprintf("observed defect: the destination zone answers another object lock configuration than the source's %s: %s", want, read)
	}
	return fmt.Sprintf("another fault: the destination zone did not answer the bucket's object lock configuration: %s", read)
}

// judgeUnlocked judges a read of the control bucket, which was created
// without object lock: every zone must answer that it has no configuration.
func judgeUnlocked(read lockRead) string {
	if !read.absent() {
		return fmt.Sprintf("control failed: a bucket created without object lock does not answer 404 %s: %s", lockAbsent, read)
	}
	return ""
}
