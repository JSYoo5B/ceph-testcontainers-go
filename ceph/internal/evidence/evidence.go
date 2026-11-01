// Package evidence holds what package ceph, the diagnostics collectors and
// the test finalization share about collecting evidence from a cluster: the
// inventory of the containers and secrets that a cluster tracks, the
// selection of the containers that one collection reads, and the redaction
// and bounding of collected text. It imports no package of this module, so
// package ceph can build an Inventory without importing its readers.
package evidence

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// CrashDocker is the part of the Docker client that reads crash directories.
type CrashDocker interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	CopyFromContainer(context.Context, string, mobycl.CopyFromContainerOptions) (mobycl.CopyFromContainerResult, error)
}

// Member is one container of a collection: a container that a cluster
// tracks, or one that a caller added. Role and Name describe it; Container is
// nil when it was never created.
type Member struct {
	Role      string
	Name      string
	Container testcontainers.Container
}

// Network is one bridge network of a cluster.
type Network struct {
	ID, Name string
}

// Inventory is what one cluster tracks for evidence, taken in one pass. It
// owns nothing: the containers stay the cluster's.
type Inventory struct {
	// Members are the tracked MON, MGR, OSD, MDS, RGW, control and client
	// toolkit containers.
	Members  []Member
	Networks []Network
	// Control is the container that runs Ceph CLI queries, and ControlPlane
	// the separate control container, when the cluster has one (it is also
	// a member).
	Control, ControlPlane testcontainers.Container
	HostNetwork, Closed   bool
	PublicAddress         string
	// Secrets are the values a report masks: keyring keys, service
	// credentials and every client key the cluster generated.
	Secrets []string
}

// Select returns the members of inv and additional, without typed nil
// handles, deduplicated by container ID, sorted by role and name and cut to
// limit, and how many were cut. An additional member without a name or role
// is named extra-<index> with role extra.
func (inv Inventory) Select(additional []Member, limit int) ([]Member, int) {
	members := slices.Clone(inv.Members)
	for i, target := range additional {
		target.Container = Handle(target.Container)
		if target.Name == "" {
			target.Name = fmt.Sprintf("extra-%d", i)
		}
		if target.Role == "" {
			target.Role = "extra"
		}
		members = append(members, target)
	}
	seen := make(map[string]bool)
	unique := members[:0]
	for _, target := range members {
		identity := "missing:" + target.Role + "/" + target.Name
		if target.Container != nil && target.Container.GetContainerID() != "" {
			identity = target.Container.GetContainerID()
		}
		if !seen[identity] {
			seen[identity] = true
			unique = append(unique, target)
		}
	}
	slices.SortFunc(unique, func(a, b Member) int {
		return strings.Compare(a.Role+"/"+a.Name, b.Role+"/"+b.Name)
	})
	if len(unique) > limit {
		return unique[:limit], len(unique) - limit
	}
	return unique, 0
}

// Handle returns ctr, or nil when ctr is a typed nil.
func Handle(ctr testcontainers.Container) testcontainers.Container {
	if ctr != nil {
		value := reflect.ValueOf(ctr)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return nil
			}
		}
	}
	return ctr
}

// SameContainer reports whether a and b are the same container: by ID, or for
// containers without one, by the same pointer.
func SameContainer(a, b testcontainers.Container) bool {
	if a == nil || b == nil {
		return false
	}
	if id := b.GetContainerID(); id != "" {
		return a.GetContainerID() == id
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	return va.Type() == vb.Type() && va.Kind() == reflect.Pointer && va.Pointer() == vb.Pointer()
}

// KeyringPattern matches the key lines of a Ceph keyring; its first group is
// the key.
var KeyringPattern = regexp.MustCompile(`(?m)^\s*key\s*=\s*(\S+)`)

// KeyringSecrets returns the keys of a keyring.
func KeyringSecrets(keyring []byte) []string {
	var secrets []string
	for _, match := range KeyringPattern.FindAllSubmatch(keyring, -1) {
		secrets = append(secrets, strings.Trim(string(match[1]), "\"'"))
	}
	return secrets
}

var secretPattern = regexp.MustCompile(`(?i)((?:secret_key|access_key|password|private_key|authorization|credential|signature|token|secret|key)\s*["']?\s*[:=]\s*["']?)([^"'\s,;}\]]+)`)
var bearerPattern = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
var privateKeyPattern = regexp.MustCompile(`(?s)-----BEGIN (?:[A-Z ]+)?PRIVATE KEY-----.*?-----END (?:[A-Z ]+)?PRIVATE KEY-----`)

// Redactor masks values and recognized secrets. Values are replaced in one
// pass, longest first, so that a replacement is never rewritten again and
// the output grows at most linearly.
func Redactor(values []string) func(string) string {
	values = slices.DeleteFunc(slices.Clone(values), func(value string) bool { return value == "" })
	slices.SortFunc(values, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	values = slices.Compact(values)
	pairs := make([]string, 0, 2*len(values))
	for _, value := range values {
		pairs = append(pairs, value, "[REDACTED]")
	}
	replacer := strings.NewReplacer(pairs...)
	return func(data string) string {
		data = replacer.Replace(data)
		data = privateKeyPattern.ReplaceAllString(data, "[REDACTED PRIVATE KEY]")
		// Bearer tokens first: the secret pattern takes only the word after
		// "Authorization:", which would leave the token behind it.
		data = bearerPattern.ReplaceAllString(data, "${1}[REDACTED]")
		return secretPattern.ReplaceAllString(data, "${1}[REDACTED]")
	}
}

// BoundText returns data as valid UTF-8 of at most limit bytes, cut at a
// rune boundary, and whether it was cut.
func BoundText(data string, limit int) (string, bool) {
	data = strings.ToValidUTF8(data, "�")
	if len(data) <= limit {
		return data, false
	}
	end := limit
	for end > 0 && !utf8.RuneStart(data[end]) {
		end--
	}
	return data[:end], true
}

// ErrorText returns the redacted, bounded text of err, "?" when it is empty,
// and whether it was cut.
func ErrorText(err error, redact func(string) string, limit int) (string, bool) {
	message, clipped := BoundText(redact(err.Error()), limit)
	if message == "" {
		message = "?"
	}
	return message, clipped
}
