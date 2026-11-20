// Package lockcookie recognizes the cookie encoding used by librbd's managed lock.
package lockcookie

import (
	"strconv"
	"strings"
)

// Valid reports whether id encodes a watch handle. An advisory ID can use the same encoding.
func Valid(id string) bool {
	suffix, ok := strings.CutPrefix(id, "auto ")
	cookie, err := strconv.ParseUint(suffix, 10, 64)
	return ok && err == nil && cookie != 0 && strconv.FormatUint(cookie, 10) == suffix
}
