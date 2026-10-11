package ceph

import (
	"bytes"
	"strings"
)

// withConfigSectionValues returns config with lines added at the start of
// section, or with a new section at the end when config has none. The keys
// must not already be set in that section.
func withConfigSectionValues(config []byte, section string, lines []string) []byte {
	if len(lines) == 0 {
		return bytes.Clone(config)
	}
	block := strings.Join(lines, "\n") + "\n"
	header := "[" + section + "]"
	var out bytes.Buffer
	inserted := false
	for _, line := range bytes.SplitAfter(config, []byte("\n")) {
		out.Write(line)
		if !inserted && strings.TrimSpace(string(line)) == header {
			if !bytes.HasSuffix(line, []byte("\n")) {
				out.WriteByte('\n')
			}
			out.WriteString(block)
			inserted = true
		}
	}
	if !inserted {
		if out.Len() > 0 && !bytes.HasSuffix(out.Bytes(), []byte("\n")) {
			out.WriteByte('\n')
		}
		out.WriteString(header + "\n" + block)
	}
	return out.Bytes()
}
