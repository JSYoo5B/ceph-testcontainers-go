package federation

import (
	"reflect"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

var _ testcontainers.Container = (*CephFSMirror)(nil)

func TestCephFSMirrorDirectoryPolicyValidation(t *testing.T) {
	base := CephFSMirrorConfig{SourceFilesystem: "source-fs", DestinationFilesystem: "backup-fs"}
	for _, directories := range [][]string{
		nil,
		{"relative"},
		{"/a", "/a/child"},
		{"/a/child", "/a"},
		{"/a", "/a/../a"},
		{"/", "/anywhere"},
		{"/valid", "/invalid\x00path"},
	} {
		config := base
		config.Directories = directories
		if _, err := normalizeCephFSMirrorConfig(config); err == nil {
			t.Errorf("expected ambiguous or invalid directory policy %q to fail", directories)
		}
	}
	config := base
	config.Directories = []string{"/first/../a", "/a-prefix", "/spaces allowed"}
	normalized, err := normalizeCephFSMirrorConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.DestinationSite != "destination" || !reflect.DeepEqual(normalized.Directories, []string{"/a", "/a-prefix", "/spaces allowed"}) {
		t.Fatalf("unexpected normalization: %+v", normalized)
	}
	if config.Directories[0] != "/first/../a" {
		t.Fatal("normalization changed the caller's directory slice")
	}
}

func TestCephFSMirrorIdentityValidation(t *testing.T) {
	valid := CephFSMirrorConfig{SourceFilesystem: "source-fs", DestinationFilesystem: "backup-fs", Directories: []string{"/"}}
	for name, configure := range map[string]func(*CephFSMirrorConfig){
		"empty source filesystem": func(c *CephFSMirrorConfig) { c.SourceFilesystem = "" },
		"filesystem slash":        func(c *CephFSMirrorConfig) { c.DestinationFilesystem = "backup/fs" },
		"filesystem whitespace":   func(c *CephFSMirrorConfig) { c.SourceFilesystem = "source fs" },
		"site separator":          func(c *CephFSMirrorConfig) { c.DestinationSite = "peer@site" },
		"site newline":            func(c *CephFSMirrorConfig) { c.DestinationSite = "site\n" },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			configure(&config)
			if _, err := normalizeCephFSMirrorConfig(config); err == nil {
				t.Fatal("expected invalid CephFS identity to fail before allocating resources")
			}
		})
	}
}
