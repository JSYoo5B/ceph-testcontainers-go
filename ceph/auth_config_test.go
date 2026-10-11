package ceph

import (
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestNormalizeAuthConfigDefaults(t *testing.T) {
	for name, test := range map[string]struct {
		config                      AuthConfig
		service, preferred, allowed string
	}{
		"key type only":        {AuthConfig{KeyType: CipherAES}, "aes", "aes", "aes"},
		"strong key type":      {AuthConfig{KeyType: CipherAES256K}, "aes256k", "aes256k", "aes256k"},
		"weaker service":       {AuthConfig{KeyType: CipherAES256K, ServiceCipher: CipherAES}, "aes", "aes256k", "aes256k,aes"},
		"weaker preferred":     {AuthConfig{KeyType: CipherAES256K, PreferredCipher: CipherAES}, "aes256k", "aes", "aes256k,aes"},
		"explicit allowed":     {AuthConfig{KeyType: CipherAES, AllowedCiphers: []CephxCipher{CipherAES256K, CipherAES}}, "aes", "aes", "aes256k,aes"},
		"lifetimes only":       {AuthConfig{MonTicketTTL: time.Second}, "", "", ""},
		"zero configuration":   {AuthConfig{}, "", "", ""},
		"allowed contains all": {AuthConfig{KeyType: CipherAES, ServiceCipher: CipherAES256K, AllowedCiphers: []CephxCipher{CipherAES, CipherAES256K}}, "aes256k", "aes", "aes,aes256k"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := normalizeAuthConfig(test.config)
			if err != nil {
				t.Fatal(err)
			}
			allowed := make([]string, len(got.AllowedCiphers))
			for i, cipher := range got.AllowedCiphers {
				allowed[i] = string(cipher)
			}
			if string(got.ServiceCipher) != test.service || string(got.PreferredCipher) != test.preferred || strings.Join(allowed, ",") != test.allowed {
				t.Fatalf("service=%s preferred=%s allowed=%v", got.ServiceCipher, got.PreferredCipher, allowed)
			}
		})
	}
}

func TestNormalizeAuthConfigRefusals(t *testing.T) {
	for name, config := range map[string]AuthConfig{
		"unknown key type":         {KeyType: "aes128"},
		"unknown service cipher":   {KeyType: CipherAES, ServiceCipher: "none"},
		"unknown allowed cipher":   {KeyType: CipherAES, AllowedCiphers: []CephxCipher{CipherAES, ""}},
		"ciphers without key type": {ServiceCipher: CipherAES},
		"duplicate allowed cipher": {KeyType: CipherAES, AllowedCiphers: []CephxCipher{CipherAES, CipherAES}},
		"allowed without key type": {AllowedCiphers: []CephxCipher{CipherAES}},
		"allowed lacks key type":   {KeyType: CipherAES256K, AllowedCiphers: []CephxCipher{CipherAES}},
		"allowed lacks service":    {KeyType: CipherAES, ServiceCipher: CipherAES256K, AllowedCiphers: []CephxCipher{CipherAES}},
		"negative MON ticket":      {MonTicketTTL: -time.Second},
		"negative service ticket":  {ServiceTicketTTL: -time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeAuthConfig(config); err == nil {
				t.Fatalf("%+v accepted", config)
			}
		})
	}
}

func TestNormalizeAuthConfigKeepsCallerSlice(t *testing.T) {
	allowed := []CephxCipher{CipherAES256K, CipherAES}
	got, err := normalizeAuthConfig(AuthConfig{KeyType: CipherAES, AllowedCiphers: allowed})
	if err != nil {
		t.Fatal(err)
	}
	got.AllowedCiphers[0] = CipherAES
	if allowed[0] != CipherAES256K {
		t.Fatal("the normalized configuration shares the caller's slice")
	}
}

func TestAuthConfigEnvironment(t *testing.T) {
	config, err := normalizeAuthConfig(AuthConfig{KeyType: CipherAES256K, ServiceCipher: CipherAES,
		MonTicketTTL: 1500 * time.Millisecond, ServiceTicketTTL: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"CEPH_KEY_TYPE":                "aes256k",
		"CEPH_AUTH_SERVICE_CIPHER":     "aes",
		"CEPH_AUTH_ALLOWED_CIPHERS":    "aes256k,aes",
		"CEPH_AUTH_PREFERRED_CIPHER":   "aes256k",
		"CEPH_AUTH_MON_TICKET_TTL":     "1.5",
		"CEPH_AUTH_SERVICE_TICKET_TTL": "120",
	}
	if got := config.environment(); !maps.Equal(got, want) {
		t.Fatalf("environment %v", got)
	}
	if got := (AuthConfig{}).environment(); len(got) != 0 {
		t.Fatalf("default configuration set %v", got)
	}
}

// Tentacle before 20.2.4 has no --key-type, like Squid: AES keys are its
// default and only type. Generation follows the detected option, not the
// release number.
func TestGenerateKeyArgsFollowsKeyTypeOption(t *testing.T) {
	for _, test := range []struct {
		version string
		option  bool
		keyType CephxCipher
		want    []string
	}{
		{"20.2.4", true, "", []string{"ceph-authtool", "--gen-print-key"}},
		{"20.2.4", true, CipherAES, []string{"ceph-authtool", "--gen-print-key", "--key-type", "aes"}},
		{"20.2.4", true, CipherAES256K, []string{"ceph-authtool", "--gen-print-key", "--key-type", "aes256k"}},
		{"20.2.3", false, "", []string{"ceph-authtool", "--gen-print-key"}},
		{"20.2.3", false, CipherAES, []string{"ceph-authtool", "--gen-print-key"}},
		{"20.2.3", false, CipherAES256K, nil},
		{"19.2.5", false, CipherAES, []string{"ceph-authtool", "--gen-print-key"}},
		{"19.2.5", false, CipherAES256K, nil},
	} {
		c := &Container{cephVersion: test.version, keyTypeOption: test.option}
		got, err := c.generateKeyArgs(test.keyType)
		if (test.want == nil) != (err != nil) || !slices.Equal(got, test.want) {
			t.Errorf("%s option=%t %q: %v, %v", test.version, test.option, test.keyType, got, err)
		}
	}
}

func TestWithClientKeyTypeRefusesUnknownCiphers(t *testing.T) {
	var settings clientOptions
	if err := WithClientKeyType("aes128")(&settings); err == nil {
		t.Fatal("unknown cipher accepted")
	}
	if err := WithClientKeyType(CipherAES256K)(&settings); err != nil || settings.keyType != CipherAES256K {
		t.Fatalf("aes256k: %v %q", err, settings.keyType)
	}
}
