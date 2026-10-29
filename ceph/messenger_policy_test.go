package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestMessengerPolicySettings(t *testing.T) {
	settings, err := MessengerPolicy{
		Monitors: []MessengerConnectionMode{MessengerSecure},
		Managers: []MessengerConnectionMode{MessengerCRC},
		OSDs:     []MessengerConnectionMode{MessengerSecure, MessengerCRC},
	}.settings()
	want := []ConfigSetting{
		{Section: "mon", Name: "ms_mon_service_mode", Value: "secure"},
		{Section: "mgr", Name: "ms_mon_service_mode", Value: "crc"},
		{Section: "osd", Name: "ms_service_mode", Value: "secure crc"},
	}
	if err != nil || !slices.Equal(settings, want) {
		t.Fatalf("settings %+v, %v", settings, err)
	}
	if settings, err := (MessengerPolicy{}).settings(); err != nil || len(settings) != 0 {
		t.Fatalf("default policy set %+v, %v", settings, err)
	}
}

func TestMessengerModeListsRefuseInvalidModes(t *testing.T) {
	for name, modes := range map[string][]MessengerConnectionMode{
		"empty":     {},
		"unknown":   {"none"},
		"blank":     {""},
		"duplicate": {MessengerCRC, MessengerCRC},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (MessengerPolicy{OSDs: modes}).settings(); err == nil {
				t.Fatal("daemon policy accepted")
			}
			if _, err := (ClientMessengerPolicy{OtherDaemons: modes}).configLines(); err == nil {
				t.Fatal("client policy accepted")
			}
			if err := WithMessengerPolicy(MessengerPolicy{Managers: modes})(&options{}); err == nil {
				t.Fatal("option accepted")
			}
		})
	}
}

func TestWithMessengerPolicyKeepsCallerSlices(t *testing.T) {
	modes := []MessengerConnectionMode{MessengerSecure}
	var settings options
	if err := WithMessengerPolicy(MessengerPolicy{Monitors: modes})(&settings); err != nil {
		t.Fatal(err)
	}
	modes[0] = MessengerCRC
	if settings.messengerPolicy.Monitors[0] != MessengerSecure {
		t.Fatal("the option shares the caller's slice")
	}
}

func TestMessengerPolicyRefusesMessengerV2Secure(t *testing.T) {
	cluster, err := Run(context.Background(), RunOptions{}.WithCustomizers(WithMessengerMode(MessengerV2Secure), WithMessengerPolicy(MessengerPolicy{Monitors: []MessengerConnectionMode{MessengerSecure}})))
	if err == nil || cluster != nil || !strings.Contains(err.Error(), "MessengerV2Secure") {
		t.Fatalf("Run accepted a policy with MessengerV2Secure: %v", err)
	}
}

func TestClientContainerConfigWritesClientSection(t *testing.T) {
	policy := WithClientMessengerPolicy(ClientMessengerPolicy{
		MonitorsAndManagers: []MessengerConnectionMode{MessengerCRC},
		OtherDaemons:        []MessengerConnectionMode{MessengerSecure, MessengerCRC},
	})
	base := "[global]\nfsid = x\n[client]\nrados osd op timeout = 25\n[client.reader]\nkeyring = k\n"
	got, err := clientContainerConfig([]byte(base), []ClientContainerOption{policy})
	want := "[global]\nfsid = x\n[client]\nms mon client mode = crc\nms client mode = secure crc\nrados osd op timeout = 25\n[client.reader]\nkeyring = k\n"
	if err != nil || string(got) != want {
		t.Fatalf("existing section:\n%s\n%v", got, err)
	}
	got, err = clientContainerConfig([]byte("[global]\nfsid = x"), []ClientContainerOption{policy})
	want = "[global]\nfsid = x\n[client]\nms mon client mode = crc\nms client mode = secure crc\n"
	if err != nil || string(got) != want {
		t.Fatalf("new section:\n%s\n%v", got, err)
	}
	if got, err := clientContainerConfig([]byte(base), nil); err != nil || string(got) != base {
		t.Fatalf("no options changed the config: %q, %v", got, err)
	}
	if _, err := clientContainerConfig([]byte(base), []ClientContainerOption{nil}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil option: %v", err)
	}
	if _, err := clientContainerConfig([]byte(base), []ClientContainerOption{WithClientMessengerPolicy(ClientMessengerPolicy{OtherDaemons: []MessengerConnectionMode{}})}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid client policy: %v", err)
	}
}
