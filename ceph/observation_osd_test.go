package ceph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	osdUUID0 = "11111111-1111-4111-8111-111111111111"
	osdUUID1 = "22222222-2222-4222-8222-222222222222"
)

func osdDumpFor(entries ...string) string {
	return `{"flags":"","osds":[` + strings.Join(entries, ",") + `]}`
}

func osdEntry(id, uuid string, up, in int) string {
	return `{"osd":` + id + `,"uuid":"` + uuid + `","up":` + itoa(up) + `,"in":` + itoa(in) + `,"weight":1}`
}

func itoa(v int) string { return string(rune('0' + v)) }

func TestReadOwnedOSDStatesSortedAndOneDump(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(osdDumpFor(osdEntry("1", osdUUID1, 1, 0), osdEntry("0", osdUUID0, 1, 1), osdEntry("7", "33333333-3333-4333-8333-333333333333", 1, 1)))}}
	owned := map[int]*osdRegistration{1: {nativeUUID: osdUUID1}, 0: {nativeUUID: osdUUID0}}
	got, err := readOwnedOSDStates(context.Background(), cli.run, owned)
	if err != nil {
		t.Fatal(err)
	}
	want := []OSDState{{ID: 0, UUID: osdUUID0, Up: true, In: true, Weight: 1}, {ID: 1, UUID: osdUUID1, Up: true, In: false, Weight: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("states = %+v, want %+v (unowned osd.7 must not appear)", got, want)
	}
	if !reflect.DeepEqual(cli.calls, []string{osdDumpCmd}) {
		t.Fatalf("calls = %q", cli.calls)
	}
}

func TestReadOwnedOSDStatesNoOwnedIsEmptyNotNil(t *testing.T) {
	cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(osdDumpFor())}}
	got, err := readOwnedOSDStates(context.Background(), cli.run, nil)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("states = %v, %v", got, err)
	}
}

func TestReadOwnedOSDStatesRefusals(t *testing.T) {
	dump := osdDumpFor(osdEntry("0", osdUUID0, 1, 1))
	cases := map[string]map[int]*osdRegistration{
		"nil registration":     {0: nil},
		"purged":               {0: {purged: true, nativeUUID: osdUUID0}},
		"empty registered id":  {0: {}},
		"nil registered id":    {0: {nativeUUID: "00000000-0000-0000-0000-000000000000"}},
		"missing from map":     {0: {nativeUUID: osdUUID0}, 5: {nativeUUID: osdUUID1}},
		"replaced native uuid": {0: {nativeUUID: osdUUID1}},
	}
	var cli *fakeCLI
	for name, owned := range cases {
		cli = &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(dump)}}
		if got, err := readOwnedOSDStates(context.Background(), cli.run, owned); got != nil || err == nil {
			t.Errorf("%s: got %+v, %v", name, got, err)
		}
	}
	cli = &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(dump)}}
	if _, err := readOwnedOSDStates(context.Background(), cli.run, map[int]*osdRegistration{5: {nativeUUID: osdUUID1}}); err == nil || !strings.Contains(err.Error(), "missing from native OSDMap") {
		t.Errorf("missing owned OSD error = %v", err)
	}
	// A nil registered UUID must not match a nil UUID reported by the map.
	nilDump := osdDumpFor(osdEntry("0", "00000000-0000-0000-0000-000000000000", 1, 1))
	cli = &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(nilDump)}}
	if got, err := readOwnedOSDStates(context.Background(), cli.run, map[int]*osdRegistration{0: {nativeUUID: "00000000-0000-0000-0000-000000000000"}}); got != nil || err == nil {
		t.Errorf("nil uuid on both sides: got %+v, %v", got, err)
	}
	// Control: the accepted registration alone passes against the same dump.
	cli = &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(dump)}}
	if got, err := readOwnedOSDStates(context.Background(), cli.run, map[int]*osdRegistration{0: {nativeUUID: osdUUID0}}); err != nil || len(got) != 1 {
		t.Fatalf("control: %+v, %v", got, err)
	}
}

func TestReadOwnedOSDStatesDumpFailuresAreNotEvidence(t *testing.T) {
	boom := errors.New("boom")
	owned := map[int]*osdRegistration{0: {nativeUUID: osdUUID0}}
	cli := &fakeCLI{errs: map[string]error{osdDumpCmd: boom}}
	if got, err := readOwnedOSDStates(context.Background(), cli.run, owned); got != nil || err != boom {
		t.Fatalf("command error = %v, %v", got, err)
	}
	for name, reply := range map[string]string{
		"empty":         ``,
		"null":          `null`,
		"missing flags": `{"osds":[]}`,
		"missing osds":  `{"flags":""}`,
	} {
		cli := &fakeCLI{replies: map[string][]byte{osdDumpCmd: []byte(reply)}}
		if got, err := readOwnedOSDStates(context.Background(), cli.run, owned); got != nil || err == nil {
			t.Errorf("%s: got %+v, %v", name, got, err)
		}
	}
}

func TestRegistrationOfCopiesFields(t *testing.T) {
	if registrationOf(nil) != nil {
		t.Fatal("nil OSD has a registration")
	}
	got := registrationOf(&OSDContainer{purged: true, nativeUUID: osdUUID0})
	if got == nil || !got.purged || got.nativeUUID != osdUUID0 {
		t.Fatalf("registration = %+v", got)
	}
}

func TestContainerOwnedOSDStateUsesRegistrations(t *testing.T) {
	c := &Container{osds: map[int]*OSDContainer{0: {nativeUUID: osdUUID0}, 1: {purged: true, nativeUUID: osdUUID1}}}
	dump, err := decodeOSDPolicyDump([]byte(osdDumpFor(osdEntry("0", osdUUID0, 1, 1), osdEntry("1", osdUUID1, 1, 1))))
	if err != nil {
		t.Fatal(err)
	}
	if st, err := c.ownedOSDState(dump, 0); err != nil || st.ID != 0 {
		t.Fatalf("owned = %+v, %v", st, err)
	}
	for _, id := range []int{1, 9} {
		if _, err := c.ownedOSDState(dump, id); err == nil {
			t.Fatalf("osd.%d accepted", id)
		}
	}
}
