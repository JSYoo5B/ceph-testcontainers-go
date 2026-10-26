package ceph

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// fakeRados records the commands of one fake client container and counts
// lookups.
type fakeRados struct {
	reply     []byte
	err       error
	lookupErr error
	calls     []string
	lookups   int
}

func (f *fakeRados) lookup(context.Context) (radosQuery, error) {
	f.lookups++
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return func(_ context.Context, args ...string) ([]byte, error) {
		f.calls = append(f.calls, strings.Join(args, " "))
		return f.reply, f.err
	}, nil
}

func TestReadPGInconsistenciesCommandAndDecode(t *testing.T) {
	f := &fakeRados{reply: []byte(scrubInconsistentFixture)}
	got, err := readPGInconsistencies(t.Context(), f.lookup, "7.0")
	if err != nil || len(got) != 1 || got[0].Name != "victim" || len(got[0].Shards) != 2 {
		t.Fatalf("objects = %+v, %v", got, err)
	}
	if want := []string{"rados list-inconsistent-obj 7.0 --format=json"}; !reflect.DeepEqual(f.calls, want) || f.lookups != 1 {
		t.Fatalf("calls = %q, lookups = %d", f.calls, f.lookups)
	}
	f = &fakeRados{reply: []byte(`{"epoch":3,"inconsistents":[]}`)}
	if got, err := readPGInconsistencies(t.Context(), f.lookup, "7.0"); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty native result = %v, %v", got, err)
	}
}

func TestReadPGInconsistenciesErrors(t *testing.T) {
	lookupErr, boom := errors.New("no client"), errors.New("boom")
	f := &fakeRados{lookupErr: lookupErr}
	if got, err := readPGInconsistencies(t.Context(), f.lookup, "7.0"); got != nil || err != lookupErr || len(f.calls) != 0 {
		t.Fatalf("lookup failure = %v, %v, calls %q", got, err, f.calls)
	}
	f = &fakeRados{err: boom}
	if got, err := readPGInconsistencies(t.Context(), f.lookup, "7.0"); got != nil || err != boom {
		t.Fatalf("command failure = %v, %v", got, err)
	}
	for name, reply := range map[string]string{"missing": `{}`, "null": `{"inconsistents":null}`, "no name": `{"inconsistents":[{"object":{}}]}`, "malformed": `{`} {
		f = &fakeRados{reply: []byte(reply)}
		if got, err := readPGInconsistencies(t.Context(), f.lookup, "7.0"); got != nil || err == nil {
			t.Fatalf("%s accepted: %v, %v", name, got, err)
		}
	}
}

func TestReadObjectWatchersCommandAndDecode(t *testing.T) {
	line := "watcher=172.19.0.6:0/3461995546 client.4193 cookie=187650986454688\n"
	f := &fakeRados{reply: []byte(line)}
	got, err := readObjectWatchers(t.Context(), f.lookup, "pool", "", "obj")
	if err != nil || len(got) != 1 || got[0].ClientID != 4193 {
		t.Fatalf("watchers = %+v, %v", got, err)
	}
	if want := []string{"rados --pool pool listwatchers obj"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("calls = %q", f.calls)
	}
	f = &fakeRados{reply: []byte(line)}
	if _, err := readObjectWatchers(t.Context(), f.lookup, "pool", "ns", "obj"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"rados --pool pool --namespace ns listwatchers obj"}; !reflect.DeepEqual(f.calls, want) {
		t.Fatalf("namespace calls = %q", f.calls)
	}
	f = &fakeRados{}
	if got, err := readObjectWatchers(t.Context(), f.lookup, "pool", "", "obj"); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no watchers = %v, %v", got, err)
	}
}

func TestReadObjectWatchersErrorAttribution(t *testing.T) {
	lookupErr, boom := errors.New("no client"), errors.New("boom")
	f := &fakeRados{lookupErr: lookupErr}
	if got, err := readObjectWatchers(t.Context(), f.lookup, "pool", "", "obj"); got != nil || err != lookupErr || len(f.calls) != 0 {
		t.Fatalf("lookup failure must be returned unwrapped: %v, %v", got, err)
	}
	f = &fakeRados{err: boom}
	got, err := readObjectWatchers(t.Context(), f.lookup, "pool", "", "obj")
	if got != nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), `"obj"`) || !strings.Contains(err.Error(), `"pool"`) {
		t.Fatalf("command failure must name the object and keep the cause: %v, %v", got, err)
	}
	f = &fakeRados{reply: []byte("garbage\n")}
	if got, err := readObjectWatchers(t.Context(), f.lookup, "pool", "", "obj"); got != nil || err == nil {
		t.Fatalf("malformed output accepted: %v, %v", got, err)
	}
}
