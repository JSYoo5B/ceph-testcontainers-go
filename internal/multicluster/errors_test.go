package multicluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type flagged struct{ set bool }

func (f *flagged) Error() string { return fmt.Sprint(f.set) }

func TestEveryBranchAndAnyNode(t *testing.T) {
	isCanceled := func(err error) bool { return err == context.Canceled }
	isSet := func(err error) bool { f, ok := err.(*flagged); return ok && f.set }
	for name, tc := range map[string]struct {
		err        error
		every, any bool
	}{
		"nil":                 {nil, false, false},
		"leaf":                {context.Canceled, true, false},
		"wrapped leaf":        {fmt.Errorf("read: %w", context.Canceled), true, false},
		"join of nils":        {errors.Join(nil, nil), false, false},
		"mixed join":          {errors.Join(context.Canceled, errors.New("query")), false, false},
		"nested join":         {errors.Join(fmt.Errorf("a: %w", errors.Join(context.Canceled, nil)), context.Canceled), true, false},
		"refused then wanted": {errors.Join(&flagged{}, fmt.Errorf("b: %w", &flagged{set: true})), false, true},
		"only refused":        {fmt.Errorf("c: %w", &flagged{}), false, false},
	} {
		if got := EveryBranch(tc.err, isCanceled); got != tc.every {
			t.Errorf("%s: EveryBranch = %t", name, got)
		}
		if got := AnyNode(tc.err, isSet); got != tc.any {
			t.Errorf("%s: AnyNode = %t", name, got)
		}
	}
}
