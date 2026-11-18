//go:build all || (integration && features && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import "testing"

func TestServicePackageReadbackUsesPhaseInventory(t *testing.T) {
	for _, kind := range []string{"rbd", "cephfs"} {
		t.Run(kind, func(t *testing.T) {
			first := servicePackageExpected(kind, 1)
			phases := servicePackageExpected(kind, 2)
			if len(first) != 1 || len(phases) != 2 || phases[0] != first[0] || phases[1] == first[0] {
				t.Fatalf("phase inventories %v then %v", first, phases)
			}
			for name, tc := range map[string]struct {
				read servicePackageReadback
				ok   bool
			}{
				"both present":            {servicePackageReadback{Listed: phases, Checked: phases}, true},
				"earlier object vanished": {servicePackageReadback{Listed: phases[1:], Checked: phases[1:]}, false},
				"new object omitted":      {servicePackageReadback{Listed: first, Checked: phases}, false},
				"empty enumeration":       {servicePackageReadback{Checked: phases}, false},
				"unchecked earlier data":  {servicePackageReadback{Listed: phases, Checked: phases[1:]}, false},
			} {
				t.Run(name, func(t *testing.T) {
					err := requireServicePackageReadback(phases, tc.read)
					if (err == nil) != tc.ok {
						t.Fatalf("readback %+v: %v", tc.read, err)
					}
				})
			}
		})
	}
}
