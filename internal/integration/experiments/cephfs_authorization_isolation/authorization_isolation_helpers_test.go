//go:build all || integration

package cephfs_authorization_isolation_test

import (
	"fmt"
	"strings"
)

// isolationSnapshotBytes is the content of the file below /dir1/child.
const isolationSnapshotBytes = "dir1-child-bytes"

// capsProbe is what one principal's probe reported on one filesystem, and
// whether its MDS caps grant writes there.
type capsProbe struct {
	Caps       string
	Filesystem string
	Writable   bool
	Result     map[string]any
}

// judgeFilesystemCaps judges the probes of every clause variant. A create
// or a mkdir that succeeds on a filesystem the caps grant only read is the
// symptom; EACCES for both on every such filesystem is its absence. Its
// error starts with "control failed" when a probe did not mount or read the
// seed, or a writable filesystem refused a create or a mkdir, and with
// "another fault" when a read-only filesystem answered something else: no
// verdict. An error outranks the symptom.
func judgeFilesystemCaps(probes []capsProbe) (symptom bool, detail string, err error) {
	if len(probes) == 0 {
		return false, "", fmt.Errorf("condition not established: no caps variant was probed")
	}
	var controls, faults, symptoms []string
	for _, p := range probes {
		name := fmt.Sprintf("caps %q on %s", p.Caps, p.Filesystem)
		if p.Result["mount"] != "ok" || p.Result["read"] != "seed-of-"+p.Filesystem {
			controls = append(controls, fmt.Sprintf("%s: %v", name, p.Result))
			continue
		}
		for _, op := range []string{"create", "mkdir"} {
			switch got := p.Result[op]; {
			case p.Writable && got != "ok":
				controls = append(controls, fmt.Sprintf("%s (writable): %s=%v", name, op, got))
			case p.Writable:
			case got == "ok":
				symptoms = append(symptoms, fmt.Sprintf("%s (read-only): %s succeeded", name, op))
			case got != "EACCES":
				faults = append(faults, fmt.Sprintf("%s (read-only): %s=%v, want EACCES", name, op, got))
			}
		}
	}
	switch {
	case len(controls) > 0:
		return false, "", fmt.Errorf("control failed: %s", strings.Join(controls, "; "))
	case len(faults) > 0:
		return false, "", fmt.Errorf("another fault: %s", strings.Join(faults, "; "))
	case len(symptoms) > 0:
		return true, fmt.Sprintf("%d of %d probes: %s", len(symptoms), len(probes)*2, strings.Join(symptoms, "; ")), nil
	}
	return false, fmt.Sprintf("%d probes: every create and mkdir on a read-only filesystem was refused with EACCES and succeeded on the writable one", len(probes)), nil
}

// judgeDeletedDirectorySnapshot judges the probes of the principal
// restricted to /dir1 before and after it deleted /dir1/child. A refused
// (EPERM or EACCES) read of the child's file through the earlier snapshot
// after the deletion is the symptom; the snapshot's bytes are its absence.
// Its error starts with "control failed" when / or /dir2 is not refused, with
// "condition not established" when the snapshot did not read before the
// deletion or the child was not deleted, and with "another fault" when the
// snapshot read after the deletion answered something else: no verdict.
func judgeDeletedDirectorySnapshot(before, after map[string]any) (symptom bool, detail string, err error) {
	for _, phase := range []struct {
		name   string
		result map[string]any
	}{{"before the deletion", before}, {"after the deletion", after}} {
		if phase.result["mount /"] != "EPERM" || phase.result["mount /dir2"] != "EPERM" {
			return false, "", fmt.Errorf("control failed: %s the principal of /dir1 was not refused / and /dir2: %v", phase.name, phase.result)
		}
	}
	if before["snapshot read"] != isolationSnapshotBytes || before["snapshot size"] != float64(len(isolationSnapshotBytes)) || before["live read"] != isolationSnapshotBytes {
		return false, "", fmt.Errorf("condition not established: before the deletion the child does not read live and through the snapshot: %v", before)
	}
	if after["deleted"] != "ok" || after["live read"] != "ENOENT" {
		return false, "", fmt.Errorf("condition not established: the live child was not deleted: %v", after)
	}
	switch read := after["snapshot read"]; {
	case read == isolationSnapshotBytes && after["snapshot size"] == float64(len(isolationSnapshotBytes)):
		return false, fmt.Sprintf("after the deletion the snapshot still reads %q", read), nil
	case read == "EPERM" || read == "EACCES":
		return true, fmt.Sprintf("after the deletion the snapshot read of the child is refused with %v (stat %v)", read, after["snapshot size"]), nil
	default:
		return false, "", fmt.Errorf("another fault: after the deletion the snapshot read answered %v with size %v, want %q or a refusal", read, after["snapshot size"], isolationSnapshotBytes)
	}
}

// tenantProbe is one subvolume of the same-named pair: its group, RADOS
// namespace and data pool, its reader's OSD caps, and what the reader's
// probe reported of its own file and objects and of the other's object.
type tenantProbe struct {
	Group     string
	Namespace string
	DataPool  string
	OSDCaps   string
	Result    map[string]any
}

// judgeSameNamedSubvolumes judges the two subvolumes of one name in two
// groups. One RADOS namespace for both, or a reader that reads the other's
// object, is the symptom; distinct namespaces with each reader refused the
// other's object (EPERM) are its absence. Its error starts with "condition
// not established" when a namespace is empty or the data pools differ, with
// "control failed" when a reader cannot read its own file or object or its
// caps do not name exactly its own namespace, and with "another fault" when
// the other's object answered something else: no verdict.
func judgeSameNamedSubvolumes(a, b tenantProbe) (symptom bool, detail string, err error) {
	if a.Namespace == "" || b.Namespace == "" || a.DataPool != b.DataPool {
		return false, "", fmt.Errorf("condition not established: the subvolumes have namespaces %q and %q in data pools %q and %q, want two namespaces of one pool", a.Namespace, b.Namespace, a.DataPool, b.DataPool)
	}
	var symptoms []string
	if a.Namespace == b.Namespace {
		symptoms = append(symptoms, fmt.Sprintf("%s and %s share the RADOS namespace %q", a.Group, b.Group, a.Namespace))
	}
	var absent []string
	for _, pair := range [][2]tenantProbe{{a, b}, {b, a}} {
		own, other := pair[0], pair[1]
		if own.Result["own file"] != "bytes-of-"+own.Group || own.Result["own object"] != "bytes-of-"+own.Group {
			return false, "", fmt.Errorf("control failed: the reader of %s does not read its own data: %v", own.Group, own.Result)
		}
		if a.Namespace != b.Namespace && (!strings.Contains(own.OSDCaps, "namespace="+own.Namespace) || strings.Contains(own.OSDCaps, "namespace="+other.Namespace)) {
			return false, "", fmt.Errorf("control failed: the reader of %s has OSD caps %q, want only its namespace %q", own.Group, own.OSDCaps, own.Namespace)
		}
		switch got := own.Result["other object"]; got {
		case "EPERM":
			absent = append(absent, fmt.Sprintf("the reader of %s is refused the object of %s", own.Group, other.Group))
		case "bytes-of-" + other.Group:
			symptoms = append(symptoms, fmt.Sprintf("the reader of %s reads the object of %s", own.Group, other.Group))
		default:
			if a.Namespace != b.Namespace {
				return false, "", fmt.Errorf("another fault: the reader of %s got %v for the object of %s, want EPERM or its bytes", own.Group, got, other.Group)
			}
		}
	}
	if len(symptoms) > 0 {
		return true, strings.Join(symptoms, "; "), nil
	}
	return false, fmt.Sprintf("namespaces %q and %q; %s", a.Namespace, b.Namespace, strings.Join(absent, "; ")), nil
}
