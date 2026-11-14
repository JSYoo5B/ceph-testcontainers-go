package workflowcheck_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestTaggedRuntimeCachePolicyByCaller(t *testing.T) {
	const directRef = "${{ github.event_name == 'workflow_dispatch' && inputs.ref || '' }}"
	const directPolicy = "${{ inputs.ref == '' || inputs.ref == github.sha }}"
	type policy struct {
		ref  string
		save any // Workflow inputs can be expressions or literal booleans.
	}
	expected := map[string]policy{
		"short.yml/scenarios":         {directRef, directPolicy},
		"arm64.yml/scenarios":         {directRef, directPolicy},
		"periodic.yml/storage":        {"${{ inputs.ref || github.sha }}", directPolicy},
		"periodic.yml/representative": {"${{ needs.pin.outputs.sha }}", "${{ needs.pin.outputs.sha == github.sha }}"},
		"ci-command.yml/runtime":      {"${{ needs.command.outputs.sha }}", false},
	}

	dir := filepath.Join("..", "..", "..", ".github", "workflows")
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		ext := filepath.Ext(file.Name())
		if ext != ".yml" && ext != ".yaml" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var workflow struct {
			Jobs map[string]struct {
				Uses string
				With map[string]any
			}
		}
		if err := yaml.Unmarshal(data, &workflow); err != nil {
			t.Fatalf("%s: %v", file.Name(), err)
		}
		for id, job := range workflow.Jobs {
			if job.Uses != "./.github/workflows/tagged-runtime.yml" {
				continue
			}
			caller := file.Name() + "/" + id
			want, known := expected[caller]
			ref, selectsRef := job.With["ref"]
			if !known {
				if selectsRef {
					t.Errorf("%s selects ref %v without a declared cache policy", caller, ref)
				}
				continue
			}
			delete(expected, caller)
			if !reflect.DeepEqual(ref, want.ref) || !reflect.DeepEqual(job.With["save_caches"], want.save) {
				t.Errorf("%s: ref=%#v save_caches=%#v, want %#v and %#v", caller, ref, job.With["save_caches"], want.ref, want.save)
			}
		}
	}
	for caller := range expected {
		t.Errorf("missing tagged-runtime caller %s", caller)
	}
}
