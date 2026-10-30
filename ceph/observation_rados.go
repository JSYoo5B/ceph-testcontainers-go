package ceph

import (
	"context"
	"fmt"
)

// The two observations that run `rados` in the client container
// (PGInconsistencies and ObjectWatchers) are implemented here. Which container
// that is stays the client-image role selection of Container.clientContainerContext
// (the control container when the client image equals the essential image,
// otherwise the feature client, refused after native commands are closed), and
// the exported methods keep their argument validation, topology lock and
// pool-policy readiness check. What moves is the lookup order, the command line
// and the error attribution, against the two narrow contracts below.

// radosQuery runs one command in an already selected client container and
// returns its standard output with the original error.
type radosQuery func(ctx context.Context, args ...string) ([]byte, error)

// radosClientLookup selects the client container for one observation. A lookup
// failure is returned as is, before any command is attempted, and is not
// attributed to the observed object.
type radosClientLookup func(ctx context.Context) (radosQuery, error)

func readPGInconsistencies(ctx context.Context, lookup radosClientLookup, pgid PGID) ([]InconsistentObject, error) {
	run, err := lookup(ctx)
	if err != nil {
		return nil, err
	}
	data, err := run(ctx, "rados", "list-inconsistent-obj", string(pgid), "--format=json")
	if err != nil {
		return nil, err
	}
	return decodeInconsistentObjects(data)
}

func readObjectWatchers(ctx context.Context, lookup radosClientLookup, pool, namespace, object string) ([]ObjectWatcher, error) {
	args := []string{"rados", "--pool", pool}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	// rados ignores --format for listwatchers and always prints text.
	run, err := lookup(ctx)
	if err != nil {
		return nil, err
	}
	data, err := run(ctx, append(args, "listwatchers", object)...)
	if err != nil {
		return nil, fmt.Errorf("list watchers of object %q in pool %q: %w", object, pool, err)
	}
	return parseObjectWatchers(string(data))
}
