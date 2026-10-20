package ceph

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// initialOSDStarts bounds how many initial OSD containers Run creates and
// starts at once.
const initialOSDStarts = 4

// startInitialOSDs adds the initial OSDs of Run. It registers them one after
// another, so that their IDs, keys and CRUSH host buckets are allocated in
// the order of the layout as with sequential AddOSDWithConfig calls, then
// creates and starts their containers concurrently and waits for all of them
// to be up and in. It holds c.mu throughout, like AddOSDWithConfig; the
// concurrent container starts do not take it. Every registered OSD stays in
// c.osds, with its container when one was created, also when another OSD
// fails, so that Terminate removes them all.
func (c *Container) startInitialOSDs(ctx context.Context) error {
	if c.settings.osds == 0 {
		return nil
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	buckets := make(map[string]bool)
	launches := make([]osdLaunch, 0, c.settings.osds)
	for i := range c.settings.osds {
		var config OSDConfig
		if len(c.settings.initialOSDs) != 0 {
			config = c.settings.initialOSDs[i]
		}
		config, err := normalizeOSDConfig(resolveOSDDefaults(c.settings, config))
		if err != nil {
			return err
		}
		launch, err := c.registerOSD(ctx, config, buckets)
		if err != nil {
			return err
		}
		launches = append(launches, launch)
	}
	if err := c.startOSDs(ctx, launches); err != nil {
		return err
	}
	ids := make([]int, len(launches))
	for i, launch := range launches {
		ids[i] = launch.osd.ID
	}
	if err := c.waitOSDsUp(ctx, ids); err != nil {
		return err
	}
	for _, launch := range launches {
		if err := c.assignOSDDeviceClass(ctx, launch.osd); err != nil {
			return err
		}
	}
	return nil
}

// startOSDs starts the containers of launches, at most initialOSDStarts at
// once and in their order. After the first failure, or once ctx ends, it
// starts no further OSD but waits for those already starting. The error of
// the first failure comes first, joined with those of the others.
func (c *Container) startOSDs(ctx context.Context, launches []osdLaunch) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	failed := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(errs) != 0
	}
	slots := make(chan struct{}, min(len(launches), initialOSDStarts))
	var skipped *osdLaunch
	for i, launch := range launches {
		acquired := false
		select {
		case slots <- struct{}{}:
			acquired = true
		case <-ctx.Done():
		}
		if !acquired {
			skipped = &launches[i]
			break
		}
		// A slot and the end of ctx can be ready together.
		if ctx.Err() != nil || failed() {
			<-slots
			skipped = &launches[i]
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if err := c.startOSD(ctx, launch); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(errs) == 0 && skipped != nil {
		return fmt.Errorf("start ceph osd.%d: %w", skipped.osd.ID, context.Cause(ctx))
	}
	return errors.Join(errs...)
}

// startInitialManagers creates and starts the first initial MGR without
// waiting for it to register, so that it boots while Run adds the initial
// OSDs, which need no MGR to register or boot. finishInitialManagers then
// waits for it and adds the others, so that the first MGR still becomes the
// active one. Every MGR is owned from the creation of its identity, as with
// AddManager.
func (c *Container) startInitialManagers(ctx context.Context) error {
	if c.settings.managers == 0 {
		return nil
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	_, err := c.startManager(ctx, daemonName(0))
	return err
}

// finishInitialManagers waits for the first initial MGR to register, then
// adds the other initial MGRs one after another.
func (c *Container) finishInitialManagers(ctx context.Context) error {
	if c.settings.managers == 0 {
		return nil
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.waitManagerRegistered(ctx, daemonName(0)); err != nil {
		return err
	}
	for i := 1; i < c.settings.managers; i++ {
		name := daemonName(i)
		if _, err := c.startManager(ctx, name); err != nil {
			return err
		}
		if err := c.waitManagerRegistered(ctx, name); err != nil {
			return err
		}
	}
	return nil
}
