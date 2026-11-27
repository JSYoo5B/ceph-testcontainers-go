// Package preflight observes, before a fixture starts, whether the Docker
// engine that the fixture would use meets the fixture's prerequisites, and
// returns a report that is safe to share. It covers the engine and the block
// I/O faults of fault/blockio. See its README for the observation levels and
// what a report does not prove.
package preflight
