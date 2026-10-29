package multicluster

import (
	"fmt"

	"github.com/testcontainers/testcontainers-go"
)

// SplitConfig separates the one configuration carrier of type O that a
// cross-cluster fixture's Run takes from the customizers of its containers.
// It refuses a Run without the carrier and one with several, even when they
// agree. kind names the fixture and option the carrier in the error.
func SplitConfig[O testcontainers.ContainerCustomizer](kind, option string, opts []testcontainers.ContainerCustomizer) (O, []testcontainers.ContainerCustomizer, error) {
	var config O
	found := false
	rest := make([]testcontainers.ContainerCustomizer, 0, len(opts))
	for _, opt := range opts {
		carrier, ok := opt.(O)
		if !ok {
			rest = append(rest, opt)
			continue
		}
		if found {
			return config, nil, invalid(fmt.Errorf("%s configuration set by more than one %s", kind, option))
		}
		config, found = carrier, true
	}
	if !found {
		return config, nil, invalid(fmt.Errorf("%s requires %s", kind, option))
	}
	return config, rest, nil
}
