package mirror

import (
	"errors"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
)

// ImageOptions is the grouped image configuration of Run. The zero value
// selects nothing: Run then derives the daemon image from the destination
// cluster's Images().RBDAux. Methods return modified copies, so a derived value
// never changes its base. Pass the value to Run through WithImageOptions; it is
// not a container customizer itself.
type ImageOptions struct {
	daemonImage    string
	daemonImageSet bool
}

// WithDaemonImage selects the image that runs the rbd-mirror daemons, replacing
// an earlier WithDaemonImage of the same value. It must meet the rbd-aux (or
// all) role requirements and is refused when it is known to be another role of
// a cluster. An explicit empty image is a selection that is refused; it is never
// read as "derive the image". Setup clients are not started from it: each
// cluster's Images().Client supplies them.
func (o ImageOptions) WithDaemonImage(image string) ImageOptions {
	o.daemonImage, o.daemonImageSet = image, true
	return o
}

// Option carries a grouped Run configuration. Like ceph.Option it changes the
// Run settings only: Run consumes it, it never reaches a daemon's container
// request and the customizers recorded for later AddDaemon calls do not include
// it.
type Option func(*runSettings) error

// Customize implements testcontainers.ContainerCustomizer. It does nothing on
// purpose; Run applies an Option to its settings before any effect.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithImageOptions is the one option function that carries an ImageOptions to
// Run. At most one WithImageOptions that selects a daemon image may be passed;
// a second selection is refused even when equal, so argument order never
// decides a conflict.
func WithImageOptions(config ImageOptions) Option {
	return func(s *runSettings) error {
		if !config.daemonImageSet {
			return nil
		}
		if s.daemonImageSet {
			return errors.New("daemon image selected twice")
		}
		s.daemonImage, s.daemonImageSet = config.daemonImage, true
		return nil
	}
}

// runSettings is the effective Run configuration assembled from Options.
type runSettings struct {
	daemonImage    string
	daemonImageSet bool
}

// splitRunOptions applies every Option to the settings and returns the
// remaining raw customizers in argument order. The result is independent of opts
// and stays nil when no raw customizer was passed. A refused composition is an
// invalid request and nothing has been created.
func splitRunOptions(opts []testcontainers.ContainerCustomizer) (runSettings, []testcontainers.ContainerCustomizer, error) {
	var settings runSettings
	var raw []testcontainers.ContainerCustomizer
	for _, opt := range opts {
		if option, ok := opt.(Option); ok {
			if option == nil {
				return runSettings{}, nil, servicehook.Invalid(errors.New("nil RBD mirror option"))
			}
			if err := option(&settings); err != nil {
				return runSettings{}, nil, servicehook.Invalid(err)
			}
			continue
		}
		raw = append(raw, opt)
	}
	return settings, raw, nil
}
