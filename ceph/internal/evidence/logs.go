package evidence

import (
	"encoding/binary"
	"errors"
	"io"
)

// ErrLogFrameCut is the error of a multiplexed Docker log stream that ends
// inside a frame header or payload.
var ErrLogFrameCut = errors.New("the log stream ends inside a frame")

// errLogFrameInvalid is the error of a frame header that names no known
// stream or has a nonzero padding byte.
var errLogFrameInvalid = errors.New("invalid Docker log frame")

// DemuxLogs copies the Docker log stream r to stdout and stderr. The stream
// of a container with a TTY is copied to stdout as it is. A multiplexed
// stream is copied frame by frame through a fixed-size buffer, so a frame
// header that advertises a huge size allocates nothing. A stream that ends
// between frames is complete; one that ends inside a frame, in its header or
// its payload, returns ErrLogFrameCut.
func DemuxLogs(r io.Reader, tty bool, stdout, stderr io.Writer) error {
	if tty {
		_, err := io.Copy(stdout, r)
		return err
	}
	header := make([]byte, 8)
	for {
		_, err := io.ReadFull(r, header)
		switch {
		case err == io.EOF:
			return nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return ErrLogFrameCut
		case err != nil:
			return err
		}
		var w io.Writer
		switch header[0] {
		case 1:
			w = stdout
		case 2:
			w = stderr
		}
		if w == nil || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return errLogFrameInvalid
		}
		if _, err := io.CopyN(w, r, int64(binary.BigEndian.Uint32(header[4:]))); err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				return ErrLogFrameCut
			}
			return err
		}
	}
}
