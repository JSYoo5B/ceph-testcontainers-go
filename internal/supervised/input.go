package supervised

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// ErrInputLimit refuses a write that would take the input past its limit;
// nothing of it is written, and the input stays open.
var ErrInputLimit = errors.New("the write would exceed the input limit")

// ErrInputEnded reports a write that the command's input did not take: the
// command ended, or its input stream failed. Part of the data may have been
// written before.
var ErrInputEnded = errors.New("the command's input has ended")

// Input is the standard input of one RunInput call.
type Input struct {
	// Limit bounds the bytes written to the command in all.
	Limit int64
	// Writes carries the data to write, in order. Each write is taken whole
	// or refused whole with ErrInputLimit.
	Writes <-chan InputWrite
	// Closed ends the input when it is closed: the command gets the end of
	// input after everything written before.
	Closed <-chan struct{}
	// Finished is closed once RunInput takes no more writes.
	Finished chan<- struct{}
}

// InputWrite is one write to an Input. RunInput owns Data once it took the
// write, and sends its outcome to Done, which must have room for it.
type InputWrite struct {
	Data []byte
	Done chan<- error
}

// Attachment is one started exec whose standard streams the caller holds.
type Attachment struct {
	// Input writes to the exec's standard input; CloseWrite ends it, and
	// Close closes the whole connection, which unblocks Input and Output.
	Input interface {
		io.Writer
		CloseWrite() error
		Close() error
	}
	// Output is the exec's stdout and stderr in Docker's multiplexed stream
	// format; it ends when the exec has ended.
	Output io.Reader
	// ExitCode returns the exit code of the exec after its output ended.
	ExitCode func(context.Context) (int, error)
	// Release frees what the attachment holds, after Close.
	Release func()
}

// Attacher starts an exec with its standard input attached. A container that
// implements it runs RunInput's supervisor itself; for any other the Docker
// engine runs it, in the container with the same ID.
type Attacher interface {
	AttachExec(ctx context.Context, cmd, env []string) (Attachment, error)
}

// dockerAttacher attaches to an exec through the Docker engine.
type dockerAttacher struct{ id string }

// hijacked is the attached connection of a Docker exec.
type hijacked struct{ *mobycl.HijackedResponse }

func (h hijacked) Write(p []byte) (int, error) { return h.Conn.Write(p) }

func (h hijacked) Close() error { return h.Conn.Close() }

func (a dockerAttacher) AttachExec(ctx context.Context, cmd, env []string) (Attachment, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return Attachment{}, err
	}
	created, err := docker.ExecCreate(ctx, a.id, mobycl.ExecCreateOptions{
		AttachStdin: true, AttachStdout: true, AttachStderr: true, Env: env, Cmd: cmd})
	if err != nil {
		_ = docker.Close()
		return Attachment{}, fmt.Errorf("container exec create: %w", err)
	}
	attached, err := docker.ExecAttach(ctx, created.ID, mobycl.ExecAttachOptions{})
	if err != nil {
		_ = docker.Close()
		return Attachment{}, fmt.Errorf("container exec attach: %w", err)
	}
	return Attachment{
		Input:  hijacked{&attached.HijackedResponse},
		Output: attached.Reader,
		ExitCode: func(ctx context.Context) (int, error) {
			inspected, err := docker.ExecInspect(ctx, created.ID, mobycl.ExecInspectOptions{})
			if err != nil {
				return 0, fmt.Errorf("container exec inspect: %w", err)
			}
			return inspected.ExitCode, nil
		},
		Release: func() { _ = docker.Close() },
	}, nil
}

// cappedBuffer keeps up to limit bytes and fails a write past it.
type cappedBuffer struct {
	data  []byte
	limit int
}

var errOversized = errors.New("supervisor output exceeds its envelope limit")

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > b.limit {
		return 0, errOversized
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// RunInput runs argv like Run, with input as its standard input. The
// supervisor reads what is written at once, up to input.Limit, and passes it
// on as the command reads; see Script. Writes end when the command ends or
// RunInput returns, and RunInput returns only after it stopped taking writes
// and its input and output work ended.
//
// The exec runs through the target's Attacher, or the Docker engine. When the
// run ends before the supervisor does, RunInput closes the connection and
// Cancel stops the invocation inside the container, as in Run.
func RunInput(ctx context.Context, ctr testcontainers.Container, label string, processLimit time.Duration, stdoutLimit, stderrLimit int, input Input, argv ...string) (Report, error) {
	var finish sync.Once
	finished := func() { finish.Do(func() { close(input.Finished) }) }
	defer finished()
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if ctr == nil {
		return Report{}, errors.New("ceph control container is unavailable")
	}
	if input.Limit <= 0 || input.Writes == nil || input.Closed == nil || input.Finished == nil {
		return Report{}, fmt.Errorf("%s input is incomplete", label)
	}
	attempt, cancel := context.WithTimeout(ctx, processLimit+ExecCushion)
	defer cancel()
	deadline, _ := attempt.Deadline()
	remaining := time.Until(deadline)
	cushion := min(ExecCushion, remaining/4)
	seconds := min(processLimit, remaining-cushion).Seconds()
	if seconds <= 0 {
		return Report{}, context.DeadlineExceeded
	}
	command := append([]string{"python3", "-c", Script,
		strconv.Itoa(stdoutLimit), strconv.Itoa(stderrLimit), strconv.FormatFloat(seconds, 'f', 9, 64)}, argv...)
	marker := MarkerDir + "/" + uuid.NewString()
	env := []string{MarkerVariable + "=" + marker, InputVariable + "=" + strconv.FormatInt(input.Limit, 10)}
	attacher, ok := ctr.(Attacher)
	if !ok {
		attacher = dockerAttacher{id: ctr.GetContainerID()}
	}
	attachment, err := attacher.AttachExec(attempt, command, env)
	if err != nil {
		// Docker may have accepted the exec and start it later.
		err = &BackendError{Op: "exec " + label + " supervisor", Err: err}
		if cancelErr := Cancel(ctx, ctr, label, marker); cancelErr != nil {
			return Report{}, errors.Join(err, &NotStoppedError{Marker: marker, Err: cancelErr})
		}
		return Report{}, err
	}
	defer attachment.Release()

	// The pump owns every write it took until it answered it. It stops at
	// the end of input, at a failed write, or when stop is closed; closing
	// the connection unblocks a write in progress.
	stop := make(chan struct{})
	pumped := make(chan struct{})
	outcome := &inputOutcome{}
	go func() {
		defer close(pumped)
		defer finished()
		for {
			select {
			case <-stop:
				return
			case <-input.Closed:
				outcome.closed = attachment.Input.CloseWrite() == nil
				return
			case write := <-input.Writes:
				if outcome.written+int64(len(write.Data)) > input.Limit {
					write.Done <- ErrInputLimit
					continue
				}
				written, err := attachment.Input.Write(write.Data)
				outcome.written += int64(written)
				if err != nil {
					write.Done <- ErrInputEnded
					return
				}
				write.Done <- nil
			}
		}
	}()
	read := make(chan error, 1)
	output := &cappedBuffer{limit: envelopeLimit(stdoutLimit, stderrLimit)}
	go func() {
		_, err := stdcopy.StdCopy(output, output, attachment.Output)
		read <- err
	}()
	// The output reader sends one result; it is received exactly once. The
	// run can end before the output, or just after it, as the connection
	// closes.
	var readErr error
	outputEnded := false
	select {
	case readErr = <-read:
		outputEnded = true
	case <-attempt.Done():
	}
	close(stop)
	_ = attachment.Input.Close()
	if !outputEnded {
		// The closed connection ends the reader; the supervisor may still
		// run, so it is stopped inside the container.
		<-read
		<-pumped
		err := &BackendError{Op: "exec " + label + " supervisor", Err: attempt.Err()}
		if cancelErr := Cancel(ctx, ctr, label, marker); cancelErr != nil {
			return Report{}, errors.Join(err, &NotStoppedError{Marker: marker, Err: cancelErr})
		}
		return Report{}, err
	}
	<-pumped
	if err := attempt.Err(); err != nil && readErr == nil {
		// The output ended with the supervisor, and the run ended after
		// that. As Run does once its exec returned, the call reports the
		// context's error and has nothing to stop.
		return Report{}, err
	}
	if readErr != nil {
		// A container that could not run python3 answers with its own
		// message instead of the multiplexed stream.
		if code, err := attachment.ExitCode(attempt); err == nil && (code == 126 || code == 127) {
			return Report{}, &ExitError{Label: label, Code: code, Marker: marker}
		}
		var err error = &BackendError{Op: "read " + label + " supervisor", Err: readErr}
		if errors.Is(readErr, errOversized) {
			err = &ExitError{Label: label, Oversized: true, Marker: marker}
		}
		if cancelErr := Cancel(ctx, ctr, label, marker); cancelErr != nil {
			return Report{}, errors.Join(err, &NotStoppedError{Marker: marker, Err: cancelErr})
		}
		return Report{}, err
	}
	code, err := attachment.ExitCode(attempt)
	if err != nil {
		return Report{}, &BackendError{Op: "inspect " + label + " supervisor", Err: err}
	}
	if code < 0 || code > 128 || code == CleanupFailedExit {
		return Report{}, errors.Join(&ExitError{Label: label, Code: code, Marker: marker}, attempt.Err())
	}
	return parseReport(attempt, label, marker, code, output.data, stdoutLimit, stderrLimit, outcome)
}
