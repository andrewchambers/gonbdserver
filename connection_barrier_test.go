package nbd

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

type barrierBackend struct {
	write1Started chan struct{}
	allowWrite1   chan struct{}
	flushStarted  chan struct{}
	allowFlush    chan struct{}
	write2Started chan struct{}
}

func newBarrierBackend() *barrierBackend {
	return &barrierBackend{
		write1Started: make(chan struct{}),
		allowWrite1:   make(chan struct{}),
		flushStarted:  make(chan struct{}),
		allowFlush:    make(chan struct{}),
		write2Started: make(chan struct{}),
	}
}

func (b *barrierBackend) WriteAt(ctx context.Context, payload []byte, offset int64, fua bool) (int, error) {
	_ = fua
	switch offset {
	case 0:
		select {
		case <-b.write1Started:
		default:
			close(b.write1Started)
		}
		select {
		case <-b.allowWrite1:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	case 4096:
		select {
		case <-b.write2Started:
		default:
			close(b.write2Started)
		}
	}
	return len(payload), nil
}

func (b *barrierBackend) ReadAt(ctx context.Context, payload []byte, offset int64) (int, error) {
	_ = ctx
	_ = offset
	return len(payload), nil
}

func (b *barrierBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	_ = ctx
	_ = offset
	return length, nil
}

func (b *barrierBackend) Flush(ctx context.Context) error {
	select {
	case <-b.flushStarted:
	default:
		close(b.flushStarted)
	}
	select {
	case <-b.allowFlush:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *barrierBackend) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func (b *barrierBackend) Geometry(ctx context.Context) (Geometry, error) {
	_ = ctx
	return Geometry{
		Size:               8192,
		MinimumBlockSize:   1,
		PreferredBlockSize: 4096,
		MaximumBlockSize:   8192,
	}, nil
}

func (b *barrierBackend) HasFua(ctx context.Context) bool {
	_ = ctx
	return false
}

func (b *barrierBackend) HasFlush(ctx context.Context) bool {
	_ = ctx
	return true
}

func waitForSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func TestDispatchFlushBarrier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	backend := newBarrierBackend()
	conn := &Connection{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		backend: backend,
		export: &Export{
			memoryBlockSize: 4096,
		},
		rxCh:   make(chan Request, 3),
		txCh:   make(chan Request, 3),
		killCh: make(chan struct{}),
	}

	const workers = 2
	conn.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go conn.Dispatch(ctx, i)
	}

	payload := make([]byte, 4096)
	conn.rxCh <- Request{
		nbdReq:      nbdRequest{NbdCommandType: NBD_CMD_WRITE},
		length:      4096,
		offset:      0,
		dispatchSeq: 1,
		reqData: [][]byte{
			payload,
		},
	}
	conn.rxCh <- Request{
		nbdReq:      nbdRequest{NbdCommandType: NBD_CMD_FLUSH},
		dispatchSeq: 2,
	}
	conn.rxCh <- Request{
		nbdReq:      nbdRequest{NbdCommandType: NBD_CMD_WRITE},
		length:      4096,
		offset:      4096,
		dispatchSeq: 3,
		reqData: [][]byte{
			payload,
		},
	}
	close(conn.rxCh)

	waitForSignal(t, backend.write1Started, "first write to start")
	select {
	case <-backend.flushStarted:
		t.Fatal("flush started before the first write completed")
	default:
	}

	close(backend.allowWrite1)
	waitForSignal(t, backend.flushStarted, "flush to start after first write")

	select {
	case <-backend.write2Started:
		t.Fatal("second write started before flush completed")
	default:
	}

	close(backend.allowFlush)
	waitForSignal(t, backend.write2Started, "second write to start after flush")

	done := make(chan struct{})
	go func() {
		conn.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timed out waiting for dispatchers to exit")
	}
}
