//go:build integration
// +build integration

package nbd

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type TestConfig struct {
	TempDir string
}

type NbdInstance struct {
	t           *testing.T
	cancel      context.CancelFunc
	done        chan struct{}
	serveErrCh  chan error
	closed      bool
	closedMutex sync.Mutex
	srv         *Server
	TestConfig
}

func StartNbd(t *testing.T, tc TestConfig) *NbdInstance {
	t.Helper()

	ni := &NbdInstance{
		t:          t,
		done:       make(chan struct{}),
		serveErrCh: make(chan error, 1),
		TestConfig: tc,
	}

	tempDir, err := os.MkdirTemp("", "gonbdserver-integration-")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	ni.TempDir = tempDir

	sock := filepath.Join(ni.TempDir, "nbd.sock")
	img := filepath.Join(ni.TempDir, "nbd.img")

	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("net.Listen(unix): %v", err)
	}

	srv, err := NewServer(Options{
		Listeners: []ListenerOptions{
			{
				Listener: ln,
				Exports: []ExportOptions{
					{
						Name:        "foo",
						OpenBackend: OpenFileBackend(FileBackendOptions{Path: img}),
						Workers:     20,
					},
				},
			},
		},
	})
	if err != nil {
		_ = ln.Close()
		t.Fatalf("NewServer: %v", err)
	}
	ni.srv = srv

	ctx, cancel := context.WithCancel(context.Background())
	ni.cancel = cancel
	go func() {
		defer close(ni.done)
		ni.serveErrCh <- ni.srv.Serve(ctx)
	}()

	// Catch immediate listen failures with a clear error, instead of timing out later.
	deadline := time.Now().Add(2 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		select {
		case err := <-ni.serveErrCh:
			if err == nil || errors.Is(err, context.Canceled) {
				t.Fatalf("server exited unexpectedly: %v", err)
			}
			t.Fatalf("server exited: %v", err)
		default:
		}
		if _, err := os.Stat(sock); err == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("timed out waiting for unix socket %q", sock)
	}

	return ni
}

func (ni *NbdInstance) CloseConnection() {
	ni.closedMutex.Lock()
	defer ni.closedMutex.Unlock()
	if ni.closed {
		return
	}
	if ni.cancel != nil {
		ni.cancel()
	}
	if ni.srv != nil {
		_ = ni.srv.Close()
	}
	ni.closed = true
}

func (ni *NbdInstance) Close() {
	ni.CloseConnection()
	select {
	case <-ni.done:
	case <-time.After(2 * time.Second):
		ni.t.Logf("timeout waiting for server shutdown")
	}
	_ = os.RemoveAll(ni.TempDir)
}

func (ni *NbdInstance) CreateFile(t *testing.T, size int64) error {
	t.Helper()
	filename := filepath.Join(ni.TempDir, "nbd.img")
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(size)
}
