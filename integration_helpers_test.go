package nbd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	testdproto "github.com/andrewchambers/gonbdserver/testd/proto"
)

type TestConfig struct {
	TempDir string
}

const testExportName = "device"

type NbdInstance struct {
	t           *testing.T
	cancel      context.CancelFunc
	done        chan struct{}
	serveErrCh  chan error
	closed      bool
	closedMutex sync.Mutex
	ln          net.Listener
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

	// Keep test paths under /tmp so the privileged helper can enforce that it only
	// touches temp paths (and never arbitrary ones).
	tempDir, err := os.MkdirTemp("/tmp", "gonbdserver-integration-")
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

	opts := Options{
		ResolveExport: func(ctx context.Context, name string) (*ExportOptions, error) {
			_ = ctx
			if name != testExportName {
				return nil, ErrNoSuchExport
			}
			return &ExportOptions{
				Name:        testExportName,
				OpenBackend: OpenFileBackend(FileBackendOptions{Path: img}),
				Workers:     20,
			}, nil
		},
		ListExports: func(ctx context.Context, yield func(name string) bool) error {
			_ = ctx
			yield(testExportName)
			return nil
		},
	}
	ni.ln = ln

	ctx, cancel := context.WithCancel(context.Background())
	ni.cancel = cancel
	go func() {
		defer close(ni.done)
		ni.serveErrCh <- ServeListener(ctx, ln, opts)
	}()

	// Catch immediate startup errors.
	select {
	case err := <-ni.serveErrCh:
		if err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("server exited unexpectedly: %v", err)
		}
		t.Fatalf("server exited: %v", err)
	case <-time.After(25 * time.Millisecond):
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
	if ni.ln != nil {
		_ = ni.ln.Close()
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

type kernelTestFS struct {
	t *testing.T

	td *testdClient

	ni *NbdInstance

	mountpoint string
}

func setupKernelTestFS(t *testing.T, sizeBytes int64) *kernelTestFS {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("kernel NBD integration test requires linux")
	}

	fs := &kernelTestFS{t: t}
	t.Cleanup(fs.cleanup)

	fs.td = dialTestdOrSkip(t)

	// Start server (removed as part of cleanup).
	fs.ni = StartNbd(t, TestConfig{})

	if err := fs.ni.CreateFile(t, sizeBytes); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	sock := filepath.Join(fs.ni.TempDir, "nbd.sock")

	// Ask the privileged helper to attach/format/mount.
	reply, err := fs.td.Start(60*time.Second, &testdproto.StartArgs{
		NbdSock: sock,
		Export:  testExportName,
	})
	if err != nil {
		t.Fatalf("testd Start: %v", err)
	}
	fs.mountpoint = reply.MountDir

	return fs
}

func (fs *kernelTestFS) remount() {
	fs.t.Helper()
	if err := fs.td.Remount(30 * time.Second); err != nil {
		fs.t.Fatalf("testd Remount: %v", err)
	}
}

func (fs *kernelTestFS) disconnect() {
	fs.t.Helper()
	if err := fs.td.Cleanup(30 * time.Second); err != nil {
		fs.t.Fatalf("testd Cleanup: %v", err)
	}
}

func (fs *kernelTestFS) cleanup() {
	// Best-effort cleanup: ignore errors and do what we can.
	if fs.td != nil {
		_ = fs.td.Cleanup(30 * time.Second)
	}
	if fs.ni != nil {
		fs.ni.Close()
	}
}

type testdClient struct {
	conn net.Conn
	rpc  *rpc.Client
}

func dialTestd(sock string, timeout time.Duration) (*testdClient, error) {
	conn, err := net.DialTimeout("unix", sock, timeout)
	if err != nil {
		return nil, err
	}
	return &testdClient{
		conn: conn,
		rpc:  rpc.NewClient(conn),
	}, nil
}

func (c *testdClient) Close() error {
	if c == nil {
		return nil
	}
	_ = c.rpc.Close()
	return c.conn.Close()
}

func (c *testdClient) call(timeout time.Duration, method string, args any, reply any) error {
	if timeout > 0 {
		_ = c.conn.SetDeadline(time.Now().Add(timeout))
		defer c.conn.SetDeadline(time.Time{})
	}
	return c.rpc.Call(testdproto.RPCService+"."+method, args, reply)
}

func (c *testdClient) Ping(timeout time.Duration) (*testdproto.PingReply, error) {
	var reply testdproto.PingReply
	if err := c.call(timeout, "Ping", &testdproto.PingArgs{}, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *testdClient) Start(timeout time.Duration, args *testdproto.StartArgs) (*testdproto.StartReply, error) {
	var reply testdproto.StartReply
	if err := c.call(timeout, "Start", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *testdClient) Remount(timeout time.Duration) error {
	var reply testdproto.RemountReply
	return c.call(timeout, "Remount", &testdproto.RemountArgs{}, &reply)
}

func (c *testdClient) Cleanup(timeout time.Duration) error {
	var reply testdproto.CleanupReply
	return c.call(timeout, "Cleanup", &testdproto.CleanupArgs{}, &reply)
}

func dialTestdOrSkip(t *testing.T) *testdClient {
	t.Helper()
	sock := strings.TrimSpace(os.Getenv("GONBDSERVER_TESTD_SOCK"))
	if sock == "" {
		sock = testdproto.DefaultSockPath
	}
	c, err := dialTestd(sock, 2*time.Second)
	if err != nil {
		t.Skipf("cannot connect to testd at %q: %v (build+run as root: go build -o /tmp/gonbdserver-testd ./testd/cmd/testd && sudo /tmp/gonbdserver-testd)", sock, err)
	}
	t.Cleanup(func() {
		_ = c.Close()
	})
	pr, err := c.Ping(2 * time.Second)
	if err != nil {
		t.Fatalf("testd at %q is not responding: %v", sock, err)
	}
	if pr.APIVersion != testdproto.APIVersion {
		t.Fatalf("testd at %q has incompatible API version %d (want %d); rebuild/restart it", sock, pr.APIVersion, testdproto.APIVersion)
	}
	return c
}

func runSqlite(sqlite3Path string, dbPath string, script string) error {
	cmd := exec.Command(sqlite3Path, "-batch", "-bail", dbPath)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		o := strings.TrimSpace(string(out))
		if o == "" {
			return err
		}
		return fmt.Errorf("%w: %s", err, o)
	}
	return nil
}

func querySqliteString(sqlite3Path string, dbPath string, query string) (string, error) {
	cmd := exec.Command(sqlite3Path, "-batch", "-bail", "-noheader", "-list", dbPath, query)
	out, err := cmd.CombinedOutput()
	if err != nil {
		o := strings.TrimSpace(string(out))
		if o == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, o)
	}
	return string(out), nil
}

func querySqliteInt64(sqlite3Path string, dbPath string, query string) (int64, error) {
	out, err := querySqliteString(sqlite3Path, dbPath, query)
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(out)
	if s == "" {
		return 0, fmt.Errorf("empty sqlite output for query %q", query)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing sqlite output %q for query %q: %w", s, query, err)
	}
	return n, nil
}
