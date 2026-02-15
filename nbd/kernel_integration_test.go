//go:build integration
// +build integration

package nbd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type kernelTestFS struct {
	t *testing.T

	sudo string

	ni *NbdInstance

	device     string
	mountpoint string
	backing    string

	nbdClient string
	mount     string
	umount    string
}

// This is a kernel-level integration test which:
// - starts gonbdserver serving a file export over a unix socket
// - attaches it to /dev/nbdX using nbd-client
// - mkfs+mounts it and performs basic filesystem operations
//
// It is guarded behind the "integration" build tag because it requires:
// - Linux kernel NBD support (modprobe nbd)
// - root privileges (via sudo) for nbd-client/mkfs/mount/umount/chown
// - nbd-client installed (usually from the "nbd" package)
func TestMountWriteRemount(t *testing.T) {
	fs := setupKernelTestFS(t, 128*1024*1024)

	if runtime.GOOS != "linux" {
		t.Skip("kernel NBD integration test requires linux")
	}

	// Basic file operations.
	want := []byte("hello from kernel nbd\n")
	p := filepath.Join(fs.mountpoint, "dir", "hello.txt")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, want, 0o644); err != nil {
		t.Fatalf("writefile: %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("readfile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("readback mismatch: got %q want %q", string(got), string(want))
	}

	// Unmount/remount to force the kernel to re-read metadata and validate that
	// a clean remount works through the NBD path.
	fs.remount()
	if got2, err := os.ReadFile(p); err != nil {
		t.Fatalf("readfile after remount: %v", err)
	} else if !bytes.Equal(got2, want) {
		t.Fatalf("readback mismatch after remount: got %q want %q", string(got2), string(want))
	}

	fs.disconnect()
}

func TestMountSQLiteParallelRemount(t *testing.T) {
	fs := setupKernelTestFS(t, 256*1024*1024)

	sqlite3 := lookPathOrSkip(t, "sqlite3")
	dbPath := filepath.Join(fs.mountpoint, "test.db")

	if err := runSqlite(sqlite3, dbPath, `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS kv(
  k INTEGER PRIMARY KEY,
  v INTEGER NOT NULL
);
`); err != nil {
		t.Fatalf("sqlite init: %v", err)
	}

	const (
		workers  = 8
		batches  = 10
		perBatch = 25
	)
	expected := int64(workers * batches * perBatch)

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()

			base := int64(w) * 1_000_000
			var b strings.Builder
			b.WriteString("PRAGMA busy_timeout=5000;\n")
			b.WriteString("PRAGMA synchronous=FULL;\n")
			for batch := 0; batch < batches; batch++ {
				b.WriteString("BEGIN;\n")
				for i := 0; i < perBatch; i++ {
					k := base + int64(batch*perBatch+i)
					fmt.Fprintf(&b, "INSERT INTO kv(k,v) VALUES(%d,1);\n", k)
				}
				b.WriteString("COMMIT;\n")
			}
			if err := runSqlite(sqlite3, dbPath, b.String()); err != nil {
				errCh <- fmt.Errorf("worker %d: %w", w, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	count, err := querySqliteInt64(sqlite3, dbPath, "SELECT COUNT(*) FROM kv;")
	if err != nil {
		t.Fatalf("sqlite count: %v", err)
	}
	if count != expected {
		t.Fatalf("unexpected row count before remount: got %d want %d", count, expected)
	}

	fs.remount()

	if out, err := querySqliteString(sqlite3, dbPath, "PRAGMA integrity_check;"); err != nil {
		t.Fatalf("sqlite integrity_check: %v", err)
	} else if strings.TrimSpace(out) != "ok" {
		t.Fatalf("sqlite integrity_check not ok: %q", strings.TrimSpace(out))
	}

	count2, err := querySqliteInt64(sqlite3, dbPath, "SELECT COUNT(*) FROM kv;")
	if err != nil {
		t.Fatalf("sqlite count after remount: %v", err)
	}
	if count2 != expected {
		t.Fatalf("unexpected row count after remount: got %d want %d", count2, expected)
	}

	fs.disconnect()
}

func setupKernelTestFS(t *testing.T, sizeBytes int64) *kernelTestFS {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("kernel NBD integration test requires linux")
	}

	fs := &kernelTestFS{t: t}
	t.Cleanup(fs.cleanup)

	fs.sudo = lookPathOrSkip(t, "sudo")
	modprobe := lookPathOrSkip(t, "modprobe")
	fs.nbdClient = lookPathOrSkip(t, "nbd-client")
	mkfs := lookPathOrSkip(t, "mkfs.ext4")
	fs.mount = lookPathOrSkip(t, "mount")
	fs.umount = lookPathOrSkip(t, "umount")
	chown := lookPathOrSkip(t, "chown")

	// Start server (removed as part of cleanup).
	fs.ni = StartNbd(t, TestConfig{Driver: "file"})

	if err := fs.ni.CreateFile(t, sizeBytes); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	fs.backing = filepath.Join(fs.ni.TempDir, "nbd.img")
	sock := filepath.Join(fs.ni.TempDir, "nbd.sock")
	waitForFile(t, sock, 3*time.Second)

	// Ensure NBD devices exist.
	if err := runPriv(fs.sudo, modprobe, "nbd"); err != nil {
		t.Skipf("modprobe nbd failed (kernel module not available/allowed?): %v", err)
	}

	fs.device = pickNBDDevice(t)
	fs.mountpoint = mountpointPath(fs.ni.TempDir)

	// Attach device to server. nbd-client daemonizes by default (unless -n/-nofork).
	if err := runPriv(fs.sudo, fs.nbdClient, "-unix", sock, fs.device, "-N", "foo", "-t", "5"); err != nil {
		t.Fatalf("nbd-client connect: %v", err)
	}
	waitForNBDReady(t, fs.device, 3*time.Second)

	// Format + mount.
	if err := runPriv(fs.sudo, mkfs, "-F", "-q", fs.device); err != nil {
		t.Fatalf("mkfs.ext4: %v", err)
	}
	if err := os.MkdirAll(fs.mountpoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	if err := runPriv(fs.sudo, fs.mount, "-t", "ext4", fs.device, fs.mountpoint); err != nil {
		t.Fatalf("mount: %v", err)
	}

	// Make the filesystem root writable by the unprivileged test process.
	uid := os.Getuid()
	gid := os.Getgid()
	if err := runPriv(fs.sudo, chown, fmt.Sprintf("%d:%d", uid, gid), fs.mountpoint); err != nil {
		t.Fatalf("chown: %v", err)
	}

	return fs
}

func (fs *kernelTestFS) remount() {
	fs.t.Helper()
	fs.unmount()
	if err := runPriv(fs.sudo, fs.mount, "-t", "ext4", fs.device, fs.mountpoint); err != nil {
		fs.t.Fatalf("mount: %v", err)
	}
}

func (fs *kernelTestFS) unmount() {
	fs.t.Helper()
	if err := runPriv(fs.sudo, fs.umount, fs.mountpoint); err != nil {
		fs.t.Fatalf("umount: %v", err)
	}
}

func (fs *kernelTestFS) disconnect() {
	fs.t.Helper()
	fs.unmount()
	if err := runPriv(fs.sudo, fs.nbdClient, "-d", fs.device); err != nil {
		fs.t.Fatalf("nbd-client disconnect: %v", err)
	}
}

func (fs *kernelTestFS) cleanup() {
	// Best-effort cleanup: ignore errors and do what we can.
	if fs.sudo != "" && fs.umount != "" && fs.mountpoint != "" {
		_ = runPriv(fs.sudo, fs.umount, fs.mountpoint)
	}
	if fs.sudo != "" && fs.nbdClient != "" && fs.device != "" {
		_ = runPriv(fs.sudo, fs.nbdClient, "-d", fs.device)
	}
	if fs.ni != nil {
		fs.ni.Close()
	}
}

func lookPathOrSkip(t *testing.T, bin string) string {
	t.Helper()
	p, err := exec.LookPath(bin)
	if err != nil {
		t.Skipf("missing required tool %q in PATH", bin)
	}
	return p
}

func runPriv(sudo string, cmd string, args ...string) error {
	// Always use sudo -n to avoid hanging on password prompts.
	cargs := append([]string{"-n", cmd}, args...)
	out, err := exec.Command(sudo, cargs...).CombinedOutput()
	if err != nil {
		o := strings.TrimSpace(string(out))
		if o == "" {
			return err
		}
		return fmt.Errorf("%w: %s", err, o)
	}
	return nil
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func mountpointPath(tempDir string) string {
	return filepath.Join(tempDir, "mnt")
}

func pickNBDDevice(t *testing.T) string {
	t.Helper()
	if dev := strings.TrimSpace(os.Getenv("GONBDSERVER_NBD_DEVICE")); dev != "" {
		if !strings.HasPrefix(dev, "/dev/nbd") {
			t.Fatalf("GONBDSERVER_NBD_DEVICE must be like /dev/nbd0, got %q", dev)
		}
		if _, err := os.Stat(dev); err != nil {
			t.Fatalf("GONBDSERVER_NBD_DEVICE %q not found: %v", dev, err)
		}
		return dev
	}
	for i := 0; i < 64; i++ {
		dev := fmt.Sprintf("/dev/nbd%d", i)
		if _, err := os.Stat(dev); err != nil {
			continue
		}
		pidPath := fmt.Sprintf("/sys/block/nbd%d/pid", i)
		if b, err := os.ReadFile(pidPath); err == nil {
			if strings.TrimSpace(string(b)) == "0" {
				return dev
			}
			continue
		}
		// If /sys doesn't expose pid, assume it is usable and let nbd-client fail if not.
		return dev
	}
	t.Skip("no /dev/nbdX devices available; ensure the nbd module is loaded and /dev/nbd0 exists")
	return ""
}

func waitForNBDReady(t *testing.T, dev string, timeout time.Duration) {
	t.Helper()
	// Prefer sysfs size as a readiness check.
	base := filepath.Base(dev) // nbd0
	sizePath := filepath.Join("/sys/block", base, "size")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(sizePath)
		if err == nil {
			if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && n > 0 {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	// Not fatal: some setups may not expose size immediately; mkfs/mount will fail with a better error.
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
