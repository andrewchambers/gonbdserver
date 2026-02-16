package nbd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// This is a kernel-level integration test which:
// - starts gonbdserver serving a file export over a unix socket
// - attaches it to /dev/nbdX using nbd-client
// - mkfs+mounts it and performs basic filesystem operations
//
// It is skipped unless prerequisites are present, because it requires:
// - Linux kernel NBD support (modprobe nbd)
// - root privileges (via the testd/cmd/testd helper) for nbd-client/mkfs/mount/umount/chown
// - nbd-client installed (usually from the "nbd" package)
func TestMountWriteRemount(t *testing.T) {
	fs := setupKernelTestFS(t, 128*1024*1024)

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

	sqlite3 := "sqlite3"
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
