package nbd

import (
	"context"
	"os"
)

// FileBackend implements Backend
type FileBackend struct {
	file *os.File
	size uint64
}

// WriteAt implements Backend.WriteAt
func (fb *FileBackend) WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) {
	n, err := fb.file.WriteAt(b, offset)
	if err != nil || !fua {
		return n, err
	}
	err = fb.file.Sync()
	if err != nil {
		return 0, err
	}
	return n, err
}

// ReadAt implements Backend.ReadAt
func (fb *FileBackend) ReadAt(ctx context.Context, b []byte, offset int64) (int, error) {
	return fb.file.ReadAt(b, offset)
}

// TrimAt implements Backend.TrimAt
func (fb *FileBackend) TrimAt(ctx context.Context, length int, offset int64) (int, error) {
	return length, nil
}

// Flush implements Backend.Flush
func (fb *FileBackend) Flush(ctx context.Context) error {
	return fb.file.Sync()
}

// Close implements Backend.Close
func (fb *FileBackend) Close(ctx context.Context) error {
	return fb.file.Close()
}

func (fb *FileBackend) Geometry(ctx context.Context) (Geometry, error) {
	return Geometry{
		Size:               fb.size,
		MinimumBlockSize:   1,
		PreferredBlockSize: 32 * 1024,
		MaximumBlockSize:   128 * 1024 * 1024,
	}, nil
}

// Size implements Backend.HasFua
func (fb *FileBackend) HasFua(ctx context.Context) bool {
	return true
}

// Size implements Backend.HasFua
func (fb *FileBackend) HasFlush(ctx context.Context) bool {
	return true
}

type FileBackendOptions struct {
	Path string
	Sync bool
}

// OpenFileBackend returns a BackendFactory that serves a host file.
func OpenFileBackend(opts FileBackendOptions) BackendFactory {
	return func(ctx context.Context, export *ExportOptions) (Backend, error) {
		perms := os.O_RDWR
		if export.ReadOnly {
			perms = os.O_RDONLY
		}
		if opts.Sync {
			perms |= os.O_SYNC
		}
		file, err := os.OpenFile(opts.Path, perms, 0o666)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return &FileBackend{
			file: file,
			size: uint64(stat.Size()),
		}, nil
	}
}
