package nbd

import (
	"context"
	"log"
	"time"
)

// Options configures serving NBD connections.
type Options struct {
	// Logger is used for all server logs. If nil, a default logger is created.
	Logger *log.Logger

	Exports       []ExportOptions
	DefaultExport string

	// DisableNoZeroes disables advertising NBD_FLAG_NO_ZEROES during negotiation.
	DisableNoZeroes bool

	// ConnectionTimeout is the maximum time allowed for negotiation. If 0,
	// a default is used.
	ConnectionTimeout time.Duration
}

// BackendFactory opens a backend instance for a negotiated export.
//
// The backend instance is owned by the connection and will be Closed when the
// connection ends.
type BackendFactory func(ctx context.Context, export *ExportOptions) (Backend, error)

// ExportOptions configures a single export.
type ExportOptions struct {
	Name        string
	Description string

	ReadOnly bool
	Workers  int // if 0, DefaultWorkers is used

	MinimumBlockSize   uint64
	PreferredBlockSize uint64
	MaximumBlockSize   uint64

	OpenBackend BackendFactory
}
