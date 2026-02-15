package nbd

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const defaultConnectionTimeout = 5 * time.Second

type sessionConfig struct {
	resolveExport     ExportResolver
	listExports       ExportLister
	defaultExport     string
	disableNoZeroes   bool
	connectionTimeout time.Duration
}

func buildSessionConfig(opts Options) (*log.Logger, *sessionConfig, error) {
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "gonbdserver:", log.LstdFlags)
	}

	if opts.ResolveExport == nil {
		return nil, nil, errors.New("nbd: ResolveExport is required")
	}

	defaultExport := opts.DefaultExport

	ct := opts.ConnectionTimeout
	if ct == 0 {
		ct = defaultConnectionTimeout
	}

	return logger, &sessionConfig{
		resolveExport:     opts.ResolveExport,
		listExports:       opts.ListExports,
		defaultExport:     defaultExport,
		disableNoZeroes:   opts.DisableNoZeroes,
		connectionTimeout: ct,
	}, nil
}

// ServeConn serves a single NBD session over conn.
//
// It runs until negotiation fails, the connection is closed, or ctx is canceled.
//
// The connection is always closed before returning.
func ServeConn(ctx context.Context, conn net.Conn, opts Options) error {
	if conn == nil {
		return errors.New("nbd: nil conn")
	}
	logger, cfg, err := buildSessionConfig(opts)
	if err != nil {
		_ = conn.Close()
		return err
	}

	c, err := newConnection(cfg, logger, conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	return c.Serve(ctx)
}

// ServeListener accepts connections from ln and serves each in its own goroutine.
//
// The listener is closed when ctx is canceled to unblock Accept. For custom
// acceptance/shutdown policies, accept connections yourself and call ServeConn.
func ServeListener(ctx context.Context, ln net.Listener, opts Options) error {
	if ln == nil {
		return errors.New("nbd: nil listener")
	}
	logger, cfg, err := buildSessionConfig(opts)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	// Ensure Accept unblocks on cancellation.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	addr := ln.Addr().Network() + ":" + ln.Addr().String()
	logger.Printf("[INFO] Starting listening on %s", addr)
	defer logger.Printf("[INFO] Stopping listening on %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}

		logger.Printf("[INFO] Connect to %s from %s", addr, conn.RemoteAddr())
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = serveConnWithConfig(ctx, conn, cfg, logger)
		}()
	}
}

func serveConnWithConfig(ctx context.Context, conn net.Conn, cfg *sessionConfig, logger *log.Logger) error {
	c, err := newConnection(cfg, logger, conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	return c.Serve(ctx)
}
