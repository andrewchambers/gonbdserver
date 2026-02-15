package nbd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const defaultConnectionTimeout = 5 * time.Second

type sessionConfig struct {
	exports           []ExportOptions
	defaultExport     string
	disableNoZeroes   bool
	connectionTimeout time.Duration
}

func buildSessionConfig(opts Options) (*log.Logger, *sessionConfig, error) {
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "gonbdserver:", log.LstdFlags)
	}

	if len(opts.Exports) == 0 {
		return nil, nil, errors.New("nbd: no exports configured")
	}

	exports := make([]ExportOptions, len(opts.Exports))
	copy(exports, opts.Exports)

	names := make(map[string]struct{}, len(exports))
	for i := range exports {
		if exports[i].Name == "" {
			return nil, nil, fmt.Errorf("nbd: export[%d] missing name", i)
		}
		if exports[i].OpenBackend == nil {
			return nil, nil, fmt.Errorf("nbd: export[%d] %q missing OpenBackend", i, exports[i].Name)
		}
		if _, ok := names[exports[i].Name]; ok {
			return nil, nil, fmt.Errorf("nbd: duplicate export name %q", exports[i].Name)
		}
		names[exports[i].Name] = struct{}{}
	}

	defaultExport := opts.DefaultExport
	if defaultExport == "" && len(exports) == 1 {
		defaultExport = exports[0].Name
	}
	if defaultExport != "" {
		if _, ok := names[defaultExport]; !ok {
			return nil, nil, fmt.Errorf("nbd: default export %q not found", defaultExport)
		}
	}

	ct := opts.ConnectionTimeout
	if ct == 0 {
		ct = defaultConnectionTimeout
	}

	return logger, &sessionConfig{
		exports:           exports,
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
