package nbd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const defaultConnectionTimeout = 5 * time.Second

type listener struct {
	logger *log.Logger

	network string
	addr    string
	nl      net.Listener

	exports         []ExportOptions
	defaultExport   string
	disableNoZeroes bool

	connectionTimeout time.Duration
}

func newListener(logger *log.Logger, o ListenerOptions) (*listener, error) {
	if o.Network == "" {
		o.Network = "tcp"
	}
	if o.Address == "" {
		return nil, errors.New("missing address")
	}
	if len(o.Exports) == 0 {
		return nil, errors.New("no exports configured")
	}

	exports := make([]ExportOptions, len(o.Exports))
	copy(exports, o.Exports)

	names := make(map[string]struct{}, len(exports))
	for i := range exports {
		if exports[i].Name == "" {
			return nil, fmt.Errorf("export[%d] missing name", i)
		}
		if exports[i].OpenBackend == nil {
			return nil, fmt.Errorf("export[%d] %q missing OpenBackend", i, exports[i].Name)
		}
		if _, ok := names[exports[i].Name]; ok {
			return nil, fmt.Errorf("duplicate export name %q", exports[i].Name)
		}
		names[exports[i].Name] = struct{}{}
	}

	defaultExport := o.DefaultExport
	if defaultExport == "" && len(exports) == 1 {
		defaultExport = exports[0].Name
	}
	if defaultExport != "" {
		if _, ok := names[defaultExport]; !ok {
			return nil, fmt.Errorf("default export %q not found", defaultExport)
		}
	}

	ct := o.ConnectionTimeout
	if ct == 0 {
		ct = defaultConnectionTimeout
	}

	return &listener{
		logger:            logger,
		network:           o.Network,
		addr:              o.Address,
		exports:           exports,
		defaultExport:     defaultExport,
		disableNoZeroes:   o.DisableNoZeroes,
		connectionTimeout: ct,
	}, nil
}

func (l *listener) close() error {
	if l.nl == nil {
		return nil
	}
	return l.nl.Close()
}

func (l *listener) acceptLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	addr := l.network + ":" + l.addr
	l.logger.Printf("[INFO] Starting listening on %s", addr)

	defer func() {
		l.logger.Printf("[INFO] Stopping listening on %s", addr)
		_ = l.close()
	}()

	// Ensure Accept unblocks on cancellation.
	go func() {
		<-ctx.Done()
		_ = l.close()
	}()

	for {
		conn, err := l.nl.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Temporary errors can happen under load; keep going.
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			l.logger.Printf("[ERROR] Error %v listening on %s", err, addr)
			continue
		}

		l.logger.Printf("[INFO] Connect to %s from %s", addr, conn.RemoteAddr())
		c, err := newConnection(l, l.logger, conn)
		if err != nil {
			l.logger.Printf("[ERROR] Error %v establishing connection to %s from %s", err, addr, conn.RemoteAddr())
			_ = conn.Close()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Serve(ctx)
		}()
	}
}
