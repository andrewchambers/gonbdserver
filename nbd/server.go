package nbd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

// Server is an NBD server instance.
//
// Create a Server with NewServer, then call Serve.
type Server struct {
	logger *log.Logger

	mu        sync.Mutex
	started   bool
	closed    bool
	cancel    context.CancelFunc
	listeners []*listener

	wg sync.WaitGroup
}

func NewServer(opts Options) (*Server, error) {
	logger := opts.Logger
	if logger == nil {
		logger = log.New(os.Stderr, "gonbdserver:", log.LstdFlags)
	}
	if len(opts.Listeners) == 0 {
		return nil, errors.New("nbd: no listeners configured")
	}

	s := &Server{logger: logger}

	for i, lo := range opts.Listeners {
		l, err := newListener(logger, lo)
		if err != nil {
			return nil, fmt.Errorf("nbd: listener[%d]: %w", i, err)
		}
		s.listeners = append(s.listeners, l)
	}

	return s, nil
}

// Serve starts all listeners and blocks until ctx is canceled or Close is called.
//
// Serve may only be called once.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("nbd: server closed")
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("nbd: Serve already called")
	}
	s.started = true
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	listeners := append([]*listener(nil), s.listeners...)
	s.mu.Unlock()

	for _, l := range listeners {
		s.wg.Add(1)
		go l.acceptLoop(ctx, &s.wg)
	}

	<-ctx.Done()
	_ = s.Close()
	s.wg.Wait()
	return nil
}

// Close stops the server (idempotent).
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	cancel := s.cancel
	listeners := append([]*listener(nil), s.listeners...)
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, l := range listeners {
		_ = l.close()
	}
	return nil
}
