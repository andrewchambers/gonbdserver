package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	testdproto "github.com/andrewchambers/gonbdserver/testd/proto"
)

func main() {
	var sock string
	var debug bool
	flag.StringVar(&sock, "sock", testdproto.DefaultSockPath, "unix socket path to listen on")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()

	if os.Geteuid() != 0 {
		fmt.Fprintln(os.Stderr, "error: testd must run as root")
		os.Exit(2)
	}

	hopts := &slog.HandlerOptions{}
	if debug {
		hopts.Level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, hopts))
	logger.Info("starting", "sock", sock, "pid", os.Getpid(), "debug", debug)

	if err := os.MkdirAll(filepath.Dir(sock), 0o755); err != nil {
		logger.Error("mkdir socket dir", "err", err)
		os.Exit(1)
	}
	_ = os.Remove(sock)

	ln, err := net.Listen("unix", sock)
	if err != nil {
		logger.Error("listen", "sock", sock, "err", err)
		os.Exit(1)
	}
	defer ln.Close()

	// Let unprivileged tests connect. The daemon still validates that NBD sockets are
	// real unix sockets under /tmp owned by the connecting uid.
	if err := os.Chmod(sock, 0o666); err != nil {
		logger.Error("chmod socket", "sock", sock, "err", err)
		os.Exit(1)
	}
	logger.Info("listening", "sock", sock, "mode", "0666")

	d := newDaemon(logger)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down (signal)")
		_ = ln.Close()      // stop accepting new clients
		_ = os.Remove(sock) // best-effort
		d.Shutdown()
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			logger.Error("accept", "err", err)
			break
		}
		logger.Debug("accepted connection", "remote", c.RemoteAddr().String())
		d.wg.Add(1)
		go d.serveConn(c)
	}

	// Ensure we always close active sessions if the listener stops for any reason.
	d.Shutdown()
	d.wg.Wait()
	logger.Info("shutdown complete")
}

type daemon struct {
	logger *slog.Logger

	opMu sync.Mutex

	sessMu   sync.Mutex
	sessions map[*session]net.Conn

	wg sync.WaitGroup

	shutdownOnce sync.Once
}

func newDaemon(logger *slog.Logger) *daemon {
	return &daemon{
		logger:   logger,
		sessions: make(map[*session]net.Conn),
	}
}

func (d *daemon) Shutdown() {
	d.shutdownOnce.Do(func() {
		d.sessMu.Lock()
		conns := make([]net.Conn, 0, len(d.sessions))
		for _, c := range d.sessions {
			conns = append(conns, c)
		}
		d.sessMu.Unlock()

		d.logger.Info("closing active sessions", "count", len(conns))
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

func (d *daemon) serveConn(c net.Conn) {
	defer d.wg.Done()

	l := d.logger.With("remote", c.RemoteAddr().String())
	uid, gid, pid, ok := peerCreds(c)
	if !ok {
		l.Error("could not determine peer credentials; refusing connection")
		_ = c.Close()
		return
	}
	l = l.With("peer_uid", uid, "peer_gid", gid, "peer_pid", pid)

	sess := &session{
		d:      d,
		logger: l,
		uid:    uid,
		gid:    gid,
	}

	d.sessMu.Lock()
	d.sessions[sess] = c
	d.sessMu.Unlock()
	defer func() {
		d.sessMu.Lock()
		delete(d.sessions, sess)
		d.sessMu.Unlock()
	}()

	srv := rpc.NewServer()
	if err := srv.RegisterName(testdproto.RPCService, sess); err != nil {
		l.Error("rpc register", "err", err)
		_ = c.Close()
		return
	}

	l.Info("session started")
	srv.ServeConn(c)
	l.Info("session ended; cleaning up")
	sess.cleanup()
	l.Info("session cleanup complete")
}

type session struct {
	d      *daemon
	logger *slog.Logger

	uid int
	gid int

	mu sync.Mutex
	fs *fsState
}

type fsState struct {
	device   string
	nbdSock  string
	export   string
	baseDir  string
	mountDir string
}

func (s *session) cleanup() {
	// Best-effort cleanup for anything this client created.
	s.d.opMu.Lock()
	defer s.d.opMu.Unlock()
	_ = s.cleanupFS(context.Background())
}

func (s *session) Ping(args *testdproto.PingArgs, reply *testdproto.PingReply) error {
	if reply != nil {
		reply.APIVersion = testdproto.APIVersion
	}
	return nil
}

func (s *session) Start(args *testdproto.StartArgs, reply *testdproto.StartReply) (err error) {
	s.d.opMu.Lock()
	defer s.d.opMu.Unlock()

	if args == nil {
		return errors.New("nil args")
	}

	start := time.Now()
	export := args.Export
	if export == "" {
		export = "device"
	}
	if args.NbdSock == "" {
		return errors.New("missing NbdSock")
	}
	if !filepath.IsAbs(args.NbdSock) {
		return errors.New("NbdSock must be an absolute path")
	}

	s.mu.Lock()
	if s.fs != nil {
		s.mu.Unlock()
		return errors.New("already started")
	}
	s.mu.Unlock()

	nbdSock, err := validateNbdSock(args.NbdSock, s.uid)
	if err != nil {
		return err
	}

	s.logger.Info("Start", "export", export, "nbd_sock", nbdSock)

	modprobe, err := exec.LookPath("modprobe")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "modprobe")
	}
	nbdClient, err := exec.LookPath("nbd-client")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "nbd-client")
	}
	mkfs, err := exec.LookPath("mkfs.ext4")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "mkfs.ext4")
	}
	mount, err := exec.LookPath("mount")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "mount")
	}
	umount, err := exec.LookPath("umount")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "umount")
	}
	chown, err := exec.LookPath("chown")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "chown")
	}

	if err := s.runCmd(context.Background(), "modprobe nbd", modprobe, "nbd"); err != nil {
		return fmt.Errorf("modprobe nbd failed: %w", err)
	}

	dev, err := pickNBDDevice()
	if err != nil {
		return err
	}

	baseDir, err := os.MkdirTemp("/tmp", "gonbdserver-testd-")
	if err != nil {
		return fmt.Errorf("creating temp dir: %w", err)
	}
	// Let the unprivileged client traverse into the session directory, without
	// granting it directory listing rights.
	if err := os.Chmod(baseDir, 0o711); err != nil {
		_ = os.RemoveAll(baseDir)
		return fmt.Errorf("chmod temp dir: %w", err)
	}
	mountDir := filepath.Join(baseDir, "mnt")

	s.mu.Lock()
	s.fs = &fsState{
		device:   dev,
		nbdSock:  nbdSock,
		export:   export,
		baseDir:  baseDir,
		mountDir: mountDir,
	}
	s.mu.Unlock()

	// Best-effort cleanup in case a previous run leaked state.
	s.logger.Debug("best-effort cleanup before start", "device", dev, "mount_dir", mountDir)
	_ = s.runCmd(context.Background(), "umount (pre)", umount, mountDir)
	_ = s.runCmd(context.Background(), "nbd-client -d (pre)", nbdClient, "-d", dev)

	defer func() {
		// If Start fails, ensure we don't leak mounts/devices.
		if err != nil {
			_ = s.cleanupFS(context.Background())
		}
	}()

	if err = s.runCmd(context.Background(), "nbd-client connect", nbdClient, "-unix", nbdSock, dev, "-N", export, "-t", "5"); err != nil {
		return fmt.Errorf("nbd-client connect: %w", err)
	}
	if err = waitForNBDReady(dev, 3*time.Second); err != nil {
		_ = s.runCmd(context.Background(), "nbd-client -d (not ready)", nbdClient, "-d", dev)
		return fmt.Errorf("nbd device not ready: %w", err)
	}

	if err = s.runCmd(context.Background(), "mkfs.ext4", mkfs, "-F", "-q", dev); err != nil {
		_ = s.runCmd(context.Background(), "nbd-client -d (mkfs failed)", nbdClient, "-d", dev)
		return fmt.Errorf("mkfs.ext4: %w", err)
	}

	if err = os.MkdirAll(mountDir, 0o755); err != nil {
		_ = s.runCmd(context.Background(), "nbd-client -d (mkdir failed)", nbdClient, "-d", dev)
		return fmt.Errorf("mkdir mountpoint: %w", err)
	}
	if err = s.runCmd(context.Background(), "mount", mount, "-t", "ext4", dev, mountDir); err != nil {
		_ = s.runCmd(context.Background(), "nbd-client -d (mount failed)", nbdClient, "-d", dev)
		return fmt.Errorf("mount: %w", err)
	}

	if err = s.runCmd(context.Background(), "chown mountpoint", chown, fmt.Sprintf("%d:%d", s.uid, s.gid), mountDir); err != nil {
		_ = s.runCmd(context.Background(), "umount (chown failed)", umount, mountDir)
		_ = s.runCmd(context.Background(), "nbd-client -d (chown failed)", nbdClient, "-d", dev)
		return fmt.Errorf("chown mountpoint: %w", err)
	}

	if reply != nil {
		reply.MountDir = mountDir
		reply.Device = dev
	}
	s.logger.Info("Start complete", "device", dev, "mount_dir", mountDir, "duration", time.Since(start))
	return nil
}

func (s *session) Remount(args *testdproto.RemountArgs, reply *testdproto.RemountReply) error {
	s.d.opMu.Lock()
	defer s.d.opMu.Unlock()

	start := time.Now()

	s.mu.Lock()
	fs := s.fs
	s.mu.Unlock()
	if fs == nil {
		return errors.New("not started")
	}

	s.logger.Info("Remount", "device", fs.device, "mount_dir", fs.mountDir)

	mount, err := exec.LookPath("mount")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "mount")
	}
	umount, err := exec.LookPath("umount")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "umount")
	}
	chown, err := exec.LookPath("chown")
	if err != nil {
		return fmt.Errorf("missing required tool %q in PATH", "chown")
	}

	if err := s.runCmd(context.Background(), "umount", umount, fs.mountDir); err != nil {
		return fmt.Errorf("umount: %w", err)
	}
	if err := s.runCmd(context.Background(), "mount", mount, "-t", "ext4", fs.device, fs.mountDir); err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	if err := s.runCmd(context.Background(), "chown mountpoint", chown, fmt.Sprintf("%d:%d", s.uid, s.gid), fs.mountDir); err != nil {
		return fmt.Errorf("chown mountpoint: %w", err)
	}

	s.logger.Info("Remount complete", "duration", time.Since(start))
	return nil
}

func (s *session) Cleanup(args *testdproto.CleanupArgs, reply *testdproto.CleanupReply) error {
	s.d.opMu.Lock()
	defer s.d.opMu.Unlock()
	return s.cleanupFS(context.Background())
}

func (s *session) cleanupFS(ctx context.Context) error {
	s.mu.Lock()
	fs := s.fs
	s.fs = nil
	s.mu.Unlock()
	if fs == nil {
		return nil
	}

	s.logger.Info("Cleanup", "device", fs.device, "mount_dir", fs.mountDir, "base_dir", fs.baseDir)

	nbdClient, err := exec.LookPath("nbd-client")
	if err != nil {
		s.logger.Error("missing required tool in PATH", "tool", "nbd-client", "err", err)
		return nil
	}
	umount, err := exec.LookPath("umount")
	if err != nil {
		s.logger.Error("missing required tool in PATH", "tool", "umount", "err", err)
		return nil
	}

	if fs.mountDir != "" {
		if err := s.runCmd(ctx, "umount", umount, fs.mountDir); err != nil {
			s.logger.Warn("umount failed", "mount_dir", fs.mountDir, "err", err)
		}
	}
	if fs.device != "" {
		if err := s.runCmd(ctx, "nbd-client -d", nbdClient, "-d", fs.device); err != nil {
			s.logger.Warn("nbd disconnect failed", "device", fs.device, "err", err)
		}
	}
	if fs.baseDir != "" {
		if err := os.RemoveAll(fs.baseDir); err != nil {
			s.logger.Warn("remove temp dir failed", "base_dir", fs.baseDir, "err", err)
		}
	}

	return nil
}

func (s *session) runCmd(ctx context.Context, op string, cmd string, args ...string) error {
	s.logger.Debug("exec", "op", op, "cmd", cmd, "args", args)
	err := runCmd(ctx, cmd, args...)
	if err != nil {
		s.logger.Debug("exec failed", "op", op, "cmd", cmd, "args", args, "err", err)
	}
	return err
}

func runCmd(ctx context.Context, cmd string, args ...string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	out, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	if err != nil {
		o := strings.TrimSpace(string(out))
		if o == "" {
			return err
		}
		return fmt.Errorf("%w: %s", err, o)
	}
	return nil
}

func pickNBDDevice() (string, error) {
	for i := 0; i < 64; i++ {
		dev := fmt.Sprintf("/dev/nbd%d", i)
		if _, err := os.Stat(dev); err != nil {
			continue
		}
		pidPath := fmt.Sprintf("/sys/block/nbd%d/pid", i)
		if b, err := os.ReadFile(pidPath); err == nil {
			if strings.TrimSpace(string(b)) == "0" {
				return dev, nil
			}
			continue
		}
		// If /sys doesn't expose pid, assume it is usable and let nbd-client fail if not.
		return dev, nil
	}
	return "", errors.New("no /dev/nbdX devices available; ensure the nbd module is loaded and /dev/nbd0 exists")
}

func waitForNBDReady(dev string, timeout time.Duration) error {
	base := filepath.Base(dev) // nbd0
	sizePath := filepath.Join("/sys/block", base, "size")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(sizePath)
		if err == nil {
			if n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && n > 0 {
				return nil
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return errors.New("timeout")
}

func peerCreds(c net.Conn) (uid, gid, pid int, ok bool) {
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, 0, 0, false
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, 0, 0, false
	}
	var ucred *syscall.Ucred
	var ctrlErr error
	if err := raw.Control(func(fd uintptr) {
		ucred, ctrlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil || ctrlErr != nil || ucred == nil {
		return 0, 0, 0, false
	}
	return int(ucred.Uid), int(ucred.Gid), int(ucred.Pid), true
}

func validateNbdSock(path string, wantUID int) (string, error) {
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return "", errors.New("NbdSock must be an absolute path")
	}
	// Limit to /tmp to reduce the daemon's ability to be tricked into touching arbitrary paths.
	if !strings.HasPrefix(clean, "/tmp/") {
		return "", fmt.Errorf("NbdSock must be under /tmp (got %q)", clean)
	}
	fi, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("stat NbdSock: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("NbdSock must not be a symlink (got %q)", clean)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return "", fmt.Errorf("NbdSock is not a unix socket (got %q)", clean)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("stat NbdSock: unexpected stat type %T", fi.Sys())
	}
	if wantUID >= 0 && int(st.Uid) != wantUID {
		return "", fmt.Errorf("NbdSock must be owned by uid %d (got uid %d)", wantUID, st.Uid)
	}
	return clean, nil
}
