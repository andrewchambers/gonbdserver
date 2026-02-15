package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Default number of workers
var DefaultWorkers = 5

// ConnectionParameters holds parameters for each inbound connection
type ConnectionParameters struct {
	ConnectionTimeout time.Duration // maximum time to complete negotiation
}

// Connection holds the details for each connection
type Connection struct {
	params             *ConnectionParameters // parameters
	conn               net.Conn              // the connection that is used as the NBD transport
	plainConn          net.Conn              // the unencrypted (original) connection
	logger             *slog.Logger          // a logger
	cfg                *sessionConfig        // immutable configuration for this connection
	export             *Export               // a pointer to the export
	backend            Backend               // the backend implementation
	wg                 sync.WaitGroup        // a waitgroup for the session; we mark this as done on exit
	rxCh               chan Request          // a channel of requests that have been received, and need to be dispatched to a worker
	txCh               chan Request          // a channel of outputs from the worker. By this time they have replies in that need to be transmitted
	name               string                // the name of the connection for logging purposes
	disconnectReceived int64                 // nonzero if disconnect has been received
	numInflight        int64                 // number of inflight requests

	memBlockCh         chan []byte // channel of memory blocks that are free
	memBlocksMaximum   int64       // maximum blocks that may be allocated
	memBlocksAllocated int64       // blocks allocated now
	memBlocksFreeLWM   int         // smallest number of blocks free over period
	memBlocksMutex     sync.Mutex  // protects memBlocksAllocated and memBlocksFreeLWM

	killCh    chan struct{} // closed by workers to indicate a hard close is required
	killed    bool          // true if killCh closed already
	killMutex sync.Mutex    // protects killed
}

// Backend is an interface implemented by the various backend drivers
type Backend interface {
	WriteAt(ctx context.Context, b []byte, offset int64, fua bool) (int, error) // write data b at offset, with force unit access optional
	ReadAt(ctx context.Context, b []byte, offset int64) (int, error)            // read to b at offset
	TrimAt(ctx context.Context, length int, offset int64) (int, error)          // trim
	Flush(ctx context.Context) error                                            // flush
	Close(ctx context.Context) error                                            // close
	Geometry(ctx context.Context) (Geometry, error)
	HasFua(ctx context.Context) bool   // does the driver support FUA?
	HasFlush(ctx context.Context) bool // does the driver support flush?
}

// Details of an export
type Export struct {
	size               uint64 // size in bytes
	minimumBlockSize   uint64 // minimum block size
	preferredBlockSize uint64 // preferred block size
	maximumBlockSize   uint64 // maximum block size
	memoryBlockSize    uint64 // block size for memory chunks
	exportFlags        uint16 // export flags in NBD format
	name               string // name of the export
	description        string // description of the export
	readonly           bool   // true if read only
	workers            int    // number of workers
}

// Request is an internal structure for propagating requests through the channels
type Request struct {
	nbdReq  nbdRequest // the request in nbd format
	nbdRep  nbdReply   // the reply in nbd format
	length  uint64     // the checked length
	offset  uint64     // the checked offset
	reqData [][]byte   // request data (e.g. for a write)
	repData [][]byte   // reply data (e.g. for a read)
	flags   uint64     // our internal flag structure characterizing the request
}

// newConection returns a new Connection object
func newConnection(cfg *sessionConfig, logger *slog.Logger, conn net.Conn) (*Connection, error) {
	if logger == nil {
		logger = slog.Default()
	}
	timeout := defaultConnectionTimeout
	if cfg != nil && cfg.connectionTimeout > 0 {
		timeout = cfg.connectionTimeout
	}
	params := &ConnectionParameters{ConnectionTimeout: timeout}
	c := &Connection{
		plainConn: conn,
		cfg:       cfg,
		logger:    logger,
		params:    params,
	}
	return c, nil
}

// NbdError translates an error returned by golang into an NBD error
//
// This function could do with some serious work!
func NbdError(err error) uint32 {
	return NBD_EIO
}

// isClosedErr returns true if the error related to use of a closed connection.
//
// this is particularly foul but is used to surpress errors that relate to use of a closed connection. This is because
// they only arise as we ourselves close the connection to get blocking reads/writes to safely terminate, and thus do
// not want to report them to the user as an error
func isClosedErr(err error) bool {
	return err != nil && errors.Is(err, net.ErrClosed)
}

// Kill kills a connection. This safely ensures the kill channel is closed if it isn't already, which will
// kill all the goroutines
func (c *Connection) Kill(ctx context.Context) {
	c.killMutex.Lock()
	defer c.killMutex.Unlock()
	if !c.killed {
		close(c.killCh)
		c.killed = true
	}
}

// Get memory for a particular length
func (c *Connection) GetMemory(ctx context.Context, length uint64) [][]byte {
	n := (length + c.export.memoryBlockSize - 1) / c.export.memoryBlockSize
	mem := make([][]byte, n, n)
	c.memBlocksMutex.Lock()
	for i := uint64(0); i < n; i++ {
		var m []byte
		var ok bool
		select {
		case <-ctx.Done():
			c.memBlocksMutex.Unlock()
			return nil
		case m, ok = <-c.memBlockCh:
			if !ok {
				c.logger.ErrorContext(ctx, "Memory channel closed")
				c.memBlocksMutex.Unlock()
				return nil
			}
		default:
			c.memBlocksFreeLWM = 0 // ensure no more are freed
			if c.memBlocksAllocated < c.memBlocksMaximum {
				c.memBlocksAllocated++
				m = make([]byte, c.export.memoryBlockSize)
			} else {
				c.memBlocksMutex.Unlock()
				select {
				case m, ok = <-c.memBlockCh:
					if !ok {
						c.logger.ErrorContext(ctx, "Memory channel closed")
						return nil
					}
				case <-ctx.Done():
					return nil
				}
				c.memBlocksMutex.Lock()
			}
		}
		mem[i] = m
	}
	if freeBlocks := len(c.memBlockCh); freeBlocks < c.memBlocksFreeLWM {
		c.memBlocksFreeLWM = freeBlocks
	}
	c.memBlocksMutex.Unlock()
	return mem
}

// Get memory for a particular length
func (c *Connection) FreeMemory(ctx context.Context, mem [][]byte) {
	n := len(mem)
	i := 0
pushloop:
	for ; i < n; i++ {
		select {
		case <-ctx.Done():
			break pushloop
		case c.memBlockCh <- mem[i]:
			mem[i] = nil
		default:
			break pushloop
		}
	}
	c.memBlocksMutex.Lock()
	defer c.memBlocksMutex.Unlock()
	for ; i < n; i++ {
		mem[i] = nil
		c.memBlocksAllocated--
	}
}

// Zero memory
func (c *Connection) ZeroMemory(ctx context.Context, mem [][]byte) {
	for i, _ := range mem {
		for j, _ := range mem[i] {
			mem[i][j] = 0
		}
	}
}

// periodically return all memory under the low water mark back to the OS
func (c *Connection) ReturnMemory(ctx context.Context) {
	defer func() {
		c.memBlocksMutex.Lock()
		alloc := c.memBlocksAllocated
		free := len(c.memBlockCh)
		lwm := c.memBlocksFreeLWM
		c.memBlocksMutex.Unlock()

		c.logger.DebugContext(ctx, "ReturnMemory exiting", "alloc", alloc, "free", free, "lwm", lwm)
		c.Kill(ctx)
		c.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			c.memBlocksMutex.Lock()
			freeBlocks := len(c.memBlockCh)
			if freeBlocks < c.memBlocksFreeLWM {
				c.memBlocksFreeLWM = freeBlocks
			}
		returnloop:
			for n := 0; n < c.memBlocksFreeLWM; n++ {
				select {
				case _, ok := <-c.memBlockCh:
					if !ok {
						return
					}
					c.memBlocksAllocated--
				default:
					break returnloop
				}
			}
			c.memBlocksFreeLWM = freeBlocks
			c.memBlocksMutex.Unlock()
		}
	}
}

// Receive is the goroutine that handles decoding conncetion data from the socket
func (c *Connection) Receive(ctx context.Context) {
	defer func() {
		c.logger.DebugContext(ctx, "Receiver exiting")
		c.Kill(ctx)
		c.wg.Done()
	}()
	for {
		req := Request{}
		if err := binary.Read(c.conn, binary.BigEndian, &req.nbdReq); err != nil {
			if nerr, ok := err.(net.Error); ok {
				if nerr.Timeout() {
					c.logger.InfoContext(ctx, "Client timeout, closing connection")
					return
				}
			}
			if isClosedErr(err) {
				// Don't report this - we closed it
				return
			}
			if err == io.EOF {
				c.logger.WarnContext(ctx, "Client closed connection abruptly")
			} else {
				c.logger.ErrorContext(ctx, "Client could not read request", "err", err)
			}
			return
		}

		if req.nbdReq.NbdRequestMagic != NBD_REQUEST_MAGIC {
			c.logger.ErrorContext(ctx, "Bad request magic", "magic", req.nbdReq.NbdRequestMagic)
			return
		}

		req.nbdRep = nbdReply{
			NbdReplyMagic: NBD_REPLY_MAGIC,
			NbdHandle:     req.nbdReq.NbdHandle,
			NbdError:      0,
		}

		cmd := req.nbdReq.NbdCommandType
		var ok bool
		if req.flags, ok = CmdTypeMap[int(cmd)]; !ok {
			c.logger.ErrorContext(ctx, "Unknown command", "cmd", cmd)
			return
		}

		if req.flags&CMDT_SET_DISCONNECT_RECEIVED != 0 {
			// we process this here as commands may otherwise be processed out
			// of order and per the spec we should not receive any more
			// commands after receiving a disconnect
			atomic.StoreInt64(&c.disconnectReceived, 1)
		}

		if req.flags&CMDT_CHECK_LENGTH_OFFSET != 0 {
			req.length = uint64(req.nbdReq.NbdLength)
			req.offset = req.nbdReq.NbdOffset
			if req.length <= 0 || req.length+req.offset > c.export.size {
				c.logger.ErrorContext(ctx, "Invalid offset/length", "offset", req.offset, "length", req.length, "size", c.export.size)
				return
			}
			if req.length&(c.export.minimumBlockSize-1) != 0 || req.offset&(c.export.minimumBlockSize-1) != 0 || req.length > c.export.maximumBlockSize {
				c.logger.ErrorContext(ctx, "Offset/length outside block size constraints",
					"cmd", req.nbdReq.NbdCommandType,
					"offset", req.offset,
					"length", req.length,
					"min_block_size", c.export.minimumBlockSize,
					"max_block_size", c.export.maximumBlockSize,
				)
				return
			}
		}

		if req.flags&CMDT_REQ_PAYLOAD != 0 {
			if req.reqData = c.GetMemory(ctx, req.length); req.reqData == nil {
				// error already logged
				return
			}
			if req.length <= 0 {
				c.logger.ErrorContext(ctx, "Invalid length", "length", req.length)
				return
			}
			length := req.length
			for i := 0; length > 0; i++ {
				blocklen := c.export.memoryBlockSize
				if blocklen > length {
					blocklen = length
				}
				n, err := io.ReadFull(c.conn, req.reqData[i][:blocklen])
				if err != nil {
					if isClosedErr(err) {
						// Don't report this - we closed it
						return
					}

					c.logger.ErrorContext(ctx, "Cannot read write payload", "err", err)
					return
				}

				if uint64(n) != blocklen {
					c.logger.ErrorContext(ctx, "Short read of write payload", "got", n, "want", blocklen)
					return

				}
				length -= blocklen
			}

		} else if req.flags&CMDT_REQ_FAKE_PAYLOAD != 0 {
			if req.reqData = c.GetMemory(ctx, req.length); req.reqData == nil {
				// error printed already
				return
			}
			c.ZeroMemory(ctx, req.reqData)
		}

		if req.flags&CMDT_REP_PAYLOAD != 0 {
			if req.repData = c.GetMemory(ctx, req.length); req.repData == nil {
				// error printed already
				return
			}
		}

		atomic.AddInt64(&c.numInflight, 1) // one more in flight
		if req.flags&CMDT_CHECK_NOT_READ_ONLY != 0 && c.export.readonly {
			req.nbdRep.NbdError = NBD_EPERM
			select {
			case c.txCh <- req:
			case <-ctx.Done():
				return
			}
		} else {
			select {
			case c.rxCh <- req:
			case <-ctx.Done():
				return
			}
		}
		// if we've recieved a disconnect, just sit waiting for the
		// context to indicate we've done
		if atomic.LoadInt64(&c.disconnectReceived) > 0 {
			select {
			case <-ctx.Done():
				return
			}
		}
	}
}

// checkpoint is an internal debugging routine
func checkpoint(t *time.Time) time.Duration {
	t1 := time.Now()
	d := t1.Sub(*t)
	*t = t1
	return d
}

// Dispatch is the goroutine used to process received items, passing the reply to the transmit goroutine
//
// one of these is run for each worker
func (c *Connection) Dispatch(ctx context.Context, n int) {
	defer func() {
		c.logger.InfoContext(ctx, "Dispatcher exiting", "worker", n)
		c.Kill(ctx)
		c.wg.Done()
	}()
	//t := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-c.rxCh:
			if !ok {
				return
			}
			fua := req.nbdReq.NbdCommandFlags&NBD_CMD_FLAG_FUA != 0

			addr := req.offset
			length := req.length
			switch req.nbdReq.NbdCommandType {
			case NBD_CMD_READ:
				for i := 0; length > 0; i++ {
					blocklen := c.export.memoryBlockSize
					if blocklen > length {
						blocklen = length
					}
					n, err := c.backend.ReadAt(ctx, req.repData[i][:blocklen], int64(addr))
					if err != nil {
						c.ZeroMemory(ctx, req.repData[i:])
						c.logger.WarnContext(ctx, "Read I/O error", "err", err, "offset", addr, "length", blocklen)
						req.nbdRep.NbdError = NbdError(err)
						break
					} else if uint64(n) != blocklen {
						c.ZeroMemory(ctx, req.repData[i:])
						c.logger.WarnContext(ctx, "Short read", "got", n, "want", blocklen, "offset", addr)
						req.nbdRep.NbdError = NBD_EIO
						break
					}
					addr += blocklen
					length -= blocklen
				}
			case NBD_CMD_WRITE, NBD_CMD_WRITE_ZEROES, NBD_CMD_WRITE_ZEROES_LEGACY:
				for i := 0; length > 0; i++ {
					blocklen := c.export.memoryBlockSize
					if blocklen > length {
						blocklen = length
					}
					n, err := c.backend.WriteAt(ctx, req.reqData[i][:blocklen], int64(addr), fua)
					if err != nil {
						c.logger.WarnContext(ctx, "Write I/O error", "err", err, "offset", addr, "length", blocklen, "fua", fua)
						req.nbdRep.NbdError = NbdError(err)
						break
					} else if uint64(n) != blocklen {
						c.logger.WarnContext(ctx, "Short write", "got", n, "want", blocklen, "offset", addr, "fua", fua)
						req.nbdRep.NbdError = NBD_EIO
						break
					}
					addr += blocklen
					length -= blocklen
				}
			case NBD_CMD_FLUSH:
				if err := c.backend.Flush(ctx); err != nil {
					c.logger.WarnContext(ctx, "Flush I/O error", "err", err)
					req.nbdRep.NbdError = NbdError(err)
					break
				}
			case NBD_CMD_TRIM:
				for i := 0; length > 0; i++ {
					blocklen := c.export.memoryBlockSize
					if blocklen > length {
						blocklen = length
					}
					n, err := c.backend.TrimAt(ctx, int(req.length), int64(addr))
					if err != nil {
						c.ZeroMemory(ctx, req.repData[i:])
						c.logger.WarnContext(ctx, "Trim I/O error", "err", err, "offset", addr, "length", blocklen)
						req.nbdRep.NbdError = NbdError(err)
						break
					} else if uint64(n) != blocklen {
						c.ZeroMemory(ctx, req.repData[i:])
						c.logger.WarnContext(ctx, "Short trim", "got", n, "want", blocklen, "offset", addr)
						req.nbdRep.NbdError = NBD_EIO
						break
					}
					addr += blocklen
					length -= blocklen
				}
			case NBD_CMD_DISC:
				c.waitForInflight(ctx, 1) // this request is itself in flight, so 1 is permissible
				c.backend.Flush(ctx)
				c.logger.InfoContext(ctx, "Client requested disconnect")
				return
			case NBD_CMD_CLOSE:
				c.waitForInflight(ctx, 1) // this request is itself in flight, so 1 is permissible
				c.backend.Flush(ctx)
				c.logger.InfoContext(ctx, "Client requested close")
				select {
				case c.txCh <- req:
				case <-ctx.Done():
				}
				c.waitForInflight(ctx, 0) // wait for this request to be no longer inflight (i.e. reply transmitted)
				c.logger.InfoContext(ctx, "Client close completed")
				return
			default:
				c.logger.ErrorContext(ctx, "Client sent unknown command", "cmd", req.nbdReq.NbdCommandType)
				return
			}
			select {
			case c.txCh <- req:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *Connection) waitForInflight(ctx context.Context, limit int64) {
	c.logger.InfoContext(ctx, "Waiting for inflight requests prior to disconnect", "limit", limit)
	for {
		if atomic.LoadInt64(&c.numInflight) <= limit {
			return
		}
		// this is pretty nasty in that it would be nicer to wait on
		// a channel or use a (non-existent) waitgroup with timer.
		// however it's only one atomic read every 10ms and this
		// will hardly ever occur
		time.Sleep(10 * time.Millisecond)
	}
}

// Transmit is the goroutine run to transmit the processed requests (now replies)
func (c *Connection) Transmit(ctx context.Context) {
	defer func() {
		c.logger.InfoContext(ctx, "Transmitter exiting")
		c.Kill(ctx)
		c.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-c.txCh:
			if !ok {
				return
			}
			if err := binary.Write(c.conn, binary.BigEndian, req.nbdRep); err != nil {
				c.logger.ErrorContext(ctx, "Cannot write reply", "err", err)
				return
			}
			if req.flags&CMDT_REP_PAYLOAD != 0 && req.repData != nil {
				length := req.length
				for i := 0; length > 0; i++ {
					blocklen := c.export.memoryBlockSize
					if blocklen > length {
						blocklen = length
					}
					if n, err := c.conn.Write(req.repData[i][:blocklen]); err != nil {
						c.logger.ErrorContext(ctx, "Cannot write reply", "err", err)
						return
					} else if uint64(n) != blocklen {
						c.logger.ErrorContext(ctx, "Short write of reply", "got", n, "want", blocklen)
						return
					}
					length -= blocklen
				}
			}
			if req.repData != nil {
				c.FreeMemory(ctx, req.repData)
			}
			if req.reqData != nil {
				c.FreeMemory(ctx, req.reqData)
			}
			// TODO: with structured replies, only do this if the 'DONE' bit is set.
			atomic.AddInt64(&c.numInflight, -1) // one less in flight
		}
	}
}

// Serve negotiates, then starts all the goroutines for processing a connection,
// then waits for them to be ended.
//
// It returns a non-nil error only if negotiation fails.
func (c *Connection) Serve(parentCtx context.Context) error {
	ctx, cancelFunc := context.WithCancel(parentCtx)

	c.rxCh = make(chan Request, 1024)
	c.txCh = make(chan Request, 1024)
	c.killCh = make(chan struct{})

	c.conn = c.plainConn
	c.name = c.plainConn.RemoteAddr().String()
	if c.name == "" {
		c.name = "[unknown]"
	}
	c.logger = c.logger.With("remote", c.name)

	defer func() {
		if c.backend != nil {
			c.backend.Close(ctx)
		}
		c.plainConn.Close()
		cancelFunc()
		c.Kill(ctx) // to ensure the kill channel is closed
		c.wg.Wait()
		close(c.rxCh)
		close(c.txCh)
		if c.memBlockCh != nil {
		freemem:
			for {
				select {
				case _, ok := <-c.memBlockCh:
					if !ok {
						break freemem
					}
				default:
					break freemem
				}
			}
			close(c.memBlockCh)
		}
		c.logger.InfoContext(ctx, "Closed connection")
	}()

	if err := c.Negotiate(ctx); err != nil {
		c.logger.InfoContext(ctx, "Negotiation failed", "err", err)
		return err
	}

	c.memBlocksMaximum = int64(((c.export.maximumBlockSize + c.export.memoryBlockSize - 1) / c.export.memoryBlockSize) * 2)
	c.memBlockCh = make(chan []byte, c.memBlocksMaximum+1)

	c.logger = c.logger.With("export", c.export.name)

	workers := c.export.workers

	if workers < 1 {
		workers = DefaultWorkers
	}

	c.logger.InfoContext(ctx, "Negotiation succeeded", "workers", workers)

	c.wg.Add(3)
	go c.Receive(ctx)
	go c.Transmit(ctx)
	go c.ReturnMemory(ctx)
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.Dispatch(ctx, i)
	}

	// Wait until either we are explicitly killed or one of our
	// workers dies
	select {
	case <-c.killCh:
		c.logger.InfoContext(ctx, "Worker forced close")
	case <-ctx.Done():
		c.logger.InfoContext(ctx, "Parent forced close")
	}

	return nil
}

// skip bytes
func skip(r io.Reader, n uint32) error {
	for n > 0 {
		l := n
		if l > 1024 {
			l = 1024
		}
		b := make([]byte, l)
		if nr, err := io.ReadFull(r, b); err != nil {
			return err
		} else if nr != int(l) {
			return errors.New("skip returned short read")
		}
		n -= l
	}
	return nil
}

// Negotiate negotiates a connection
func (c *Connection) Negotiate(ctx context.Context) error {
	c.conn.SetDeadline(time.Now().Add(c.params.ConnectionTimeout))

	// We send a newstyle header
	nsh := nbdNewStyleHeader{
		NbdMagic:       NBD_MAGIC,
		NbdOptsMagic:   NBD_OPTS_MAGIC,
		NbdGlobalFlags: NBD_FLAG_FIXED_NEWSTYLE,
	}

	if c.cfg == nil || !c.cfg.disableNoZeroes {
		nsh.NbdGlobalFlags |= NBD_FLAG_NO_ZEROES
	}

	if err := binary.Write(c.conn, binary.BigEndian, nsh); err != nil {
		return errors.New("Cannot write magic header")
	}

	// next they send client flags
	var clf nbdClientFlags

	if err := binary.Read(c.conn, binary.BigEndian, &clf); err != nil {
		return errors.New("Cannot read client flags")
	}

	done := false
	// now we get options
	for !done {
		var opt nbdClientOpt
		if err := binary.Read(c.conn, binary.BigEndian, &opt); err != nil {
			return errors.New("Cannot read option (perhaps client dropped the connection)")
		}
		if opt.NbdOptMagic != NBD_OPTS_MAGIC {
			return errors.New("Bad option magic")
		}
		if opt.NbdOptLen > 65536 {
			return errors.New("Option is too long")
		}
		switch opt.NbdOptId {
		case NBD_OPT_EXPORT_NAME, NBD_OPT_INFO, NBD_OPT_GO:
			var name []byte

			clientSupportsBlockSizeConstraints := false

			if opt.NbdOptId == NBD_OPT_EXPORT_NAME {
				name = make([]byte, opt.NbdOptLen)
				n, err := io.ReadFull(c.conn, name)
				if err != nil {
					return err
				}
				if uint32(n) != opt.NbdOptLen {
					return errors.New("Incomplete name")
				}
			} else {
				var nameLength uint32
				if err := binary.Read(c.conn, binary.BigEndian, &nameLength); err != nil {
					return errors.New("Bad export name length")
				}
				if nameLength > 4096 {
					return errors.New("Name is too long")
				}
				name = make([]byte, nameLength)
				n, err := io.ReadFull(c.conn, name)
				if err != nil {
					return err
				}
				if uint32(n) != nameLength {
					return errors.New("Incomplete name")
				}
				var numInfoElements uint16
				if err := binary.Read(c.conn, binary.BigEndian, &numInfoElements); err != nil {
					return errors.New("Bad number of info elements")
				}
				for i := uint16(0); i < numInfoElements; i++ {
					var infoElement uint16
					if err := binary.Read(c.conn, binary.BigEndian, &infoElement); err != nil {
						return errors.New("Bad number of info elements")
					}
					switch infoElement {
					case NBD_INFO_BLOCK_SIZE:
						clientSupportsBlockSizeConstraints = true
					}
				}
				l := 2 + 2*uint32(numInfoElements) + 4 + uint32(nameLength)
				if opt.NbdOptLen > l {
					if err := skip(c.conn, opt.NbdOptLen-l); err != nil {
						return err
					}
				} else if opt.NbdOptLen < l {
					return errors.New("Option length too short")
				}
			}

			if len(name) == 0 {
				if c.cfg != nil {
					name = []byte(c.cfg.defaultExport)
				}
			}

			if len(name) == 0 {
				err := errors.New("No export name specified")
				if opt.NbdOptId == NBD_OPT_EXPORT_NAME {
					// we have to just abort here
					return err
				}
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_ERR_UNKNOWN,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send info error")
				}
				break
			}

			// Next find our export
			ec, err := c.resolveExport(ctx, string(name))
			if err != nil {
				if opt.NbdOptId == NBD_OPT_EXPORT_NAME {
					// we have to just abort here
					return err
				}
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_ERR_UNKNOWN,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send info error")
				}
				break
			}

			// Now we know we are going to go with the export for sure
			// any failure beyond here and we are going to drop the
			// connection (assuming we aren't doing NBD_OPT_INFO)
			export, err := c.connectExport(ctx, ec)
			if err != nil {
				if opt.NbdOptId == NBD_OPT_EXPORT_NAME {
					return err
				}
				c.logger.InfoContext(ctx, "Could not connect client to export", "export", string(name), "err", err)
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_ERR_UNKNOWN,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot send info error")
				}
				break
			}

			// for the reply
			name = []byte(export.name)
			description := []byte(export.description)

			if opt.NbdOptId == NBD_OPT_EXPORT_NAME {
				// this option has a unique reply format
				ed := nbdExportDetails{
					NbdExportSize:  export.size,
					NbdExportFlags: export.exportFlags,
				}
				if err := binary.Write(c.conn, binary.BigEndian, ed); err != nil {
					return errors.New("Cannot write export details")
				}
			} else {
				// Send NBD_INFO_EXPORT
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: 12,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info export pt1")
				}
				ir := nbdInfoExport{
					NbdInfoType:          NBD_INFO_EXPORT,
					NbdExportSize:        export.size,
					NbdTransmissionFlags: export.exportFlags,
				}
				if err := binary.Write(c.conn, binary.BigEndian, ir); err != nil {
					return errors.New("Cannot write info export pt2")
				}

				// Send NBD_INFO_NAME
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: uint32(2 + len(name)),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info name pt1")
				}
				if err := binary.Write(c.conn, binary.BigEndian, uint16(NBD_INFO_NAME)); err != nil {
					return errors.New("Cannot write name id")
				}
				if err := binary.Write(c.conn, binary.BigEndian, name); err != nil {
					return errors.New("Cannot write name")
				}

				// Send NBD_INFO_DESCRIPTION
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: uint32(2 + len(description)),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info description pt1")
				}
				if err := binary.Write(c.conn, binary.BigEndian, uint16(NBD_INFO_DESCRIPTION)); err != nil {
					return errors.New("Cannot write description id")
				}
				if err := binary.Write(c.conn, binary.BigEndian, description); err != nil {
					return errors.New("Cannot write description")
				}

				// Send NBD_INFO_BLOCK_SIZE
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_INFO,
					NbdOptReplyLength: 14,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot write info block size pt1")
				}
				ir2 := nbdInfoBlockSize{
					NbdInfoType:           NBD_INFO_BLOCK_SIZE,
					NbdMinimumBlockSize:   uint32(export.minimumBlockSize),
					NbdPreferredBlockSize: uint32(export.preferredBlockSize),
					NbdMaximumBlockSize:   uint32(export.maximumBlockSize),
				}
				if err := binary.Write(c.conn, binary.BigEndian, ir2); err != nil {
					return errors.New("Cannot write info block size pt2")
				}

				replyType := NBD_REP_ACK

				if export.minimumBlockSize > 1 && !clientSupportsBlockSizeConstraints {
					replyType = NBD_REP_ERR_BLOCK_SIZE_REQD
				}

				// Send ACK or error
				or = nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   replyType,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot info ack")
				}
				if opt.NbdOptId == NBD_OPT_INFO || or.NbdOptReplyType&NBD_REP_FLAG_ERROR != 0 {
					// Disassociate the backend as we are not closing
					c.backend.Close(ctx)
					c.backend = nil
					break
				}
			}

			if clf.NbdClientFlags&NBD_FLAG_C_NO_ZEROES == 0 && opt.NbdOptId == NBD_OPT_EXPORT_NAME {
				// send 124 bytes of zeroes.
				zeroes := make([]byte, 124, 124)
				if err := binary.Write(c.conn, binary.BigEndian, zeroes); err != nil {
					return errors.New("Cannot write zeroes")
				}
			}
			c.export = export
			done = true

		case NBD_OPT_LIST:
			if c.cfg == nil || c.cfg.listExports == nil {
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_ERR_UNSUP,
					NbdOptReplyLength: 0,
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					return errors.New("Cannot reply to unsupported list option")
				}
				break
			}

			var listWriteErr error
			if err := c.cfg.listExports(ctx, func(name string) bool {
				if name == "" {
					return true
				}
				b := []byte(name)
				or := nbdOptReply{
					NbdOptReplyMagic:  NBD_REP_MAGIC,
					NbdOptId:          opt.NbdOptId,
					NbdOptReplyType:   NBD_REP_SERVER,
					NbdOptReplyLength: uint32(len(b) + 4),
				}
				if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
					listWriteErr = errors.New("Cannot send list item")
					return false
				}
				l := uint32(len(b))
				if err := binary.Write(c.conn, binary.BigEndian, l); err != nil {
					listWriteErr = errors.New("Cannot send list name length")
					return false
				}
				if n, err := c.conn.Write(b); err != nil || n != len(b) {
					listWriteErr = errors.New("Cannot send list name")
					return false
				}
				return true
			}); err != nil {
				return err
			}
			if listWriteErr != nil {
				return listWriteErr
			}

			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptId:          opt.NbdOptId,
				NbdOptReplyType:   NBD_REP_ACK,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot send list ack")
			}
		case NBD_OPT_ABORT:
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptId:          opt.NbdOptId,
				NbdOptReplyType:   NBD_REP_ACK,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot send abort ack")
			}
			return errors.New("Connection aborted by client")
		default:
			// eat the option
			if err := skip(c.conn, opt.NbdOptLen); err != nil {
				return err
			}
			// say it's unsuppported
			or := nbdOptReply{
				NbdOptReplyMagic:  NBD_REP_MAGIC,
				NbdOptId:          opt.NbdOptId,
				NbdOptReplyType:   NBD_REP_ERR_UNSUP,
				NbdOptReplyLength: 0,
			}
			if err := binary.Write(c.conn, binary.BigEndian, or); err != nil {
				return errors.New("Cannot reply to unsupported option")
			}
		}
	}

	c.conn.SetDeadline(time.Time{})
	return nil
}

func (c *Connection) resolveExport(ctx context.Context, name string) (*ExportOptions, error) {
	if c.cfg == nil || c.cfg.resolveExport == nil {
		return nil, errors.New("No export resolver configured")
	}
	ec, err := c.cfg.resolveExport(ctx, name)
	if err != nil {
		return nil, err
	}
	if ec == nil {
		return nil, ErrNoSuchExport
	}
	if ec.Name == "" {
		// Don't mutate shared state returned by the user.
		x := *ec
		x.Name = name
		ec = &x
	} else if ec.Name != name {
		return nil, fmt.Errorf("Resolver returned export %q for name %q", ec.Name, name)
	}
	if ec.OpenBackend == nil {
		return nil, fmt.Errorf("Export %q has no backend", name)
	}
	return ec, nil
}

// round a uint64 up to the next power of two
func roundUpToNextPowerOfTwo(x uint64) uint64 {
	var r uint64 = 1
	for i := 0; i < 64; i++ {
		if x <= r {
			return r
		}
		r = r << 1
	}
	return 0 // won't fit in uint64 :-(
}

// connectExport generates an export for a given name, and connects to it using the chosen backend
func (c *Connection) connectExport(ctx context.Context, ec *ExportOptions) (*Export, error) {
	if ec.OpenBackend == nil {
		return nil, fmt.Errorf("export %q has no backend", ec.Name)
	}

	backend, err := ec.OpenBackend(ctx, ec)
	if err != nil {
		return nil, err
	}

	geom, err := backend.Geometry(ctx)
	if err != nil {
		_ = backend.Close(ctx)
		return nil, err
	}
	size := geom.Size
	minimumBlockSize := geom.MinimumBlockSize
	preferredBlockSize := geom.PreferredBlockSize
	maximumBlockSize := geom.MaximumBlockSize

	if c.backend != nil {
		_ = c.backend.Close(ctx)
	}
	c.backend = backend

	if ec.MinimumBlockSize != 0 {
		minimumBlockSize = ec.MinimumBlockSize
	}
	if ec.PreferredBlockSize != 0 {
		preferredBlockSize = ec.PreferredBlockSize
	}
	if ec.MaximumBlockSize != 0 {
		maximumBlockSize = ec.MaximumBlockSize
	}
	if minimumBlockSize == 0 {
		minimumBlockSize = 1
	}
	minimumBlockSize = roundUpToNextPowerOfTwo(minimumBlockSize)
	preferredBlockSize = roundUpToNextPowerOfTwo(preferredBlockSize)
	// ensure preferredBlockSize is a multiple of the minimum block size
	preferredBlockSize = preferredBlockSize & ^(minimumBlockSize - 1)
	if preferredBlockSize < minimumBlockSize {
		preferredBlockSize = minimumBlockSize
	}
	// ensure maximumBlockSize is a multiple of preferredBlockSize
	maximumBlockSize = maximumBlockSize & ^(preferredBlockSize - 1)
	if maximumBlockSize < preferredBlockSize {
		maximumBlockSize = preferredBlockSize
	}

	flags := uint16(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_WRITE_ZEROES | NBD_FLAG_SEND_CLOSE)
	if ec.ReadOnly {
		flags |= NBD_FLAG_READ_ONLY
	}
	if backend.HasFua(ctx) {
		flags |= NBD_FLAG_SEND_FUA
	}
	if backend.HasFlush(ctx) {
		flags |= NBD_FLAG_SEND_FLUSH
	}

	size = size & ^(minimumBlockSize - 1)
	return &Export{
		size:               size,
		exportFlags:        flags,
		name:               ec.Name,
		readonly:           ec.ReadOnly,
		workers:            ec.Workers,
		description:        ec.Description,
		minimumBlockSize:   minimumBlockSize,
		preferredBlockSize: preferredBlockSize,
		maximumBlockSize:   maximumBlockSize,
		memoryBlockSize:    preferredBlockSize,
	}, nil
}
