package zconns

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gokit/npkg/nbytes"
	"github.com/gokit/npkg/nerror"
	"github.com/gokit/npkg/nxid"
	"golang.org/x/sync/errgroup"
)

//*********************************************************************************************
// ZTimedConn
//*********************************************************************************************

var (
	noTime = time.Time{}
)

// ZTimeout defines a function which takes previous timeout returning same or
// new timeout to use for next operation.
type ZTimeout func(time.Duration) time.Duration

// NowTime defines a function which consistently returns
// a new time.
type NowTime func() time.Time

// ZTimedConn implements a wrapper around a net.Conn which guards giving connection
// with appropriate read/write timeout.
type ZTimedConn struct {
	net.Conn
	readTimeout  ZTimeout
	writeTimeout ZTimeout
	defaultRead  time.Duration
	defaultWrite time.Duration
	lastRead     time.Duration
	lastWrite    time.Duration
}

// NewZTimedConn returns a new instance of a ZTimedConn.
func NewZTimedConn(conn net.Conn, defaultRead time.Duration, defaultWrite time.Duration, rd ZTimeout, wd ZTimeout) *ZTimedConn {
	return &ZTimedConn{
		Conn:         conn,
		readTimeout:  rd,
		writeTimeout: wd,
		defaultRead:  defaultRead,
		defaultWrite: defaultWrite,
	}
}

// Write calls the underline connection read with provided timeout.
func (c *ZTimedConn) Write(b []byte) (int, error) {
	c.lastWrite = c.readTimeout(c.lastWrite)
	var writeErr = c.Conn.SetWriteDeadline(time.Now().Add(c.lastWrite))
	if writeErr != nil {
		return 0, writeErr
	}

	var writeCount, err = c.Conn.Write(b)
	if err != nil {
		_ = c.Conn.SetWriteDeadline(noTime)
		return writeCount, err
	}

	var resetErr = c.Conn.SetWriteDeadline(noTime)
	if resetErr != nil {
		return writeCount, resetErr
	}

	c.lastWrite = c.defaultWrite
	return writeCount, nil
}

// Read calls the underline connection read with provided timeout.
func (c *ZTimedConn) Read(b []byte) (int, error) {
	c.lastRead = c.readTimeout(c.lastRead)
	var readErr = c.Conn.SetReadDeadline(time.Now().Add(c.lastRead))
	if readErr != nil {
		return 0, readErr
	}

	var readCount, err = c.Conn.Read(b)
	if err != nil {
		_ = c.Conn.SetReadDeadline(noTime)
		return readCount, err
	}

	var resetErr = c.Conn.SetReadDeadline(noTime)
	if resetErr != nil {
		return readCount, resetErr
	}

	c.lastRead = c.defaultRead
	return readCount, nil
}

//**********************************************************************
// Workers
//**********************************************************************

// TCPWorker implements the ZConnWorker interface for handling TCP
// payload read/write handling.
type TCPWorker struct {
	Debug bool

	// Buffer represents the underline maximum allowed space reading
	// from reader to writer, it will be used as a constant read space
	// till all bytes have being read from underline reader to writer.
	Buffer []byte
}

// ServeRead handles reading data from underline connection into payload writer.
func (zc *TCPWorker) ServeRead(ctx context.Context, src io.Reader, zp *ZPayload) error {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	//if wt, ok := src.(io.WriterTo); ok {
	//	var n, err = wt.WriteTo(zp.Writer)
	//	if err != nil && err != io.EOF {
	//		if zc.Debug {
	//			log.Printf("[TCPWorker.ServeRead] | Failed to finish read copy: %s", err)
	//		}
	//		return err
	//	}
	//
	//	if zc.Debug {
	//		log.Printf("[TCPWorker.ServeRead] | Read %d bytes from connection", n)
	//	}
	//	return nil
	//}

	var read, err = copyBuffer(zp.Writer, src, zc.Buffer)
	if err != nil {
		if nerror.IsAny(err, nbytes.ErrEOS) {
			if zc.Debug {
				log.Printf("[TCPWorker.ServeRead] | Read %d bytes from connection", read)
			}
			return nil
		}

		if zc.Debug {
			log.Printf("[TCPWorker.ServeRead] | Failed to finish read copy: %s", err)
		}
		return err
	}

	if zc.Debug {
		log.Printf("[TCPWorker.ServeRead] | Read %d bytes from connection", read)
	}
	return nil
}

// ServeRead handles reading data from Reader from payload into the underline connection.
func (zc *TCPWorker) ServeWrite(ctx context.Context, dest io.Writer, zp *ZPayload) error {
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	//if rt, ok := dest.(io.ReaderFrom); ok {
	//	var n, err = rt.ReadFrom(zp.Reader)
	//	if err != nil && err != io.EOF {
	//		log.Printf("[TCPWorker.ServeWrite] | Failed copy new data into connection: %s", err)
	//		return err
	//	}
	//
	//	if zc.Debug {
	//		log.Printf("[TCPWorker.ServeWrite] | Written %d bytes into connection", n)
	//	}
	//	return nil
	//}

	var written, err = copyBuffer(dest, zp.Reader, zc.Buffer)
	if err != nil {
		if zc.Debug {
			log.Printf("[TCPWorker.ServeWrite] | Failed copy new data into connection: %s", err)
		}
		return err
	}

	if zc.Debug {
		log.Printf("[TCPWorker.ServeWrite] | Written %d bytes into connection", written)
	}

	if err := zp.Reader.Close(); err != nil {
		if zc.Debug {
			log.Printf("[TCPWorker.ServeWrite] | Failed flushing new data into connection: %s", err)
		}
		return err
	}
	return nil
}

// UDPWorker implements the ZConnWorker interface for handling UDP
// payload read/write handling.
type UDPWorker struct {
	Debug bool
	// Buffer represents the underline maximum allowed space reading
	// from reader to writer, it will be used as a constant read space
	// till all bytes have being read from underline reader to writer.
	Buffer []byte
}

// ServeRead handles reading data from underline connection into payload writer.
func (zc *UDPWorker) ServeRead(ctx context.Context, src io.Reader, zp *ZPayload) error {

	return nil
}

// ServeRead handles reading data from Reader from payload into the underline connection.
func (zc *UDPWorker) ServeWrite(ctx context.Context, src io.Writer, zp *ZPayload) error {

	return nil
}

//**********************************************************************
// Server
//**********************************************************************

// ServerHandler defines an interface type for handling connection
// in isolated goroutines.
type ServerHandler interface {
	ServeConn(context.Context, net.Conn) error
}

// Server implements a basic, structured wrapper around a net.Listener
// to handle and process incoming connections.
type Server struct {
	Debug    bool
	Handler  ServerHandler
	Listener net.Listener
	ctx      context.Context
	waiter   *errgroup.Group
}

// NewServer returns a new instance of a Server.
func NewServer(ctx context.Context, handler ServerHandler, listener net.Listener, debug bool) *Server {
	var gp *errgroup.Group
	gp, ctx = errgroup.WithContext(ctx)

	return &Server{
		Debug:    debug,
		ctx:      ctx,
		waiter:   gp,
		Handler:  handler,
		Listener: listener,
	}
}

// Serve initializes the connection handling and reading goroutine.
// It returns immediately without block. It depends on you to use the
// Server.Wait to block as needed till server is closed.
func (s *Server) Serve() {
	go s.handleClosure()
	s.waiter.Go(s.serviceListener)
}

// Wait blocks underline server till connection
// has returned. It returns any possible error that
// occurred.
func (s *Server) Wait() error {
	return s.waiter.Wait()
}

func (s *Server) handleClosure() {
	<-s.ctx.Done()
	if s.Debug {
		log.Println("[Server.handleClosure] | Closing net.Listener")
	}
	if err := s.Listener.Close(); err != nil {
		if s.Debug {
			log.Printf("[Server] | Closing net.Listener with error: %s", err)
		}
		return
	}
	if s.Debug {
		log.Println("[Server.handleClosure] | Closed net.Listener")
	}
}

// serviceListener blocks and handles incoming connections
// by server. It will only ever stop working if accepts
// failed or the Handler.ServerConn returns a ErrKillConnection
// error.
func (s *Server) serviceListener() error {
	for {
		conn, err := s.Listener.Accept()
		if err != nil {
			if tmpErr, ok := err.(net.Error); ok && tmpErr.Temporary() {
				if s.Debug {
					log.Printf("[Server.serviceListener] | net.Listener received temporary error: %s", tmpErr)
				}
				continue
			}

			if s.Debug {
				log.Printf("[Server.serviceListener] | Closed net.Listener Accept loop: %s", err)
			}
			return err
		}

		s.spawnConnectionRoutine(conn)
	}
}

func (s *Server) spawnConnectionRoutine(conn net.Conn) {
	s.waiter.Go(func() error {
		var remoteAddr = conn.RemoteAddr()
		if err := s.Handler.ServeConn(s.ctx, conn); err != nil {
			if s.Debug {
				log.Printf("[Server.spawnConnectionRoutine] | Closing connection for %s with error: %s", remoteAddr, err)
			}
			if err == ErrKillConnection {
				return err
			}
		}
		if s.Debug {
			log.Printf("[Server.spawnConnectionRoutine] | Closed connection for addr: %s", remoteAddr)
		}
		return nil
	})
}

//*********************************************************************************************
// Next Message Format: NX {MAX_64_INT}\r\n\r\n
// Data Message Format: DX {HEADER} {DATA_BYTES}\r\n\r\n
// Delimiter Format: \r\n
//**********************************************************************************************

var (
	// ErrKillConnection defines a returnable error from a
	// ZConnReader and ZConnWriter to ensure giving connection
	// get's cosed due to some underline error.
	ErrKillConnection = errors.New("unrecoverable error")
)

// ZApply defines a function type to apply a change to the ZConn instance.
type ZApply func(*ZConn)

// ZConnNowTime sets a giving function to be used to provide
// timing.
func ZConnNowTime(fn NowTime) ZApply {
	return func(conn *ZConn) {
		conn.nowTime = fn
	}
}

// ZConnWriteTimeout sets the ZTimeout function type to be used for write
// calls for a ZConn.
func ZConnWriteTimeout(t time.Duration) ZApply {
	return func(conn *ZConn) {
		conn.writeTime = t
	}
}

// ZConnReadTimeout sets the ZTimeout function type to be used for read
// calls for a ZConn.
func ZConnReadTimeout(t time.Duration) ZApply {
	return func(conn *ZConn) {
		conn.readTime = t
	}
}

// ZConnWriteBuffer sets the buffer space to be used for write
// calls for a ZConn.
func ZConnWriteBuffer(buffer int) ZApply {
	return func(conn *ZConn) {
		conn.writeBuffer = buffer
	}
}

// ZConnReadBuffer sets the buffer space to be used for read
// calls for a ZConn.
func ZConnReadBuffer(buffer int) ZApply {
	return func(conn *ZConn) {
		conn.readBuffer = buffer
	}
}

// ZConnMaxWrite sets the maximum allowed read size for each data stream.
// The maximum allowed size for each unique message.
func ZConnMaxRead(max int) ZApply {
	return func(conn *ZConn) {
		conn.maxRead = max
	}
}

// ZConnMaxWrite sets the maximum allowed data size for collected data stream.
// The maximum allowed size for each unique message.
func ZConnMaxWrite(max int) ZApply {
	return func(conn *ZConn) {
		conn.maxWrite = max
	}
}

// ZConnDebugMode enables the debug mode.
func ZConnDebugMode() ZApply {
	return func(conn *ZConn) {
		conn.debug = true
	}
}

// ZConnParentContext sets the parent context for giving
// ZConn to respect cancellation.
func ZConnParentContext(ctx context.Context) ZApply {
	return func(conn *ZConn) {
		conn.ctx, conn.ctxCanceler = context.WithCancel(ctx)
	}
}

// ZConnWorker defines a interface type for the servicing of
// an underline read and write requests.
type ZConnWorker interface {
	ServeRead(context.Context, io.Reader, *ZPayload) error
	ServeWrite(context.Context, io.Writer, *ZPayload) error
}

// SetZConnWorker sets the underline worker to be used for handling
// read/write requests for a ZConn.
func SetZConnWorker(worker ZConnWorker) ZApply {
	return func(conn *ZConn) {
		conn.worker = worker
	}
}

var (
	zPayloads = sync.Pool{
		New: func() interface{} {
			var payload ZPayload
			payload.Reset()
			return payload
		},
	}
)

// ZPayload defines an underline structure for writing data
// into an underline ZConn.
type ZPayload struct {
	Flush  bool
	Err    chan error
	Done   chan struct{}
	Addr   chan net.Addr
	Reader io.ReadCloser
	Writer io.WriteCloser
}

// AcquireZPayload returns a new ZPayload for use.
func AcquireZPayload() *ZPayload {
	if nzp, ok := zPayloads.Get().(*ZPayload); ok {
		return nzp
	}

	var payload ZPayload
	payload.Reset()
	return &payload
}

// ReleaseZPayload returns giving ZPayload into underline object pool
// for reuse.
// Never re-use a ZPayload reference you've previously released.
func ReleaseZPayload(zp *ZPayload) {
	zp.Clear()
	zPayloads.Put(zp)
}

// Clear clears ZPayload fields.
func (z *ZPayload) Clear() {
	z.Writer = nil
	z.Reader = nil

	if len(z.Err) == 1 {
		<-z.Err
	}
	if len(z.Done) == 1 {
		<-z.Done
	}
	if len(z.Addr) == 1 {
		<-z.Addr
	}
}

// Reset giving ZPayload fields.
func (z *ZPayload) Reset() {
	z.Err = make(chan error, 1)
	z.Addr = make(chan net.Addr, 1)
	z.Done = make(chan struct{}, 1)
}

func (z *ZPayload) verify() {
	if z.Done == nil {
		panic("ZPayload.Done can not be nil")
	}
	if z.Err == nil {
		panic("ZPayload.Err can not be nil")
	}
	if z.Reader == nil && z.Writer == nil {
		panic("ZPayload.Reader & ZPayload.Writer can not be nil")
	}
}

// ZPayloadStream defines an underline channel type which represent
// a channel for the writing and reading of ZPayload.
type ZPayloadStream chan *ZPayload

// ZConn implements a simple connection wrapper around a
// net.Conn.
type ZConn struct {
	id            nxid.ID
	laddr         net.Addr
	addr          net.Addr
	maxRead       int
	maxWrite      int
	readBuffer    int
	writeBuffer   int
	nowTime       NowTime
	readTime      time.Duration
	writeTime     time.Duration
	debug         bool
	conn          net.Conn
	worker        ZConnWorker
	readRequests  ZPayloadStream
	writeRequests ZPayloadStream
	waiter        sync.WaitGroup
	ctx           context.Context
	ctxCanceler   context.CancelFunc
	streamWriter  *nbytes.DelimitedStreamWriter
	streamReader  *nbytes.DelimitedStreamReader
	clm           sync.Mutex
	closedBit     int64
}

const (
	defaultTimeout     = time.Second * 5
	defaultReadBuffer  = 1024 * 4
	defaultWriteBuffer = 1024 * 4
	defaultEscape      = "/&"
	defaultDelimiter   = "\r\n"
)

// NewZConn returns a new instance of a ZConn.
func NewZConn(conn net.Conn, fns ...ZApply) *ZConn {
	var zc = new(ZConn)
	zc.conn = conn
	zc.id = nxid.New()
	zc.addr = conn.RemoteAddr()
	zc.laddr = conn.LocalAddr()
	zc.readBuffer = defaultReadBuffer
	zc.writeBuffer = defaultWriteBuffer
	zc.writeTime = defaultTimeout
	zc.readTime = defaultTimeout
	zc.readRequests = make(ZPayloadStream, 10)
	zc.writeRequests = make(ZPayloadStream, 10)

	for _, fn := range fns {
		fn(zc)
	}

	if zc.ctx == nil {
		zc.ctx, zc.ctxCanceler = context.WithCancel(context.Background())
	}

	if zc.nowTime == nil {
		zc.nowTime = time.Now
	}

	if zc.worker == nil {
		switch conn.(type) {
		case *net.TCPConn:
			zc.worker = &TCPWorker{
				Debug:  zc.debug,
				Buffer: make([]byte, 1024),
			}
		case *net.UDPConn:
			zc.worker = &UDPWorker{
				Debug:  zc.debug,
				Buffer: make([]byte, 1024),
			}
		default:
			panic("ZConn.Worker must be provided")
		}
	}

	if zc.debug {
		log.Printf("[Zconn] | %s | Using %d bytes for read buffer and %d bytes for writer buffer", zc.id, zc.writeBuffer, zc.readBuffer)
	}

	zc.streamReader = &nbytes.DelimitedStreamReader{
		Src:        zc.conn,
		ReadBuffer: zc.readBuffer,
		Escape:     []byte(defaultEscape),
		Delimiter:  []byte(defaultDelimiter),
	}

	if err := zc.streamReader.Init(); err != nil {
		panic(err)
	}

	zc.streamWriter = &nbytes.DelimitedStreamWriter{
		Dest:        zc.conn,
		WriteBuffer: zc.writeBuffer,
		Escape:      []byte(defaultEscape),
		Delimiter:   []byte(defaultDelimiter),
	}

	if err := zc.streamWriter.Init(); err != nil {
		panic(err)
	}

	if err := zc.conn.SetWriteDeadline(zc.nowTime().Add(zc.writeTime)); err != nil {
		if zc.debug {
			log.Printf("[Zconn] | %s | Failed to set read timeout: %s", zc.id, err)
		}
		panic(err)
	}

	if err := zc.conn.SetReadDeadline(zc.nowTime().Add(zc.readTime)); err != nil {
		if zc.debug {
			log.Printf("[Zconn] | %s | Failed to set read timeout: %s", zc.id, err)
		}
		panic(err)
	}

	zc.handleClosure()

	return zc
}

func (zc *ZConn) isClosed() bool {
	return atomic.LoadInt64(&zc.closedBit) == 1
}

func (zc *ZConn) setClosed() {
	zc.clm.Lock()
	atomic.StoreInt64(&zc.closedBit, 1)
	zc.clm.Unlock()
}

func (zc *ZConn) handleClosure() {
	zc.waiter.Add(1)
	go func() {
		defer zc.waiter.Done()
		<-zc.ctx.Done()
		zc.setClosed()
	}()
}

func (zc *ZConn) Close() error {
	if zc.ctxCanceler != nil {
		zc.ctxCanceler()
	}

	if zc.debug {
		log.Printf("[Zconn] | %s | Awaiting gorotines closure", zc.id)
	}
	zc.waiter.Wait()
	if zc.debug {
		log.Printf("[Zconn] | %s | Closed gorotines", zc.id)
	}
	return nil
}

// Flush flushes underline ZConn write buffer.
func (zc *ZConn) Flush() error {
	return zc.streamWriter.HardFlush()
}

// ReadFrom reads data from provided Reader into connection.
func (zc *ZConn) ReadFrom(w io.ReadCloser, flush bool) error {
	var req = AcquireZPayload()
	req.Reader = w
	req.Flush = flush

	defer ReleaseZPayload(req)

	select {
	case <-zc.ctx.Done():
		return zc.ctx.Err()
	default:
	}

	return zc.handleWriteRequest(req)
}

// WriteTo writes data from connection into provided WriteCloser.
func (zc *ZConn) WriteTo(w io.WriteCloser) error {
	var req = AcquireZPayload()
	req.Writer = w

	defer ReleaseZPayload(req)

	select {
	case <-zc.ctx.Done():
		return zc.ctx.Err()
	default:
	}

	return zc.handleReadRequest(req)
}

// RemoteAddr returns giving remote net.Addr of ZConn.
func (zc *ZConn) RemoteAddr() net.Addr {
	return zc.addr
}

// LocalAddr returns giving net.Addr of ZConn.
func (zc *ZConn) LocalAddr() net.Addr {
	return zc.laddr
}

func (zc *ZConn) handleWriteRequest(req *ZPayload) error {
	req.verify()

	var err = zc.writeUntil(req)
	if err != nil {
		if zc.debug {
			log.Printf("[Zconn] | %s | Failed connection writing process: %s", zc.id, err)
		}

		if req.Err != nil {
			req.Err <- err
		}

		if err == ErrKillConnection {
			if zc.debug {
				log.Printf("[Zconn] | %s | Closing write loop", zc.id)
			}
			zc.ctxCanceler()
		}

		return err
	}

	req.Done <- struct{}{}
	return nil
}

func (zc *ZConn) handleReadRequest(req *ZPayload) error {
	req.verify()

	if req.Addr != nil {
		req.Addr <- zc.addr
	}

	var err = zc.readUntil(req)
	if err != nil {
		if zc.debug {
			log.Printf("[Zconn] | %s | Failed connection writing process: %s", zc.id, err)
		}

		if req.Err != nil {
			req.Err <- err
		}

		if err == ErrKillConnection {
			if zc.debug {
				log.Printf("[Zconn] | %s | Closing write loop", zc.id)
			}
			zc.ctxCanceler()
		}

		return err
	}

	req.Done <- struct{}{}
	return nil
}

func (zc *ZConn) writeUntil(req *ZPayload) error {
	var err error

	for {
		if zc.isClosed() {
			return ErrKillConnection
		}

		if err = zc.worker.ServeWrite(zc.ctx, zc.streamWriter, req); err != nil {
			if zc.debug {
				log.Printf("[Zconn] | %s | Read Call error: %s", zc.id, err)
			}

			if tmpErr, ok := err.(net.Error); ok {
				if zc.debug {
					log.Printf("[Zconn] | %s | TmpError: %t : %t", zc.id, tmpErr.Timeout(), tmpErr.Temporary())
				}
				if tmpErr.Timeout() {
					continue
				}
			}
			return err
		}
		break
	}

	// Get current total buffered content.
	var written int
	written, err = zc.streamWriter.End()
	if err != nil {
		if zc.debug {
			log.Printf("[Zconn] | %s | Failed connection flushing process: %s", zc.id, err)
		}
		return err
	}

	if zc.debug {
		log.Printf("[Zconn] | %s | Written %d to underline buffered writer", zc.id, written)
	}

	if req.Flush {
		if err := zc.streamWriter.HardFlush(); err != nil {
			if zc.debug {
				log.Printf("[Zconn] | %s | Failed hard flushing as requested: %s", zc.id, err)
			}
			return err
		}
		return nil
	}

	return nil
}

func (zc *ZConn) readUntil(req *ZPayload) error {
	var err error

	for {
		if zc.isClosed() {
			return ErrKillConnection
		}

		if err = zc.worker.ServeRead(zc.ctx, zc.streamReader, req); err != nil {
			if zc.debug {
				log.Printf("[Zconn] | %s | Read Call error: %s", zc.id, err)
			}

			if tmpErr, ok := err.(net.Error); ok {
				if zc.debug {
					log.Printf("[Zconn] | %s | TmpError: %t : %t", zc.id, tmpErr.Timeout(), tmpErr.Temporary())
				}
				if tmpErr.Timeout() {
					continue
				}
			}
			return err
		}
		return nil
	}
}

func copyByteSlice(b []byte, factor int) []byte {
	var nextCap = gradualExpansion(cap(b), len(b), factor)
	var newList = make([]byte, nextCap)
	var copied = copy(newList, b)
	return newList[:copied]
}

func gradualExpansion(capacity int, last int, factor int) int {
	if last == 0 {
		last = 1
	}
	var pb = capacity * factor
	var inc = (pb / capacity) + factor
	return inc + (capacity / last)
}

const maxConsecutiveEmptyReads = 5

// copyBuffer is the actual implementation of Copy and CopyBuffer.
// if buf is nil, one is allocated.
func copyBuffer(dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	if buf == nil {
		panic("can not use nil buffer")
	}

	var lastRetry int
	for {
		if lastRetry >= maxConsecutiveEmptyReads {
			return 0, io.EOF
		}

		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}

			if ew != nil {
				err = ew
				break
			}

			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}

		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}

		if nr > 0 {
			continue
		}

		lastRetry++
	}
	return written, err
}
