package nbytes

import (
	"bufio"
	"bytes"
	"io"
	"sync/atomic"

	"github.com/gokit/npkg/nerror"
)

var (
	// ErrInvalidEscapeAndDelimiter defines error returned when delimiter and escape values
	// are the same.
	ErrInvalidEscapeAndDelimiter = nerror.New("delimiter and escape values can not be the same")
)

//****************************************************************************
// DelimitedStreamWriter
//****************************************************************************

const (
	swap = '&'
)

// DelimitedStreamWriter implements a giving byte encoder which will append
// to a giving set of byte after closure of writer with a ending delimiter
// as separator. It escapes all appearance of that giving byte using the escape
// byte value provided.
//
// It's not safe for concurrent use.
type DelimitedStreamWriter struct {
	// Dest is the destination writer where all escaped
	// byte sequence will be written to.
	Dest io.Writer

	// Swap is the character to use (defaults to '&') if we find
	// a giving bug where the last element in a byte sequence is the
	// first character of the delimiter. This can use the reader to fail
	// to reproduce input, hence it swaps that with this and the reader will
	// unwswap accordingly.
	//
	// Must be the same with reader.
	Swap byte

	// Escape is the escape sequence of bytes to be used when incoming stream
	// as delimiter as part of it's content, we use this to signal to reader that
	// this is escaped and not a actual ending of byte sequence.
	//
	// Must be the same with reader.
	Escape []byte

	// Delimiter is the byte sequence which is used to denote end of stream, it
	// must be entirely different and unique.
	//
	// Must be the same with reader.
	//
	// It will be added to the end of a giving sequence when the End() method is called.
	Delimiter []byte

	// WriteBuffer defines that amount of buffer space to be provided to the writer
	// before flushing.
	// We use bufio.Writer underneath the stream writer to provide efficient
	// writing of streams, this allows user to set the totally amount of content to be
	// written first before flushing into destination writer. This makes things more efficient
	// and reduces costly calls to Dest.Write.
	WriteBuffer int

	index  int
	count  int64
	buffer *bytes.Buffer
	escape *bytes.Buffer
	cache  *bufio.Writer
}

// Available returns available bytes buffered in underline bufio.Writer.
func (dw *DelimitedStreamWriter) Available() int {
	return dw.cache.Available()
}

// Buffered returns total bytes buffered in underline bufio.Writer.
func (dw *DelimitedStreamWriter) Buffered() int {
	return dw.cache.Buffered()
}

// HardFlush flushes giving underline data to destination writer.
func (dw *DelimitedStreamWriter) HardFlush() error {
	return dw.cache.Flush()
}

// End adds delimiter to underline writer to indicate end of byte stream section.
// This allows us indicate to giving stream as ending as any other occurrence of giving
// stream is closed.
func (dw *DelimitedStreamWriter) End() (int, error) {
	if !dw.hasInit() {
		panic("Uninitialized, ensure to call DelimitedStreamWriter.Init()")
	}

	var available = dw.cache.Available()
	if err := dw.flush(); err != nil {
		written := int(atomic.LoadInt64(&dw.count))
		return written, err
	}

	var hasFlushed bool
	var nowAvailable = dw.cache.Available()
	if nowAvailable < available {
		hasFlushed = true
	}

	written := int(atomic.LoadInt64(&dw.count))
	var buffered = dw.cache.Buffered()

	// we know we will be writing
	if buffered+len(dw.Delimiter) >= nowAvailable {
		hasFlushed = true
	}

	n, err := dw.cache.Write(dw.Delimiter)
	if err != nil {
		return written, err
	}

	written += n
	if hasFlushed {
		if err := dw.cache.Flush(); err != nil {
			return written, err
		}
	}

	dw.index = 0
	dw.buffer.Reset()
	dw.escape.Reset()
	atomic.StoreInt64(&dw.count, 0)
	return written, nil
}

// Write implements the io.Writer interface and handles the writing of
// a byte slice in accordance with escaping and delimiting rule.
func (dw *DelimitedStreamWriter) Write(bs []byte) (int, error) {
	var count int
	for _, b := range bs {
		if err := dw.writeByte(b); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// Init initializes giving writer and must be called first of all.
func (dw *DelimitedStreamWriter) Init() error {
	escapeLen := len(dw.Escape)
	delimLen := len(dw.Delimiter)
	if dw.buffer == nil && dw.cache == nil {
		if bytes.Equal(dw.Escape, dw.Delimiter) {
			return ErrInvalidEscapeAndDelimiter
		}

		writeBuffer := dw.WriteBuffer
		if writeBuffer <= 0 {
			writeBuffer = defaultBuffer
		}

		possibleSize := (escapeLen * 2) + delimLen
		if writeBuffer < possibleSize {
			writeBuffer += possibleSize
		}

		if dw.Swap == 0 {
			dw.Swap = swap
		}

		dw.escape = bytes.NewBuffer(make([]byte, 0, escapeLen))
		dw.buffer = bytes.NewBuffer(make([]byte, 0, delimLen))
		dw.cache = bufio.NewWriterSize(dw.Dest, writeBuffer)
	}

	return nil
}

func (dw *DelimitedStreamWriter) hasInit() bool {
	return dw.buffer != nil && dw.escape != nil && dw.cache != nil
}

// writeByte writes individual byte element into underline stream,
// ensuring to adequately escape all appearing delimiter within
// writing stream.
func (dw *DelimitedStreamWriter) writeByte(b byte) error {
	escapeLen := len(dw.Escape)

	if !dw.hasInit() {
		panic("Uninitialized, ensure to call DelimitedStreamWriter.Init()")
	}

	// if we have not started buffering normally and we found escape character
	// then use escape logic.
	if dw.buffer.Len() == 0 && dw.escape.Len() == 0 && b == dw.Escape[dw.index] {
		dw.index++

		atomic.AddInt64(&dw.count, 1)
		if err := dw.escape.WriteByte(b); err != nil {
			return err
		}

		return nil
	}

	// if the next one does not matches our escape character again, flush and move down wards
	// to delimiter checks.
	if dw.buffer.Len() == 0 && dw.escape.Len() != 0 && b != dw.Escape[dw.index] {
		if _, err := dw.escape.WriteTo(dw.cache); err != nil {
			return err
		}

		dw.index = 0
		dw.escape.Reset()
	}

	// if the next one matches our escape character again, continue written to escape.
	if dw.buffer.Len() == 0 && dw.escape.Len() != 0 && b == dw.Escape[dw.index] {
		atomic.AddInt64(&dw.count, 1)
		dw.index++

		if err := dw.escape.WriteByte(b); err != nil {
			return err
		}

		if dw.escape.Len() == escapeLen {
			if _, err := dw.escape.WriteTo(dw.cache); err != nil {
				return err
			}

			dw.index = 0
			dw.escape.Reset()
		}

		return nil
	}

	// if we are empty and do not match set then write
	if dw.buffer.Len() == 0 && b != dw.Delimiter[0] {
		atomic.AddInt64(&dw.count, 1)
		if err := dw.cache.WriteByte(b); err != nil {
			return err
		}
		return nil
	}

	// Does the giving byte match our delimiters first character?
	// if so, cache in buffer till we have full buffer set to compare.
	if dw.buffer.Len() == 0 && b == dw.Delimiter[0] {
		atomic.AddInt64(&dw.count, 1)
		if err := dw.buffer.WriteByte(b); err != nil {
			return err
		}
		return nil
	}

	space := dw.buffer.Cap() - dw.buffer.Len()

	// if we are now collecting possible match found delimiter
	// within incoming b stream, then we keep collecting till
	// we have enough to check against.
	if dw.buffer.Len() != 0 && space > 0 {
		atomic.AddInt64(&dw.count, 1)

		if err := dw.buffer.WriteByte(b); err != nil {
			return err
		}

		// if we were left with a single space then this was filled, hence then
		// it's time to check and flush.
		if space == 1 {
			return dw.flush()
		}
	}

	return nil
}

// flush compares existing buffered data if it matches delimiter, escapes
// it and flushes into buffered writer else, flushes buffered data into
// buffered writer and continues streaming.
func (dw *DelimitedStreamWriter) flush() error {
	if bytes.Equal(dw.buffer.Bytes(), dw.Delimiter) {
		escapeN, err := dw.cache.Write(dw.Escape)
		if err != nil {
			return err
		}

		atomic.AddInt64(&dw.count, int64(escapeN))

		if _, err := dw.cache.Write(dw.Delimiter); err != nil {
			return err
		}

		dw.buffer.Reset()
		return nil
	}

	// if the last item in buffer is the first character of delimiter
	// then escape and and swap with writer swap.
	if dw.buffer.Len() == 1 && dw.buffer.Bytes()[0] == dw.Delimiter[0] {
		escapeN, err := dw.cache.Write(dw.Escape)
		if err != nil {
			return err
		}

		atomic.AddInt64(&dw.count, int64(escapeN))

		if err := dw.cache.WriteByte(dw.Swap); err != nil {
			return err
		}

		dw.buffer.Reset()
		return nil
	}

	next, err := dw.buffer.WriteTo(dw.cache)
	if err != nil {
		return err
	}

	atomic.AddInt64(&dw.count, int64(next))
	return nil
}

//****************************************************************************
// DelimitedStreamReader
//****************************************************************************

const defaultBuffer = 1024

var (
	// ErrEOS is sent when a giving byte stream section is reached, that is
	// we've found the ending delimiter representing the end of a message stream
	// among incoming multi-duplexed stream.
	ErrEOS = nerror.New("end of stream set")
)

// DelimitedStreamReader continuously reads incoming byte sequence decoding
// them by unwrapping cases where delimiter was escaped as it appeared as part
// of normal byte stream. It ends it's encoding when giving delimiter is read.
//
// It's not safe for concurrent use.
type DelimitedStreamReader struct {
	// Src defines the reader which will be read from, it is expected
	// that the contents were already encoded using the DelimitedStreamWriter
	// ensuring adequately escaping and delimitation according to spec.
	Src io.Reader

	// Swap is the character to use (defaults to '&') if we find
	// a giving bug where the last element in a byte sequence is the
	// first character of the delimiter. This can use the reader to fail
	// to reproduce input, this is used to indicate to reader to unswap
	// when seen as a representation of the first character of the delimiter.
	//
	// Must be the same with writer.
	Swap byte

	// Escape is the escape sequence of bytes to be used when incoming stream
	// as delimiter as part of it's content, we use this to signal to reader that
	// this is escaped and not a actual ending of byte sequence.
	//
	// Must be the same with writer.
	Escape []byte

	// Delimiter is the byte sequence which is used to expected at end of stream, it
	// must be entirely different and unique. The reader will watch out for this has
	// indicator of the end of a giving stream sequence.
	//
	// Must be the same with writer.
	Delimiter []byte

	// ReadBuffer defines that amount of buffer space to be provided to the reader.
	// We use the bufio.Reader underneath for efficient reading and sequencing giving
	// adequate value here will provide efficient reading and operation of reader, with
	// reduce cost to calling the Src.Reader.Read method.
	ReadBuffer int

	index     int
	escapes   int
	comparing bool
	keepRead  bool
	unit      bool
	unitLen   int

	buffer *bufio.Reader
	//cached  *bytes.Buffer
	builder *BuildReader
}

// Read implements the io.Read interface providing writing of incoming
// byte sequence from underline reader, transforming and un-escaping
// escaped byte sequencing and writing result into passed byte slice.
func (dr *DelimitedStreamReader) Read(b []byte) (int, error) {
	var count int
	for {
		if dr.keepRead {
			//if dr.cached.Len() == 0 {
			//	dr.keepRead = false
			//}

			if dr.builder.Len() == 0 {
				dr.keepRead = false
			}

			//n, err := dr.cached.Read(b)
			n, err := dr.builder.Read(b)
			count += n

			if err != nil {
				return count, err
			}

			//if dr.cached.Len() == 0 {
			if dr.builder.Len() == 0 {
				dr.keepRead = false

				// if this is a unit, meaning a set unit of a stream
				// in a multi-stream, return EOS (End of stream) error
				// to signal end of a giving stream.
				if dr.unit {
					dr.builder.Reset(false)

					dr.unit = false
					return count, ErrEOS
				}
			}

			// if we maxed out available write space, return.
			if n == len(b) {
				return count, nil
			}

			// skip written bytes and retry.
			b = b[n:]
			continue
		}

		space := len(b)

		// continuously attempt to read till we have found a possible
		// byte sequence or an error.
		state, err := dr.readTill(space)
		if err != nil {

			// if an error occurred, read what we have in cache
			// and return error.

			//if dr.cached.Len() > 0 {
			//	n, _ := dr.cached.Read(b)
			//	count += n
			//	return count, err
			//}

			if dr.builder.Len() > 0 {
				n, _ := dr.builder.Read(b)
				count += n
				return count, err
			}

			return count, err
		}

		// if we have found a possible stepping area
		// switch on exhausting reading and read till
		// cache is empty.
		if state {
			dr.keepRead = true
			continue
		}
	}
}

// Init initializes giving reader and must be called.
func (dr *DelimitedStreamReader) Init() error {
	escapeLen := len(dr.Escape)
	delimLen := len(dr.Delimiter)
	delims := escapeLen + delimLen
	if dr.builder == nil && dr.buffer == nil {
		if bytes.Equal(dr.Escape, dr.Delimiter) {
			return ErrInvalidEscapeAndDelimiter
		}

		if dr.Swap == 0 {
			dr.Swap = swap
		}

		readBuffer := dr.ReadBuffer
		if readBuffer <= 0 {
			readBuffer = defaultBuffer
		}
		if readBuffer < delims {
			readBuffer += delims
		}

		dr.buffer = bufio.NewReaderSize(dr.Src, readBuffer)
		dr.builder = BuildReaderFor(make([]byte, 0, 1024))
		//dr.cached = bytes.NewBuffer(make([]byte, 0, 1024))
	}
	return nil
}

func (dr *DelimitedStreamReader) readTill(space int) (bool, error) {
	if dr.builder == nil && dr.buffer == nil {
		panic("Uninitialized, ensure to call DelimitedStreamReader.Init()")
	}

	escapeLen := len(dr.Escape)
	delimLen := len(dr.Delimiter)

	// if we are out of reading space and we have
	// more elements in cache, then return and
	// let it read off
	//if dr.cached.Len() >= space {
	//	return true, nil
	//}

	if dr.builder.Len() >= space {
		return true, nil
	}

	// if we are comparing and we reached end of escape index,
	// we need to check if next set matches delimiter or an escaped
	// delimiter first byte escape, then swap escaped byte or add
	// delimiter as being escaped.
	if dr.comparing && dr.index >= escapeLen {

		// We will check if this is a case of an escaped swap.
		swapped, err := dr.buffer.Peek(1)
		if err != nil {
			return false, err
		}

		// if the giving next byte is exactly the swap byte, then replace
		// with first character of delimiter.
		if swapped[0] == dr.Swap {
			if _, err := dr.buffer.Discard(1); err != nil {
				return false, err
			}

			dr.index = 0
			dr.comparing = false

			//if err := dr.cached.WriteByte(dr.Delimiter[0]); err != nil {
			//	return false, err
			//}

			if err := dr.builder.WriteByte(dr.Delimiter[0]); err != nil {
				return false, err
			}

			return false, nil
		}

		// We will check if this is a case of an escaped delimiter.
		next, err := dr.buffer.Peek(delimLen)
		if err != nil {
			return false, err
		}

		if bytes.Equal(dr.Delimiter, next) {
			dr.index = 0
			dr.comparing = false

			//if _, err := dr.cached.Write(next); err != nil {
			//	return false, err
			//}

			if _, err := dr.builder.Write(next); err != nil {
				return false, err
			}

			if _, err := dr.buffer.Discard(delimLen); err != nil {
				return false, err
			}

			return false, nil
		}

		// we properly just found a possible escape sequence showing up
		// and not a escaping of delimiter, so add then move on.
		dr.index = 0
		dr.comparing = false

		//if _, err := dr.cached.Write(dr.Escape); err != nil {
		//	return false, err
		//}

		if _, err := dr.builder.Write(dr.Escape); err != nil {
			return false, err
		}

		return false, nil
	}

	bm, err := dr.buffer.ReadByte()
	if err != nil {
		return false, err
	}

	// if we are not comparing and we don't match giving escape sequence
	// at index, then just cache and return.
	if !dr.comparing && bm != dr.Escape[dr.index] {

		// if we match delimiter first character here, then
		// check if we have other match.
		if bm == dr.Delimiter[0] {
			next, err := dr.buffer.Peek(delimLen - 1)
			if err != nil {
				return false, err
			}

			if bytes.Equal(dr.Delimiter[1:], next) {
				_, _ = dr.buffer.Discard(len(next))
				dr.unit = true
				return true, nil
			}
		}

		//if err := dr.cached.WriteByte(bm); err != nil {
		//	return false, err
		//}

		if err := dr.builder.WriteByte(bm); err != nil {
			return false, err
		}

		return false, nil
	}

	// if not comparing and we match, increment index and
	// setup comparing to true then return.
	if !dr.comparing && bm == dr.Escape[dr.index] {
		dr.index++
		dr.comparing = true
		return false, nil
	}

	// if we are and the byte matches the next byte in escape, increment and
	// return.
	if dr.comparing && bm == dr.Escape[dr.index] {
		dr.index++
		return false, nil
	}

	// if we are comparing and we are at a point where
	// the next byte does not match our next sequence
	if dr.comparing && bm != dr.Escape[dr.index] {
		var part []byte

		if dr.index > 1 {
			part = dr.Escape[:dr.index-1]
		} else {
			part = dr.Escape[:dr.index]
		}

		dr.index = 0
		dr.comparing = false

		// write part of escape sequence that had being checked.
		//if _, err := dr.cached.Write(part); err != nil {
		//	return false, err
		//}

		if _, err := dr.builder.Write(part); err != nil {
			return false, err
		}

		if err := dr.buffer.UnreadByte(); err != nil {
			return false, err
		}
	}

	return false, nil
}
