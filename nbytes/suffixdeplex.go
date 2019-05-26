package nbytes

import (
	"errors"
	"io"

	"github.com/gokit/npkg/nunsafe"
)

const (
	defaultSwap      = '|'
	defaultEscape    = "&"
	defaultDelimiter = "\r\n"
	defaultBuffer    = 4 * 1024
)

var (
	ErrNoMore = errors.New("no more")
)

// SuffixReader implements a suffix delimiting reader which transforms
// a byte sequence into individual parts.
type SuffixReader struct {
	// Dest is the destination writer where all escaped
	// byte sequence will be written to.
	Source io.Reader

	// Swap is the character to use (defaults to '&') if we find
	// a giving bug where the last element in a byte sequence is the
	// first character of the delimiter. This can use the reader to fail
	// to reproduce input, hence it swaps that with this and the reader will
	// unswap accordingly.
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

	// ReadBuffer defines the underline buffer space to be used for all reading operations.
	ReadBuffer int

	buf   []buf
	lexer *ByteLexer
}

// NewSuffixReader returns a new instance of a Reader.
func NewSuffixReader(w io.Reader) *SuffixReader {
	var sr SuffixReader
	sr.Source = w
	sr.Swap = defaultSwap
	sr.ReadBuffer = defaultBuffer
	sr.Delimiter = nunsafe.String2Bytes(defaultDelimiter)
	sr.Escape = nunsafe.String2Bytes(defaultEscape)

	sr.buf = make([]byte, 0, sr.ReadBuffer)
	sr.lexer = NewByteLexer(sr.buf)
	return &sr
}

// Read reads data into provided slice.
func (sr *SuffixReader) Read(b []byte) (int, error) {
	var maxReadSpace = len(b)

	var lastRead int
	var left = sr.lexer.Rem()
	if left > 0 {
		// part which was left in last read.
		// move it into front matter.
		var part = sr.lexer.ScanLeft()
		lastRead = copy(sr.buf, part)
	}

	if _, err := sr.readMoreBytes(sr.buf[lastRead:]); err != nil {
		return 0, err
	}

}

func (sr *SuffixReader) readNext() error {
	err := sr.lexer.Next()
	if err != nil {
		return err
	}

	switch sr.lexer.Index() {
	case sr.Escape[0]:
	case sr.Swap:
	default:
		return sr.readNext()
	}
}

func (sr *SuffixReader) readSwapped() error {

	return nil
}

func (sr *SuffixReader) readEscape() error {

	return nil
}

func (sr *SuffixReader) readMoreBytes(b []byte) (int, error) {
	var read, err = sr.Source.Read(b)
	if err != nil {
		return read, err
	}

	// TODO: What do we do for zero reads.
	if read == 0 {
		return read, ErrNoMore
	}

	return read, nil
}
