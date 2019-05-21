package nbytes

import (
	"unicode/utf8"
	"unsafe"
)

// BuildReader implements a Bytes Builder with a wrapped reader to allow
// reading off content from a provided Builder.
type BuildReader struct {
	builder *Builder
	i       int
}

// NewBuilderReader returns a new instance of BuildReader.
func NewBuildReader() *BuildReader {
	var br Builder
	br.copyCheck()
	return &BuildReader{
		builder: &br,
		i:       -1,
	}
}

// BuilderReaderWith returns a new instance of BuildReader using the Builder.
func BuildReaderWith(bm *Builder) *BuildReader {
	return &BuildReader{
		builder: bm,
		i:       -1,
	}
}

// BuilderReaderFor returns a new instance of BuildReader using the byte slice.
func BuildReaderFor(bm []byte) *BuildReader {
	var br Builder
	br.buf = bm
	br.copyCheck()

	return &BuildReader{
		builder: &br,
		i:       -1,
	}
}

// Builder returns the underline builder for reader.
func (r *BuildReader) Builder() *Builder {
	return r.builder
}

// Reset resets underline builder.
// It reuses underline buffer unless argument is true,
// which allows for efficient reading and managing of reader.
func (r *BuildReader) Reset(dontReuse bool) {
	var lastBuf = r.builder.buf
	r.builder.Reset()
	r.i = -1

	if !dontReuse {
		r.builder.full = lastBuf
		r.builder.buf = lastBuf[:0]
	}
}

// Read provides a Read method wrapper around a provided Builder.
func (r *BuildReader) Read(b []byte) (n int, err error) {
	if r.i >= r.builder.Len() {
		return 0, ErrEOS
	}

	if r.i <= 0 {
		r.i++
		n = 0
		return
	}

	n = copy(b, r.builder.buf[r.i:])
	r.i += n
	return
}

// Len returns the current length of reader.
func (r *BuildReader) Len() int {
	if r.i >= r.builder.Len() {
		return 0
	}

	return r.builder.Len() - r.i
}

// ReadByte returns a single byte from the underline byte slice.
func (r *BuildReader) ReadByte() (byte, error) {
	if r.i >= r.builder.Len() {
		return 0, ErrEOS
	}

	if r.i >= r.builder.Len() {
		return 0, ErrEOS
	}

	nextByte := r.builder.buf[r.i]
	r.i++
	return nextByte, nil
}

// Write writes new data into reader.
func (r *BuildReader) Write(b []byte) (int, error) {
	if r.i <= -1 {
		r.i = 0
	}
	return r.builder.Write(b)
}

// WriteByte writes new data into reader.
func (r *BuildReader) WriteByte(b byte) error {
	if r.i <= -1 {
		r.i = 0
	}
	return r.builder.WriteByte(b)
}

// A Builder is used to efficiently build a string using Write methods.
// It minimizes memory copying. The zero value is ready to use.
// Do not copy a non-zero Builder.
type Builder struct {
	addr *Builder // of receiver, to detect copies by value
	full []byte
	buf  []byte
}

// NewBuilder returns new reader.
func NewBuilder() *Builder {
	var bm Builder
	bm.copyCheck()
	return &bm
}

// BuilderWith returns new reader.
func BuilderWith(m []byte) *Builder {
	var bm Builder
	bm.copyCheck()
	bm.buf = m
	return &bm
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input. noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
// This was copied from the runtime; see issues 23382 and 7921.
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

func (b *Builder) copyCheck() {
	if b.addr == nil {
		// This hack works around a failing of Go's escape analysis
		// that was causing b to escape and be heap allocated.
		// See issue 23382.
		// TODO: once issue 7921 is fixed, this should be reverted to
		// just "b.addr = b".
		b.addr = (*Builder)(noescape(unsafe.Pointer(b)))
	} else if b.addr != b {
		panic("strings: illegal use of non-zero Builder copied by value")
	}
}

// Bytes returns the accumulated byte slice.
func (b *Builder) Bytes() []byte {
	return b.buf
}

// Copy returns the accumulated byte slice.
func (b *Builder) Copy() []byte {
	var buf = make([]byte, len(b.buf))
	copy(buf, b.buf)
	return buf
}

// String returns the accumulated string.
func (b *Builder) String() string {
	return *(*string)(unsafe.Pointer(&b.buf))
}

// Len returns the number of accumulated bytes; b.Len() == len(b.String()).
func (b *Builder) Len() int { return len(b.buf) }

// Cap returns the capacity of the builder's underlying byte slice. It is the
// total space allocated for the string being built and includes any bytes
// already written.
func (b *Builder) Cap() int { return cap(b.buf) }

// Reset resets the Builder to be empty.
func (b *Builder) Reset() {
	b.addr = nil
	b.buf = nil
}

// grow copies the buffer to a new, larger buffer so that there are at least n
// bytes of capacity beyond len(b.buf).
func (b *Builder) grow(n int) {
	buf := make([]byte, len(b.buf), 2*cap(b.buf)+n)
	copy(buf, b.buf)
	b.buf = buf
}

// Grow grows b's capacity, if necessary, to guarantee space for
// another n bytes. After Grow(n), at least n bytes can be written to b
// without another allocation. If n is negative, Grow panics.
func (b *Builder) Grow(n int) {
	b.copyCheck()
	if n < 0 {
		panic("strings.Builder.Grow: negative count")
	}

	if cap(b.buf)-len(b.buf) < n {
		b.grow(n)
	}
}

// Write appends the contents of p to b's buffer.
// Write always returns len(p), nil.
func (b *Builder) Write(p []byte) (int, error) {
	b.copyCheck()

	if b.buf == nil && b.full != nil {
		b.buf = b.full[:0]
	}

	b.buf = append(b.buf, p...)
	return len(p), nil
}

// WriteByte appends the byte c to b's buffer.
// The returned error is always nil.
func (b *Builder) WriteByte(c byte) error {
	b.copyCheck()

	if b.buf == nil && b.full != nil {
		b.buf = b.full[:0]
	}

	b.buf = append(b.buf, c)
	return nil
}

// WriteRune appends the UTF-8 encoding of Unicode code point r to b's buffer.
// It returns the length of r and a nil error.
func (b *Builder) WriteRune(r rune) (int, error) {
	b.copyCheck()
	if r < utf8.RuneSelf {
		b.buf = append(b.buf, byte(r))
		return 1, nil
	}

	l := len(b.buf)
	if cap(b.buf)-l < utf8.UTFMax {
		b.grow(utf8.UTFMax)
	}

	n := utf8.EncodeRune(b.buf[l:l+utf8.UTFMax], r)
	b.buf = b.buf[:l+n]
	return n, nil
}

// WriteString appends the contents of s to b's buffer.
// It returns the length of s and a nil error.
func (b *Builder) WriteString(s string) (int, error) {
	b.copyCheck()
	b.buf = append(b.buf, s...)
	return len(s), nil
}
