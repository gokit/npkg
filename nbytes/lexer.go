package nbytes

import "io"

// ByteLexer implements a better efficient
type ByteLexer struct {
	buf []byte
	err error
	cp  int
	ln  int
	lx  int
	p   int
	lp  int
}

// NewByteLexer returns a new lexer for a giving byte.
func NewByteLexer(b []byte) *ByteLexer {
	return &ByteLexer{
		buf: b,
		p:   -1,
		ln:  len(b),
		cp:  cap(b),
		lx:  len(b) - 1,
	}
}

// Index returns giving byte for current index of byte lexer.
func (bl *ByteLexer) Index() byte {
	if bl.p == -1 {
		return 0
	}
	return bl.buf[bl.p]
}

// Rem returns giving total remaining bytes from current position.
// It uses the index as count.
func (bl *ByteLexer) Rem() int {
	if bl.p == -1 {
		return bl.lx
	}
	return bl.lx - bl.p
}

// ScanLeft returns the current slice portion of giving lexer
// from current position to the end
func (bl *ByteLexer) ScanLeft() []byte {
	if bl.p >= bl.lx {
		return nil
	}
	if bl.p == -1 {
		return nil
	}
	return bl.buf[bl.p:]
}

// Scan returns the current slice portion of giving lexer
func (bl *ByteLexer) Scan() []byte {
	if bl.p >= bl.lx {
		return nil
	}
	if bl.p == -1 {
		return nil
	}
	return bl.buf[bl.lp:bl.p]
}

// Record records current position for later use by
// call to ByteLexer.Scan.
func (bl *ByteLexer) Record() {
	if bl.err != nil {
		return
	}
	bl.lp = bl.p
}

// Move moves giving lexer current point a giving length forward
// from it's current position, if the total move is more than available
// length in bytes, then current index pointer is set to the last index
// of byte slice.
func (bl *ByteLexer) Move(n int) {
	if bl.err != nil {
		return
	}

	var next = bl.p + n
	if next >= bl.lx {
		bl.p = bl.lx
		bl.err = io.EOF
		return
	}
	bl.p = next
}

// Next moves on to the next byte position.
// Basically one increment from last.
func (bl *ByteLexer) Next() error {
	var next = bl.p + 1
	if next >= bl.ln {
		bl.err = io.EOF
		return bl.err
	}
	bl.p = next
	return nil
}

// Bytes returns giving byte for buffer.
func (bl *ByteLexer) Bytes() []byte {
	return bl.buf
}

// Err returns the associated error of giving lexer.
func (bl *ByteLexer) Err() error {
	return bl.err
}

// Position returns the current and last marked position.
func (bl *ByteLexer) Position() (last, current int) {
	last = bl.lp
	current = bl.p
	return
}

// Resets resets lexer to provided byte slice.
func (bl *ByteLexer) Reset(b []byte) {
	bl.cp = cap(b)
	bl.ln = len(b)
	bl.lx = len(b) - 1
	bl.buf = b
	bl.p = -1
	bl.err = nil
}
