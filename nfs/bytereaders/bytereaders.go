package bytereaders

import (
	"bytes"
	"errors"
	"io"
	"time"

	filesystem "github.com/influx6/npkg/nfs"
)

// ByteReaderFunc defines a type which returns a byte.Reader
// for a given string
type ByteReaderFunc func(string) (*bytes.Reader, int64, error)

// FileFromByteReader returns a nfs.GetFile function which returns
// a new nfs.File from provided bytes.Reader.
func FileFromByteReader(fn ByteReaderFunc) filesystem.GetFile {
	return func(path string) (filesystem.File, error) {
		reader, size, err := fn(path)
		if err != nil {
			return nil, err
		}

		return filesystem.NewVirtualFile(reader, path, size, time.Now()), nil
	}
}

// ReaderFunc defines a type which returns a io.Reader
// for a given string
type ReaderFunc func(string) (io.Reader, int64, error)

// FileFromReader uses the ReaderFunc type to return a nfs.File
// from the returned reader if the type is either a nfs.VirtualFilesystem
// or a bytes.Reader.
func FileFromReader(fn ReaderFunc) filesystem.GetFile {
	return func(path string) (filesystem.File, error) {
		reader, size, err := fn(path)
		if err != nil {
			return nil, err
		}

		switch rreader := reader.(type) {
		case filesystem.File:
			return rreader, nil
		case *filesystem.VirtualFile:
			return rreader, nil
		case *bytes.Reader:
			return filesystem.NewVirtualFile(rreader, path, size, time.Now()), nil
		default:
			return nil, errors.New("Expected bytes.Reader type")
		}
	}
}
