package nbytes_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/gokit/errors"
	mb "github.com/gokit/npkg/nbytes"
	"github.com/stretchr/testify/require"
)

func TestMultiStreamReadingAndwriting(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	sentences := []string{
		"I went into park stream all alone before the isle lands.",
		"Isle lands of YOR, before the dream verse began we found the diskin.",
		"Break fast in bed, love and eternality for ever",
		"Awaiting the ending seen of waiting for you?",
		"Done be such a waste!",
		"{\"log\":\"token\", \"centry\":\"20\"}",
	}

	for _, sentence := range sentences {
		written, err := writer.Write([]byte(sentence))
		require.NoError(t, err)
		require.Len(t, sentence, written)

		streamWritten, err := writer.End()
		require.NoError(t, err)
		require.True(t, streamWritten >= written)
	}

	reader := &mb.DelimitedStreamReader{
		Src:       bytes.NewReader(dest.Bytes()),
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	for index, sentence := range sentences {
		res := make([]byte, len(sentence)+10)
		read, err := reader.Read(res)
		require.Len(t, sentence, read, "Sentence at index %d with read %q", index, res[:read])
		require.Equal(t, mb.ErrEOS, errors.UnwrapDeep(err), "Sentence at index %d with read %q", index, res[:read])
		require.Equal(t, sentence, string(res[:read]), "Sentence at index %d", index)
	}

}

func TestDelimitedStreamWriterWithDelimiterAndEscape(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the :///clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	require.NoError(t, err)
	require.Len(t, data, written)

	processed, err := writer.End()
	require.NoError(t, err)
	require.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///clouds of endless :///streams beyond the shore//")
	require.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithAllDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the ://////////////////////////////////////")
	written, err := writer.Write(data)
	require.NoError(t, err)
	require.Len(t, data, written)

	processed, err := writer.End()
	require.NoError(t, err)
	require.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:/&//")
	require.Equal(t, expected, dest.Bytes(), "Expected %q to produce matched", data)
}

func TestDelimitedStreamWriterWithMoreEscapeWithDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the :///clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	require.NoError(t, err)
	require.Len(t, data, written)

	processed, err := writer.End()
	require.NoError(t, err)
	require.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///clouds of endless :///streams beyond the shore//")
	require.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the //clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	require.NoError(t, err)
	require.Len(t, data, written)

	processed, err := writer.End()
	require.NoError(t, err)
	require.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :///clouds of endless :///streams beyond the shore//")
	require.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the clouds of endless streams beyond the shore")
	written, err := writer.Write(data)
	require.NoError(t, err)
	require.Len(t, data, written)

	processed, err := writer.End()
	require.NoError(t, err)
	require.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the clouds of endless streams beyond the shore//")
	require.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithSet(t *testing.T) {
	specs := []struct {
		In  string
		Out string
		Err error
	}{
		{
			In:  "Wondering out the clouds of endless streams beyond the shore//",
			Out: "Wondering out the clouds of endless streams beyond the shore",
		},
		{
			In:  "Wondering out the :///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the //clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:/&//",
			Out: "Wondering out the ://////////////////////////////////////",
		},
	}

	for ind, spec := range specs {
		dest := bytes.NewBuffer(make([]byte, 0, 128))
		writer := &mb.DelimitedStreamWriter{
			Dest:      dest,
			Escape:    []byte(":/"),
			Delimiter: []byte("//"),
		}

		writer.Write([]byte(spec.Out))

		_, err := writer.End()
		require.NoError(t, err)
		require.Equal(t, spec.In, string(dest.Bytes()), "Index %d\n Data: %q\nEncoded: %q", ind, spec.Out, dest.Bytes())
	}
}

func TestDelimitedStreamReaderWithSet(t *testing.T) {
	specs := []struct {
		In   string
		Out  string
		Err  error
		Fail bool
	}{
		{
			In:  "Wondering out the clouds of endless streams beyond the shore//",
			Out: "Wondering out the clouds of endless streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the //clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:   "Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///",
			Out:  "Wondering out the ://///////////////////////////////////",
			Err:  io.EOF,
			Fail: true,
		},
		{
			In:  "Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:/&//",
			Out: "Wondering out the ://////////////////////////////////////",
			Err: mb.ErrEOS,
		},
	}

	for ind, spec := range specs {
		reader := &mb.DelimitedStreamReader{
			Src:       bytes.NewReader([]byte(spec.In)),
			Escape:    []byte(":/"),
			Delimiter: []byte("//"),
		}

		res := make([]byte, len(spec.In))
		read, err := reader.Read(res)
		require.Error(t, err)
		require.Equal(t, spec.Err, errors.UnwrapDeep(err))

		if !spec.Fail {
			require.Equal(t, spec.Out, string(res[:read]), "Failed at index %d:\n In: %q\n Out: %q\n", ind, spec.In, res[:read])
		}
	}
}
