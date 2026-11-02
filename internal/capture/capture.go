// Package capture records raw venue frames to gzip files and reads them back for replay.
//
// File format: gzip stream of
//
//	magic "MMCAP1\n" then records: kind u8 | venue u8 | recv_ns i64 BE | len u32 BE | payload
//
// Raw payloads (not normalised events) are stored, so replays exercise the real decoders and the
// exact resync behaviour. Writers rotate by size and stop at a total byte cap.
package capture

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/venue"
)

const magic = "MMCAP1\n"

// Writer appends frames to rotating gzip files in a directory. Safe for concurrent use.
type Writer struct {
	mu        sync.Mutex
	dir       string
	rotate    int64 // uncompressed bytes per file
	capTotal  int64 // uncompressed bytes across files; 0 = unlimited
	written   int64
	fileBytes int64
	seq       int
	f         *os.File
	gz        *gzip.Writer
	bw        *bufio.Writer
	Dropped   uint64
	Records   uint64
}

// NewWriter creates dir and opens the first file.
func NewWriter(dir string, rotateBytes, capBytes int64) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, rotate: rotateBytes, capTotal: capBytes}
	return w, w.open()
}

func (w *Writer) open() error {
	w.seq++
	f, err := os.Create(filepath.Join(w.dir, fmt.Sprintf("capture-%05d.mmcap.gz", w.seq)))
	if err != nil {
		return err
	}
	w.f = f
	w.gz, _ = gzip.NewWriterLevel(f, gzip.BestSpeed)
	w.bw = bufio.NewWriterSize(w.gz, 1<<20)
	w.fileBytes = 0
	_, err = w.bw.WriteString(magic)
	return err
}

func (w *Writer) closeFile() error {
	if w.f == nil {
		return nil
	}
	err := errors.Join(w.bw.Flush(), w.gz.Close(), w.f.Close())
	w.f = nil
	return err
}

// Write appends one frame.
func (w *Writer) Write(fr *venue.Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := int64(14 + len(fr.Payload))
	if w.capTotal > 0 && w.written+n > w.capTotal {
		w.Dropped++
		return nil
	}
	if w.f == nil {
		return errors.New("capture closed")
	}
	if w.rotate > 0 && w.fileBytes+n > w.rotate {
		if err := w.closeFile(); err != nil {
			return err
		}
		if err := w.open(); err != nil {
			return err
		}
	}
	var hdr [14]byte
	hdr[0] = byte(fr.Kind)
	hdr[1] = byte(fr.Venue)
	binary.BigEndian.PutUint64(hdr[2:], uint64(fr.RecvNs))
	binary.BigEndian.PutUint32(hdr[10:], uint32(len(fr.Payload)))
	if _, err := w.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(fr.Payload); err != nil {
		return err
	}
	w.written += n
	w.fileBytes += n
	w.Records++
	return nil
}

// Close flushes and closes the current file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeFile()
}

// Reader reads frames from one capture file.
type Reader struct {
	gz  *gzip.Reader
	br  *bufio.Reader
	buf []byte
}

// NewReader wraps an io.Reader positioned at the start of a capture file.
func NewReader(r io.Reader) (*Reader, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(gz, 1<<20)
	m := make([]byte, len(magic))
	if _, err := io.ReadFull(br, m); err != nil || string(m) != magic {
		return nil, errors.New("not a marketmux capture")
	}
	return &Reader{gz: gz, br: br}, nil
}

// Next reads the next frame. The payload aliases an internal buffer (valid until the next call)
// unless copyPayload is set. Returns io.EOF at the end.
func (r *Reader) Next(fr *venue.Frame, copyPayload bool) error {
	var hdr [14]byte
	if _, err := io.ReadFull(r.br, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return io.EOF // truncated tail (e.g. crash while writing): stop cleanly
		}
		return err
	}
	n := int(binary.BigEndian.Uint32(hdr[10:]))
	if n > 64<<20 {
		return fmt.Errorf("record too large: %d", n)
	}
	if cap(r.buf) < n {
		r.buf = make([]byte, n)
	}
	r.buf = r.buf[:n]
	if _, err := io.ReadFull(r.br, r.buf); err != nil {
		return io.EOF
	}
	fr.Kind = venue.FrameKind(hdr[0])
	fr.Venue = model.Venue(hdr[1])
	fr.RecvNs = int64(binary.BigEndian.Uint64(hdr[2:]))
	if copyPayload {
		fr.Payload = append([]byte(nil), r.buf...)
	} else {
		fr.Payload = r.buf
	}
	return nil
}

// Files lists capture files in a directory in order.
func Files(dir string) ([]string, error) {
	m, err := filepath.Glob(filepath.Join(dir, "capture-*.mmcap.gz"))
	sort.Strings(m)
	return m, err
}

// ReadAll loads every frame of the capture files in dir (payloads copied).
func ReadAll(dir string) ([]venue.Frame, error) {
	files, err := Files(dir)
	if err != nil {
		return nil, err
	}
	var out []venue.Frame
	for _, p := range files {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		r, err := NewReader(f)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		for {
			var fr venue.Frame
			if err := r.Next(&fr, true); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				f.Close()
				return nil, err
			}
			out = append(out, fr)
		}
		f.Close()
	}
	return out, nil
}
