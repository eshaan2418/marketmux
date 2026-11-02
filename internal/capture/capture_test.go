package capture

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/venue"
)

func TestRoundTripRotationAndCap(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 2000, 10_000) // tiny rotation and cap
	if err != nil {
		t.Fatal(err)
	}
	var want []venue.Frame
	for i := 0; i < 500; i++ {
		f := venue.Frame{Kind: venue.FrameWS, Venue: model.Kraken, RecvNs: int64(i) * 1000,
			Payload: bytes.Repeat([]byte{byte('a' + i%26)}, 50)}
		if err := w.Write(&f); err != nil {
			t.Fatal(err)
		}
		want = append(want, f)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := Files(dir)
	if len(files) < 3 {
		t.Fatalf("expected rotation, got %d files", len(files))
	}
	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(got)) != w.Records || w.Dropped == 0 || len(got)+int(w.Dropped) != 500 {
		t.Fatalf("records %d dropped %d read %d", w.Records, w.Dropped, len(got))
	}
	for i := range got {
		if got[i].RecvNs != want[i].RecvNs || got[i].Venue != want[i].Venue || !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Fatalf("frame %d differs", i)
		}
	}
}

// A file cut off mid-record (crash while writing) yields every complete record, then EOF.
func TestTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, 0, 0)
	for i := 0; i < 10; i++ {
		_ = w.Write(&venue.Frame{Kind: venue.FrameWS, Venue: model.Gemini, RecvNs: int64(i), Payload: []byte("payload-0123456789")})
	}
	_ = w.Close()
	files, _ := Files(dir)
	raw, _ := os.ReadFile(files[0])
	zr, _ := gzip.NewReader(bytes.NewReader(raw))
	plain, _ := io.ReadAll(zr)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(plain[:len(plain)-7]) // cut the last record
	_ = zw.Close()
	_ = os.WriteFile(filepath.Join(dir, "capture-00001.mmcap.gz"), buf.Bytes(), 0o644)
	got, err := ReadAll(dir)
	if err != nil || len(got) != 9 {
		t.Fatalf("got %d frames, err %v", len(got), err)
	}
}
