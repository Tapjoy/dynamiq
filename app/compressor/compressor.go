package compressor

import (
	"bytes"
	"compress/lzw"
	"compress/zlib"

	"github.com/Sirupsen/logrus"
)

// Compressor represents the set of actions needed to compress / decompress a piece
// of data
type Compressor interface {
	Compress(value []byte) ([]byte, error)
	Decompress(value []byte) ([]byte, error)
}

// ZLib Compressor performs decently in terms of speed, but the real
// gain is how well it compresses data

// NewZlibCompressor returns a new instance of a Compressor using the ZLib format
func NewZlibCompressor() ZlibCompressor {
	// If we had any setup, put it here
	return ZlibCompressor{}
}

// ZlibCompressor represents a Compressor using ZLib
type ZlibCompressor struct {
}

// Compress compresses a series of bytes, and returns the compressed data in bytes
func (z ZlibCompressor) Compress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(value)
	w.Close()
	return b.Bytes(), err
}

// Decompress decompresses a series of bytes, and returns the compressed data in bytes
func (z ZlibCompressor) Decompress(value []byte) ([]byte, error) {
	b := bytes.NewReader(value)

	r, err := zlib.NewReader(b)
	if err != nil {
		logrus.Error("Error decompressing data: ", err)
		return make([]byte, 0), err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	r.Close()

	return buf.Bytes(), err
}

// LZW Compressor is much faster than ZLib, but does not compress nearly as well

// LZWCompressor represents a compressor using the LZW formula for compression
type LZWCompressor struct {
	litWidth int
}

// NewLZWCompressor returns a compressor using the LZW formula for compression
func NewLZWCompressor(width int) LZWCompressor {
	return LZWCompressor{
		litWidth: width,
	}
}

// Compress compresses a series of bytes, and returns the compressed data in bytes
func (l LZWCompressor) Compress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	w := lzw.NewWriter(&b, lzw.LSB, l.litWidth)
	_, _ = w.Write(value)
	w.Close()
	return b.Bytes(), nil
}

// Decompress decompresses a series of bytes, and returns the compressed data in bytes
func (l LZWCompressor) Decompress(value []byte) ([]byte, error) {
	b := bytes.NewReader(value)
	r := lzw.NewReader(b, lzw.LSB, l.litWidth)
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	r.Close()

	return buf.Bytes(), nil
}
