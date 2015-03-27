package compressor

import (
	"bytes"
	"compress/lzw"
	"compress/zlib"
	"github.com/Sirupsen/logrus"
)

type Compressor interface {
	Compress(value []byte) ([]byte, error)
	Decompress(value []byte) ([]byte, error)
}

// ZLib Compressor performs decently in terms of speed, but the real
// gain is how well it compresses data
func NewZlibCompressor() ZlibCompressor {
	// If we had any setup, put it here
	return ZlibCompressor{}
}

type ZlibCompressor struct {
}

func (z ZlibCompressor) Compress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(value)
	w.Close()
	return b.Bytes(), nil
}

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

type LZWCompressor struct {
	litWidth int
}

// LZW Compressor is much faster than ZLib, but does not compress nearly as well
func NewLZWCompressor(width int) LZWCompressor {
	return LZWCompressor{
		litWidth: width,
	}
}

func (l LZWCompressor) Compress(value []byte) ([]byte, error) {
	var b bytes.Buffer
	w := lzw.NewWriter(&b, lzw.LSB, l.litWidth)
	_, _ = w.Write(value)
	w.Close()
	return b.Bytes(), nil
}

func (l LZWCompressor) Decompress(value []byte) ([]byte, error) {
	b := bytes.NewReader(value)
	r := lzw.NewReader(b, lzw.LSB, l.litWidth)
	//if err != nil {
	//logrus.Error("Error decompressing data: ", err)
	//return make([]byte, 0), err
	//}
	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	r.Close()

	return buf.Bytes(), nil
}
