package compressor

import (
	"bytes"
	"compress/zlib"
	"github.com/Sirupsen/logrus"
)

type Compressor interface {
	Compress(value []byte) ([]byte, error)
	Decompress(value []byte) ([]byte, error)
}

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
