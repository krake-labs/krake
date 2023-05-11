package internal

import "bytes"

type File interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

type MemoryFile struct {
	buf *bytes.Buffer
}

func NewMemoryFile() *MemoryFile {
	return &MemoryFile{buf: bytes.NewBuffer([]byte{})}
}

func (f *MemoryFile) Read(b []byte) (int, error) {
	return f.buf.Read(b)
}

func (f *MemoryFile) Write(b []byte) (int, error) {
	return f.buf.Write(b)
}

func (f *MemoryFile) Close() error {
	return nil
}
