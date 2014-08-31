package rdbtools

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestSetMetadataString(t *testing.T) {
	md := SetMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "SetMetadata{Key: foobar, Len: 10}", md.String())
}

func TestReadSet(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "set", p.readSet)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.SetMetadataCh:
			equals(t, "set", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.SetDataCh:
			equals(t, "a", DataToString(d))
		case <-end:
			stop = true
		}
	}
}

func TestReadSetNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadSetEncodedLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0xC0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, ErrUnexpectedEncodedLength, err)
}

func TestReadSetNoEntry(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go func() {
		md := <-p.ctx.SetMetadataCh
		equals(t, "set", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadInt16Set(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(10)             // string length
	br.Write([]byte{2, 0, 0, 0}) // encoding
	br.Write([]byte{1, 0, 0, 0}) // len
	br.Write([]byte{1, 0})       // value
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "set", p.readIntSet)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.SetMetadataCh:
			equals(t, "set", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.SetDataCh:
			equals(t, int16(1), d)
		case <-end:
			stop = true
		}
	}
}

func TestReadInt32Set(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(12)             // string length
	br.Write([]byte{4, 0, 0, 0}) // encoding
	br.Write([]byte{1, 0, 0, 0}) // len
	br.Write([]byte{1, 0, 0, 0}) // value
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "set", p.readIntSet)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.SetMetadataCh:
			equals(t, "set", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.SetDataCh:
			equals(t, int32(1), d)
		case <-end:
			stop = true
		}
	}
}

func TestReadInt64Set(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(16)                         // string length
	br.Write([]byte{8, 0, 0, 0})             // encoding
	br.Write([]byte{1, 0, 0, 0})             // len
	br.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0}) // value
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "set", p.readIntSet)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.SetMetadataCh:
			equals(t, "set", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.SetDataCh:
			equals(t, int64(1), d)
		case <-end:
			stop = true
		}
	}
}

func TestReadIntSetNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadIntSetNoEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadIntSetNoLength(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(4)
	br.Write([]byte{0, 0, 0, 0})
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadIntSetNoInt16Value(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(8)
	br.Write([]byte{2, 0, 0, 0})
	br.Write([]byte{1, 0, 0, 0})
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go func() {
		md := <-p.ctx.SetMetadataCh
		equals(t, "set", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadIntSetNoInt32Value(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(8)
	br.Write([]byte{4, 0, 0, 0})
	br.Write([]byte{1, 0, 0, 0})
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go func() {
		md := <-p.ctx.SetMetadataCh
		equals(t, "set", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadIntSetNoInt64Value(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(8)
	br.Write([]byte{8, 0, 0, 0})
	br.Write([]byte{1, 0, 0, 0})
	br.Flush()

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	go func() {
		md := <-p.ctx.SetMetadataCh
		equals(t, "set", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readIntSet(KeyObject{Key: []byte("set")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}
