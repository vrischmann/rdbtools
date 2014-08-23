package rdbtools

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestReadList(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	p := NewParser(
		ParserContext{
			ListMetadataCh: make(chan ListMetadata, 1),
			ListDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "list", p.readList)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.ListMetadataCh:
			equals(t, "list", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.ListDataCh:
			equals(t, "a", DataToString(d))
		case <-end:
			stop = true
		}
	}
}

func TestReadListNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadListEncodedLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(0xC0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, ErrUnexpectedEncodedLength, err)
}

func TestReadListNoElementData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			ListMetadataCh: make(chan ListMetadata, 1),
			ListDataCh:     make(chan interface{}, 1),
		},
	)

	go func() {
		md := <-p.ctx.ListMetadataCh
		equals(t, "list", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadListInZipList(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(18)             // String length
	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // Special flag
	br.WriteString("foobar")

	br.Flush()

	p := NewParser(
		ParserContext{
			ListMetadataCh: make(chan ListMetadata, 1),
			ListDataCh:     make(chan interface{}, 1),
		},
	)

	go readAndNotify(t, &buffer, "list", p.readListInZipList)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.ListMetadataCh:
			equals(t, "list", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.ListDataCh:
			equals(t, "foobar", DataToString(d))
		case <-end:
			stop = true
		}
	}
}

func TestReadListInZipListNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readListInZipList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadListInZipListFail(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readListInZipList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}
