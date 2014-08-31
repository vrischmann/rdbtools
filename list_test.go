package rdbtools

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestListMetadataString(t *testing.T) {
	md := ListMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "ListMetadata{Key: foobar, Len: 10}", md.String())
}

func TestReadList(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	ctx := ParserContext{
		ListMetadataCh: make(chan ListMetadata),
		ListDataCh:     make(chan interface{}),
	}
	p := &parser{ctx: ctx}

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

	p := &parser{}
	err := p.readList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadListEncodedLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(0xC0)
	br.Flush()

	p := &parser{}
	err := p.readList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, ErrUnexpectedEncodedLength, err)
}

func TestReadListNoElementData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.Flush()

	ctx := ParserContext{ListMetadataCh: make(chan ListMetadata)}
	p := &parser{ctx: ctx}

	go func() {
		md := <-ctx.ListMetadataCh
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

	ctx := ParserContext{
		ListMetadataCh: make(chan ListMetadata),
		ListDataCh:     make(chan interface{}),
	}
	p := &parser{ctx: ctx}

	go readAndNotify(t, &buffer, "list", p.readListInZipList)

	stop := false
	for !stop {
		select {
		case md := <-ctx.ListMetadataCh:
			equals(t, "list", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-ctx.ListDataCh:
			equals(t, "foobar", DataToString(d))
		case <-end:
			stop = true
		}
	}
}

func TestReadListInZipListNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := &parser{}
	err := p.readListInZipList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadListInZipListFail(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0)
	br.Flush()

	p := &parser{}
	err := p.readListInZipList(KeyObject{Key: []byte("list")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}
