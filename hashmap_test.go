package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestReadHashMap(t *testing.T) {
	var buffer bytes.Buffer
	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)       // hashmap len
	br.WriteByte(3)       // key len
	br.WriteString("foo") // key
	br.WriteByte(3)       // value len
	br.WriteString("bar") // value
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readHashMap)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "foo", DataToString(d.Key))
			equals(t, "bar", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadHashMapNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readHashMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadHashMapEncodedLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0xC0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readHashMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, ErrUnexpectedEncodedLength, err)
}

func TestReadHashMapNoEntryKey(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readHashMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadHashMapNoEntryValue(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteString("a")
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readHashMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadHashMapInZipList(t *testing.T) {
	var buffer bytes.Buffer
	br := bufio.NewWriter(&buffer)

	br.WriteByte(26)             // String length
	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{2, 0})       // zlLen

	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // Special flag
	br.WriteString("foobar")
	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // special flag
	br.WriteString("foobar")

	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readHashMapInZipList)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "foobar", DataToString(d.Key))
			equals(t, "foobar", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadHashMapInZipListNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readHashMapInZipList(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadHashMapInZipListEmptyZipList(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.WriteByte(0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readHashMapInZipList(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, "unexpected EOF", err.Error())
}

func TestReadZipMap(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(7)    // string length
	br.WriteByte(1)    // map len
	br.WriteByte(1)    // key len
	br.WriteByte('a')  // key
	br.WriteByte(1)    // value len
	br.WriteByte(0)    // free byte
	br.WriteByte('b')  // value
	br.WriteByte(0xFF) // end of map
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readZipMap)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "a", DataToString(d.Key))
			equals(t, "b", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadZipMapBigKey(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(11)                                // string length
	br.WriteByte(1)                                 // map len
	br.WriteByte(253)                               // key len (special)
	binary.Write(br, binary.LittleEndian, int32(1)) // real key len
	br.WriteByte('a')                               // key
	br.WriteByte(1)                                 // value len
	br.WriteByte(0)                                 // free byte
	br.WriteByte('b')
	br.WriteByte(0xFF) // end of map
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readZipMap)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "a", DataToString(d.Key))
			equals(t, "b", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadZipMapBigMapLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(7)    // string length
	br.WriteByte(254)  // map len (special)
	br.WriteByte(1)    // key len
	br.WriteByte('a')  // key
	br.WriteByte(1)    // value len
	br.WriteByte(0)    // free byte
	br.WriteByte('b')  // value
	br.WriteByte(0xFF) // end of map
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readZipMap)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "a", DataToString(d.Key))
			equals(t, "b", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadZipMapSkipFreeBytes(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(11)             // string length
	br.WriteByte(1)              // map len (special)
	br.WriteByte(1)              // key len
	br.WriteByte('a')            // key
	br.WriteByte(1)              // value len
	br.WriteByte(4)              // free byte
	br.WriteByte('b')            // value
	br.Write([]byte{0, 0, 0, 0}) // free bytes
	br.WriteByte(0xFF)           // end of map
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go readAndNotify(t, &buffer, "hashmap", p.readZipMap)

	stop := false
	for !stop {
		select {
		case md := <-p.ctx.HashMetadataCh:
			equals(t, "hashmap", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-p.ctx.HashDataCh:
			equals(t, "a", DataToString(d.Key))
			equals(t, "b", DataToString(d.Value))
		case <-end:
			stop = true
		}
	}
}

func TestReadZipMapNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})
	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapNoMapLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(0)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapNoFirstByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(1)
	br.WriteByte(1)
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailEntryKeyLength(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(3)
	br.WriteByte(1)
	br.WriteByte(253)
	br.WriteByte(0xFF)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, "unexpected EOF", err.Error())
}

func TestReadZipMapFailEntryKeyData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(2)
	br.WriteByte(1)
	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailEntryValByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(3)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailEntryValLength(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(5)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(253)
	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, "unexpected EOF", err.Error())
}

func TestReadZipMapFailEntryFreeByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(4)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailEntryValData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(5)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(0)
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailSkipFreeBytes(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(6)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(4)
	br.WriteByte('b')
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
		},
	)

	go func() {
		md := <-p.ctx.HashMetadataCh
		equals(t, "hashmap", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadZipMapFailLastReadByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(6)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(0)
	br.WriteByte('b')
	br.Flush()

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan HashEntry, 1),
		},
	)

	go func() {
		stop := false
		for !stop {
			select {
			case md := <-p.ctx.HashMetadataCh:
				equals(t, "hashmap", DataToString(md.Key))
				equals(t, int64(1), md.Len)
			case d := <-p.ctx.HashDataCh:
				equals(t, "a", DataToString(d.Key))
				equals(t, "b", DataToString(d.Value))
			case <-end:
				stop = true
			}
		}
	}()

	err := p.readZipMap(KeyObject{Key: []byte("hashmap")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
	end <- true
}
