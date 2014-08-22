package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"strings"
	"testing"
)

func mustParse(t *testing.T, p *Parser, r io.Reader) {
	err := p.Parse(r)
	if err != nil {
		t.Fatalf("Error while parsing; err=%s", err)
	}
}

func TestReadMagicString(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteString("REDIS")
	br.Flush()

	err := readMagicString(bufio.NewReader(&buffer))
	ok(t, err)

	// No data
	buffer.Reset()

	err = readMagicString(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// Not enough data
	buffer.Reset()
	br.WriteString("FOO")
	br.Flush()

	err = readMagicString(bufio.NewReader(&buffer))
	equals(t, ErrInvalidMagicString, err)

	// Invalid data
	buffer.Reset()
	br.WriteString("FOOBA")
	br.Flush()

	err = readMagicString(bufio.NewReader(&buffer))
	equals(t, ErrInvalidMagicString, err)
}

func TestReadVersionNumber(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteString("0006")
	br.Flush()

	err := readVersionNumber(bufio.NewReader(&buffer))
	ok(t, err)

	// No data
	buffer.Reset()

	err = readVersionNumber(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// Not enough data
	buffer.Reset()
	br.WriteString("FOO")
	br.Flush()

	err = readVersionNumber(bufio.NewReader(&buffer))
	equals(t, ErrInvalidRDBVersionNumber, err)

	// Not a number
	buffer.Reset()
	br.WriteString("foob")
	br.Flush()

	err = readVersionNumber(bufio.NewReader(&buffer))
	equals(t, "strconv.ParseInt: parsing \"foob\": invalid syntax", err.Error())

	// Wrong version number
	buffer.Reset()
	br.WriteString("0010")
	br.Flush()

	err = readVersionNumber(bufio.NewReader(&buffer))
	equals(t, ErrInvalidRDBVersionNumber, err)
}

func TestReadDatabase(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0xFE) // indicate next database
	br.WriteByte(0)
	br.Flush()

	p := NewParser(
		ParserContext{
			DbCh: make(chan int, 1),
		},
	)

	go func() {
		db := <-p.ctx.DbCh
		equals(t, int(0), db)
	}()

	err := p.readDatabase(bufio.NewReader(&buffer))
	ok(t, err)

	// No data
	buffer.Reset()

	err = p.readDatabase(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// No more databases
	buffer.Reset()
	br.WriteByte(0x01)
	br.Flush()

	err = p.readDatabase(bufio.NewReader(&buffer))
	equals(t, errNoMoreDatabases, err)

	// No db number after flag
	buffer.Reset()
	br.WriteByte(0xFE)
	br.Flush()

	err = p.readDatabase(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	// 6 bits encoding
	br.WriteByte(1)
	br.Flush()

	l, e, err := readLen(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int64(1), l)
	equals(t, false, e)

	// 14 bits encoding
	buffer.Reset()
	br.WriteByte(0x41)
	br.WriteByte(1)
	br.Flush()

	l, e, err = readLen(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int64(257), l)
	equals(t, false, e)

	// 32 bit encoding
	buffer.Reset()
	br.WriteByte(0xB0)
	binary.Write(br, binary.BigEndian, int32(1))
	br.Flush()

	l, e, err = readLen(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int64(1), l)
	equals(t, false, e)

	// special encoding
	buffer.Reset()
	br.WriteByte(0xD1)
	br.Flush()

	l, e, err = readLen(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int64(17), l)
	equals(t, true, e)

	// 14 bits encoding - no additional byte
	buffer.Reset()
	br.WriteByte(0x41)
	br.Flush()

	l, e, err = readLen(bufio.NewReader(&buffer))
	equals(t, int64(-1), l)
	equals(t, false, e)
	equals(t, io.EOF, err)

	// 32 bits encoding - no additional data
	buffer.Reset()
	br.WriteByte(0x80)
	br.Flush()

	l, e, err = readLen(bufio.NewReader(&buffer))
	equals(t, int64(-1), l)
	equals(t, false, e)
	equals(t, io.EOF, err)
}

func TestReadDoubleValue(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	// Negative inf
	br.WriteByte(0xFF)
	br.Flush()

	v, err := readDoubleValue(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, true, math.IsInf(v, -1))

	// Positive inf
	buffer.Reset()
	br.WriteByte(0xFE)
	br.Flush()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, true, math.IsInf(v, 1))

	// NaN
	buffer.Reset()
	br.WriteByte(0xFD)
	br.Flush()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, true, math.IsNaN(v))

	// Normal case
	buffer.Reset()
	br.WriteByte(4)
	br.WriteString("20.1")
	br.Flush()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, 20.1, v)

	// No data
	buffer.Reset()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// No additional bytes
	buffer.Reset()
	br.WriteByte(1)
	br.Flush()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// Not a float value
	buffer.Reset()
	br.WriteByte(6)
	br.WriteString("foobar")
	br.Flush()

	v, err = readDoubleValue(bufio.NewReader(&buffer))
	equals(t, "strconv.ParseFloat: parsing \"foobar\": invalid syntax", err.Error())
}

func TestReadLZFString(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	data := []byte{1, 97, 97, 224, 246, 0, 1, 97, 97}

	br.WriteByte(byte(len(data)))
	br.WriteByte(0x41) // ulen - 256
	br.WriteByte(3)    // +3
	br.Write(data)
	br.Flush()

	v, err := readLZFString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, strings.Repeat("a", 259), DataToString(v))

	// No clen data
	buffer.Reset()

	v, err = readLZFString(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// No ulen data
	buffer.Reset()
	br.WriteByte(1)
	br.Flush()

	v, err = readLZFString(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// No cdata
	buffer.Reset()
	br.WriteByte(1)
	br.WriteByte(1)
	br.Flush()

	v, err = readLZFString(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadString(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	// Length prefixed string
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	v, err := readString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, "a", DataToString(v))

	// Int8 encoding
	buffer.Reset()
	br.WriteByte(0xC0)
	br.WriteByte(1)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int8(1), v)

	// Int16 encoding
	buffer.Reset()
	br.WriteByte(0xC1)
	binary.Write(br, binary.LittleEndian, int16(1))
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int16(1), v)

	// Int32 encoding
	buffer.Reset()
	br.WriteByte(0xC2)
	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, int32(1), v)

	// LZF string
	data := []byte{1, 97, 97, 224, 246, 0, 1, 97, 97}
	buffer.Reset()
	br.WriteByte(0xC3)
	br.WriteByte(byte(len(data)))
	br.WriteByte(0x41) // ulen - 256
	br.WriteByte(3)    // +3
	br.Write(data)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	ok(t, err)
	equals(t, strings.Repeat("a", 259), DataToString(v))

	// Length prefixed - no data
	buffer.Reset()
	br.WriteByte(1)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	equals(t, nil, v)
	equals(t, io.EOF, err)

	// Int8 encoding - no data
	buffer.Reset()
	br.WriteByte(0xC0)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	equals(t, nil, v)
	equals(t, io.EOF, err)

	// Int16 encoding - no data
	buffer.Reset()
	br.WriteByte(0xC1)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	equals(t, nil, v)
	equals(t, io.EOF, err)

	// Int32 encoding - no data
	buffer.Reset()
	br.WriteByte(0xC2)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	equals(t, nil, v)
	equals(t, io.EOF, err)

	// LZF string - no data
	buffer.Reset()
	br.WriteByte(0xC3)
	br.Flush()

	v, err = readString(bufio.NewReader(&buffer))
	equals(t, nil, v)
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairErrors(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(ParserContext{})

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)

	// No more key value pair 0xFE
	buffer.Reset()
	br.WriteByte(0xFE)
	br.Flush()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, errNoMoreKeyValuePair, err)

	// No more key value pair 0xFF
	buffer.Reset()
	br.WriteByte(0xFF)
	br.Flush()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, errNoMoreKeyValuePair, err)

	// No key data
	buffer.Reset()
	br.WriteByte(0)
	br.Flush()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairStringEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			StringObjectCh: make(chan StringObject, 1),
		},
	)

	go func() {
		v := <-p.ctx.StringObjectCh
		equals(t, "a", DataToString(v.Key))
		equals(t, "b", DataToString(v.Value))
	}()

	br.WriteByte(0)   // string encoding
	br.WriteByte(1)   // key length
	br.WriteByte('a') // key data
	br.WriteByte(1)   // string length
	br.WriteByte('b') // string data
	br.Flush()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No string data
	buffer.Reset()
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.Flush()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairListEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			ListMetadataCh: make(chan ListMetadata, 1),
			ListDataCh:     make(chan interface{}, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.ListMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.ListDataCh
		equals(t, "v", DataToString(v))
	}

	br.WriteByte(1)   // list encoding
	br.WriteByte(1)   // key length
	br.WriteByte('a') // key data
	br.WriteByte(1)   // list length
	br.WriteByte(1)   // element length
	br.WriteByte('v') // element data
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No list data
	buffer.Reset()
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(1)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairSetEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.SetMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.SetDataCh
		equals(t, "v", DataToString(v))
	}

	br.WriteByte(2)   // set encoding
	br.WriteByte(1)   // key length
	br.WriteByte('a') // key data
	br.WriteByte(1)   // set length
	br.WriteByte(1)   // element length
	br.WriteByte('v') // element data
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No set data
	buffer.Reset()
	br.WriteByte(2)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(1)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairSortedSetEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			SortedSetMetadataCh: make(chan SortedSetMetadata, 1),
			SortedSetEntriesCh:  make(chan SortedSetEntry, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.SortedSetMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.SortedSetEntriesCh
		equals(t, "v", DataToString(v.Value))
		equals(t, 20.1, v.Score)
	}

	br.WriteByte(3)        // sorted set encoding
	br.WriteByte(1)        // key length
	br.WriteByte('a')      // key data
	br.WriteByte(1)        // sorted set length
	br.WriteByte(1)        // entry val length
	br.WriteByte('v')      // entry val data
	br.WriteByte(4)        // entry score length
	br.WriteString("20.1") // entry score data
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No sorted set data
	buffer.Reset()
	br.WriteByte(3)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('v')
	br.WriteByte(4)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairHashMapEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan StringObject, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.HashMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.HashDataCh
		equals(t, "a", DataToString(v.Key))
		equals(t, "b", DataToString(v.Value))
	}

	br.WriteByte(4)   // hash map encoding
	br.WriteByte(1)   // key length
	br.WriteByte('a') // key data
	br.WriteByte(1)   // hash map length
	br.WriteByte(1)   // entry key length
	br.WriteByte('a') // entry key data
	br.WriteByte(1)   // entry val length
	br.WriteByte('b') // entry val data
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No hash map data
	buffer.Reset()
	br.WriteByte(4)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairZipMapEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan StringObject, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.HashMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.HashDataCh
		equals(t, "a", DataToString(v.Key))
		equals(t, "b", DataToString(v.Value))
	}

	br.WriteByte(9)    // hash map encoding
	br.WriteByte(1)    // key length
	br.WriteByte('a')  // key data
	br.WriteRune(7)    // string length
	br.WriteByte(1)    // hash map length
	br.WriteByte(1)    // entry key length
	br.WriteByte('a')  // entry key data
	br.WriteByte(1)    // entry val length
	br.WriteRune(0)    // free bytes
	br.WriteByte('b')  // entry val data
	br.WriteByte(0xFF) // end
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No hash map data
	buffer.Reset()
	br.WriteByte(9)   // hash map encoding
	br.WriteByte(1)   // key length
	br.WriteByte('a') // key data
	br.WriteByte(6)   // string length
	br.WriteByte(1)   // hash map length
	br.WriteByte(1)   // entry key length
	br.WriteByte('a') // entry key data
	br.WriteByte(1)   // entry val length
	br.WriteRune(0)   // free bytes
	br.WriteByte('b') // entry val data
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairZipListEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			ListMetadataCh: make(chan ListMetadata, 1),
			ListDataCh:     make(chan interface{}, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.ListMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.ListDataCh
		equals(t, "a", DataToString(v))
	}

	br.WriteByte(10) // zip list encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(13)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No zip list data
	buffer.Reset()
	br.WriteByte(10) // zip list encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(12)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)
	br.WriteByte(1)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairIntSetEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			SetMetadataCh: make(chan SetMetadata, 1),
			SetDataCh:     make(chan interface{}, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.SetMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.SetDataCh
		equals(t, int16(1), v)
	}

	br.WriteByte(11) // intset encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(10)
	binary.Write(br, binary.LittleEndian, uint32(2))
	binary.Write(br, binary.LittleEndian, uint32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No zip list data
	buffer.Reset()
	br.WriteByte(11) // zip list encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(8)
	binary.Write(br, binary.LittleEndian, uint32(2))
	binary.Write(br, binary.LittleEndian, uint32(1))
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairSortedSetInZipListEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			SortedSetMetadataCh: make(chan SortedSetMetadata, 1),
			SortedSetEntriesCh:  make(chan SortedSetEntry, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.SortedSetMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.SortedSetEntriesCh
		equals(t, "a", DataToString(v.Value))
		equals(t, 1.2, v.Score)
	}

	br.WriteByte(12) // sorted set in ziplist encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(18)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(2))
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(0)
	br.WriteByte(3)
	br.WriteString("1.2")
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No zip list data
	buffer.Reset()
	br.WriteByte(12)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(15)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(2))
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(0)
	br.WriteByte(3)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairHashMapInZipListEncoding(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	p := NewParser(
		ParserContext{
			HashMetadataCh: make(chan HashMetadata, 1),
			HashDataCh:     make(chan StringObject, 1),
		},
	)

	mf := func() {
		l := <-p.ctx.HashMetadataCh
		equals(t, "a", DataToString(l.Key))
		equals(t, int64(1), l.Len)
	}

	df := func() {
		v := <-p.ctx.HashDataCh
		equals(t, "a", DataToString(v.Key))
		equals(t, "b", DataToString(v.Value))
	}

	br.WriteByte(13) // hashmap in ziplist encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(16)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(2))
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('b')
	br.Flush()

	go mf()
	go df()

	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	ok(t, err)

	// No zip list data
	buffer.Reset()
	br.WriteByte(13) // hashmap in ziplist encoding
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(15)
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int32(0))
	binary.Write(br, binary.LittleEndian, int16(2))
	br.WriteByte(0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.WriteByte(0)
	br.WriteByte(1)
	br.Flush()

	go mf()
	go df()

	err = p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadKeyValuePairUnknownValueType(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)

	br.WriteByte(0xF0)
	br.WriteByte(1)
	br.WriteByte('a')
	br.Flush()

	p := NewParser(ParserContext{})
	err := p.readKeyValuePair(bufio.NewReader(&buffer))
	equals(t, ErrUnknownValueType, err)
}

func TestParse(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{
		DbCh:           make(chan int, 1),
		StringObjectCh: make(chan StringObject, 1),
		endOfFileCh:    make(chan struct{}, 1),
	})

	br := bufio.NewWriter(&buffer)
	br.WriteString("REDIS")  // magic string
	br.WriteString("0006")   // RDB version
	br.WriteByte(0xFE)       // next database byte
	br.WriteByte(0)          // database number
	br.WriteByte(0)          // string
	br.WriteByte(1)          // key len
	br.WriteByte('a')        // key data
	br.WriteByte(6)          // string len
	br.WriteString("foobar") // string data
	br.WriteByte(0xFF)       // end of file
	br.Flush()

	go mustParse(t, p, bufio.NewReader(&buffer))

	stop := false
	for !stop {
		select {
		case v, ok := <-p.ctx.StringObjectCh:
			if !ok {
				p.ctx.StringObjectCh = nil
				break
			}
			equals(t, "a", DataToString(v.Key))
			equals(t, "foobar", DataToString(v.Value))
		case v, ok := <-p.ctx.DbCh:
			if !ok {
				p.ctx.DbCh = nil
				break
			}
			equals(t, int(0), v)
		}

		if p.ctx.Invalid() {
			break
		}
	}
}

func TestParseNoMagicString(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})

	err := p.Parse(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestParseNoVersionNumber(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})

	br := bufio.NewWriter(&buffer)

	br.WriteString("REDIS")
	br.Flush()

	err := p.Parse(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestParseNoDatabaseNumber(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{})

	br := bufio.NewWriter(&buffer)

	br.WriteString("REDIS")
	br.WriteString("0006")
	br.Flush()

	err := p.Parse(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestParseNoKeyValuePair(t *testing.T) {
	var buffer bytes.Buffer

	p := NewParser(ParserContext{
		DbCh: make(chan int, 1),
	})

	go func() {
		v := <-p.ctx.DbCh
		equals(t, int(0), v)
	}()

	br := bufio.NewWriter(&buffer)

	br.WriteString("REDIS")
	br.WriteString("0006")
	br.WriteByte(0xFE)
	br.WriteByte(0)
	br.Flush()

	err := p.Parse(bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}
