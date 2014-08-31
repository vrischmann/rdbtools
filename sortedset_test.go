package rdbtools

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestSortedSetMetadataString(t *testing.T) {
	md := SortedSetMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "SortedSetMetadata{Key: foobar, Len: 10}", md.String())
}

func TestReadSortedSet(t *testing.T) {
	var buffer bytes.Buffer
	br := bufio.NewWriter(&buffer)

	br.WriteByte(2) // Sorted set len
	br.WriteByte(3) // Entry key len
	br.WriteString("bar")
	br.WriteByte(4) // Entry score length
	br.WriteString("20.1")
	br.WriteByte(6) // Entry key len
	br.WriteString("foobar")
	br.WriteByte(2) // Entry score length
	br.WriteString("62")
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go readAndNotify(t, &buffer, "zset", p.readSortedSet)

	stop := false
	i := 0
	for !stop {
		select {
		case md := <-ctx.SortedSetMetadataCh:
			equals(t, "zset", DataToString(md.Key))
			equals(t, int64(2), md.Len)
		case d := <-ctx.SortedSetEntriesCh:
			v := DataToString(d.Value)
			switch i {
			case 0:
				equals(t, "bar", v)
				equals(t, 20.1, d.Score)
				equals(t, "SortedSetEntry{Value: bar, Score: 20.1000}", d.String())
			case 1:
				equals(t, "foobar", v)
				equals(t, 62.0, d.Score)
				equals(t, "SortedSetEntry{Value: foobar, Score: 62.0000}", d.String())
			}
			i++
		case <-end:
			stop = true
		}
	}
}

func TestReadSortedSetNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := &parser{}
	err := p.readSortedSet(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadSortedSetEncodedLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(0xC0)
	br.Flush()

	p := &parser{}
	err := p.readSortedSet(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, ErrUnexpectedEncodedLength, err)
}

func TestReadSortedSetNoEntryKey(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go func() {
		md := <-ctx.SortedSetMetadataCh
		equals(t, "zset", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readSortedSet(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadSortedSetNoEntryScore(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(1)
	br.WriteByte(1)
	br.WriteString("a")
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go func() {
		md := <-ctx.SortedSetMetadataCh
		equals(t, "zset", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readSortedSet(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadSortedSetInZipList(t *testing.T) {
	var buffer bytes.Buffer
	br := bufio.NewWriter(&buffer)

	br.WriteByte(24)             // String length
	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{2, 0})       // zlLen
	br.WriteByte(0)              // len prev entry
	br.WriteByte(6)              // Special flag
	br.WriteString("foobar")
	br.WriteByte(0) // len prev entry
	br.WriteByte(4) // special flag
	br.WriteString("43.2")
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go readAndNotify(t, &buffer, "zset", p.readSortedSetInZipList)

	stop := false
	for !stop {
		select {
		case md := <-ctx.SortedSetMetadataCh:
			equals(t, "zset", DataToString(md.Key))
			equals(t, int64(1), md.Len)
		case d := <-ctx.SortedSetEntriesCh:
			v := DataToString(d.Value)
			equals(t, "foobar", v)
			equals(t, 43.2, d.Score)
		case <-end:
			stop = true
		}
	}
}

func TestReadSortedSetInZipListIntScore(t *testing.T) {
	var buffer bytes.Buffer
	br := bufio.NewWriter(&buffer)

	writeVal := func() {
		br.WriteByte(0)
		br.WriteByte(2)
		br.WriteString("fo")
	}

	br.WriteByte(64)
	br.WriteByte(64)
	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{12, 0})      // zlLen
	writeVal()
	br.WriteByte(0)
	br.WriteByte(242)
	writeVal()
	br.WriteByte(0)
	br.WriteByte(0xFE)
	br.WriteByte(10)
	writeVal()
	br.WriteByte(0)
	br.WriteByte(0xF0)
	br.Write([]byte{1, 1, 1})
	writeVal()
	br.WriteByte(0)
	br.WriteByte(0xE0)
	br.Write([]byte{1, 1, 0, 0, 0, 0, 0, 0})
	writeVal()
	br.WriteByte(0)
	br.WriteByte(0xD0)
	br.Write([]byte{1, 1, 0, 0})
	writeVal()
	br.WriteByte(0)
	br.WriteByte(0xC0)
	br.Write([]byte{1, 1})
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go readAndNotify(t, &buffer, "zset", p.readSortedSetInZipList)

	stop := false
	i := 0
	for !stop {
		select {
		case md := <-ctx.SortedSetMetadataCh:
			equals(t, "zset", DataToString(md.Key))
			equals(t, int64(6), md.Len)
		case d := <-ctx.SortedSetEntriesCh:
			v := DataToString(d.Value)
			switch i {
			case 0:
				equals(t, "fo", v)
				equals(t, float64(1), d.Score)
			case 1:
				equals(t, "fo", v)
				equals(t, float64(10), d.Score)
			case 2:
				equals(t, "fo", v)
				equals(t, float64(65793), d.Score)
			case 3:
				equals(t, "fo", v)
				equals(t, float64(257), d.Score)
			case 4:
				equals(t, "fo", v)
				equals(t, float64(257), d.Score)
			case 5:
				equals(t, "fo", v)
				equals(t, float64(257), d.Score)
			}
			i++
		case <-end:
			stop = true
		}
	}
}

func TestReadSortedSetInZipListNoData(t *testing.T) {
	var buffer bytes.Buffer

	p := &parser{}
	err := p.readSortedSetInZipList(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, io.EOF, err)
}

func TestReadSortedSetInZipListWrongScore(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	br.WriteByte(26)             // String length
	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{2, 0})       // zlLen
	br.WriteByte(0)              // len prev entry
	br.WriteByte(6)              // Special flag
	br.WriteString("foobar")
	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // special flag
	br.WriteString("foobar")
	br.Flush()

	ctx := ParserContext{
		SortedSetMetadataCh: make(chan SortedSetMetadata),
		SortedSetEntriesCh:  make(chan SortedSetEntry),
	}
	p := &parser{ctx: ctx}

	go func() {
		md := <-ctx.SortedSetMetadataCh
		equals(t, "zset", DataToString(md.Key))
		equals(t, int64(1), md.Len)
	}()

	err := p.readSortedSetInZipList(KeyObject{Key: []byte("zset")}, bufio.NewReader(&buffer))
	equals(t, "strconv.ParseFloat: parsing \"foobar\": invalid syntax", err.Error())
}
