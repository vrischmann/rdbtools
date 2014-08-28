package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadZipListStringLengthLte63(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // Special flag
	br.WriteString("foobar")

	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, "foobar", DataToString(e))
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListStringLengthLte16383(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0x57) // Special flag
	br.WriteByte(0x70) // additional length byte
	for i := 0; i < 1000; i++ {
		br.WriteString("foobar")
	}

	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, strings.Repeat("foobar", 1000), DataToString(e))
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListStringLengthGte16384(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0x80) // Special flag
	binary.Write(br, binary.LittleEndian, int32(30000))
	for i := 0; i < 5000; i++ {
		br.WriteString("foobar")
	}

	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, strings.Repeat("foobar", 5000), DataToString(e))
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt16(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xC0) // Special flag
	binary.Write(br, binary.LittleEndian, int16(1))
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int16(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt32(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xD0) // Special flag
	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int32(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt64(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xE0) // Special flag
	binary.Write(br, binary.LittleEndian, int64(1))
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int64(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt24(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xF0) // Special flag
	br.Write([]byte{0, 0, 1})
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int32(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt8(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xFE) // Special flag
	br.WriteByte(1)
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int8(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListInt4(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	br.Write([]byte{0, 0, 0, 0}) // zlBytes
	br.Write([]byte{0, 0, 0, 0}) // zlTail
	br.Write([]byte{1, 0})       // zlLen

	br.WriteByte(0)    // len prev entry
	br.WriteByte(0xF2) // Special flag
	br.Flush()

	onLenCallback := func(length int64) error {
		equals(t, int64(1), length)
		return nil
	}

	onElementCallback := func(e interface{}) error {
		equals(t, int(1), e)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	ok(t, err)
}

func TestReadZipListNoZlBytes(t *testing.T) {
	var buffer bytes.Buffer
	p := NewParser(ParserContext{})
	err := p.readZipList(bufio.NewReader(&buffer), nil, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListNoZlTail(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	err := p.readZipList(bufio.NewReader(&buffer), nil, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListNoZlLen(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	err := p.readZipList(bufio.NewReader(&buffer), nil, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListNoPrevEntryLength(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

// error on flag reading
func TestReadZipListPrevEntryLengthLte253(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0) // prev entry len
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

// error on flag reading
func TestReadZipListPrevEntryLengthEq254(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0xFE) // prev entry len
	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListPrevEntryLengthEq254NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0xFE) // prev entry len
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListPrevEntryLengthUnexpected(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0xFF) // prev entry len
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, ErrUnexpectedPrevLengthEntryByte, err)
}

func TestReadZipListStringLengthLte63NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0) // prev entry len
	br.WriteByte(1) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListStringLengthLte16383NoAdditionalByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0x40) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListStringLengthLte16383NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0x41) // flag
	br.WriteByte(0)
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListStringLengthGte16384NoAdditionalBytes(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0x80) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListStringLengthGte16384NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0x80) // flag
	binary.Write(br, binary.LittleEndian, int32(1))
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListInt16NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0xC0) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListInt32NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0xD0) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListInt64NoData(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0xE0) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListInt24NoAdditionalBytes(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0xF0) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListInt8NoAdditionalByte(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0)    // prev entry len
	br.WriteByte(0xFE) // flag
	br.Flush()

	onLenCallback := func(l int64) error {
		equals(t, int64(1), l)
		return nil
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, io.EOF, err)
}

func TestReadZipListOnLenCallbackError(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.Flush()

	myErr := errors.New("myErr")

	onLenCallback := func(l int64) error {
		return myErr
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, nil)
	equals(t, myErr, err)
}

func TestReadZipListOnElementCallbackError(t *testing.T) {
	var buffer bytes.Buffer

	br := bufio.NewWriter(&buffer)
	p := NewParser(ParserContext{})

	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int32(1))
	binary.Write(br, binary.LittleEndian, int16(1))
	br.WriteByte(0) // len prev entry
	br.WriteByte(6) // Special flag
	br.WriteString("foobar")
	br.Flush()

	myErr := errors.New("myErr")

	onLenCallback := func(l int64) error {
		return nil
	}
	onElementCallback := func(e interface{}) error {
		return myErr
	}

	err := p.readZipList(bufio.NewReader(&buffer), onLenCallback, onElementCallback)
	equals(t, myErr, err)
}
