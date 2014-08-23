package rdbtools

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
)

type Parser struct {
	ctx ParserContext
	r   io.Reader
}

const (
	RedisRdbVersion = 6
)

var (
	errNoMoreDatabases               = errors.New("errNoMoreDatabases")
	errNoMoreKeyValuePair            = errors.New("errNoMoreKeyValuePair")
	ErrInvalidMagicString            = errors.New("invalid magic string")
	ErrInvalidRDBVersionNumber       = errors.New("invalid RDB version number")
	ErrUnexpectedEncodedLength       = errors.New("unexpected encoded length")
	ErrUnknownValueType              = errors.New("unknown value type")
	ErrUnknownLengthEncoding         = errors.New("unknown length encoding")
	ErrUnexpectedPrevLengthEntryByte = errors.New("unexpected prev length entry byte")
)

type ParserContext struct {
	DbCh                chan int
	StringObjectCh      chan StringObject
	ListMetadataCh      chan ListMetadata
	ListDataCh          chan interface{}
	SetMetadataCh       chan SetMetadata
	SetDataCh           chan interface{}
	HashMetadataCh      chan HashMetadata
	HashDataCh          chan HashEntry
	SortedSetMetadataCh chan SortedSetMetadata
	SortedSetEntriesCh  chan SortedSetEntry
	endOfFileCh         chan struct{}
}

func (c *ParserContext) closeChannels() {
	if c.DbCh != nil {
		close(c.DbCh)
	}
	if c.StringObjectCh != nil {
		close(c.StringObjectCh)
	}
	if c.ListMetadataCh != nil {
		close(c.ListMetadataCh)
	}
	if c.ListDataCh != nil {
		close(c.ListDataCh)
	}
	if c.SetMetadataCh != nil {
		close(c.SetMetadataCh)
	}
	if c.SetDataCh != nil {
		close(c.SetDataCh)
	}
	if c.HashMetadataCh != nil {
		close(c.HashMetadataCh)
	}
	if c.HashDataCh != nil {
		close(c.HashDataCh)
	}
	if c.SortedSetMetadataCh != nil {
		close(c.SortedSetMetadataCh)
	}
	if c.SortedSetEntriesCh != nil {
		close(c.SortedSetEntriesCh)
	}
	close(c.endOfFileCh)
}

func (c *ParserContext) Invalid() bool {
	return c.DbCh == nil && c.StringObjectCh == nil && c.ListMetadataCh == nil && c.ListDataCh == nil && c.SetMetadataCh == nil && c.SetDataCh == nil && c.HashMetadataCh == nil && c.HashDataCh == nil && c.SortedSetMetadataCh == nil && c.SortedSetEntriesCh == nil
}

func NewParser(ctx ParserContext) *Parser {
	ctx.endOfFileCh = make(chan struct{})
	return &Parser{ctx: ctx}
}

func (p *Parser) Parse(r io.Reader) (err error) {
	br := bufio.NewReader(r)

	if err = readMagicString(br); err != nil {
		return err
	}
	if err = readVersionNumber(br); err != nil {
		return err
	}
	for {
		if err = p.readDatabase(br); err != nil && err != errNoMoreDatabases {
			return err
		} else if err != nil && err == errNoMoreDatabases {
			break
		}

		for {
			if err = p.readKeyValuePair(br); err != nil && err != errNoMoreKeyValuePair {
				return err
			} else if err != nil && err == errNoMoreKeyValuePair {
				break
			}
		}
	}

	// End of file byte - we don't handle the error here because we already handled it in the loop before
	// via the Peek() call
	//
	// Also, we don't check for validity of the byte here. This is because in the loop before, we will continue to loop
	// until we find a valid 0xFE (next database) or 0xFF (end of file) byte. If the data happens to contain a wrong EOF byte,
	// the loop would just continue and error out somewhere.
	br.ReadByte()

	p.ctx.closeChannels()

	return nil
}

func readMagicString(r *bufio.Reader) error {
	data := make([]byte, 5)
	read, err := r.Read(data)
	if err != nil {
		return err
	}

	if read != 5 {
		return ErrInvalidMagicString
	}

	if string(data) != "REDIS" {
		return ErrInvalidMagicString
	}

	return nil
}

func readVersionNumber(r *bufio.Reader) error {
	data := make([]byte, 4)
	read, err := r.Read(data)
	if err != nil {
		return err
	}

	if read != 4 {
		return ErrInvalidRDBVersionNumber
	}

	val := string(data)
	ival, err := strconv.Atoi(val)
	if err != nil {
		return err
	}

	if ival < 1 || ival > RedisRdbVersion {
		return ErrInvalidRDBVersionNumber
	}

	return nil
}

func (p *Parser) readDatabase(r *bufio.Reader) error {
	data, err := r.Peek(1)
	if err != nil {
		return err
	}

	if data[0] != 0xFE {
		return errNoMoreDatabases
	} else {
		r.ReadByte() // Discard
	}

	var dbNumber uint8
	if err := binary.Read(r, binary.BigEndian, &dbNumber); err != nil {
		return err
	}

	if p.ctx.DbCh != nil {
		p.ctx.DbCh <- int(dbNumber)
	}

	return nil
}

func readLen(r *bufio.Reader) (int64, bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return -1, false, err
	}

	bits := (b & 0xC0) >> 6
	switch bits {
	case 0:
		return int64(b) & 0x3f, false, nil
	case 1:
		newB, err := r.ReadByte()
		if err != nil {
			return -1, false, err
		}
		return int64((int64(b)&0x3f)<<8) | int64(newB), false, nil
	case 2:
		var tmp uint32
		if err := binary.Read(r, binary.BigEndian, &tmp); err != nil {
			return -1, false, err
		}

		return int64(tmp), false, nil
	default:
		return int64(b) & 0x3f, true, nil
	}
}

func readDoubleValue(r *bufio.Reader) (float64, error) {
	l, err := r.ReadByte()
	if err != nil {
		return 0, err
	}

	switch l {
	case 255:
		return math.Inf(-1), nil
	case 254:
		return math.Inf(1), nil
	case 253:
		return math.NaN(), nil
	default:
		bytes, err := readBytes(r, int64(l))
		if err != nil {
			return 0, err
		}

		return strconv.ParseFloat(string(bytes), 64)
	}
}

func readBytes(r *bufio.Reader, length int64) ([]byte, error) {
	bytes := make([]byte, length)
	_, err := io.ReadFull(r, bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func readLZFString(r *bufio.Reader) ([]byte, error) {
	clen, _, err := readLen(r)
	if err != nil {
		return nil, err
	}

	ulen, _, err := readLen(r)
	if err != nil {
		return nil, err
	}

	cdata, err := readBytes(r, clen)
	if err != nil {
		return nil, err
	}

	return lzfDecompress(cdata, ulen), nil
}

func readString(r *bufio.Reader) (interface{}, error) {
	l, e, err := readLen(r)
	if err != nil {
		return nil, err
	}

	var bytes []byte
	if e {
		// Encoded string
		switch l {
		case 0: // INT8
			var i int8
			if err = binary.Read(r, binary.LittleEndian, &i); err != nil {
				return nil, err
			}
			return i, nil
		case 1: // INT16
			var i int16
			if err = binary.Read(r, binary.LittleEndian, &i); err != nil {
				return nil, err
			}
			return i, nil
		case 2: // INT32
			var i int32
			if err = binary.Read(r, binary.LittleEndian, &i); err != nil {
				return nil, err
			}
			return i, nil
		case 3: // LZF
			bytes, err = readLZFString(r)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// Length prefixed string
		bytes, err = readBytes(r, l)
		if err != nil {
			return nil, err
		}
	}

	return bytes, nil
}

func (p *Parser) readKeyValuePair(r *bufio.Reader) error {
	data, err := r.Peek(1)
	if err != nil {
		return err
	}

	if data[0] == 0xFE || data[0] == 0xFF {
		return errNoMoreKeyValuePair
	}

	b, _ := r.ReadByte() // We can't have an error here, it would have been caught in the call to Peek() above

	// Read expiry time in seconds
	if b == 0xFD {
		// TODO use the expiry time
		var tmp int32
		if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
			return err
		}
	}

	// Read expiry time in milliseconds
	if b == 0xFC {
		// TODO use the expiry time
		var tmp int64
		if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
			return err
		}
	}

	if b == 0xFD || b == 0xFC {
		b, err = r.ReadByte()
		if err != nil {
			return err
		}
	}

	key, err := readString(r)
	if err != nil {
		return err
	}

	switch b {
	case 0: // String encoding
		value, err := readString(r)
		if err != nil {
			return err
		}

		if p.ctx.StringObjectCh != nil {
			p.ctx.StringObjectCh <- StringObject{Key: KeyObject{Key: key}, Value: value}
		}
	case 1: // List encoding
		if err := p.readList(key, r); err != nil {
			return err
		}
	case 2: // Set encoding
		if err := p.readSet(key, r); err != nil {
			return err
		}
	case 3: // Sorted set encoding
		if err := p.readSortedSet(key, r); err != nil {
			return err
		}
	case 4: // Hash encoding
		if err := p.readHashMap(key, r); err != nil {
			return err
		}
	case 9: // Zipmap encoding
		if err := p.readZipMap(key, r); err != nil {
			return err
		}
	case 10: // Zip list encoding
		if err := p.readListInZipList(key, r); err != nil {
			return err
		}
	case 11: // int set encoding
		if err := p.readIntSet(key, r); err != nil {
			return err
		}
	case 12: // Sorted set in zip list encoding
		if err := p.readSortedSetInZipList(key, r); err != nil {
			return err
		}
	case 13: // hash map in zip list encoding
		if err := p.readHashMapInZipList(key, r); err != nil {
			return err
		}
	default:
		return ErrUnknownValueType
	}

	return nil
}
