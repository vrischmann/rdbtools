package rdbtools

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
)

type Parser interface {
	Parse(r io.Reader) (err error)
}

// Parser is the main parser for RDB files
type parser struct {
	ctx     ParserContext
	r       io.Reader
	scratch [4]byte
}

const (
	// The last version of RDB files
	RedisRdbVersion = 6
)

var (
	errNoMoreDatabases               = errors.New("errNoMoreDatabases")
	errNoMoreKeyValuePair            = errors.New("errNoMoreKeyValuePair")
	ErrInvalidMagicString            = errors.New("invalid magic string")
	ErrInvalidRDBVersionNumber       = errors.New("invalid RDB version number")
	ErrInvalidChecksum               = errors.New("invalid checksum")
	ErrUnexpectedEncodedLength       = errors.New("unexpected encoded length")
	ErrUnknownValueType              = errors.New("unknown value type")
	ErrUnknownLengthEncoding         = errors.New("unknown length encoding")
	ErrUnexpectedPrevLengthEntryByte = errors.New("unexpected prev length entry byte")
)

// A ParserContext holds the channels used to receive data from the parser
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

// Invalid returns true if the context is invalid (all channels are nil), false otherwise.
// This is needed to actually terminate parsing if you use a for-select loop
func (c *ParserContext) Invalid() bool {
	return c.DbCh == nil && c.StringObjectCh == nil && c.ListMetadataCh == nil && c.ListDataCh == nil && c.SetMetadataCh == nil && c.SetDataCh == nil && c.HashMetadataCh == nil && c.HashDataCh == nil && c.SortedSetMetadataCh == nil && c.SortedSetEntriesCh == nil
}

// Create a new parser using the provided context
func NewParser(ctx ParserContext) Parser {
	ctx.endOfFileCh = make(chan struct{})
	return &parser{ctx: ctx}
}

// Parse a RDB file reading data from the provided reader r
// Any error occurring while parsing will be returned here
func (p *parser) Parse(r io.Reader) (err error) {
	cr := newChecksumReader(r)

	if err = readMagicString(cr); err != nil {
		return err
	}

	var rdbVersion int
	if rdbVersion, err = readVersionNumber(cr); err != nil {
		return err
	}

	for {
		if err = p.readDatabase(cr); err != nil && err != errNoMoreDatabases {
			return err
		} else if err != nil && err == errNoMoreDatabases {
			break
		}

		for {
			if err = p.readKeyValuePair(cr); err != nil && err != errNoMoreKeyValuePair {
				return err
			} else if err != nil && err == errNoMoreKeyValuePair {
				break
			}
		}

		if p.scratch[0] == 0xFF {
			break
		}
	}

	// Read the CRC64 checksum with RDB version >= 5
	if rdbVersion >= 5 {
		sum := cr.checksum

		var checksum uint64
		if err := binary.Read(cr, binary.LittleEndian, &checksum); err != nil {
			return err
		}

		if sum != checksum {
			return ErrInvalidChecksum
		}
	}

	p.ctx.closeChannels()

	return nil
}

func readMagicString(r io.Reader) error {
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

func readVersionNumber(r io.Reader) (int, error) {
	data := make([]byte, 4)
	read, err := r.Read(data)
	if err != nil {
		return -1, err
	}

	if read != 4 {
		return -1, ErrInvalidRDBVersionNumber
	}

	val := string(data)
	ival, err := strconv.Atoi(val)
	if err != nil {
		return -1, err
	}

	if ival < 1 || ival > RedisRdbVersion {
		return -1, ErrInvalidRDBVersionNumber
	}

	return ival, nil
}

func (p *parser) readDatabase(r io.Reader) error {
	// Might have read the 0xFE byte already in the last readKeyValuePair call
	if p.scratch[0] != 0xFE {
		_, err := io.ReadFull(r, p.scratch[0:1])
		if err != nil {
			return err
		}

		if p.scratch[0] != 0xFE {
			return errNoMoreDatabases
		}
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

func (p *parser) readLen(r io.Reader) (int64, bool, error) {
	_, err := io.ReadFull(r, p.scratch[0:1])
	if err != nil {
		return -1, false, err
	}

	b := p.scratch[0]

	bits := (b & 0xC0) >> 6
	switch bits {
	case 0:
		return int64(b) & 0x3f, false, nil
	case 1:
		_, err := io.ReadFull(r, p.scratch[0:1])
		if err != nil {
			return -1, false, err
		}
		return int64((int64(b)&0x3f)<<8) | int64(p.scratch[0]), false, nil
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

func (p *parser) readDoubleValue(r io.Reader) (float64, error) {
	_, err := io.ReadFull(r, p.scratch[0:1])
	if err != nil {
		return 0, err
	}

	l := p.scratch[0]

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

func readBytes(r io.Reader, length int64) ([]byte, error) {
	bytes := make([]byte, length)
	_, err := io.ReadFull(r, bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (p *parser) readLZFString(r io.Reader) ([]byte, error) {
	clen, _, err := p.readLen(r)
	if err != nil {
		return nil, err
	}

	ulen, _, err := p.readLen(r)
	if err != nil {
		return nil, err
	}

	cdata, err := readBytes(r, clen)
	if err != nil {
		return nil, err
	}

	return lzfDecompress(cdata, ulen), nil
}

func (p *parser) readString(r io.Reader) (interface{}, error) {
	l, e, err := p.readLen(r)
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
			bytes, err = p.readLZFString(r)
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

func (p *parser) readKeyValuePair(r io.Reader) error {
	_, err := io.ReadFull(r, p.scratch[0:1])
	if err != nil {
		return err
	}

	if p.scratch[0] == 0xFE || p.scratch[0] == 0xFF {
		return errNoMoreKeyValuePair
	}

	b := p.scratch[0]

	// Read expiry time in seconds
	var expiryTime int64 = -1
	if b == 0xFD {
		var tmp uint32
		if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
			return err
		}
		expiryTime = int64(int64(tmp) * 1000)
	}

	// Read expiry time in milliseconds
	if b == 0xFC {
		if err := binary.Read(r, binary.LittleEndian, &expiryTime); err != nil {
			return err
		}
	}

	// If the byte was a expiry time flag, we need to reread a byte
	if b == 0xFD || b == 0xFC {
		_, err := io.ReadFull(r, p.scratch[0:1])
		b = p.scratch[0]
		if err != nil {
			return err
		}
	}

	keyStr, err := p.readString(r)
	if err != nil {
		return err
	}

	key := NewKeyObject(keyStr, expiryTime)

	switch b {
	case 0: // String encoding
		value, err := p.readString(r)
		if err != nil {
			return err
		}

		if p.ctx.StringObjectCh != nil {
			p.ctx.StringObjectCh <- StringObject{Key: key, Value: value}
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
