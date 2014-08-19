package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

func (p *Parser) readHashMap(key interface{}, r *bufio.Reader) error {
	l, e, err := readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	p.ctx.HashMetadataCh <- HashMetadata{Key: key, Len: l}

	for i := int64(0); i < l; i++ {
		entryKey, err := readString(r)
		if err != nil {
			return err
		}

		entryValue, err := readString(r)
		if err != nil {
			return err
		}

		p.ctx.HashDataCh <- StringObject{Key: entryKey, Value: entryValue}
	}

	return nil
}

func (p *Parser) readHashMapInZipList(key interface{}, r *bufio.Reader) error {
	data, err := readString(r)
	if err != nil {
		return err
	}

	var entryKey interface{} = nil
	onLenCallback := func(length int64) error {
		p.ctx.HashMetadataCh <- HashMetadata{Key: key, Len: length / 2}
		return nil
	}
	onElementCallback := func(e interface{}) error {
		if entryKey == nil {
			entryKey = e
		} else {
			p.ctx.HashDataCh <- StringObject{Key: entryKey, Value: e}
			entryKey = nil
		}
		return nil
	}
	dr := bufio.NewReader(bytes.NewReader(data.([]byte)))

	if err := readZipList(dr, onLenCallback, onElementCallback); err != nil {
		return err
	}

	return nil
}

func readZipMapLength(r *bufio.Reader, b byte) (int64, error) {
	var l uint32
	switch b {
	case 253:
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return -1, err
		}
	default:
		l = uint32(b)
	}

	return int64(l), nil
}

// Read a hash map encoded as a zipmap (Redis < 2.6)
func (p *Parser) readZipMap(key interface{}, r *bufio.Reader) error {
	data, err := readString(r)
	if err != nil {
		return err
	}

	dr := bufio.NewReader(bytes.NewReader(data.([]byte)))

	// Hash map length, valid only when < 254
	mapLen, err := dr.ReadByte()
	if err != nil {
		return err
	}

	b, err := dr.ReadByte()
	if err != nil {
		return err
	}

	// This is fugly
	// We need the length of the hashmap before we start sending hashmap entries
	// so that the metadata we send is before the data, and is also correct.
	// Users will rely on this to know when to end processing entries for a given hashmap.
	//
	// This is why we have to buffer the entries and then sending them once we processed the RDB data.
	var results []StringObject
	if mapLen >= 254 {
		results = make([]StringObject, 0)
	} else {
		p.ctx.HashMetadataCh <- HashMetadata{Key: key, Len: int64(mapLen)}
	}

	for b != 0xFF {
		// Entry key data
		l, err := readZipMapLength(dr, b)
		if err != nil {
			return err
		}

		entryKey, err := readBytes(dr, l)
		if err != nil {
			return err
		}

		// Entry value data
		b, err = dr.ReadByte()
		if err != nil {
			return err
		}

		l, err = readZipMapLength(dr, b)
		if err != nil {
			return err
		}

		// FYI, that free shit is weird
		free, err := dr.ReadByte()
		if err != nil {
			return err
		}

		entryValue, err := readBytes(dr, l)
		if err != nil {
			return err
		}

		// skip if necessary
		if free > 0 {
			if _, err = readBytes(dr, int64(free)); err != nil {
				return err
			}
		}

		if mapLen >= 254 {
			results = append(results, StringObject{Key: entryKey, Value: entryValue})
		} else {
			p.ctx.HashDataCh <- StringObject{Key: entryKey, Value: entryValue}
		}

		b, err = dr.ReadByte()
		if err != nil {
			return err
		}
	}

	if mapLen >= 254 {
		p.ctx.HashMetadataCh <- HashMetadata{Key: key, Len: int64(len(results))}
		for _, e := range results {
			p.ctx.HashDataCh <- e
		}
	}

	return nil
}
