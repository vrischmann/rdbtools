package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Represents the metadata of a set, which is the key and the set length
type SetMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the set metadata
func (m SetMetadata) String() string {
	return fmt.Sprintf("SetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

func (p *parser) readSet(key KeyObject, r io.Reader) error {
	l, e, err := p.readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	if p.ctx.SetMetadataCh != nil {
		p.ctx.SetMetadataCh <- SetMetadata{Key: key, Len: l}
	}

	for i := int64(0); i < l; i++ {
		value, err := p.readString(r)
		if err != nil {
			return err
		}

		if p.ctx.SetDataCh != nil {
			p.ctx.SetDataCh <- value
		}
	}

	return nil
}

func (p *parser) readIntSet(key KeyObject, r io.Reader) error {
	data, err := p.readString(r)
	if err != nil {
		return err
	}

	dr := bufio.NewReader(bytes.NewReader(data.([]byte)))

	// read encoding (2, 4, 8 bytes per int)
	var encoding uint32
	if err := binary.Read(dr, binary.LittleEndian, &encoding); err != nil {
		return err
	}

	// read length of contents
	var length uint32
	if err := binary.Read(dr, binary.LittleEndian, &length); err != nil {
		return err
	}

	if p.ctx.SetMetadataCh != nil {
		p.ctx.SetMetadataCh <- SetMetadata{Key: key, Len: int64(length)}
	}

	// decode contents
	for i := uint32(0); i < length; i++ {
		var e interface{}
		switch encoding {
		case 2:
			var i int16
			if err := binary.Read(dr, binary.LittleEndian, &i); err != nil {
				return err
			}
			e = i
		case 4:
			var i int32
			if err := binary.Read(dr, binary.LittleEndian, &i); err != nil {
				return err
			}
			e = i
		case 8:
			var i int64
			if err := binary.Read(dr, binary.LittleEndian, &i); err != nil {
				return err
			}
			e = i
		}

		if p.ctx.SetDataCh != nil {
			p.ctx.SetDataCh <- e
		}
	}

	return nil
}
