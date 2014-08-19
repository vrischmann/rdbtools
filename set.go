package rdbtools

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

func (p *Parser) readSet(key interface{}, r *bufio.Reader) error {
	l, e, err := readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	p.ctx.SetMetadataCh <- SetMetadata{Key: key, Len: l}

	for i := int64(0); i < l; i++ {
		value, err := readString(r)
		if err != nil {
			return err
		}

		p.ctx.SetDataCh <- value
	}

	return nil
}

func (p *Parser) readIntSet(key interface{}, r *bufio.Reader) error {
	data, err := readString(r)
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

	p.ctx.SetMetadataCh <- SetMetadata{Key: key, Len: int64(length)}

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
		p.ctx.SetDataCh <- e
	}

	return nil
}
