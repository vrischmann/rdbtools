package rdbtools

import (
	"bufio"
	"bytes"
)

func (p *Parser) readList(key KeyObject, r *bufio.Reader) error {
	l, e, err := readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	p.ctx.ListMetadataCh <- ListMetadata{Key: key, Len: l}

	for i := int64(0); i < l; i++ {
		value, err := readString(r)
		if err != nil {
			return err
		}

		p.ctx.ListDataCh <- value
	}

	return nil
}

func (p *Parser) readListInZipList(key KeyObject, r *bufio.Reader) error {
	data, err := readString(r)
	if err != nil {
		return err
	}

	onLenCallback := func(length int64) error {
		p.ctx.ListMetadataCh <- ListMetadata{Key: key, Len: length}
		return nil
	}
	onElementCallback := func(e interface{}) error {
		p.ctx.ListDataCh <- e
		return nil
	}
	dr := bufio.NewReader(bytes.NewReader(data.([]byte)))

	if err := readZipList(dr, onLenCallback, onElementCallback); err != nil {
		return err
	}

	return nil
}
