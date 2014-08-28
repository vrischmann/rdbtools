package rdbtools

import (
	"bufio"
	"bytes"
	"io"
)

func (p *Parser) readList(key KeyObject, r io.Reader) error {
	l, e, err := p.readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	p.ctx.ListMetadataCh <- ListMetadata{Key: key, Len: l}

	for i := int64(0); i < l; i++ {
		value, err := p.readString(r)
		if err != nil {
			return err
		}

		p.ctx.ListDataCh <- value
	}

	return nil
}

func (p *Parser) readListInZipList(key KeyObject, r io.Reader) error {
	data, err := p.readString(r)
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

	if err := p.readZipList(dr, onLenCallback, onElementCallback); err != nil {
		return err
	}

	return nil
}
