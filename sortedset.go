package rdbtools

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Represents the metadata of a sorted set, which is the key and the sorted set length
type SortedSetMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the sorted set metadata
func (m SortedSetMetadata) String() string {
	return fmt.Sprintf("SortedSetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

// Represents an entry in a sorted set.
type SortedSetEntry struct {
	Value interface{}
	Score float64
}

// Returns a visualization of a sorted set entry
func (e SortedSetEntry) String() string {
	return fmt.Sprintf("SortedSetEntry{Value: %s, Score: %0.4f}", DataToString(e.Value), e.Score)
}

func (p *parser) readSortedSet(key KeyObject, r io.Reader) error {
	l, e, err := p.readLen(r)
	if err != nil {
		return err
	}
	if e {
		return ErrUnexpectedEncodedLength
	}

	p.ctx.SortedSetMetadataCh <- SortedSetMetadata{Key: key, Len: l}

	for i := int64(0); i < l; i++ {
		value, err := p.readString(r)
		if err != nil {
			return err
		}

		score, err := p.readDoubleValue(r)
		if err != nil {
			return err
		}

		e := SortedSetEntry{Value: value, Score: score}
		p.ctx.SortedSetEntriesCh <- e
	}

	return nil
}

func (p *parser) readSortedSetInZipList(key KeyObject, r io.Reader) error {
	data, err := p.readString(r)
	if err != nil {
		return err
	}

	var el interface{} = nil
	onLenCallback := func(length int64) error {
		p.ctx.SortedSetMetadataCh <- SortedSetMetadata{Key: key, Len: length / 2}
		return nil
	}
	onElementCallback := func(e interface{}) error {
		if el == nil {
			el = e
		} else {
			var score float64
			switch v := e.(type) {
			case []byte:
				score, err = strconv.ParseFloat(string(v), 64)
				if err != nil {
					return err
				}
			case int8:
				score = float64(v)
			case int:
				score = float64(v)
			case int16:
				score = float64(v)
			case int32:
				score = float64(v)
			case int64:
				score = float64(v)
			}

			p.ctx.SortedSetEntriesCh <- SortedSetEntry{Value: el, Score: score}
			el = nil
		}

		return nil
	}
	dr := bufio.NewReader(bytes.NewReader(data.([]byte)))

	if err := p.readZipList(dr, onLenCallback, onElementCallback); err != nil {
		return err
	}

	return nil
}
