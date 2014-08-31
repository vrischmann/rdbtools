package rdbtools

import (
	"encoding/binary"
	"io"
)

type zipListOnLenCallback func(length int64) error
type zipListOnElementCallback func(element interface{}) error

func (p *parser) readZipList(r io.Reader, onLenCallback zipListOnLenCallback, onElementCallback zipListOnElementCallback) error {
	var zlBytes int32
	var zlTail int32
	var zlLen int16
	var err error

	if err = binary.Read(r, binary.LittleEndian, &zlBytes); err != nil {
		return err
	}

	if err = binary.Read(r, binary.LittleEndian, &zlTail); err != nil {
		return err
	}

	if err = binary.Read(r, binary.LittleEndian, &zlLen); err != nil {
		return err
	}

	if err := onLenCallback(int64(zlLen)); err != nil {
		return err
	}

	for i := 0; i < int(zlLen); i++ {
		_, err := io.ReadFull(r, p.scratch[0:1])
		if err != nil {
			return err
		}

		b := p.scratch[0]

		// Read length of the previous entry
		// We don't use it though
		if b <= 0xFD { // 253
			// Do nothing
		} else if b == 0xFE { // 254
			var tmp int32
			if err = binary.Read(r, binary.LittleEndian, &tmp); err != nil {
				return err
			}
			// Do nothing
		} else {
			return ErrUnexpectedPrevLengthEntryByte
		}

		_, err = io.ReadFull(r, p.scratch[0:1])
		if err != nil {
			return err
		}

		flag := p.scratch[0]
		var data interface{}

		if (flag & 0xC0) == 0 {
			// String with length <= 63 bytes
			length := int64(flag & 0x3F)
			data, err = readBytes(r, length)
			if err != nil {
				return err
			}
		} else if (flag & 0xC0) == 0x40 {
			// String with length <= 16383 bytes
			_, err = io.ReadFull(r, p.scratch[0:1])
			if err != nil {
				return err
			}

			length := (int64(flag&0x3F) << 8) | int64(p.scratch[0])
			data, err = readBytes(r, length)
			if err != nil {
				return err
			}
		} else if (flag & 0xC0) == 0x80 {
			// String with length >= 16384 bytes
			var tmp int32
			if err := binary.Read(r, binary.BigEndian, &tmp); err != nil {
				return err
			}

			length := int64(tmp)
			data, err = readBytes(r, length)
			if err != nil {
				return err
			}
		} else if (flag & 0xF0) == 0xC0 {
			// int16
			var tmp int16
			if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
				return err
			}

			data = tmp
		} else if (flag & 0xF0) == 0xD0 {
			// int32
			var tmp int32
			if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
				return err
			}

			data = tmp
		} else if (flag & 0xF0) == 0xE0 {
			// int64
			var tmp int64
			if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
				return err
			}

			data = tmp
		} else if flag == 0xF0 {
			// int24
			ab, err := readBytes(r, 3)
			if err != nil {
				return err
			}

			tmp := uint32(ab[0])<<8 | uint32(ab[1])<<16 | uint32(ab[2])<<24
			data = int32(tmp) >> 8
		} else if flag == 0xFE {
			// int8
			_, err := io.ReadFull(r, p.scratch[0:1])
			if err != nil {
				return err
			}

			data = int8(p.scratch[0])
		} else if (flag & 0xF0) == 0xF0 {
			// int4
			data = (int(int(flag) & 0x0F)) - 1
		}

		if err := onElementCallback(data); err != nil {
			return err
		}
	}

	return nil
}
