package rdbtools

import (
	"bufio"
	"encoding/binary"
)

type zipListOnLenCallback func(length int64) error
type zipListOnElementCallback func(element interface{}) error

func readZipList(r *bufio.Reader, onLenCallback zipListOnLenCallback, onElementCallback zipListOnElementCallback) error {
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
		b, err := r.ReadByte()
		if err != nil {
			return err
		}

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

		flag, err := r.ReadByte()
		if err != nil {
			return err
		}

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
			tmp, err := r.ReadByte()
			if err != nil {
				return err
			}

			length := (int64(flag&0x3F) << 8) | int64(tmp)
			data, err = readBytes(r, length)
			if err != nil {
				return err
			}
		} else if (flag & 0xC0) == 0x80 {
			// String with length >= 16384 bytes
			var tmp int32
			if err := binary.Read(r, binary.LittleEndian, &tmp); err != nil {
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
			bytes, err := readBytes(r, 3)
			if err != nil {
				return err
			}

			data = (int32(bytes[0]) << 16) | (int32(bytes[1]) << 8) | int32(bytes[2])
		} else if flag == 0xFE {
			// int8
			tmp, err := r.ReadByte()
			if err != nil {
				return err
			}

			data = int8(tmp)
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
