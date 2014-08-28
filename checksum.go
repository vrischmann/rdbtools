package rdbtools

import "io"

const (
	crcPolynomial uint64 = 0x95AC9329AC4BC9B5
)

type checksumReader struct {
	r        io.Reader
	checksum uint64
	crcTable []uint64
	update   bool
}

func newChecksumReader(r io.Reader) *checksumReader {
	return &checksumReader{
		r:        r,
		checksum: 0,
		crcTable: makeCRCTable(),
		update:   true,
	}
}

func (r *checksumReader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	r.updateChecksum(p[:n])
	return n, err
}

func (r *checksumReader) updateChecksum(p []byte) {
	for _, e := range p {
		lookupIndex := byte(r.checksum) ^ e
		r.checksum = (r.checksum >> 8) ^ r.crcTable[lookupIndex]
	}
}

func makeCRCTable() []uint64 {
	table := make([]uint64, 256)
	var i uint64
	var j uint64

	for i = 0; i < 256; i++ {
		crc := i
		for j = 0; j < 8; j++ {
			if (crc & 1) == 1 {
				crc = (crc >> 1) ^ crcPolynomial
			} else {
				crc = (crc >> 1)
			}
		}
		table[i] = crc
	}

	return table
}
