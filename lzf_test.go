package rdbtools

import (
	"strings"
	"testing"
)

func TestLzfDecompress(t *testing.T) {
	data := []byte{1, 97, 97, 224, 246, 0, 1, 97, 97}
	ulen := int64(259)

	output := lzfDecompress(data, ulen)
	expected := strings.Repeat("a", int(ulen))
	if string(output) != expected {
		t.Errorf("expected %s but got %s", expected, string(output))
	}
}

func TestLzfDecompressNoData(t *testing.T) {
	output := lzfDecompress([]byte{}, 0)
	if len(output) != 0 {
		t.Errorf("expected empty slice but got %s", string(output))
	}
}
