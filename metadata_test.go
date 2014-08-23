package rdbtools

import (
	"testing"
)

func TestListMetadataString(t *testing.T) {
	md := ListMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "ListMetadata{Key: foobar, Len: 10}", md.String())
}

func TestSetMetadataString(t *testing.T) {
	md := SetMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "SetMetadata{Key: foobar, Len: 10}", md.String())
}

func TestHashMetadataString(t *testing.T) {
	md := HashMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "HashMetadata{Key: foobar, Len: 10}", md.String())
}

func TestSortedSetMetadataString(t *testing.T) {
	md := SortedSetMetadata{Key: KeyObject{Key: "foobar"}, Len: 10}
	equals(t, "SortedSetMetadata{Key: foobar, Len: 10}", md.String())
}
