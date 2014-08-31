package rdbtools

import "fmt"

// Represents the metadata of a list, which is the key and the list length
type ListMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the list metadata
func (m ListMetadata) String() string {
	return fmt.Sprintf("ListMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

// Represents the metadata of a set, which is the key and the set length
type SetMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the set metadata
func (m SetMetadata) String() string {
	return fmt.Sprintf("SetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

// Represents the metadata of a hash, which is the key and the hash length
type HashMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the hash metadata
func (m HashMetadata) String() string {
	return fmt.Sprintf("HashMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

// Represents the metadata of a sorted set, which is the key and the sorted set length
type SortedSetMetadata struct {
	Key KeyObject
	Len int64
}

// Returns a visualization of the sorted set metadata
func (m SortedSetMetadata) String() string {
	return fmt.Sprintf("SortedSetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}
