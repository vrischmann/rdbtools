package rdbtools

import "fmt"

type ListMetadata struct {
	Key interface{}
	Len int64
}

func (m ListMetadata) String() string {
	return fmt.Sprintf("ListMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

type SetMetadata struct {
	Key interface{}
	Len int64
}

func (m SetMetadata) String() string {
	return fmt.Sprintf("SetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

type HashMetadata struct {
	Key interface{}
	Len int64
}

func (m HashMetadata) String() string {
	return fmt.Sprintf("HashMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}

type SortedSetMetadata struct {
	Key interface{}
	Len int64
}

func (m SortedSetMetadata) String() string {
	return fmt.Sprintf("SortedSetMetadata{Key: %s, Len: %d}", DataToString(m.Key), m.Len)
}
