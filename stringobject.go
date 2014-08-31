package rdbtools

import "fmt"

// Represents a Redis string (which you get/set with SET, GET, MSET, MGET, etc).
type StringObject struct {
	Key   KeyObject
	Value interface{}
}

// Returns a visualization of the string.
func (s StringObject) String() string {
	return fmt.Sprintf("StringObject{Key: %s, Value: '%s'}", DataToString(s.Key), DataToString(s.Value))
}
