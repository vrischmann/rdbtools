package rdbtools

import "fmt"

type StringObject struct {
	Key   interface{}
	Value interface{}
}

func (s StringObject) String() string {
	return fmt.Sprintf("StringObject{Key: %s, Value: '%s'}", DataToString(s.Key), DataToString(s.Value))
}
