package rdbtools

import "testing"

func TestStringObjectString(t *testing.T) {
	s := StringObject{Key: "foo", Value: "bar"}
	equals(t, "StringObject{Key: foo, Value: 'bar'}", s.String())
}
