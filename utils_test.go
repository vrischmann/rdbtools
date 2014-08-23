package rdbtools

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

var (
	end = make(chan bool, 1)
)

type myCustomStringer struct{}

func (s myCustomStringer) String() string {
	return "foobar"
}

func TestDataToString(t *testing.T) {
	equals(t, "foobar", "foobar")
	equals(t, "foobar", DataToString(myCustomStringer{}))
	equals(t, "1", DataToString(uint8(1)))
	equals(t, "1", DataToString(int8(1)))
	equals(t, "1", DataToString(uint16(1)))
	equals(t, "1", DataToString(int16(1)))
	equals(t, "1", DataToString(uint32(1)))
	equals(t, "1", DataToString(int32(1)))
	equals(t, "1", DataToString(uint64(1)))
	equals(t, "1", DataToString(int64(1)))
	equals(t, "1", DataToString(int(1)))
	equals(t, "1", DataToString(uint(1)))

	defer func() {
		e := recover()
		equals(t, "unknown type", e)
	}()
	DataToString(io.EOF)
}

// Call the read function f and report errors if there are any
func readAndNotify(t *testing.T, r io.Reader, key string, f func(KeyObject, *bufio.Reader) error) {
	err := f(KeyObject{Key: []byte(key)}, bufio.NewReader(r))
	if err != nil {
		t.Error(err)
	}
	end <- true
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
