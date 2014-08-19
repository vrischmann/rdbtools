package rdbtools

import "fmt"

func DataToString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case uint8, int8, uint16, int16, uint32, int32, uint64, int64, int, uint:
		return fmt.Sprintf("%d", v)
	case []byte:
		return string(v)
	default:
		panic("unknown type")
	}
}
