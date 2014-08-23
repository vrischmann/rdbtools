package rdbtools

import (
	"fmt"
	"time"
)

type KeyObject struct {
	ExpiryTime time.Time
	Key        interface{}
}

func (k KeyObject) Expired() bool {
	return k.ExpiryTime.After(time.Now())
}

func (k KeyObject) String() string {
	if !k.ExpiryTime.IsZero() {
		return fmt.Sprintf("KeyObject{ExpiryTime: %s, Key: %s}", k.ExpiryTime, DataToString(k.Key))
	}

	return fmt.Sprintf("%s", DataToString(k.Key))
}
