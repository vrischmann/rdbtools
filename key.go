package rdbtools

import (
	"fmt"
	"time"
)

type KeyObject struct {
	ExpiryTime time.Time
	Key        interface{}
}

func NewKeyObject(key interface{}, expiryTime int64) KeyObject {
	k := KeyObject{
		Key: key,
	}
	if expiryTime >= 0 {
		k.ExpiryTime = time.Unix(expiryTime/1000, 0)
	}

	return k
}

func (k KeyObject) Expired() bool {
	return k.ExpiryTime.Before(time.Now())
}

func (k KeyObject) String() string {
	if !k.ExpiryTime.IsZero() {
		return fmt.Sprintf("KeyObject{ExpiryTime: %s, Key: %s}", k.ExpiryTime, DataToString(k.Key))
	}

	return fmt.Sprintf("%s", DataToString(k.Key))
}
