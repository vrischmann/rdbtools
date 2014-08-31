package rdbtools

import (
	"fmt"
	"time"
)

// Represents a Redis key.
type KeyObject struct {
	ExpiryTime time.Time   // The expiry time of the key. If none, this object IsZero() method will return true
	Key        interface{} // The key value
}

// Create a new key. If expiryTime >= 0 it will be used.
func NewKeyObject(key interface{}, expiryTime int64) KeyObject {
	k := KeyObject{
		Key: key,
	}
	if expiryTime >= 0 {
		k.ExpiryTime = time.Unix(expiryTime/1000, 0).UTC()
	}

	return k
}

// Returns true if the key is expired (meaning the key's expiry time is before now), false otherwise.
func (k KeyObject) Expired() bool {
	return k.ExpiryTime.Before(time.Now())
}

// Return a visualization of the key.
func (k KeyObject) String() string {
	if !k.ExpiryTime.IsZero() {
		return fmt.Sprintf("KeyObject{ExpiryTime: %s, Key: %s}", k.ExpiryTime, DataToString(k.Key))
	}

	return fmt.Sprintf("%s", DataToString(k.Key))
}
