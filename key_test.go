package rdbtools

import (
	"testing"
	"time"
)

func TestNewKeyObject(t *testing.T) {
	k := NewKeyObject("test", -1)

	equals(t, "test", k.Key)
	equals(t, true, k.ExpiryTime.IsZero())
	equals(t, "test", k.String())
}

// With expiry time, not yet expired
func TestNewKeyObjectNotExpired(t *testing.T) {
	dt := time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC)
	k := NewKeyObject("test", dt.Unix()*1000)

	equals(t, "test", k.Key)
	equals(t, false, k.ExpiryTime.IsZero())
	equals(t, false, k.Expired())
	equals(t, "KeyObject{ExpiryTime: 2100-01-01 00:00:00 +0000 UTC, Key: test}", k.String())
}

// With expiry time, expired
func TestNewKeyObjectExpired(t *testing.T) {
	dt := time.Now().Add(time.Second * -10)
	k := NewKeyObject("test", dt.Unix()*1000)

	equals(t, "test", k.Key)
	equals(t, false, k.ExpiryTime.IsZero())
	equals(t, true, k.Expired())
}
