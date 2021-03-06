package tharness

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/influx6/npkg/nstorage"
)

func TestByteStoreRemoveKeys(t *testing.T, store nstorage.ByteStore) {
	var keys []string
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		keys = append(keys, key)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var values, err = store.GetAnyKeys(keys...)
	require.NoError(t, err)
	require.Len(t, values, len(keys))

	require.NoError(t, store.RemoveKeys(keys...))

	var _, err2 = store.GetAllKeys(keys...)
	require.Error(t, err2)
}

func TestByteStoreScanMatch(t *testing.T, store nstorage.ByteStore) {
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var result, err = store.ScanMatch(2, 0, "", "")

	require.NoError(t, err, "ScanMatch should not fail")
	require.Len(t, result.Keys, 2)

	if len(result.LastKey) == 0 {
		require.Equal(t, int(result.LastIndex), 2)
	} else {
		require.NotEmpty(t, result.LastKey)
		require.Equal(t, result.Keys[len(result.Keys)-1], result.LastKey)
	}

	result, err = store.ScanMatch(2, result.LastIndex, result.LastKey, "")

	require.NoError(t, err, "ScanMatch should not fail")
	require.Len(t, result.Keys, 2)

	if len(result.LastKey) == 0 {
		require.Equal(t, int(result.LastIndex), 4)
	} else {
		require.NotEmpty(t, result.LastKey)
		require.Equal(t, result.Keys[len(result.Keys)-1], result.LastKey)
	}
}

func TestByteStoreGetAnykeys(t *testing.T, store nstorage.ByteStore) {
	var keys []string
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		keys = append(keys, key)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var values, err = store.GetAnyKeys(keys...)

	require.NoError(t, err)
	require.Len(t, values, len(keys))
}

func TestByteStoreGetAllkeys(t *testing.T, store nstorage.ByteStore) {
	var keys []string
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		keys = append(keys, key)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var values, err = store.GetAllKeys(keys...)

	require.NoError(t, err)
	require.Len(t, values, len(keys))
}

func TestByteStoreFindAll(t *testing.T, store nstorage.ByteStore) {
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var keys, keyErr = store.Keys()
	require.NoError(t, keyErr)
	require.NotEmpty(t, keys)

	var count int
	var err = store.Each(func(val []byte, k string) error {
		require.NotEmpty(t, val)
		require.NotEmpty(t, k)
		count++
		return nil
	})

	require.NoError(t, err)
	require.True(t, count >= 10)
}

func TestByteStoreFindPrefix(t *testing.T, store nstorage.ByteStore) {
	var keys []string
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		keys = append(keys, key)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var inKeys, getKeysErr = store.Keys()
	require.NoError(t, getKeysErr)
	require.NotEmpty(t, inKeys)

	var returnedKeys, keyErr = store.EachKeyMatch("day-.+")
	require.NoError(t, keyErr)
	require.NotEmpty(t, returnedKeys)

	for _, v := range keys {
		require.True(t, hasSuffixInList(returnedKeys, v))
	}

	var returnedKeys2, keyErr2 = store.EachKeyMatch("day-0")
	require.NoError(t, keyErr2)
	require.Len(t, returnedKeys2, 1)
}

func hasSuffixInList(v []string, k string) bool {
	for _, vk := range v {
		if strings.HasSuffix(vk, k) {
			return true
		}
	}
	return false
}

func TestByteStoreFindEach(t *testing.T, store nstorage.ByteStore) {
	for i := 0; i < 10; i++ {
		var key = fmt.Sprintf("day-%d", i)
		require.NoError(t, store.Save(key, string2Bytes("i")))
	}

	var keys, keyErr = store.Keys()
	require.NoError(t, keyErr)
	require.NotEmpty(t, keys)

	var count int
	var err = store.Each(func(val []byte, k string) error {
		if count >= 2 {
			return nstorage.ErrJustStop
		}
		count++
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestByteStore(t *testing.T, store nstorage.ByteStore) {
	require.NoError(t, store.Save("day", string2Bytes("wrecker")))
	var val, err = store.Get("day")
	require.NoError(t, err)
	require.Equal(t, "wrecker", bytes2String(val))

	var exist bool
	exist, err = store.Exists("day")
	require.NoError(t, err)
	require.True(t, exist)

	var newValue = "tweeter"
	require.NoError(t, store.Update("day", string2Bytes(newValue)))

	exist, err = store.Exists("day")
	require.NoError(t, err)
	require.True(t, exist)

	var keyValue, kerr = store.Get("day")
	require.NoError(t, kerr)
	require.Equal(t, newValue, string(keyValue))

	var count int
	require.NoError(t, store.Each(func(v []byte, k string) error {
		count++
		return nil
	}))

	require.NotEqual(t, 0, count)

	val, err = store.Remove("day")
	require.NoError(t, err)
	require.Equal(t, "tweeter", bytes2String(val))
}

func TestExpiryReset(t *testing.T, store nstorage.ExpirableStore) {
	require.NoError(t, store.SaveTTL("day", string2Bytes("wrecker"), 3*time.Second))

	var val, err = store.Get("day")
	require.NoError(t, err)
	require.Equal(t, "wrecker", bytes2String(val))

	ttl, err := store.TTL("day")
	require.NoError(t, err)
	require.True(t, ttl <= 3*time.Second)

	require.NoError(t, store.ResetTTL("day", time.Second))

	ttl, err = store.TTL("day")
	require.NoError(t, err)
	require.True(t, ttl <= time.Second)

	var terr = store.ResetTTL("day", 0)
	require.NoError(t, terr)

	ttl, err = store.TTL("day")
	require.NoError(t, err)
	require.True(t, ttl <= 0)

	var val2, err2 = store.Get("day")
	require.NoError(t, err2)
	require.Equal(t, "wrecker", bytes2String(val2))
}

func TestExpirableStore(t *testing.T, store nstorage.ExpirableStore) {
	require.NoError(t, store.SaveTTL("day", string2Bytes("wrecker"), 3*time.Second))

	var val, err = store.Get("day")
	require.NoError(t, err)
	require.Equal(t, "wrecker", bytes2String(val))

	ttl, err := store.TTL("day")
	require.NoError(t, err)
	require.True(t, ttl > 0)

	require.NoError(t, store.ExtendTTL("day", time.Second))

	ttl, err = store.TTL("day")
	require.NoError(t, err)
	require.True(t, ttl > 0)

	require.NoError(t, store.UpdateTTL("day", string2Bytes("tweeter"), time.Second))

	val, err = store.Get("day")
	require.NoError(t, err)
	require.Equal(t, "tweeter", bytes2String(val))

	var ttl2, terr = store.TTL("day")
	require.NoError(t, terr)
	require.True(t, ttl > ttl2, fmt.Sprintf("TTL1: %q, TTL2: %q", ttl, ttl2))
}

func bytes2String(bc []byte) string {
	return *(*string)(unsafe.Pointer(&bc))
}

func string2Bytes(bc string) []byte {
	return *(*[]byte)(unsafe.Pointer(&bc))
}

var alphabets = "abcdefghijklmnoprz"

func randomString() string {
	var in = rand.Intn(len(alphabets))
	return string(alphabets[in])
}
