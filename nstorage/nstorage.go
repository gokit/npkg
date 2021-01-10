package nstorage

import "time"

// ByteStore defines a storage interface defining what we expect to
// be provided for storing a byte slice with a underline key.
type ByteStore interface {
	Keys() ([]string, error)
	Save(string, []byte) error
	Get(string) ([]byte, error)
	Exists(string) (bool, error)
	Update(string, []byte) error
	Remove(string) ([]byte, error)
	Each(fn func([]byte, string) bool) error
	ErrorEach(fn func([]byte, string) error) error
}

// QueryableByteStore defines a queryable store which can
// take a function to find giving matching elements.
type QueryableByteStore interface {
	ByteStore

	Find(fn func([]byte, string) bool) error
}

// ExpirableStore composes the ByteStore providing the
// expiration versions of Save and TTL extension.
type ExpirableStore interface {
	ByteStore

	// TTL should return current expiration value of
	// giving key in millisecond.
	TTL(string) (time.Duration, error)

	// ExtendTTL should extend expiration by giving duration,
	// by add new duration to the remaining ttl of key.
	//
	// A zero value should persist key.
	ExtendTTL(string, time.Duration) error

	// ResetTTL should extend reset to giving duration.
	//
	// A zero value should persist key.
	ResetTTL(string, time.Duration) error

	// SaveTTL save giving key with giving expiration.
	SaveTTL(string, []byte, time.Duration) error

	// UpdateTTL updates giving key with giving value and use new expiration.
	// It should update key's value and add giving duration
	// to remaining time of key.
	//
	// A zero value should persist key.
	UpdateTTL(string, []byte, time.Duration) error
}
