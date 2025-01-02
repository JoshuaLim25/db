package db

// Iterator provides sequential access to key-value pairs
type Iterator interface {
	Next() (key, val []byte)
	ContainsNext() bool
}

// KV defines the core key-value database interface
type KV interface {
	Get(key []byte) (val []byte, ok bool)
	Set(key, val []byte)
	Delete(key []byte)
	FindLarger(key []byte) Iterator
}