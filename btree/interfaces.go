package btree

// Iterator provides sequential access to key-value pairs
type Iterator interface {
	Next() (key, val []byte)
	ContainsNext() bool
}