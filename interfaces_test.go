package db

import (
	"testing"
)

// TestIteratorInterface ensures Iterator interface is well-defined
func TestIteratorInterface(t *testing.T) {
	// This is a compile-time test to ensure interfaces are properly defined
	var _ Iterator = (*mockIterator)(nil)
}

// TestKVInterface ensures KV interface is well-defined
func TestKVInterface(t *testing.T) {
	// This is a compile-time test to ensure interfaces are properly defined
	var _ KV = (*mockKV)(nil)
}

// Mock implementations for testing
type mockIterator struct{}

func (m *mockIterator) Next() (key, val []byte) {
	return nil, nil
}

func (m *mockIterator) ContainsNext() bool {
	return false
}

type mockKV struct{}

func (m *mockKV) Get(key []byte) (val []byte, ok bool) {
	return nil, false
}

func (m *mockKV) Set(key, val []byte) {}

func (m *mockKV) Delete(key []byte) {}

func (m *mockKV) FindLarger(key []byte) Iterator {
	return &mockIterator{}
}