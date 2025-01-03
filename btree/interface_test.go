package btree

import (
	"testing"

	"github.com/JoshuaLim25/db"
	"github.com/stretchr/testify/assert"
)

// TestBTreeImplementsKV ensures BTree implements KV interface
func TestBTreeImplementsKV(t *testing.T) {
	var _ db.KV = (*BTree)(nil)
	
	// Test that we can actually use BTree as KV
	var kv db.KV = New()
	
	kv.Set([]byte("test"), []byte("value"))
	val, ok := kv.Get([]byte("test"))
	assert.True(t, ok, "Should find the key")
	assert.Equal(t, []byte("value"), val, "Should return correct value")
	
	iter := kv.FindLarger([]byte("a"))
	assert.NotNil(t, iter, "Should return an iterator")
	
	// Test iterator interface compliance
	var _ db.Iterator = iter
}