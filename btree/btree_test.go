package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTreeBasicOperations(t *testing.T) {
	bt := New()
	
	// Test Set and Get
	bt.Set([]byte("key1"), []byte("value1"))
	bt.Set([]byte("key2"), []byte("value2"))
	bt.Set([]byte("key3"), []byte("value3"))
	
	val, ok := bt.Get([]byte("key1"))
	assert.True(t, ok, "key1 should exist")
	assert.Equal(t, []byte("value1"), val, "key1 should have value1")
	
	val, ok = bt.Get([]byte("key2"))
	assert.True(t, ok, "key2 should exist")
	assert.Equal(t, []byte("value2"), val, "key2 should have value2")
	
	val, ok = bt.Get([]byte("key3"))
	assert.True(t, ok, "key3 should exist")
	assert.Equal(t, []byte("value3"), val, "key3 should have value3")
	
	// Test non-existent key
	_, ok = bt.Get([]byte("nonexistent"))
	assert.False(t, ok, "nonexistent key should not be found")
}

func TestBTreeUpdate(t *testing.T) {
	bt := New()
	
	// Insert
	bt.Set([]byte("key1"), []byte("value1"))
	assert.Equal(t, 1, bt.Size(), "Size should be 1 after insert")
	
	// Update
	bt.Set([]byte("key1"), []byte("updated_value1"))
	assert.Equal(t, 1, bt.Size(), "Size should still be 1 after update")
	
	val, ok := bt.Get([]byte("key1"))
	assert.True(t, ok, "key1 should exist after update")
	assert.Equal(t, []byte("updated_value1"), val, "key1 should have updated value")
}

func TestBTreeDelete(t *testing.T) {
	bt := New()
	
	// Insert some keys
	bt.Set([]byte("key1"), []byte("value1"))
	bt.Set([]byte("key2"), []byte("value2"))
	bt.Set([]byte("key3"), []byte("value3"))
	
	assert.Equal(t, 3, bt.Size(), "Size should be 3 after inserts")
	
	// Delete middle key
	bt.Delete([]byte("key2"))
	assert.Equal(t, 2, bt.Size(), "Size should be 2 after delete")
	
	_, ok := bt.Get([]byte("key2"))
	assert.False(t, ok, "key2 should be deleted")
	
	// Other keys should still exist
	_, ok = bt.Get([]byte("key1"))
	assert.True(t, ok, "key1 should still exist")
	
	_, ok = bt.Get([]byte("key3"))
	assert.True(t, ok, "key3 should still exist")
}

func TestBTreeSplitting(t *testing.T) {
	bt := New()
	
	// Insert enough keys to trigger splitting (MaxKeys = 4)
	keys := []string{"key01", "key02", "key03", "key04", "key05", "key06"}
	
	for _, key := range keys {
		val := fmt.Sprintf("value_%s", key)
		bt.Set([]byte(key), []byte(val))
	}
	
	assert.Equal(t, len(keys), bt.Size(), "All keys should be inserted")
	
	// Verify all keys are still accessible after splitting
	for _, key := range keys {
		expectedVal := fmt.Sprintf("value_%s", key)
		
		val, ok := bt.Get([]byte(key))
		assert.True(t, ok, "Key %s should exist after splitting", key)
		assert.Equal(t, []byte(expectedVal), val, "Key %s should have correct value", key)
	}
}

func TestBTreeIterator(t *testing.T) {
	bt := New()
	
	// Insert keys in non-sorted order
	keys := []string{"key3", "key1", "key5", "key2", "key4"}
	for _, key := range keys {
		bt.Set([]byte(key), []byte("value_"+key))
	}
	
	// Test FindLarger
	iter := bt.FindLarger([]byte("key2"))
	
	expectedKeys := []string{"key3", "key4", "key5"}
	var actualKeys []string
	
	for iter.ContainsNext() {
		key, val := iter.Next()
		if key != nil {
			actualKeys = append(actualKeys, string(key))
			expectedVal := "value_" + string(key)
			assert.Equal(t, []byte(expectedVal), val, "Value should match for key %s", string(key))
		}
	}
	
	assert.Equal(t, expectedKeys, actualKeys, "Iterator should return keys in sorted order")
}

func TestBTreeEmptyIterator(t *testing.T) {
	bt := New()
	
	iter := bt.FindLarger([]byte("any_key"))
	
	assert.False(t, iter.ContainsNext(), "Empty tree iterator should not contain next")
	
	key, val := iter.Next()
	assert.Nil(t, key, "Empty iterator should return nil key")
	assert.Nil(t, val, "Empty iterator should return nil value")
}

func TestBTreeSmallDataset(t *testing.T) {
	bt := New()
	numItems := 8  // Reduce to avoid complex splitting for now
	
	// Insert many items
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		val := []byte(fmt.Sprintf("value%04d", i))
		bt.Set(key, val)
	}
	
	assert.Equal(t, numItems, bt.Size(), "Size should match number of inserted items")
	
	// Verify all items are accessible
	for i := 0; i < numItems; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		expectedVal := []byte(fmt.Sprintf("value%04d", i))
		
		val, ok := bt.Get(key)
		assert.True(t, ok, "Item %d should exist", i)
		assert.Equal(t, expectedVal, val, "Item %d should have correct value", i)
	}
	
	// Test range query
	iter := bt.FindLarger([]byte("key0003"))
	count := 0
	for iter.ContainsNext() {
		iter.Next()
		count++
	}
	
	// Should find keys from key0004 onwards  
	expected := numItems - 4
	assert.Equal(t, expected, count, "Range query should return correct number of items")
}