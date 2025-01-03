package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeCreation(t *testing.T) {
	// Test leaf node creation
	leaf := NewLeafNode()
	
	assert.True(t, leaf.IsLeaf(), "NewLeafNode should create a leaf node")
	assert.Equal(t, 0, leaf.NumKeys, "New leaf node should have 0 keys")
	assert.False(t, leaf.IsFull(), "New leaf node should not be full")
	assert.False(t, leaf.IsUnderflow(), "New leaf node should not be in underflow state")
	
	// Test internal node creation
	internal := NewInternalNode()
	
	assert.False(t, internal.IsLeaf(), "NewInternalNode should create an internal node")
	assert.Equal(t, 0, internal.NumKeys, "New internal node should have 0 keys")
}

func TestNodeCapacity(t *testing.T) {
	node := NewLeafNode()
	
	// Fill up the node
	for i := 0; i < MaxKeys; i++ {
		node.Keys[i] = []byte("key")
		node.Values[i] = []byte("value")
		node.NumKeys++
		
		if i < MaxKeys-1 {
			assert.False(t, node.IsFull(), "Node should not be full with %d keys", i+1)
		}
	}
	
	assert.True(t, node.IsFull(), "Node should be full with MaxKeys keys")
}

func TestNodeUnderflow(t *testing.T) {
	node := NewLeafNode()
	parent := NewInternalNode()
	node.Parent = parent  // Set parent so underflow logic applies
	
	// Add minimum keys
	for i := 0; i < MinKeys; i++ {
		node.Keys[i] = []byte("key")
		node.Values[i] = []byte("value")
		node.NumKeys++
	}
	
	assert.False(t, node.IsUnderflow(), "Node with MinKeys should not be in underflow")
	
	// Remove one key to trigger underflow
	node.NumKeys--
	
	assert.True(t, node.IsUnderflow(), "Node with fewer than MinKeys should be in underflow")
}

func TestNodeAccessors(t *testing.T) {
	node := NewLeafNode()
	
	// Add some test data
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	
	node.Keys[0] = testKey
	node.Values[0] = testValue
	node.NumKeys = 1
	
	// Test KeyAt
	key := node.KeyAt(0)
	assert.NotNil(t, key, "KeyAt(0) should return a key")
	assert.Equal(t, "test_key", string(key), "KeyAt(0) should return test_key")
	
	key = node.KeyAt(1)
	assert.Nil(t, key, "KeyAt(1) should return nil for out of bounds")
	
	// Test ValueAt
	val := node.ValueAt(0)
	assert.NotNil(t, val, "ValueAt(0) should return a value")
	assert.Equal(t, "test_value", string(val), "ValueAt(0) should return test_value")
	
	val = node.ValueAt(1)
	assert.Nil(t, val, "ValueAt(1) should return nil for out of bounds")
}

func TestInternalNodeChildren(t *testing.T) {
	internal := NewInternalNode()
	child := NewLeafNode()
	
	internal.Children[0] = child
	internal.NumKeys = 0 // No keys yet, but one child
	
	assert.Equal(t, child, internal.ChildAt(0), "ChildAt(0) should return the child node")
	assert.Nil(t, internal.ChildAt(1), "ChildAt(1) should return nil for non-existent child")
	
	// Test that leaf nodes return nil for ChildAt
	leaf := NewLeafNode()
	assert.Nil(t, leaf.ChildAt(0), "Leaf node should return nil for ChildAt")
}